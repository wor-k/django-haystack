# encoding: utf-8
from __future__ import absolute_import, division, print_function, unicode_literals

try:
    # Prefer simplejson, if installed.
    import simplejson as json
except ImportError:
    import json

from django.core.exceptions import ImproperlyConfigured

from haystack.backends import BaseEngine
from haystack.constants import DJANGO_CT, DJANGO_ID, ID
from haystack.exceptions import MissingDependency
from haystack.models import SearchResult
from haystack.utils import log as logging
from haystack.utils.app_loading import haystack_get_model
from haystack.backends.solr_backend import SolrSearchBackend, SolrSearchQuery



try:
    from pysolr import Solr, SolrCloud, ZooKeeper, SolrError
except ImportError:
    raise MissingDependency("The 'solr' backend requires the installation of 'pysolr'. Please refer to the documentation.")

try:
    from kazoo.client import KazooClient, KazooState
except ImportError:
    raise MissingDependency("The 'solr cloud' backend requires the installation of 'kazoo'.")


class SolrCloudSearchBackend(SolrSearchBackend):

    def __init__(self, connection_alias, **connection_options):

        if not 'ZOOKEEPER_URL' in connection_options:
            raise ImproperlyConfigured("You must specify a 'ZOOKEEPER_URL' in your settings for connection '%s'." % connection_alias)
        if not 'SOLR_COLLECTION' in connection_options:
            raise ImproperlyConfigured("You must specify a 'SOLR_COLLECTION' in your settings for connection '%s'." % connection_alias)

        super(SolrSearchBackend, self).__init__(connection_alias, **connection_options) # skip SolrSearchBackend
        self.conn = self.get_solr_cloud(connection_options['ZOOKEEPER_URL'], connection_options['SOLR_COLLECTION'],
                                  timeout=self.timeout, **connection_options.get('KWARGS', {}))
        self.log = logging.getLogger('haystack')

    def get_solr_cloud(self, zookeeper_url, collection, timeout=60, **kwargs):
        # TODO: make self.zk a memoized property. Put it to init of sth.
        if not getattr(self, 'zk', None):
            self.zk = ZooKeeperCustomCollection(zookeeper_url)
        return SolrCloud(self.zk, collection, timeout=timeout, **kwargs)

    def _process_results(self, raw_results, highlight=False, result_class=None, distance_point=None):
        #TODO: !!!!!!!! to improve performance by query to DB as batch.
        from haystack import connections
        results = []
        hits = raw_results.hits
        facets = {}
        stats = {}
        spelling_suggestion = None

        if result_class is None:
            result_class = SearchResult

        if hasattr(raw_results,'stats'):
            stats = raw_results.stats.get('stats_fields',{})

        if hasattr(raw_results, 'facets'):
            facets = {
                'fields': raw_results.facets.get('facet_fields', {}),
                'dates': raw_results.facets.get('facet_dates', {}),
                'queries': raw_results.facets.get('facet_queries', {}),
            }

            for key in ['fields']:
                for facet_field in facets[key]:
                    # Convert to a two-tuple, as Solr's json format returns a list of
                    # pairs.
                    facets[key][facet_field] = list(zip(facets[key][facet_field][::2], facets[key][facet_field][1::2]))

        if self.include_spelling is True:
            if hasattr(raw_results, 'spellcheck'):
                if len(raw_results.spellcheck.get('suggestions', [])):
                    # For some reason, it's an array of pairs. Pull off the
                    # collated result from the end.
                    spelling_suggestion = raw_results.spellcheck.get('suggestions')[-1]

        unified_index = connections[self.connection_alias].get_unified_index()
        indexed_models = unified_index.get_indexed_models()

        for raw_result in raw_results.docs:
            app_label, model_name = raw_result[DJANGO_CT].split('.')
            additional_fields = {}
            model = haystack_get_model(app_label, model_name)

            if model and model in indexed_models:
                index = unified_index.get_index(model)
                index_field_map = index.field_map
                for key, value in raw_result.items():
                    string_key = str(key)
                    # re-map key if alternate name used
                    if string_key in index_field_map:
                        string_key = index_field_map[key]

                    if string_key in index.fields and hasattr(index.fields[string_key], 'convert'):
                        additional_fields[string_key] = index.fields[string_key].convert(value)
                    else:
                        additional_fields[string_key] = self.conn._to_python(value)

                del(additional_fields[DJANGO_CT])
                del(additional_fields[DJANGO_ID])
                del(additional_fields['score'])

                if raw_result[ID] in getattr(raw_results, 'highlighting', {}):
                    additional_fields['highlighted'] = raw_results.highlighting[raw_result[ID]]

                if distance_point:
                    additional_fields['_point_of_origin'] = distance_point

                    if raw_result.get('__dist__'):
                        from haystack.utils.geo import Distance
                        additional_fields['_distance'] = Distance(km=float(raw_result['__dist__']))
                    else:
                        additional_fields['_distance'] = None

                result = result_class(app_label, model_name, raw_result[DJANGO_ID], raw_result['score'], **additional_fields)
                results.append(result)
            else:
                hits -= 1

        return {
            'results': results,
            'hits': hits,
            'stats': stats,
            'facets': facets,
            'spelling_suggestion': spelling_suggestion,
        }



class ZooKeeperCustomCollection(ZooKeeper):
    COLLECTIONS = '/collections'
    STATE = 'state.json'


    LOG = logging.getLogger('pysolr')


    def __init__(self, zkServerAddress, zkClientTimeout=15, zkClientConnectTimeout=15):
        if KazooClient is None:
            logging.error('ZooKeeper requires the `kazoo` library to be installed')
            raise RuntimeError

        self.collections = {}
        self.liveNodes = {}
        self.aliases = {}
        self.state = None

        self.zk = KazooClient(zkServerAddress, read_only=True)

        self.zk.start()

        def connectionListener(state):
            if state == KazooState.LOST:
                self.state = state
            elif state == KazooState.SUSPENDED:
                self.state = state
        self.zk.add_listener(connectionListener)

        @self.zk.ChildrenWatch(ZooKeeperCustomCollection.COLLECTIONS)
        def watchCollections(children):
            if not children:
                self.LOG.warning("No collections available: no collections defined?")
            else:
                for collection in children:
                    data = self.zk.get('/'.join([ZooKeeperCustomCollection.COLLECTIONS, collection, ZooKeeperCustomCollection.STATE]))
                    self.collections[collection] = json.loads(data[0].decode('utf-8'))[collection]
                self.LOG.info('Updated collections: %s', self.collections)

        @self.zk.ChildrenWatch(ZooKeeper.LIVE_NODES_ZKNODE)
        def watchLiveNodes(children):
            self.liveNodes = children
            self.LOG.info("Updated live nodes: %s", children)

        @self.zk.DataWatch(ZooKeeper.ALIASES)
        def watchAliases(data, stat):
            if data:
                json_data = json.loads(data.decode('utf-8'))
                if ZooKeeper.COLLECTION in json_data:
                    self.aliases = json_data[ZooKeeper.COLLECTION]
                else:
                    self.LOG.warning('Expected to find %s in alias update %s',
                                ZooKeeper.COLLECTION, json_data.keys())
            else:
                self.aliases = None
            self.LOG.info("Updated aliases: %s", self.aliases)



class SolrCloudEngine(BaseEngine):
    backend = SolrCloudSearchBackend
    query = SolrSearchQuery
