# encoding: utf-8
from __future__ import absolute_import, division, print_function, unicode_literals

try:
    # Prefer simplejson, if installed.
    import simplejson as json
except ImportError:
    import json

from django.core.exceptions import ImproperlyConfigured

from haystack.backends import BaseEngine
from haystack.exceptions import MissingDependency
from haystack.utils import log as logging
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

#    def _process_results(self, raw_results, highlight=False, result_class=None, distance_point=None):
#        to_return = super(SolrCloudSearchBackend, self)._process_results(raw_results, highlight, result_class, distance_point)
#        to_return['results'] = self.pre_load_result_objects(to_return['results'])
#        return to_return

#    def pre_load_result_objects(self, results):
#        pks = []
#        model = None
#        for result in results:
#            pks.append(result.pk)
#            if not model:
#                model = result.model
#            else:
#                if model is not result.model:
#                    self.log.warning("Not support multiple type of models")
#                    return
#        searchindex = results[0].searchindex
#        query_result = searchindex.read_queryset().filter(pk__in=pks)
#        query_result_id_dict = dict((str(o.pk), o) for o in query_result)
#        for result in results:
#            obj = query_result_id_dict[result.pk]
#            result._set_object(obj)


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
                    try:
                        data = self.zk.get('/'.join([ZooKeeperCustomCollection.COLLECTIONS, collection, ZooKeeperCustomCollection.STATE]))
                        self.collections[collection] = json.loads(data[0].decode('utf-8'))[collection]
                    except:
                        self.LOG.exception("Path: %s get failed" % '/'.join([ZooKeeperCustomCollection.COLLECTIONS, collection, ZooKeeperCustomCollection.STATE]))
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
