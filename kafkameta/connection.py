import logging
import json
import os
from datetime import datetime

from kazoo.client import KazooClient

from .topic import KafkaTopic
from .partition import KafkaPartition
from .broker import KafkaBroker

logger = logging.getLogger('kafkameta.connection')


class KafkaConnection():
    _topics_path = 'brokers/topics'
    _brokers_path = 'brokers/ids'
    _topics_config_path = 'config/topics'

    def __init__(self, hosts=['localhost:2181'], zk_path=None, load_state=False, kazoo_client=None):
        """
        @param hosts         : list, zk host: port
        @param zk_path       : str, root of where kafka info stored
        @param load_state    : bool, by default avoid loading all state data on initializing due to all the ZK calls required
        @param kazoo_client  : KazooClient, use as KazooClient instead of creating new connection object
        """

        if kazoo_client:
            self._client = kazoo_client
            if zk_path:
                self._client.chroot = zk_path
        else:
            self._client            = KazooClient(hosts=','.join(hosts))
            self._client.chroot     = zk_path
            self._hosts             = hosts

        self._topics            = {}
        self._brokers           = {}
        self._last_state_update = None

        self._client.start()

        # Load up metadata
        self._load_topics()
        self._load_partitions()
        self._load_brokers()

        if load_state:
            self.update()

    def __getitem__(self, topic):
        return self._topics[topic]

    def __repr__(self):
        return '<{0} topics={1}, zk_hosts={2}>'.format(self.__class__.__name__,str(self._topics.keys()), self._hosts)

    @property
    def topics(self):
        return self._topics

    @property
    def brokers(self):
        return self._brokers

    @property
    def connected(self):
        return self._client.connected

    def _load_brokers(self):
        brokers = self._client.get_children(self._brokers_path)

        for broker in brokers:
            broker_data = json.loads(self._client.get(os.path.join(self._brokers_path, broker))[0])
            broker_obj = KafkaBroker(broker, broker_data['host'], broker_data['port'], broker_data['jmx_port'], long(broker_data['timestamp']))
            self._brokers[int(broker)] = broker_obj

    def _load_topics(self):
        topics = self._client.get_children(self._topics_path)

        for topic in topics:
            top_obj = KafkaTopic(topic, client=self._client)
            self._topics[topic] = top_obj

    def _load_partitions(self):
        for topic in self._topics:
            logger.debug('Loading partition data for {0}'.format(topic))
            replicas = json.loads(self._client.get(os.path.join(self._topics_path, topic))[0])['partitions']
            replicas = dict((int(k), v) for k, v in replicas.items())

            for partition, val in replicas.iteritems():
                part_obj = KafkaPartition(partition, topic, replicas=val, client=self._client)
                self._topics[topic].add_partition(part_obj)

    def disconnect(self):
        self._client.stop()

    def update(self, topic=None):
        """
        Update partitions with latest metadata
        """
        if topic:
            self._topics[topic].update()
        else:
            self._load_brokers()
            self._load_topics()
            self._load_partitions()

            for t in self._topics.itervalues():
                t.update()

        self._last_state_update = datetime.now()
