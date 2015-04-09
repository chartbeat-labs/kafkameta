import json
import logging
import os

logger = logging.getLogger('kafkameta.partition')

class KafkaPartition(object):

    def __init__(self, id_, topic, replicas=None, isr=None, leader=None, client=None):
        """
        @param _id       : int, partition id number
        @param _topic    : str, name of topic partition associated with
        @param _replicas : list, broker ids that should contain replicas
        @param _leader   : int, broker id of leader
        @param _isr      : list, replicas that are in-sync with leader
        @param _client   : KazooClient, connection to ZK
        """
        self._id       = id_
        self._topic    = topic
        self._replicas = replicas
        self._leader   = leader
        self._isr      = isr
        self._client   = client
        self._topics_state_path = 'brokers/topics/{0}'.format(topic)

    @property
    def id(self):
        return self._id

    @property
    def leader(self):
        return self._leader

    @property
    def replicas(self):
        return sorted(self._replicas)

    @property
    def isr(self):
        return sorted(self._isr)

    @property
    def topic(self):
        return self._topic

    def __repr__(self):
        return '<{0} {1}>'.format(self.__class__.__name__, self.__dict__)

    def update(self):
        # update replicas
        replicas = json.loads(self._client.get(self._topics_state_path)[0])['partitions'][str(self._id)]
        self._replicas = replicas

        # update partition state
        state = self._client.get(os.path.join(self._topics_state_path, 'partitions', str(self._id), 'state'))[0]
        logger.debug('Loading {0} partition {1} state'.format(self._topic, self._id))
        self.add_state(state)

    def add_state(self, raw_data):
        raw_data = json.loads(raw_data)
        for k, v in raw_data.iteritems():
            setattr(self, '_' + k, v)
