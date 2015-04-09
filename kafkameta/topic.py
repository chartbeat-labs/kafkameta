import os
import json
import logging

logger = logging.getLogger('kafkameta.topic')


class KafkaTopic():
    _topics_config_path = 'config/topics'

    def __init__(self, name, client=None):
        """
        @param _name: str, name of topic
        """
        self._name       = name
        self._partitions = {}
        self._config     = {}
        self._client     = client

    def __getitem__(self, partition):
        return self._partitions[partition]

    def __repr__(self):
        return '<{0} name={1}, partitions={2}>'.format(self.__class__.__name__, self._name, self._partitions.keys())

    def __iter__(self):
        return self._partitions.itervalues()

    @property
    def config(self):
        return self._config

    @property
    def name(self):
        return self._name

    @property
    def partitions(self):
        return self._partitions.values()

    def update(self):
        """
        populate latest config, partition state
        """
        # update config
        self._config = json.loads(self._client.get(os.path.join(self._topics_config_path, self._name))[0])['config']
        # update partitions
        for partition in self._partitions.itervalues():
            partition.update()

    def add_partition(self, part_obj):
        self._partitions[part_obj.id] = part_obj
