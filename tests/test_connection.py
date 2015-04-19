import unittest
import mock
import random
import os
import json

from datetime import datetime

from kazoo.client import KazooClient
from kazoo.testing import KazooTestHarness

from kafkameta.connection import KafkaConnection

def _generate_topic_data(brokers, partitions, replica_count):
    brokers = range(1, brokers)
    topic_data = {}

    for partition in range(1, partitions):
        topic_data[str(partition)] = random.sample(brokers, replica_count)

    return { 'version': 1, 'partitions': topic_data }

class KafkaConnectionTest(KazooTestHarness):

    def setUp(self):
        self.config = {
            'zk_path': '/kafka',
            'topics': ['testtopic'],
            'brokers': [],
        }

        self.setup_zookeeper()
        self.client.ensure_path(self.config['zk_path'])
        self.client.chroot = self.config['zk_path']

        num_brokers = random.randint(4, 12)
        num_partitions = random.randint(4, 12)
        num_replicas = 3

        # setup root path
        self.client.ensure_path(self.config['zk_path'])

        # setup topics path
        for t in self.config['topics']:
            topic_path = os.path.join('brokers/topics', t)
            topic_data = _generate_topic_data(num_brokers, num_partitions, num_replicas)

            topic_config_path = os.path.join('config/topics', t)
            topic_config_data = {"version":1,"config":{"segment.bytes":"33554432","retention.bytes":"536870912"}}

            self.client.ensure_path(topic_path)
            self.client.ensure_path(topic_config_path)

            self.client.set(topic_path, json.dumps(topic_data).encode('utf-8'))
            self.client.set(topic_config_path, json.dumps(topic_config_data).encode('utf-8'))

            # create random number of partitions per topic
            for part_num in range(0, num_partitions):
                partition_path = os.path.join('brokers/topics', t, 'partitions/{0}/state'.format(part_num))
                partition_state = {"controller_epoch": random.randint(1, num_brokers),
                                "leader": random.randint(1, num_brokers),
                                "version": 1,
                                "leader_epoch": random.randint(1, num_brokers),
                                "isr": random.sample(range(1, num_brokers), 3)}
                self.client.ensure_path(partition_path)
                self.client.set(partition_path, json.dumps(partition_state).encode('utf-8'))

        # setup brokers path
        for broker in range(0, num_brokers):
            broker_path = os.path.join('brokers/ids/', str(broker))
            self.config['brokers'].append(broker)
            broker_data = {"jmx_port":9999, "timestamp":"1428510920828", "host":"broker{0}.chartbeat.net".format(broker), "version":1, "port":9092}
            self.client.ensure_path(broker_path)
            self.client.set(broker_path, json.dumps(broker_data).encode('utf-8'))

        self.conn = KafkaConnection(self.config['zk_path'], kazoo_client=self.client)


    def tearDown(self):
        self.teardown_zookeeper()

    def test_connection_init_no_state(self):
        self.assertTrue(self.conn.connected)
        self.assertEquals(self.conn._last_state_update, None)

    def test_connection_init_state(self):
        conn = KafkaConnection(self.config['zk_path'], load_state=True, kazoo_client=self.client)
        self.assertTrue(conn.connected)
        self.assertIsInstance(conn._last_state_update, datetime)

    def test_disconnect(self):
        self.conn.disconnect()
        self.assertFalse(self.conn.connected)

    def test_topics(self):
        self.assertEqual(self.conn.topics.keys(), self.config['topics'])

    def test_brokers(self):
        self.assertEqual(self.conn.brokers.keys(), self.config['brokers'])
