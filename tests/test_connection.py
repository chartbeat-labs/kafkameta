from datetime import datetime

from kafkameta.connection import KafkaConnection
from utils import KafkaTest


class KafkaConnectionTest(KafkaTest):

    def test_connection_init_no_state(self):
        self.assertTrue(self.conn.connected)
        self.assertEquals(self.conn._last_state_update, None)

    def test_connection_init_state(self):
        conn = KafkaConnection(self.config['zk_path'], load_state=True, kazoo_client=self.client)
        self.assertTrue(conn.connected)
        self.assertTrue(type(conn._last_state_update) is datetime)

    def test_disconnect(self):
        self.conn.disconnect()
        self.assertFalse(self.conn.connected)

    def test_topics(self):
        self.assertEqual(self.conn.topics.keys(), self.config['topics'])

    def test_brokers(self):
        self.assertEqual(self.conn.brokers.keys(), self.config['brokers'])
