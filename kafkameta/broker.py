from datetime import datetime

class KafkaBroker(object):

    def __init__(self, id_, host, port, jmx_port, timestamp):
        self._id = id_
        self._host = host
        self._port = port
        self._jmx_port = jmx_port
        self._timestamp = datetime.fromtimestamp(timestamp/1000)

    def __repr__(self):
        return '<{0} host={1}, port={2}>'.format(self.__class__.__name__, self._host, self._port)

    @property
    def id(self):
        return self._id

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def jmx_port(self):
        return self._jmx_port

    @property
    def timestamp(self):
        return self._timestamp
