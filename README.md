Kafka Metadata Python Library
=============================
[![Build Status](https://travis-ci.org/chartbeat-labs/kafkameta.svg?branch=master)](https://travis-ci.org/chartbeat-labs/kafkameta)

This library pulls data from ZooKeeper about Kafka's configuration vs using Kafka's Metadata API.
Kafka's Metadata API as of 0.8.2 has issues where it does not reflect the current state of the cluster.

According to `https://issues.apache.org/jira/browse/KAFKA-1367`, parts of the API may be removed in the future version.

**DISCLAIMER**: This is not ready for Production use, the library is still evolving so we may push changes that break backwards compatability

Example
-------
```
from kafkameta import KafkaConnection
kc = KafkaConnection(hosts=['zk01:2181'], zk_path='/cb/kafka/pingqueue')

print kc.topics.keys()
print kc.brokers.keys()

# load partition state, optionally you can pass load_state=True with KafkaConnection
# We don't do this by default since it can be very slow for large number of topics and partitions
kc.update()

# print partition 1 state from topic pings
print kc.topics['pings'][1]
```

Tests
-----
Still a work in progress

License
-----------
Apache License 2.0


