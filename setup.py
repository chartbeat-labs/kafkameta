#!/usr/bin/env python

from distutils.core import setup
from kafkameta import __version__

setup(name='kafkameta',
      version=__version__,
      description='Query Kafka metadata via Zookeeper',
      author='Justin Lintz',
      author_email='jlintz@gmail.com',
      license='Apache License 2.0',
      url='https://github.com/chartbeat/kafkameta',
      install_requires=['kazoo'],
      packages=['kafkameta'],
     )
