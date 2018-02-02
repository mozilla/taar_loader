#!/usr/bin/env python

from setuptools import setup

setup(name='taar_loader',
      version='1.0',
      description='Migration tool to move data from Telemetry S3 buckets over to DynamoDB',
      author='Victor Ng',
      author_email='vng@mozilla.com',
      url='https://github.com/crankycoder/taar_loader',
      license='MPL 2.0',
      packages=['taar_loader'])
