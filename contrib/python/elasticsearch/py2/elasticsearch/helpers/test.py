#  Licensed to Elasticsearch B.V. under one or more contributor
#  license agreements. See the NOTICE file distributed with
#  this work for additional information regarding copyright
#  ownership. Elasticsearch B.V. licenses this file to you under
#  the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

# type: ignore

import os
import time
from os.path import abspath, dirname, join
from unittest import SkipTest, TestCase

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError

if "ELASTICSEARCH_URL" in os.environ:
    ELASTICSEARCH_URL = os.environ["ELASTICSEARCH_URL"]
else:
    ELASTICSEARCH_URL = "https://elastic:changeme@localhost:9200"

CA_CERTS = None
if ELASTICSEARCH_URL.startswith("https://"):
    CA_CERTS = join(dirname(dirname(dirname(abspath(__file__)))), ".ci/certs/ca.pem")


def get_test_client(nowait=False, **kwargs):
    # construct kwargs from the environment
    kw = {"timeout": 30, "ca_certs": CA_CERTS}

    if "PYTHON_CONNECTION_CLASS" in os.environ:
        from elasticsearch import connection

        kw["connection_class"] = getattr(
            connection, os.environ["PYTHON_CONNECTION_CLASS"]
        )

    kw.update(kwargs)
    client = Elasticsearch(ELASTICSEARCH_URL, **kw)

    # wait for yellow status
    for _ in range(1 if nowait else 100):
        try:
            client.cluster.health(wait_for_status="yellow")
            return client
        except ConnectionError:
            time.sleep(0.1)
    else:
        # timeout
        raise SkipTest("Elasticsearch failed to start.")


class ElasticsearchTestCase(TestCase):
    @staticmethod
    def _get_client():
        return get_test_client()

    @classmethod
    def setup_class(cls):
        cls.client = cls._get_client()

    def teardown_method(self, _):
        # Hidden indices expanded in wildcards in ES 7.7
        expand_wildcards = ["open", "closed"]
        if self.es_version() >= (7, 7):
            expand_wildcards.append("hidden")

        self.client.indices.delete_data_stream(
            name="*", ignore=404, expand_wildcards=expand_wildcards
        )
        self.client.indices.delete(
            index="*", ignore=404, expand_wildcards=expand_wildcards
        )
        self.client.indices.delete_template(name="*", ignore=404)

    def es_version(self):
        if not hasattr(self, "_es_version"):
            self._es_version = es_version(self.client)
        return self._es_version


def _get_version(version_string):
    if "." not in version_string:
        return ()
    version = version_string.strip().split(".")
    return tuple(int(v) if v.isdigit() else 999 for v in version)


def es_version(client):
    return _get_version(client.info()["version"]["number"])
