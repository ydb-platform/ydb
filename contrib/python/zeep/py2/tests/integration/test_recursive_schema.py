import os

import zeep

import yatest.common


def test_hello_world():
    path = yatest.common.test_source_path("integration/recursive_schema_main.wsdl")
    client = zeep.Client(path)
    client.wsdl.dump()
