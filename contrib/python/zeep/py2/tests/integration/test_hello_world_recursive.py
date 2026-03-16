import os

import zeep

import yatest.common


def test_hello_world():
    path = yatest.common.test_source_path("integration/hello_world_recursive.wsdl")
    client = zeep.Client(path)
    client.wsdl.dump()
