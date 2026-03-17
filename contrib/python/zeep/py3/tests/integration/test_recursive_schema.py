import os

import zeep


def test_hello_world():
    import yatest.common as yc
    path = yc.test_source_path("integration/recursive_schema_main.wsdl")
    client = zeep.Client(path)
    client.wsdl.dump()
