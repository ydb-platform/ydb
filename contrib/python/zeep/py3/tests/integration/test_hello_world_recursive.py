import os

import zeep


def test_hello_world():
    import yatest.common as yc
    path = yc.test_source_path("integration/hello_world_recursive.wsdl")
    client = zeep.Client(path)
    client.wsdl.dump()
