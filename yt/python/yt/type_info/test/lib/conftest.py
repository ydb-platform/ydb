import cyson

import six

import sys


# Emulate module `yt.yson` in tests to get rid of the dependency on yt/python
class FakeYsonModule:
    YsonError = ValueError

    @staticmethod
    def loads(yson):
        reader = cyson.Reader if six.text_type not in six.string_types else cyson.UnicodeReader
        return cyson.loads(yson, Reader=reader)

    @staticmethod
    def dumps(yson, yson_format="text"):
        if hasattr(yson, "to_yson_type"):
            yson = yson.to_yson_type()

        return cyson.dumps(yson, format=yson_format)


class FakeYtModule:
    yson = FakeYsonModule


sys.modules["yt"] = FakeYtModule
sys.modules["yt.yson"] = FakeYtModule.yson
