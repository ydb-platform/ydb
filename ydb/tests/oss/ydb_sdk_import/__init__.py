from ydb.tests.oss.canonical import is_oss
import os
import sys

if is_oss:
    python_2 = os.getenv('PYTHON2_YDB_IMPORT')
    sdk_path = ""
    if python_2:
        sdk_path = "ydb.public.sdk.python2"
    else:
        sdk_path = "ydb.public.sdk.python3"

    from ydb.public.api.grpc import *  # noqa
    sys.modules[sdk_path + ".ydb._grpc.common"] = sys.modules["ydb.public.api.grpc"]
    from ydb.public.api import protos  # noqa
    sys.modules[sdk_path + ".ydb._grpc.common.protos"] = sys.modules["ydb.public.api.protos"]

    if python_2:
        from ydb.public.sdk.python2 import ydb # noqa
        from ydb.public.sdk.python2.ydb import Driver, DriverConfig, SessionPool # noqa
    else:
        from ydb.public.sdk.python3 import ydb # noqa
        from ydb.public.sdk.python3.ydb import Driver, DriverConfig, SessionPool # noqa
else:
    import ydb # noqa
    from ydb import Driver, DriverConfig, SessionPool #noqa