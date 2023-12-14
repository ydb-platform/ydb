import sys
try:
    from ydb.public.api.grpc import *  # noqa
    sys.modules["ydb._grpc.common"] = sys.modules["ydb.public.api.grpc"]

    from ydb.public.api import protos
    sys.modules["ydb._grpc.common.protos"] = sys.modules["ydb.public.api.protos"]
except ImportError:
    from contrib.ydb.public.api.grpc import *  # noqa
    sys.modules["ydb._grpc.common"] = sys.modules["contrib.ydb.public.api.grpc"]

    from contrib.ydb.public.api import protos
    sys.modules["ydb._grpc.common.protos"] = sys.modules["contrib.ydb.public.api.protos"]
