import sys

from ydb.public.api.grpc import *  # noqa
sys.modules["ydb.public.sdk.python3.ydb._grpc.common"] = sys.modules["ydb.public.api.grpc"]
from ydb.public.api import protos  # noqa
sys.modules["ydb.public.sdk.python3.ydb._grpc.common.protos"] = sys.modules["ydb.public.api.protos"]
