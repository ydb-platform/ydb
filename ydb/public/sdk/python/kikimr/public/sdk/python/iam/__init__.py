from ydb.iam import *  # noqa
import sys
import warnings

warnings.warn("using kikimr.public.sdk.python.iam module is deprecated. please use ydb.iam import instead")

sys.modules['kikimr.public.sdk.python.iam.auth'] = sys.modules['ydb.iam.auth']
