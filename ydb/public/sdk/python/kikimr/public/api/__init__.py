from ydb.public.api import *  # noqa
import sys
import warnings

sys.modules['kikimr.public.api'] = sys.modules['ydb.public.api']
warnings.warn("using kikimr.public.api module is deprecated. please use ydb.public.api import instead")
