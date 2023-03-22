from .credentials import *  # noqa
from .driver import *  # noqa
from .global_settings import *  # noqa
from .table import *  # noqa
from .issues import *  # noqa
from .types import *  # noqa
from .scheme import *  # noqa
from .settings import *  # noqa
from .resolver import *  # noqa
from .export import *  # noqa
from .auth_helpers import *  # noqa
from .operation import *  # noqa
from .scripting import *  # noqa
from .import_client import *  # noqa
from .tracing import *  # noqa
from .topic import *  # noqa

try:
    import ydb.aio as aio  # noqa
except Exception:
    pass

try:
    import kikimr.public.sdk.python.ydb_v3_new_behavior  # noqa
    global_allow_split_transactions(False)  # noqa
    global_allow_truncated_result(False)  # noqa
except ModuleNotFoundError:
    # Old, deprecated

    import warnings
    warnings.warn("Used deprecated behavior, for fix ADD PEERDIR kikimr/public/sdk/python/ydb_v3_new_behavior")

    global_allow_split_transactions(True)  # noqa
    global_allow_truncated_result(True)  # noqa
