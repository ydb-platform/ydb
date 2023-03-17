from ydb.tests.oss.canonical import is_oss
import os


if is_oss:
    if os.getenv('PYTHON2_YDB_IMPORT'):
        from ydb.public.sdk.python2 import ydb # noqa
    else:
        from ydb.public.sdk.python3 import ydb # noqa
else:
    import ydb # noqa
