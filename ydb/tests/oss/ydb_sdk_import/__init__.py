from ydb.tests.oss.canonical import is_oss

if is_oss:
    from ydb.public.sdk.python import ydb # noqa
else:
    import ydb # noqa
