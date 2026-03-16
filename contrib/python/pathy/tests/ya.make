PY3TEST()

PEERDIR(
    contrib/python/pathy
    contrib/python/mock
    contrib/python/spacy
)

SRCDIR(contrib/python/pathy)

TEST_SRCS(
    pathy/_tests/__init__.py
    pathy/_tests/conftest.py
    pathy/_tests/test_azure.py
    pathy/_tests/test_base.py
    pathy/_tests/test_cli.py
    pathy/_tests/test_clients.py
    pathy/_tests/test_file.py
    pathy/_tests/test_gcs.py
    pathy/_tests/test_pathy.py
    pathy/_tests/test_pathy_scandir.py
    pathy/_tests/test_pure_pathy.py
    pathy/_tests/test_s3.py
    pathy/_tests/test_windows.py
)

NO_LINT()

END()
