PY3TEST()

PEERDIR(
    contrib/python/nbformat
    contrib/python/testpath
)

SRCDIR(contrib/python/nbformat)

NO_LINT()

TEST_SRCS(
    nbformat/corpus/tests/__init__.py
    nbformat/corpus/tests/test_words.py
    tests/__init__.py
    tests/base.py
    tests/test_api.py
    tests/test_convert.py
    tests/test_nbformat.py
    tests/test_reader.py
    tests/test_sign.py
    tests/test_validator.py
    tests/v1/__init__.py
    tests/v1/nbexamples.py
    tests/v1/test_json.py
    tests/v1/test_nbbase.py
    tests/v2/__init__.py
    tests/v2/nbexamples.py
    tests/v2/test_json.py
    tests/v2/test_nbbase.py
    tests/v2/test_nbpy.py
    tests/v3/__init__.py
    tests/v3/formattest.py
    tests/v3/nbexamples.py
    tests/v3/test_json.py
    tests/v3/test_misc.py
    tests/v3/test_nbbase.py
    tests/v3/test_nbpy.py
    tests/v4/__init__.py
    tests/v4/formattest.py
    tests/v4/nbexamples.py
    tests/v4/test_convert.py
    tests/v4/test_json.py
    tests/v4/test_nbbase.py
    tests/v4/test_validate.py
)

DATA(
    arcadia/contrib/python/nbformat/nbformat/v4
    arcadia/contrib/python/nbformat/tests
)

END()
