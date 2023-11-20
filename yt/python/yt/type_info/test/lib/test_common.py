import yt.type_info as ti

import pytest
import pkgutil


def load_and_parse(path):
    testdata = pkgutil.get_data(__package__, path)
    testlines = [line for line in testdata.split(b"\n") if not line.startswith(b"#")]
    tests = (b"\n".join(testlines)).split(b";;")

    parsed_tests = [tuple(test_arg.strip() for test_arg in test.split(b"::"))
                    for test in tests]

    return [test for test in parsed_tests if any(test)]


def pytest_generate_tests(metafunc):
    if metafunc.cls is not TestCommon:
        return

    TEST_DATA_PATH_PREFIX = "library/cpp/type_info/ut/test-data"

    if "test_good" in metafunc.function.__name__:
        tests = load_and_parse(TEST_DATA_PATH_PREFIX + "/good-types.txt")
        argnames = ["yson_type", "string_type"]
    else:
        tests = load_and_parse(TEST_DATA_PATH_PREFIX + "/bad-types.txt")
        argnames = ["yson_type", "error_text"]

    argvalues = [tuple(test[:len(argnames)]) for test in tests]
    ids = [str(id) for id in range(1, len(tests) + 1)]

    metafunc.parametrize(argnames, argvalues, ids=ids)


class TestCommon:
    def test_good(self, yson_type, string_type):
        py_type = ti.deserialize_yson(yson_type)

        assert str(py_type) == string_type.decode("utf-8")

        yson_type2 = ti.serialize_yson(py_type, human_readable=True)
        py_type2 = ti.deserialize_yson(yson_type2)
        assert py_type == py_type2

    def test_bad(self, yson_type, error_text):
        with pytest.raises(ValueError, match=error_text.decode("utf-8")):
            ti.deserialize_yson(yson_type)
