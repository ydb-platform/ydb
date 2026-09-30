import _dart_fields as df


class Unit:
    def __init__(self, **values):
        self.values = values

    def get(self, name):
        return self.values.get(name, "")

    get_subst = get


def test_ts_check_sources_include_tests_and_spaces():
    unit = Unit(
        MODDIR="project",
        _TS_GLOB_FILES='${ARCADIA_ROOT}/project/src/index.ts "${ARCADIA_ROOT}/project/tests/my test.ts"',
    )
    assert df.TestFiles.ts_check_srcs(unit, (), {}) == df.serialize_list(["src/index.ts", "tests/my test.ts"])


def test_ts_check_for_sources_are_relative_to_tested_module():
    unit = Unit(
        TS_TEST_FOR="yes",
        TS_TEST_FOR_PATH="project",
        MODDIR="project/tests",
        _TS_GLOB_FILES="${ARCADIA_ROOT}/project/src/index.ts",
    )
    assert df.TestFiles.ts_check_srcs(unit, (), {}) == df.serialize_list(["src/index.ts"])


def test_ts_check_without_sources():
    assert df.TestFiles.ts_check_srcs(Unit(), (), {}) == ""
