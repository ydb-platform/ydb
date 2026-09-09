import _dart_fields as df


class Unit:
    def __init__(self, **values):
        self.values = values

    def get(self, name):
        return self.values.get(name, "")

    get_subst = get


def test_facade_typecheck_uses_library_sources():
    unit = Unit(
        _TS_LEGACY_FACADE="yes",
        MODDIR="project",
        _TS_GLOB_FILES='${ARCADIA_ROOT}/project/src/index.ts "${ARCADIA_ROOT}/project/test/my test.ts"',
    )

    assert df.TestFiles.tsc_typecheck_input_files(unit, (), {}) == df.serialize_list(
        ["src/index.ts", "test/my test.ts"]
    )


def test_legacy_typecheck_preserves_inputs():
    unit = Unit(
        TS_INPUT_FILES="${ARCADIA_ROOT}/project/src/index.ts",
        TS_INPUT_TEST_FILES="${ARCADIA_ROOT}/project/test/index.ts",
    )

    assert df.TestFiles.tsc_typecheck_input_files(unit, (), {}) == df.serialize_list(
        ["$S/project/src/index.ts", "$S/project/test/index.ts"]
    )


def test_facade_typecheck_without_sources():
    assert df.TestFiles.tsc_typecheck_input_files(Unit(_TS_LEGACY_FACADE="yes"), (), {}) == ""
