import pytest

import _dart_fields as df
import lib.test_const as consts
import ytest


class Unit:
    def __init__(self, **values):
        self.values = values

    def get(self, name):
        return self.values.get(name, "")

    get_subst = get

    def path(self):
        return '$S/project'


@pytest.mark.parametrize('size', consts.TestSize.sizes())
@pytest.mark.parametrize(
    'sanitizer,tags,skipped',
    [
        ('address', ['ya:perftest'], True),
        ('memory', ['ya:perftest'], True),
        ('thread', ['ya:perftest'], True),
        ('undefined', ['ya:perftest'], True),
        ('leak', ['ya:perftest'], True),
        ('address', ['ya:perftest', 'ya:not_autocheck'], True),
        ('address', ['ya:perftest', 'ya:perftest_with_sanitizers'], False),
        ('memory', ['ya:perftest', 'ya:perftest_with_sanitizers'], False),
        ('thread', ['ya:perftest', 'ya:perftest_with_sanitizers'], False),
        ('undefined', ['ya:perftest', 'ya:perftest_with_sanitizers'], False),
        ('leak', ['ya:perftest', 'ya:perftest_with_sanitizers'], False),
        ('address', ['ya:perftest', 'ya:perftest_with_sanitizers', 'ya:not_autocheck'], False),
        ('address', ['ya:perftest_with_sanitizers'], False),
        ('', ['ya:perftest'], False),
        ('no', ['ya:perftest'], False),
        ('', ['ya:perftest', 'ya:perftest_with_sanitizers'], False),
        ('no', ['ya:perftest', 'ya:perftest_with_sanitizers'], False),
        ('address', [], False),
        ('', [], False),
    ],
)
def test_sanitized_perftest_is_disabled(size, sanitizer, tags, skipped):
    tags = tags + ['custom_tag']
    if size == consts.TestSize.Large:
        tags.append(consts.YaTestTags.Fat)
    data = ytest.dump_test(
        Unit(SANITIZER_TYPE=sanitizer),
        {
            'SCRIPT-REL-PATH': 'py.test',
            'SOURCE-FOLDER-PATH': 'project',
            'TEST-NAME': 'test',
            'SIZE': size,
            'TAG': df.serialize_list(tags),
        },
    )
    record = dict(line.split(': ', 1) for line in data.splitlines() if ': ' in line)
    if skipped:
        assert record[df.SkipTest.KEY] == 'Performance tests are disabled under sanitizers'
        tags = tags + [consts.YaTestTags.NotAutocheck]
    else:
        assert df.SkipTest.KEY not in record
    actual_tags = df.deserialize_list(record[df.Tag.KEY])
    assert actual_tags == sorted(set(tags))


@pytest.mark.parametrize(
    'tags,not_autocheck',
    [
        (['ya:perftest'], True),
        (['ya:perftest', 'ya:not_autocheck'], True),
        (['ya:perftest', 'ya:perftest_with_sanitizers'], False),
        (['ya:perftest', 'ya:perftest_with_sanitizers', 'ya:not_autocheck'], True),
    ],
)
def test_sanitized_perftest_preserves_existing_skip_reason(tags, not_autocheck):
    data = ytest.dump_test(
        Unit(SANITIZER_TYPE='address'),
        {
            'SCRIPT-REL-PATH': 'gtest',
            'SOURCE-FOLDER-PATH': 'project',
            'TEST-NAME': 'test',
            'TAG': df.serialize_list(tags),
            'SKIP_TEST': 'Explicit skip',
        },
    )
    assert 'SKIP_TEST: Explicit skip\n' in data
    record = dict(line.split(': ', 1) for line in data.splitlines() if ': ' in line)
    assert ('ya:not_autocheck' in df.deserialize_list(record[df.Tag.KEY])) == not_autocheck


def test_ts_check_sources_include_tests_and_spaces():
    unit = Unit(
        MODDIR="project",
        _TS_GLOB_FILES='${ARCADIA_ROOT}/project/src/index.ts "${ARCADIA_ROOT}/project/tests/my test.ts"',
    )
    assert df.TestFiles.ts_check_srcs(unit, (), {}) == df.serialize_list(["src/index.ts", "tests/my test.ts"])


def test_ts_check_for_sources_are_relative_to_tested_module():
    unit = Unit(
        TS_FOR="yes",
        TS_FOR_PATH="project",
        MODDIR="project/tests",
        _TS_GLOB_FILES="${ARCADIA_ROOT}/project/src/index.ts",
    )
    assert df.TestFiles.ts_check_srcs(unit, (), {}) == df.serialize_list(["src/index.ts"])


def test_ts_check_without_sources():
    assert df.TestFiles.ts_check_srcs(Unit(), (), {}) == ""
