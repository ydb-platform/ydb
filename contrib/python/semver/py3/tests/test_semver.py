from argparse import Namespace
from contextlib import contextmanager
import pytest  # noqa

from semver import (
    VersionInfo,
    bump_build,
    bump_major,
    bump_minor,
    bump_patch,
    bump_prerelease,
    cmd_bump,
    cmd_check,
    cmd_compare,
    compare,
    createparser,
    deprecated,
    finalize_version,
    format_version,
    main,
    match,
    max_ver,
    min_ver,
    parse,
    parse_version_info,
    process,
    replace,
    cmd_nextver,
)

SEMVERFUNCS = [
    compare,
    createparser,
    bump_build,
    bump_major,
    bump_minor,
    bump_patch,
    bump_prerelease,
    finalize_version,
    format_version,
    match,
    max_ver,
    min_ver,
    parse,
    process,
    replace,
]


@contextmanager
def does_not_raise(item):
    yield item


@pytest.mark.parametrize(
    "string,expected", [("rc", "rc"), ("rc.1", "rc.2"), ("2x", "3x")]
)
def test_should_private_increment_string(string, expected):
    assert VersionInfo._increment_string(string) == expected


@pytest.fixture
def version():
    return VersionInfo(
        major=1, minor=2, patch=3, prerelease="alpha.1.2", build="build.11.e0f985a"
    )


@pytest.mark.parametrize(
    "func", SEMVERFUNCS, ids=[func.__name__ for func in SEMVERFUNCS]
)
def test_fordocstrings(func):
    assert func.__doc__, "Need a docstring for function %r" % func.__name


@pytest.mark.parametrize(
    "ver",
    [
        {"major": -1},
        {"major": 1, "minor": -2},
        {"major": 1, "minor": 2, "patch": -3},
        {"major": 1, "minor": -2, "patch": 3},
    ],
)
def test_should_not_allow_negative_numbers(ver):
    with pytest.raises(ValueError, match=".* is negative. .*"):
        VersionInfo(**ver)


@pytest.mark.parametrize(
    "version,expected",
    [
        # no. 1
        (
            "1.2.3-alpha.1.2+build.11.e0f985a",
            {
                "major": 1,
                "minor": 2,
                "patch": 3,
                "prerelease": "alpha.1.2",
                "build": "build.11.e0f985a",
            },
        ),
        # no. 2
        (
            "1.2.3-alpha-1+build.11.e0f985a",
            {
                "major": 1,
                "minor": 2,
                "patch": 3,
                "prerelease": "alpha-1",
                "build": "build.11.e0f985a",
            },
        ),
        (
            "0.1.0-0f",
            {"major": 0, "minor": 1, "patch": 0, "prerelease": "0f", "build": None},
        ),
        (
            "0.0.0-0foo.1",
            {"major": 0, "minor": 0, "patch": 0, "prerelease": "0foo.1", "build": None},
        ),
        (
            "0.0.0-0foo.1+build.1",
            {
                "major": 0,
                "minor": 0,
                "patch": 0,
                "prerelease": "0foo.1",
                "build": "build.1",
            },
        ),
    ],
)
def test_should_parse_version(version, expected):
    result = parse(version)
    assert result == expected


@pytest.mark.parametrize(
    "version,expected",
    [
        # no. 1
        (
            "1.2.3-rc.0+build.0",
            {
                "major": 1,
                "minor": 2,
                "patch": 3,
                "prerelease": "rc.0",
                "build": "build.0",
            },
        ),
        # no. 2
        (
            "1.2.3-rc.0.0+build.0",
            {
                "major": 1,
                "minor": 2,
                "patch": 3,
                "prerelease": "rc.0.0",
                "build": "build.0",
            },
        ),
    ],
)
def test_should_parse_zero_prerelease(version, expected):
    result = parse(version)
    assert result == expected


@pytest.mark.parametrize(
    "left,right",
    [
        ("1.0.0", "2.0.0"),
        ("1.0.0-alpha", "1.0.0-alpha.1"),
        ("1.0.0-alpha.1", "1.0.0-alpha.beta"),
        ("1.0.0-alpha.beta", "1.0.0-beta"),
        ("1.0.0-beta", "1.0.0-beta.2"),
        ("1.0.0-beta.2", "1.0.0-beta.11"),
        ("1.0.0-beta.11", "1.0.0-rc.1"),
        ("1.0.0-rc.1", "1.0.0"),
    ],
)
def test_should_get_less(left, right):
    assert compare(left, right) == -1


@pytest.mark.parametrize(
    "left,right",
    [
        ("2.0.0", "1.0.0"),
        ("1.0.0-alpha.1", "1.0.0-alpha"),
        ("1.0.0-alpha.beta", "1.0.0-alpha.1"),
        ("1.0.0-beta", "1.0.0-alpha.beta"),
        ("1.0.0-beta.2", "1.0.0-beta"),
        ("1.0.0-beta.11", "1.0.0-beta.2"),
        ("1.0.0-rc.1", "1.0.0-beta.11"),
        ("1.0.0", "1.0.0-rc.1"),
    ],
)
def test_should_get_greater(left, right):
    assert compare(left, right) == 1


def test_should_match_simple():
    assert match("2.3.7", ">=2.3.6") is True


def test_should_no_match_simple():
    assert match("2.3.7", ">=2.3.8") is False


@pytest.mark.parametrize(
    "left,right,expected",
    [
        ("2.3.7", "!=2.3.8", True),
        ("2.3.7", "!=2.3.6", True),
        ("2.3.7", "!=2.3.7", False),
    ],
)
def test_should_match_not_equal(left, right, expected):
    assert match(left, right) is expected


@pytest.mark.parametrize(
    "left,right,expected",
    [
        ("2.3.7", "<2.4.0", True),
        ("2.3.7", ">2.3.5", True),
        ("2.3.7", "<=2.3.9", True),
        ("2.3.7", ">=2.3.5", True),
        ("2.3.7", "==2.3.7", True),
        ("2.3.7", "!=2.3.7", False),
    ],
)
def test_should_not_raise_value_error_for_expected_match_expression(
    left, right, expected
):
    assert match(left, right) is expected


@pytest.mark.parametrize(
    "left,right", [("2.3.7", "=2.3.7"), ("2.3.7", "~2.3.7"), ("2.3.7", "^2.3.7")]
)
def test_should_raise_value_error_for_unexpected_match_expression(left, right):
    with pytest.raises(ValueError):
        match(left, right)


@pytest.mark.parametrize("version", ["01.2.3", "1.02.3", "1.2.03"])
def test_should_raise_value_error_for_zero_prefixed_versions(version):
    with pytest.raises(ValueError):
        parse(version)


@pytest.mark.parametrize(
    "left,right", [("foo", "bar"), ("1.0", "1.0.0"), ("1.x", "1.0.0")]
)
def test_should_raise_value_error_for_invalid_value(left, right):
    with pytest.raises(ValueError):
        compare(left, right)


@pytest.mark.parametrize(
    "left,right", [("1.0.0", ""), ("1.0.0", "!"), ("1.0.0", "1.0.0")]
)
def test_should_raise_value_error_for_invalid_match_expression(left, right):
    with pytest.raises(ValueError):
        match(left, right)


def test_should_follow_specification_comparison():
    """
    produce comparison chain:
    1.0.0-alpha < 1.0.0-alpha.1 < 1.0.0-beta.2 < 1.0.0-beta.11
    < 1.0.0-rc.1 < 1.0.0-rc.1+build.1 < 1.0.0 < 1.0.0+0.3.7 < 1.3.7+build
    < 1.3.7+build.2.b8f12d7 < 1.3.7+build.11.e0f985a
    and in backward too.
    """
    chain = [
        "1.0.0-alpha",
        "1.0.0-alpha.1",
        "1.0.0-beta.2",
        "1.0.0-beta.11",
        "1.0.0-rc.1",
        "1.0.0",
        "1.3.7+build",
    ]
    versions = zip(chain[:-1], chain[1:])
    for low_version, high_version in versions:
        assert (
            compare(low_version, high_version) == -1
        ), "%s should be lesser than %s" % (low_version, high_version)
        assert (
            compare(high_version, low_version) == 1
        ), "%s should be higher than %s" % (high_version, low_version)


@pytest.mark.parametrize("left,right", [("1.0.0-beta.2", "1.0.0-beta.11")])
def test_should_compare_rc_builds(left, right):
    assert compare(left, right) == -1


@pytest.mark.parametrize(
    "left,right", [("1.0.0-rc.1", "1.0.0"), ("1.0.0-rc.1+build.1", "1.0.0")]
)
def test_should_compare_release_candidate_with_release(left, right):
    assert compare(left, right) == -1


@pytest.mark.parametrize(
    "left,right",
    [
        ("2.0.0", "2.0.0"),
        ("1.1.9-rc.1", "1.1.9-rc.1"),
        ("1.1.9+build.1", "1.1.9+build.1"),
        ("1.1.9-rc.1+build.1", "1.1.9-rc.1+build.1"),
    ],
)
def test_should_say_equal_versions_are_equal(left, right):
    assert compare(left, right) == 0


@pytest.mark.parametrize(
    "left,right,expected",
    [("1.1.9-rc.1", "1.1.9-rc.1+build.1", 0), ("1.1.9-rc.1", "1.1.9+build.1", -1)],
)
def test_should_compare_versions_with_build_and_release(left, right, expected):
    assert compare(left, right) == expected


@pytest.mark.parametrize(
    "left,right,expected",
    [
        ("1.0.0+build.1", "1.0.0", 0),
        ("1.0.0-alpha.1+build.1", "1.0.0-alpha.1", 0),
        ("1.0.0+build.1", "1.0.0-alpha.1", 1),
        ("1.0.0+build.1", "1.0.0-alpha.1+build.1", 1),
    ],
)
def test_should_ignore_builds_on_compare(left, right, expected):
    assert compare(left, right) == expected


def test_should_correctly_format_version():
    assert format_version(3, 4, 5) == "3.4.5"
    assert format_version(3, 4, 5, "rc.1") == "3.4.5-rc.1"
    assert format_version(3, 4, 5, prerelease="rc.1") == "3.4.5-rc.1"
    assert format_version(3, 4, 5, build="build.4") == "3.4.5+build.4"
    assert format_version(3, 4, 5, "rc.1", "build.4") == "3.4.5-rc.1+build.4"


def test_should_bump_major():
    assert bump_major("3.4.5") == "4.0.0"


def test_should_bump_minor():
    assert bump_minor("3.4.5") == "3.5.0"


def test_should_bump_patch():
    assert bump_patch("3.4.5") == "3.4.6"


def test_should_versioninfo_bump_major_and_minor():
    v = parse_version_info("3.4.5")
    expected = parse_version_info("4.1.0")
    assert v.bump_major().bump_minor() == expected


def test_should_versioninfo_bump_minor_and_patch():
    v = parse_version_info("3.4.5")
    expected = parse_version_info("3.5.1")
    assert v.bump_minor().bump_patch() == expected


def test_should_versioninfo_bump_patch_and_prerelease():
    v = parse_version_info("3.4.5-rc.1")
    expected = parse_version_info("3.4.6-rc.1")
    assert v.bump_patch().bump_prerelease() == expected


def test_should_versioninfo_bump_patch_and_prerelease_with_token():
    v = parse_version_info("3.4.5-dev.1")
    expected = parse_version_info("3.4.6-dev.1")
    assert v.bump_patch().bump_prerelease("dev") == expected


def test_should_versioninfo_bump_prerelease_and_build():
    v = parse_version_info("3.4.5-rc.1+build.1")
    expected = parse_version_info("3.4.5-rc.2+build.2")
    assert v.bump_prerelease().bump_build() == expected


def test_should_versioninfo_bump_prerelease_and_build_with_token():
    v = parse_version_info("3.4.5-rc.1+b.1")
    expected = parse_version_info("3.4.5-rc.2+b.2")
    assert v.bump_prerelease().bump_build("b") == expected


def test_should_versioninfo_bump_multiple():
    v = parse_version_info("3.4.5-rc.1+build.1")
    expected = parse_version_info("3.4.5-rc.2+build.2")
    assert v.bump_prerelease().bump_build().bump_build() == expected
    expected = parse_version_info("3.4.5-rc.3")
    assert v.bump_prerelease().bump_build().bump_build().bump_prerelease() == expected


def test_should_versioninfo_to_dict(version):
    resultdict = version.to_dict()
    assert isinstance(resultdict, dict), "Got type from to_dict"
    assert list(resultdict.keys()) == ["major", "minor", "patch", "prerelease", "build"]


def test_should_versioninfo_to_tuple(version):
    result = version.to_tuple()
    assert isinstance(result, tuple), "Got type from to_dict"
    assert len(result) == 5, "Different length from to_tuple()"


def test_should_ignore_extensions_for_bump():
    assert bump_patch("3.4.5-rc1+build4") == "3.4.6"


def test_should_get_max():
    assert max_ver("3.4.5", "4.0.2") == "4.0.2"


def test_should_get_max_same():
    assert max_ver("3.4.5", "3.4.5") == "3.4.5"


def test_should_get_min():
    assert min_ver("3.4.5", "4.0.2") == "3.4.5"


def test_should_get_min_same():
    assert min_ver("3.4.5", "3.4.5") == "3.4.5"


def test_should_get_more_rc1():
    assert compare("1.0.0-rc1", "1.0.0-rc0") == 1


@pytest.mark.parametrize(
    "left,right,expected",
    [
        ("1.2.3-rc.2", "1.2.3-rc.10", "1.2.3-rc.2"),
        ("1.2.3-rc2", "1.2.3-rc10", "1.2.3-rc10"),
        # identifiers with letters or hyphens are compared lexically in ASCII sort
        # order.
        ("1.2.3-Rc10", "1.2.3-rc10", "1.2.3-Rc10"),
        # Numeric identifiers always have lower precedence than non-numeric
        # identifiers.
        ("1.2.3-2", "1.2.3-rc", "1.2.3-2"),
        # A larger set of pre-release fields has a higher precedence than a
        # smaller set, if all of the preceding identifiers are equal.
        ("1.2.3-rc.2.1", "1.2.3-rc.2", "1.2.3-rc.2"),
        # When major, minor, and patch are equal, a pre-release version has lower
        # precedence than a normal version.
        ("1.2.3", "1.2.3-1", "1.2.3-1"),
        ("1.0.0-alpha", "1.0.0-alpha.1", "1.0.0-alpha"),
    ],
)
def test_prerelease_order(left, right, expected):
    assert min_ver(left, right) == expected


@pytest.mark.parametrize(
    "version,token,expected",
    [
        ("3.4.5-rc.9", None, "3.4.5-rc.10"),
        ("3.4.5", None, "3.4.5-rc.1"),
        ("3.4.5", "dev", "3.4.5-dev.1"),
        ("3.4.5", "", "3.4.5-rc.1"),
    ],
)
def test_should_bump_prerelease(version, token, expected):
    token = "rc" if not token else token
    assert bump_prerelease(version, token) == expected


def test_should_ignore_build_on_prerelease_bump():
    assert bump_prerelease("3.4.5-rc.1+build.4") == "3.4.5-rc.2"


@pytest.mark.parametrize(
    "version,expected",
    [
        ("3.4.5-rc.1+build.9", "3.4.5-rc.1+build.10"),
        ("3.4.5-rc.1+0009.dev", "3.4.5-rc.1+0010.dev"),
        ("3.4.5-rc.1", "3.4.5-rc.1+build.1"),
        ("3.4.5", "3.4.5+build.1"),
    ],
)
def test_should_bump_build(version, expected):
    assert bump_build(version) == expected


@pytest.mark.parametrize(
    "version,expected",
    [
        ("1.2.3", "1.2.3"),
        ("1.2.3-rc.5", "1.2.3"),
        ("1.2.3+build.2", "1.2.3"),
        ("1.2.3-rc.1+build.5", "1.2.3"),
        ("1.2.3-alpha", "1.2.3"),
        ("1.2.0", "1.2.0"),
    ],
)
def test_should_finalize_version(version, expected):
    assert finalize_version(version) == expected


def test_should_compare_version_info_objects():
    v1 = VersionInfo(major=0, minor=10, patch=4)
    v2 = VersionInfo(major=0, minor=10, patch=4, prerelease="beta.1", build=None)

    # use `not` to enforce using comparision operators
    assert v1 != v2
    assert v1 > v2
    assert v1 >= v2
    assert not (v1 < v2)
    assert not (v1 <= v2)
    assert not (v1 == v2)

    v3 = VersionInfo(major=0, minor=10, patch=4)

    assert not (v1 != v3)
    assert not (v1 > v3)
    assert v1 >= v3
    assert not (v1 < v3)
    assert v1 <= v3
    assert v1 == v3

    v4 = VersionInfo(major=0, minor=10, patch=5)
    assert v1 != v4
    assert not (v1 > v4)
    assert not (v1 >= v4)
    assert v1 < v4
    assert v1 <= v4
    assert not (v1 == v4)


def test_should_compare_version_dictionaries():
    v1 = VersionInfo(major=0, minor=10, patch=4)
    v2 = dict(major=0, minor=10, patch=4, prerelease="beta.1", build=None)

    assert v1 != v2
    assert v1 > v2
    assert v1 >= v2
    assert not (v1 < v2)
    assert not (v1 <= v2)
    assert not (v1 == v2)

    v3 = dict(major=0, minor=10, patch=4)

    assert not (v1 != v3)
    assert not (v1 > v3)
    assert v1 >= v3
    assert not (v1 < v3)
    assert v1 <= v3
    assert v1 == v3

    v4 = dict(major=0, minor=10, patch=5)
    assert v1 != v4
    assert not (v1 > v4)
    assert not (v1 >= v4)
    assert v1 < v4
    assert v1 <= v4
    assert not (v1 == v4)


@pytest.mark.parametrize(
    "t",  # fmt: off
    (
        (1, 0, 0),
        (1, 0),
        (1,),
        (1, 0, 0, "pre.2"),
        (1, 0, 0, "pre.2", "build.4"),
    ),  # fmt: on
)
def test_should_compare_version_tuples(t):
    v0 = VersionInfo(major=0, minor=4, patch=5, prerelease="pre.2", build="build.4")
    v1 = VersionInfo(major=3, minor=4, patch=5, prerelease="pre.2", build="build.4")

    assert v0 < t
    assert v0 <= t
    assert v0 != t
    assert not v0 == t
    assert v1 > t
    assert v1 >= t
    # Symmetric
    assert t > v0
    assert t >= v0
    assert t < v1
    assert t <= v1
    assert t != v0
    assert not t == v0


@pytest.mark.parametrize(
    "lst",  # fmt: off
    (
        [1, 0, 0],
        [1, 0],
        [1],
        [1, 0, 0, "pre.2"],
        [1, 0, 0, "pre.2", "build.4"],
    ),  # fmt: on
)
def test_should_compare_version_list(lst):
    v0 = VersionInfo(major=0, minor=4, patch=5, prerelease="pre.2", build="build.4")
    v1 = VersionInfo(major=3, minor=4, patch=5, prerelease="pre.2", build="build.4")

    assert v0 < lst
    assert v0 <= lst
    assert v0 != lst
    assert not v0 == lst
    assert v1 > lst
    assert v1 >= lst
    # Symmetric
    assert lst > v0
    assert lst >= v0
    assert lst < v1
    assert lst <= v1
    assert lst != v0
    assert not lst == v0


@pytest.mark.parametrize(
    "s",  # fmt: off
    (
        "1.0.0",
        # "1.0",
        # "1",
        "1.0.0-pre.2",
        "1.0.0-pre.2+build.4",
    ),  # fmt: on
)
def test_should_compare_version_string(s):
    v0 = VersionInfo(major=0, minor=4, patch=5, prerelease="pre.2", build="build.4")
    v1 = VersionInfo(major=3, minor=4, patch=5, prerelease="pre.2", build="build.4")

    assert v0 < s
    assert v0 <= s
    assert v0 != s
    assert not v0 == s
    assert v1 > s
    assert v1 >= s
    # Symmetric
    assert s > v0
    assert s >= v0
    assert s < v1
    assert s <= v1
    assert s != v0
    assert not s == v0


@pytest.mark.parametrize("s", ("1", "1.0", "1.0.x"))
def test_should_not_allow_to_compare_invalid_versionstring(s):
    v = VersionInfo(major=3, minor=4, patch=5, prerelease="pre.2", build="build.4")
    with pytest.raises(ValueError):
        v < s
    with pytest.raises(ValueError):
        s > v


def test_should_not_allow_to_compare_version_with_int():
    v1 = VersionInfo(major=3, minor=4, patch=5, prerelease="pre.2", build="build.4")
    with pytest.raises(TypeError):
        v1 > 1
    with pytest.raises(TypeError):
        1 > v1
    with pytest.raises(TypeError):
        v1.compare(1)


def test_should_compare_prerelease_with_numbers_and_letters():
    v1 = VersionInfo(major=1, minor=9, patch=1, prerelease="1unms", build=None)
    v2 = VersionInfo(major=1, minor=9, patch=1, prerelease=None, build="1asd")
    assert v1 < v2
    assert compare("1.9.1-1unms", "1.9.1+1") == -1


def test_parse_version_info_str_hash():
    s_version = "1.2.3-alpha.1.2+build.11.e0f985a"
    v = parse_version_info(s_version)
    assert v.__str__() == s_version
    d = {}
    d[v] = ""  # to ensure that VersionInfo are hashable


def test_equal_versions_have_equal_hashes():
    v1 = parse_version_info("1.2.3-alpha.1.2+build.11.e0f985a")
    v2 = parse_version_info("1.2.3-alpha.1.2+build.22.a589f0e")
    assert v1 == v2
    assert hash(v1) == hash(v2)
    d = {}
    d[v1] = 1
    d[v2] = 2
    assert d[v1] == 2
    s = set()
    s.add(v1)
    assert v2 in s


def test_parse_method_for_version_info():
    s_version = "1.2.3-alpha.1.2+build.11.e0f985a"
    v = VersionInfo.parse(s_version)
    assert str(v) == s_version


def test_immutable_major(version):
    with pytest.raises(AttributeError, match="attribute 'major' is readonly"):
        version.major = 9


def test_immutable_minor(version):
    with pytest.raises(AttributeError, match="attribute 'minor' is readonly"):
        version.minor = 9


def test_immutable_patch(version):
    with pytest.raises(AttributeError, match="attribute 'patch' is readonly"):
        version.patch = 9


def test_immutable_prerelease(version):
    with pytest.raises(AttributeError, match="attribute 'prerelease' is readonly"):
        version.prerelease = "alpha.9.9"


def test_immutable_build(version):
    with pytest.raises(AttributeError, match="attribute 'build' is readonly"):
        version.build = "build.99.e0f985a"


def test_immutable_unknown_attribute(version):
    # "no new attribute can be set"
    with pytest.raises(AttributeError):
        version.new_attribute = "forbidden"


def test_version_info_should_be_iterable(version):
    assert tuple(version) == (
        version.major,
        version.minor,
        version.patch,
        version.prerelease,
        version.build,
    )


def test_should_compare_prerelease_and_build_with_numbers():
    assert VersionInfo(major=1, minor=9, patch=1, prerelease=1, build=1) < VersionInfo(
        major=1, minor=9, patch=1, prerelease=2, build=1
    )
    assert VersionInfo(1, 9, 1, 1, 1) < VersionInfo(1, 9, 1, 2, 1)
    assert VersionInfo("2") < VersionInfo(10)
    assert VersionInfo("2") < VersionInfo("10")


def test_should_be_able_to_use_strings_as_major_minor_patch():
    v = VersionInfo("1", "2", "3")
    assert isinstance(v.major, int)
    assert isinstance(v.minor, int)
    assert isinstance(v.patch, int)
    assert v.prerelease is None
    assert v.build is None
    assert VersionInfo("1", "2", "3") == VersionInfo(1, 2, 3)


def test_using_non_numeric_string_as_major_minor_patch_throws():
    with pytest.raises(ValueError):
        VersionInfo("a")
    with pytest.raises(ValueError):
        VersionInfo(1, "a")
    with pytest.raises(ValueError):
        VersionInfo(1, 2, "a")


def test_should_be_able_to_use_integers_as_prerelease_build():
    v = VersionInfo(1, 2, 3, 4, 5)
    assert isinstance(v.prerelease, str)
    assert isinstance(v.build, str)
    assert VersionInfo(1, 2, 3, 4, 5) == VersionInfo(1, 2, 3, "4", "5")


@pytest.mark.parametrize(
    "version, index, expected",
    [
        # Simple positive indices
        ("1.2.3-rc.0+build.0", 0, 1),
        ("1.2.3-rc.0+build.0", 1, 2),
        ("1.2.3-rc.0+build.0", 2, 3),
        ("1.2.3-rc.0+build.0", 3, "rc.0"),
        ("1.2.3-rc.0+build.0", 4, "build.0"),
        ("1.2.3-rc.0", 0, 1),
        ("1.2.3-rc.0", 1, 2),
        ("1.2.3-rc.0", 2, 3),
        ("1.2.3-rc.0", 3, "rc.0"),
        ("1.2.3", 0, 1),
        ("1.2.3", 1, 2),
        ("1.2.3", 2, 3),
        # Special cases
        ("1.0.2", 1, 0),
    ],
)
def test_version_info_should_be_accessed_with_index(version, index, expected):
    version_info = VersionInfo.parse(version)
    assert version_info[index] == expected


@pytest.mark.parametrize(
    "version, slice_object, expected",
    [
        # Slice indices
        ("1.2.3-rc.0+build.0", slice(0, 5), (1, 2, 3, "rc.0", "build.0")),
        ("1.2.3-rc.0+build.0", slice(0, 4), (1, 2, 3, "rc.0")),
        ("1.2.3-rc.0+build.0", slice(0, 3), (1, 2, 3)),
        ("1.2.3-rc.0+build.0", slice(0, 2), (1, 2)),
        ("1.2.3-rc.0+build.0", slice(3, 5), ("rc.0", "build.0")),
        ("1.2.3-rc.0", slice(0, 4), (1, 2, 3, "rc.0")),
        ("1.2.3-rc.0", slice(0, 3), (1, 2, 3)),
        ("1.2.3-rc.0", slice(0, 2), (1, 2)),
        ("1.2.3", slice(0, 10), (1, 2, 3)),
        ("1.2.3", slice(0, 3), (1, 2, 3)),
        ("1.2.3", slice(0, 2), (1, 2)),
        # Special cases
        ("1.2.3-rc.0+build.0", slice(3), (1, 2, 3)),
        ("1.2.3-rc.0+build.0", slice(0, 5, 2), (1, 3, "build.0")),
        ("1.2.3-rc.0+build.0", slice(None, 5, 2), (1, 3, "build.0")),
        ("1.2.3-rc.0+build.0", slice(5, 0, -2), ("build.0", 3)),
        ("1.2.0-rc.0+build.0", slice(3), (1, 2, 0)),
    ],
)
def test_version_info_should_be_accessed_with_slice_object(
    version, slice_object, expected
):
    version_info = VersionInfo.parse(version)
    assert version_info[slice_object] == expected


@pytest.mark.parametrize(
    "version, index",
    [
        ("1.2.3", 3),
        ("1.2.3", slice(3, 4)),
        ("1.2.3", 4),
        ("1.2.3", slice(4, 5)),
        ("1.2.3", 5),
        ("1.2.3", slice(5, 6)),
        ("1.2.3-rc.0", 5),
        ("1.2.3-rc.0", slice(5, 6)),
        ("1.2.3-rc.0", 6),
        ("1.2.3-rc.0", slice(6, 7)),
    ],
)
def test_version_info_should_throw_index_error(version, index):
    version_info = VersionInfo.parse(version)
    with pytest.raises(IndexError, match=r"Version part undefined"):
        version_info[index]


@pytest.mark.parametrize(
    "version, index",
    [
        ("1.2.3", -1),
        ("1.2.3", -2),
        ("1.2.3", slice(-2, 2)),
        ("1.2.3", slice(2, -2)),
        ("1.2.3", slice(-2, -2)),
    ],
)
def test_version_info_should_throw_index_error_when_negative_index(version, index):
    version_info = VersionInfo.parse(version)
    with pytest.raises(IndexError, match=r"Version index cannot be negative"):
        version_info[index]


@pytest.mark.parametrize(
    "cli,expected",
    [
        (["bump", "major", "1.2.3"], Namespace(bump="major", version="1.2.3")),
        (["bump", "minor", "1.2.3"], Namespace(bump="minor", version="1.2.3")),
        (["bump", "patch", "1.2.3"], Namespace(bump="patch", version="1.2.3")),
        (
            ["bump", "prerelease", "1.2.3"],
            Namespace(bump="prerelease", version="1.2.3"),
        ),
        (["bump", "build", "1.2.3"], Namespace(bump="build", version="1.2.3")),
        # ---
        (["compare", "1.2.3", "2.1.3"], Namespace(version1="1.2.3", version2="2.1.3")),
        # ---
        (["check", "1.2.3"], Namespace(version="1.2.3")),
    ],
)
def test_should_parse_cli_arguments(cli, expected):
    parser = createparser()
    assert parser
    result = parser.parse_args(cli)
    del result.func
    assert result == expected


@pytest.mark.parametrize(
    "func,args,expectation",
    [
        # bump subcommand
        (cmd_bump, Namespace(bump="major", version="1.2.3"), does_not_raise("2.0.0")),
        (cmd_bump, Namespace(bump="minor", version="1.2.3"), does_not_raise("1.3.0")),
        (cmd_bump, Namespace(bump="patch", version="1.2.3"), does_not_raise("1.2.4")),
        (
            cmd_bump,
            Namespace(bump="prerelease", version="1.2.3-rc1"),
            does_not_raise("1.2.3-rc2"),
        ),
        (
            cmd_bump,
            Namespace(bump="build", version="1.2.3+build.13"),
            does_not_raise("1.2.3+build.14"),
        ),
        # compare subcommand
        (
            cmd_compare,
            Namespace(version1="1.2.3", version2="2.1.3"),
            does_not_raise("-1"),
        ),
        (
            cmd_compare,
            Namespace(version1="1.2.3", version2="1.2.3"),
            does_not_raise("0"),
        ),
        (
            cmd_compare,
            Namespace(version1="2.4.0", version2="2.1.3"),
            does_not_raise("1"),
        ),
        # check subcommand
        (cmd_check, Namespace(version="1.2.3"), does_not_raise(None)),
        (cmd_check, Namespace(version="1.2"), pytest.raises(ValueError)),
        # nextver subcommand
        (
            cmd_nextver,
            Namespace(version="1.2.3", part="major"),
            does_not_raise("2.0.0"),
        ),
        (
            cmd_nextver,
            Namespace(version="1.2", part="major"),
            pytest.raises(ValueError),
        ),
        (
            cmd_nextver,
            Namespace(version="1.2.3", part="nope"),
            pytest.raises(ValueError),
        ),
    ],
)
def test_should_process_parsed_cli_arguments(func, args, expectation):
    with expectation as expected:
        result = func(args)
        assert result == expected


def test_should_process_print(capsys):
    rc = main(["bump", "major", "1.2.3"])
    assert rc == 0
    captured = capsys.readouterr()
    assert captured.out.rstrip() == "2.0.0"


def test_should_process_raise_error(capsys):
    rc = main(["bump", "major", "1.2"])
    assert rc != 0
    captured = capsys.readouterr()
    assert captured.err.startswith("ERROR")


def test_should_raise_systemexit_when_called_with_empty_arguments():
    with pytest.raises(SystemExit):
        main([])


def test_should_raise_systemexit_when_bump_iscalled_with_empty_arguments():
    with pytest.raises(SystemExit):
        main(["bump"])


def test_should_process_check_iscalled_with_valid_version(capsys):
    result = main(["check", "1.1.1"])
    assert not result
    captured = capsys.readouterr()
    assert not captured.out


@pytest.mark.parametrize(
    "version,parts,expected",
    [
        ("3.4.5", dict(major=2), "2.4.5"),
        ("3.4.5", dict(major="2"), "2.4.5"),
        ("3.4.5", dict(major=2, minor=5), "2.5.5"),
        ("3.4.5", dict(minor=2), "3.2.5"),
        ("3.4.5", dict(major=2, minor=5, patch=10), "2.5.10"),
        ("3.4.5", dict(major=2, minor=5, patch=10, prerelease="rc1"), "2.5.10-rc1"),
        (
            "3.4.5",
            dict(major=2, minor=5, patch=10, prerelease="rc1", build="b1"),
            "2.5.10-rc1+b1",
        ),
        ("3.4.5-alpha.1.2", dict(major=2), "2.4.5-alpha.1.2"),
        ("3.4.5-alpha.1.2", dict(build="x1"), "3.4.5-alpha.1.2+x1"),
        ("3.4.5+build1", dict(major=2), "2.4.5+build1"),
    ],
)
def test_replace_method_replaces_requested_parts(version, parts, expected):
    assert replace(version, **parts) == expected


def test_replace_raises_TypeError_for_invalid_keyword_arg():
    with pytest.raises(TypeError, match=r"replace\(\).*unknown.*"):
        assert replace("1.2.3", unknown="should_raise")


@pytest.mark.parametrize(
    "version,parts,expected",
    [
        ("3.4.5", dict(major=2, minor=5), "2.5.5"),
        ("3.4.5", dict(major=2, minor=5, patch=10), "2.5.10"),
        ("3.4.5-alpha.1.2", dict(major=2), "2.4.5-alpha.1.2"),
        ("3.4.5-alpha.1.2", dict(build="x1"), "3.4.5-alpha.1.2+x1"),
        ("3.4.5+build1", dict(major=2), "2.4.5+build1"),
    ],
)
def test_should_return_versioninfo_with_replaced_parts(version, parts, expected):
    assert VersionInfo.parse(version).replace(**parts) == VersionInfo.parse(expected)


def test_replace_raises_ValueError_for_non_numeric_values():
    with pytest.raises(ValueError):
        VersionInfo.parse("1.2.3").replace(major="x")


def test_should_versioninfo_isvalid():
    assert VersionInfo.isvalid("1.0.0") is True
    assert VersionInfo.isvalid("foo") is False


@pytest.mark.parametrize(
    "func, args, kwargs",
    [
        (bump_build, ("1.2.3",), {}),
        (bump_major, ("1.2.3",), {}),
        (bump_minor, ("1.2.3",), {}),
        (bump_patch, ("1.2.3",), {}),
        (bump_prerelease, ("1.2.3",), {}),
        (compare, ("1.2.1", "1.2.2"), {}),
        (format_version, (3, 4, 5), {}),
        (finalize_version, ("1.2.3-rc.5",), {}),
        (match, ("1.0.0", ">=1.0.0"), {}),
        (parse, ("1.2.3",), {}),
        (parse_version_info, ("1.2.3",), {}),
        (replace, ("1.2.3",), dict(major=2, patch=10)),
        (max_ver, ("1.2.3", "1.2.4"), {}),
        (min_ver, ("1.2.3", "1.2.4"), {}),
    ],
)
def test_should_raise_deprecation_warnings(func, args, kwargs):
    with pytest.warns(
        DeprecationWarning, match=r"Function 'semver.[_a-zA-Z]+' is deprecated."
    ) as record:
        func(*args, **kwargs)
        if not record:
            pytest.fail("Expected a DeprecationWarning for {}".format(func.__name__))
    assert len(record), "Expected one DeprecationWarning record"


def test_deprecated_deco_without_argument():
    @deprecated
    def mock_func():
        return True

    with pytest.deprecated_call():
        assert mock_func()


def test_next_version_with_invalid_parts():
    version = VersionInfo.parse("1.0.1")
    with pytest.raises(ValueError):
        version.next_version("invalid")


@pytest.mark.parametrize(
    "version, part, expected",
    [
        # major
        ("1.0.4-rc.1", "major", "2.0.0"),
        ("1.1.0-rc.1", "major", "2.0.0"),
        ("1.1.4-rc.1", "major", "2.0.0"),
        ("1.2.3", "major", "2.0.0"),
        ("1.0.0-rc.1", "major", "1.0.0"),
        # minor
        ("0.2.0-rc.1", "minor", "0.2.0"),
        ("0.2.5-rc.1", "minor", "0.3.0"),
        ("1.3.1", "minor", "1.4.0"),
        # patch
        ("1.3.2", "patch", "1.3.3"),
        ("0.1.5-rc.2", "patch", "0.1.5"),
        # prerelease
        ("0.1.4", "prerelease", "0.1.5-rc.1"),
        ("0.1.5-rc.1", "prerelease", "0.1.5-rc.2"),
        # special cases
        ("0.2.0-rc.1", "patch", "0.2.0"),  # same as "minor"
        ("1.0.0-rc.1", "patch", "1.0.0"),  # same as "major"
        ("1.0.0-rc.1", "minor", "1.0.0"),  # same as "major"
    ],
)
def test_next_version_with_versioninfo(version, part, expected):
    ver = VersionInfo.parse(version)
    next_version = ver.next_version(part)
    assert isinstance(next_version, VersionInfo)
    assert str(next_version) == expected


@pytest.mark.parametrize(
    "version, expected",
    [
        (
            VersionInfo(major=1, minor=2, patch=3, prerelease=None, build=None),
            "VersionInfo(major=1, minor=2, patch=3, prerelease=None, build=None)",
        ),
        (
            VersionInfo(major=1, minor=2, patch=3, prerelease="r.1", build=None),
            "VersionInfo(major=1, minor=2, patch=3, prerelease='r.1', build=None)",
        ),
        (
            VersionInfo(major=1, minor=2, patch=3, prerelease="dev.1", build=None),
            "VersionInfo(major=1, minor=2, patch=3, prerelease='dev.1', build=None)",
        ),
        (
            VersionInfo(major=1, minor=2, patch=3, prerelease="dev.1", build="b.1"),
            "VersionInfo(major=1, minor=2, patch=3, prerelease='dev.1', build='b.1')",
        ),
        (
            VersionInfo(major=1, minor=2, patch=3, prerelease="r.1", build="b.1"),
            "VersionInfo(major=1, minor=2, patch=3, prerelease='r.1', build='b.1')",
        ),
        (
            VersionInfo(major=1, minor=2, patch=3, prerelease="r.1", build="build.1"),
            "VersionInfo(major=1, minor=2, patch=3, prerelease='r.1', build='build.1')",
        ),
    ],
)
def test_repr(version, expected):
    assert repr(version) == expected


def test_subclass_from_versioninfo():
    class SemVerWithVPrefix(VersionInfo):
        @classmethod
        def parse(cls, version):
            if not version[0] in ("v", "V"):
                raise ValueError(
                    "{v!r}: version must start with 'v' or 'V'".format(v=version)
                )
            return super(SemVerWithVPrefix, cls).parse(version[1:])

        def __str__(self):
            # Reconstruct the tag.
            return "v" + super(SemVerWithVPrefix, self).__str__()

    v = SemVerWithVPrefix.parse("v1.2.3")
    assert str(v) == "v1.2.3"
