from functools import cmp_to_key

from build.plugins.lib.nots.semver import Version


def test_from_str():
    # arrange
    version_str = "1.2.3"

    # act
    version = Version.from_str(version_str)

    # assert
    assert version.major == 1
    assert version.minor == 2
    assert version.patch == 3


def test_from_str_bad_version():
    # arrange
    version_str = "best version imaginable"
    error = None

    # act
    try:
        Version.from_str(version_str)
    except Exception as exception:
        error = exception

    # assert
    assert error is not None


def test_is_stable_true():
    # arrange
    version_str = "1.2.3"

    # act + assert
    assert Version.is_stable(version_str)


def test_is_stable_false():
    # arrange
    version_str = "1.2.3-beta1"

    # act + assert
    assert not Version.is_stable(version_str)


def test_is_stable_incorrect():
    # arrange
    version_str = "v1.2.3"

    # act + assert
    assert not Version.is_stable(version_str)


def test_cmp_lt():
    # arrange
    a = Version.from_str("1.2.3")
    b = Version.from_str("1.2.5")

    # act + assert
    assert Version.cmp(a, b) == -1


def test_cmp_gt():
    # arrange
    a = Version.from_str("1.2.3")
    b = Version.from_str("1.2.2")

    # act + assert
    assert Version.cmp(a, b) == 1


def test_cmp_eq():
    # arrange
    a = Version.from_str("1.2.3")
    b = Version.from_str("1.2.3")

    # act + assert
    assert Version.cmp(a, b) == 0


def test_cmp_lt_str():
    # arrange
    a = "1.2.3"
    b = "1.2.5"

    # act + assert
    assert Version.cmp(a, b) == -1


def test_cmp_gt_str():
    # arrange
    a = "1.2.3"
    b = "1.2.2"

    # act + assert
    assert Version.cmp(a, b) == 1


def test_cmp_eq_str():
    # arrange
    a = "1.2.3"
    b = "1.2.3"

    # act + assert
    assert Version.cmp(a, b) == 0


def test_cmp_usage_in_sorted_asc():
    # arrange
    unsorted = ["1.2.3", "2.4.2", "1.2.7"]

    # act + assert
    assert sorted(unsorted, key=cmp_to_key(Version.cmp)) == ["1.2.3", "1.2.7", "2.4.2"]


def test_cmp_usage_in_sorted_desc():
    # arrange
    unsorted = ["1.2.3", "2.4.2", "1.2.7"]

    # act + assert
    assert sorted(unsorted, key=cmp_to_key(Version.cmp), reverse=True) == ["2.4.2", "1.2.7", "1.2.3"]


def test_init_negative_numbers():
    # arrange
    major = 1
    minor = -2
    patch = 3

    error = None

    # act
    try:
        Version(major, minor, patch)
    except Exception as exception:
        error = exception

    # assert
    assert isinstance(error, ValueError)
    assert str(error) == "'minor' is negative. A version can only be positive."


def test_eq():
    # arrange
    version_a = Version.from_str("1.2.3")
    version_b = Version.from_str("1.2.3")

    # act + assert
    assert version_a == version_b


def test_eq_negative():
    # arrange
    version_a = Version.from_str("1.2.3")
    version_b = Version.from_str("3.2.1")

    # act + assert
    assert not version_a == version_b


def test_eq_with_str():
    # arrange
    version = Version.from_str("1.2.3")

    # act + assert
    assert version == "1.2.3"
    assert not version == "1.2.4"


def test_eq_with_invalid_str():
    # arrange
    version = Version.from_str("1.2.3")

    # act + assert
    assert not version == "bla-bla"
    assert not version == "1.2.3-beta"


def test_ne():
    # arrange
    version_a = Version.from_str("3.2.1")
    version_b = Version.from_str("1.2.3")

    # act + assert
    assert version_a != version_b


def test_ne_negative():
    # arrange
    version_a = Version.from_str("1.2.3")
    version_b = Version.from_str("1.2.3")

    # act + assert
    assert not version_a != version_b


def test_ne_with_str():
    # arrange
    version = Version.from_str("1.2.3")

    # act + assert
    assert version != "1.2.4"
    assert not version != "1.2.3"


def test_gt():
    # arrange
    version_a = Version.from_str("3.2.1")
    version_b = Version.from_str("1.2.3")

    # act + assert
    assert version_a > version_b


def test_ge_equals():
    # arrange
    version_a = Version.from_str("1.2.3")
    version_b = Version.from_str("1.2.3")

    # act + assert
    assert version_a >= version_b


def test_ge_exceeds():
    # arrange
    version_a = Version.from_str("3.2.1")
    version_b = Version.from_str("1.2.3")

    # act + assert
    assert version_a >= version_b


def test_lt():
    # arrange
    version_a = Version.from_str("1.2.3")
    version_b = Version.from_str("3.2.1")

    # act + assert
    assert version_a < version_b


def test_le_equals():
    # arrange
    version_a = Version.from_str("1.2.3")
    version_b = Version.from_str("1.2.3")

    # act + assert
    assert version_a <= version_b


def test_le_is_less():
    # arrange
    version_a = Version.from_str("1.2.3")
    version_b = Version.from_str("3.2.1")

    # act + assert
    assert version_a <= version_b


def test_to_tuple():
    # arrange
    version = Version.from_str("1.2.3")

    # act + assert
    assert version.as_tuple() == (1, 2, 3)
