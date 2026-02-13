"""Unicode version level tests for wcwidth."""
# local
import wcwidth


def test_list_versions_single():
    """list_versions returns only the latest version."""
    versions = wcwidth.list_versions()
    assert len(versions) == 1
    assert versions[0] == "17.0.0"


def test_latest():
    """wcwidth._wcmatch_version('latest') returns the latest version."""
    expected = wcwidth.list_versions()[-1]
    result = wcwidth._wcmatch_version('latest')
    assert result == expected


def test_auto():
    """wcwidth._wcmatch_version('auto') returns the latest version."""
    expected = wcwidth.list_versions()[-1]
    result = wcwidth._wcmatch_version('auto')
    assert result == expected


def test_wcmatch_version_always_latest():
    """_wcmatch_version always returns latest regardless of input."""
    latest = wcwidth.list_versions()[-1]
    assert wcwidth._wcmatch_version('4.1.0') == latest
    assert wcwidth._wcmatch_version('9.0.0') == latest
    assert wcwidth._wcmatch_version('auto') == latest
    assert wcwidth._wcmatch_version('latest') == latest
    assert wcwidth._wcmatch_version('invalid') == latest
    assert wcwidth._wcmatch_version('999.0.0') == latest
    assert wcwidth._wcmatch_version('x.y.z') == latest
    assert wcwidth._wcmatch_version('8') == latest
    assert wcwidth._wcmatch_version('5.0.5') == latest


def test_env_var_ignored(monkeypatch):
    """UNICODE_VERSION environment variable is ignored."""
    monkeypatch.setenv('UNICODE_VERSION', '4.1.0')
    wcwidth._wcmatch_version.cache_clear()
    assert wcwidth._wcmatch_version('auto') == wcwidth.list_versions()[-1]


def test_unicode_version_param_ignored():
    """unicode_version parameter is ignored in wcwidth/wcswidth."""
    # wcwidth should work the same regardless of unicode_version value
    assert wcwidth.wcwidth('A', unicode_version='4.1.0') == 1
    assert wcwidth.wcwidth('A', unicode_version='latest') == 1
    assert wcwidth.wcwidth('A', unicode_version='invalid') == 1

    # wcswidth should work the same regardless of unicode_version value
    assert wcwidth.wcswidth('hello', unicode_version='4.1.0') == 5
    assert wcwidth.wcswidth('hello', unicode_version='latest') == 5
    assert wcwidth.wcswidth('hello', unicode_version='invalid') == 5
