from types import SimpleNamespace

import nots
import pytest

from nots import _library_for_paths, _parse_ts_checks, _ts_check_size


def test_parse_ts_checks_preserves_command_with_spaces_and_multiple_checks():
    raw = (
        "$_TS_CHECK_LIST~~~nots:lint lint no "
        "~~~nots:custom lint yes eslint src --max-warnings 0 "
        "~~~nots:test test no"
    )

    assert _parse_ts_checks(raw, "~~~") == [
        ["nots:lint", "lint", "no", ""],
        ["nots:custom", "lint", "yes", "eslint src --max-warnings 0"],
        ["nots:test", "test", "no", ""],
    ]


@pytest.mark.parametrize(
    ("check_type", "timeout_medium", "expected_size"),
    [
        ("lint", "no", "SMALL"),
        ("lint", "yes", "MEDIUM"),
        ("test", "no", None),
        ("test", "yes", "MEDIUM"),
    ],
)
def test_ts_check_size(check_type, timeout_medium, expected_size):
    assert _ts_check_size(check_type, timeout_medium) == expected_size


@pytest.mark.parametrize(
    ("target", "source", "build"),
    [
        ("apps/app/storybook", "apps/app", "apps/app__storybook.5cbbd79a82b1b2f1230b09c5ffc3399c"),
        ("apps/app/./storybook", "apps/app", "apps/app__storybook.5cbbd79a82b1b2f1230b09c5ffc3399c"),
        (
            "apps/app/examples/storybook",
            "apps/app",
            "apps/app__examples__storybook.deec0c58db6b8098c3cca86d0222c41f",
        ),
        (
            "apps/app/examples__storybook",
            "apps/app",
            "apps/app__examples__storybook.9be05a7729604fbea954bfac1d86f3d3",
        ),
    ],
)
def test_library_for_paths(target, source, build):
    assert _library_for_paths(target, source) == (source, build)


@pytest.mark.parametrize("source", ["apps/app/storybook", "apps/other", "../app", "/apps/app", "."])
def test_library_for_rejects_non_parent_sources(source):
    with pytest.raises(ValueError, match="parent project"):
        _library_for_paths("apps/app/storybook", source)


@pytest.mark.parametrize(
    "settings",
    [
        {"_WITH_NODE_MODULES": "yes"},
        {"_WITH_NODE_MODULES": "yes", "_WITH_NODE_MODULES_PROD": "yes"},
        {"NODE_MODULES_FOR": "apps/other:apps/app/storybook"},
    ],
)
def test_library_for_rejects_node_modules_bundle(monkeypatch, settings):
    errors = []
    values = {"_TS_LIBRARY_FOR": "yes", "MODDIR": "apps/app/storybook", **settings}
    unit = SimpleNamespace(get=values.get)
    monkeypatch.setattr(nots.ymake, "report_configure_error", errors.append, raising=False)

    nots._TS_LIBRARY_CONFIGURE(unit)

    assert errors == ["TS_LIBRARY_FOR does not support WITH_NODE_MODULES or NODE_MODULES_FOR"]
