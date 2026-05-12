from textwrap import dedent

import pytest
from pytest import MonkeyPatch, Pytester

from typeguard import CollectionCheckStrategy, ForwardRefPolicy, TypeCheckConfiguration


@pytest.fixture
def config(monkeypatch: MonkeyPatch) -> TypeCheckConfiguration:
    config = TypeCheckConfiguration()
    monkeypatch.setattr("typeguard._pytest_plugin.global_config", config)
    return config


def test_config_options(pytester: Pytester, config: TypeCheckConfiguration) -> None:
    pytester.makepyprojecttoml(
        '''
        [tool.pytest.ini_options]
        typeguard-packages = """
        mypackage
        otherpackage"""
        typeguard-debug-instrumentation = true
        typeguard-typecheck-fail-callback = "mypackage:failcallback"
        typeguard-forward-ref-policy = "ERROR"
        typeguard-collection-check-strategy = "ALL_ITEMS"
        '''
    )
    pytester.makepyfile(
        mypackage=(
            dedent(
                """
            def failcallback():
                pass
            """
            )
        )
    )

    pytester.plugins = ["typeguard"]
    pytester.syspathinsert()
    pytestconfig = pytester.parseconfigure()
    assert pytestconfig.getini("typeguard-packages") == ["mypackage", "otherpackage"]
    assert config.typecheck_fail_callback.__name__ == "failcallback"
    assert config.debug_instrumentation is True
    assert config.forward_ref_policy is ForwardRefPolicy.ERROR
    assert config.collection_check_strategy is CollectionCheckStrategy.ALL_ITEMS


def test_commandline_options(
    pytester: Pytester, config: TypeCheckConfiguration
) -> None:
    pytester.makepyfile(
        mypackage=(
            dedent(
                """
            def failcallback():
                pass
            """
            )
        )
    )

    pytester.plugins = ["typeguard"]
    pytester.syspathinsert()
    pytestconfig = pytester.parseconfigure(
        "--typeguard-packages=mypackage,otherpackage",
        "--typeguard-typecheck-fail-callback=mypackage:failcallback",
        "--typeguard-debug-instrumentation",
        "--typeguard-forward-ref-policy=ERROR",
        "--typeguard-collection-check-strategy=ALL_ITEMS",
    )
    assert pytestconfig.getoption("typeguard_packages") == "mypackage,otherpackage"
    assert config.typecheck_fail_callback.__name__ == "failcallback"
    assert config.debug_instrumentation is True
    assert config.forward_ref_policy is ForwardRefPolicy.ERROR
    assert config.collection_check_strategy is CollectionCheckStrategy.ALL_ITEMS
