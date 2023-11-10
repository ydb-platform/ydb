import argparse
import pytest


def pytest_addoption(parser):
    pytest_addoption.__annotations__ = {
        'parser': argparse.ArgumentParser,
        'return': None
    }
    parser.addoption("--yq-version",
                     action="append",
                     default=[],
                     help=("YQ version to run tests against. "
                           "Multiple values might be specified. "
                           "Available values: 'v1', 'v2' and 'all'"))


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "yq_version(v1, v2, ...): mark test to run only on named environment"
    )


def _get_configured_yq_versions_from_config(config):
    _get_configured_yq_versions_from_config.__annotations__ = {
        'config': pytest.Config,
        'return': set[str]
    }
    versions = set(config.option.yq_version)
    if not versions or 'all' in versions:
        versions = {'v1', 'v2'}
    return versions


def _get_available_yq_versions_for_test(config, yq_version_mark):
    _get_available_yq_versions_for_test.__annotations__ = {
        'config': pytest.Config,
        'yq_version_mark': pytest.Mark,
        'return': set[str]
    }
    cli_configured_versions = _get_configured_yq_versions_from_config(config)
    supported_versions_by_test = set(yq_version_mark.args)
    return cli_configured_versions & supported_versions_by_test


def pytest_generate_tests(metafunc):
    pytest_generate_tests.__annotations__ = {
        'metafunc': pytest.Metafunc,
        'return': None
    }
    if 'yq_version' in metafunc.fixturenames:
        marker = metafunc.definition.get_closest_marker('yq_version')
        if marker is not None:
            available_yq_versions = _get_available_yq_versions_for_test(
                metafunc.config, marker)
            if available_yq_versions:
                metafunc.parametrize(
                    argnames='yq_version',
                    argvalues=available_yq_versions)
        else:
            pytest.fail("yq_version marker was not specified: " + str(metafunc.function))
    if 'stats_mode' in metafunc.fixturenames:
        marker = metafunc.definition.get_closest_marker('stats_mode')
        if marker is not None:
            metafunc.parametrize(
                argnames='stats_mode',
                argvalues=[marker.args[0]])


def pytest_collection_modifyitems(config, items):
    pytest_collection_modifyitems.__annotations__ = {
        'config': pytest.Config,
        'items': list[pytest.Item],
        'return': None
    }
    removed = []
    kept = []
    for item in items:
        marker = item.get_closest_marker('yq_version')
        if 'yq_version' in item.fixturenames and marker is not None:
            available_yq_versions = _get_available_yq_versions_for_test(
                config, marker)
            if not available_yq_versions:
                removed.append(item)
                continue
        kept.append(item)
    if removed:
        config.hook.pytest_deselected(items=removed)
        items[:] = kept
