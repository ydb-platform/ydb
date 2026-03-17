"""Pytest plugin entry point. Used for any fixtures needed."""

import pytest

from . import cucumber_json
from . import generation
from . import gherkin_terminal_reporter
from . import given, when, then
from . import reporting
from .utils import CONFIG_STACK


def pytest_addhooks(pluginmanager):
    """Register plugin hooks."""
    from pytest_bdd import hooks

    pluginmanager.add_hookspecs(hooks)


@given("trace")
@when("trace")
@then("trace")
def trace():
    """Enter pytest's pdb trace."""
    pytest.set_trace()


def pytest_addoption(parser):
    """Add pytest-bdd options."""
    add_bdd_ini(parser)
    cucumber_json.add_options(parser)
    generation.add_options(parser)
    gherkin_terminal_reporter.add_options(parser)


def add_bdd_ini(parser):
    parser.addini("bdd_features_base_dir", "Base features directory.")


@pytest.mark.trylast
def pytest_configure(config):
    """Configure all subplugins."""
    CONFIG_STACK.append(config)
    cucumber_json.configure(config)
    gherkin_terminal_reporter.configure(config)


def pytest_unconfigure(config):
    """Unconfigure all subplugins."""
    CONFIG_STACK.pop()
    cucumber_json.unconfigure(config)


@pytest.mark.hookwrapper
def pytest_runtest_makereport(item, call):
    outcome = yield
    reporting.runtest_makereport(item, call, outcome.get_result())


@pytest.mark.tryfirst
def pytest_bdd_before_scenario(request, feature, scenario):
    reporting.before_scenario(request, feature, scenario)


@pytest.mark.tryfirst
def pytest_bdd_step_error(request, feature, scenario, step, step_func, step_func_args, exception):
    reporting.step_error(request, feature, scenario, step, step_func, step_func_args, exception)


@pytest.mark.tryfirst
def pytest_bdd_before_step(request, feature, scenario, step, step_func):
    reporting.before_step(request, feature, scenario, step, step_func)


@pytest.mark.tryfirst
def pytest_bdd_after_step(request, feature, scenario, step, step_func, step_func_args):
    reporting.after_step(request, feature, scenario, step, step_func, step_func_args)


def pytest_cmdline_main(config):
    return generation.cmdline_main(config)


def pytest_bdd_apply_tag(tag, function):
    mark = getattr(pytest.mark, tag)
    return mark(function)
