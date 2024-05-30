import argparse

import allure
import allure_commons
import os

from allure_commons.types import LabelType, Severity
from allure_commons.logger import AllureFileLogger
from allure_commons.utils import get_testplan

from allure_pytest.utils import allure_label, allure_labels, allure_full_name
from allure_pytest.helper import AllureTestHelper, AllureTitleHelper
from allure_pytest.listener import AllureListener

from allure_pytest.utils import ALLURE_DESCRIPTION_MARK, ALLURE_DESCRIPTION_HTML_MARK
from allure_pytest.utils import ALLURE_LABEL_MARK, ALLURE_LINK_MARK


def pytest_addoption(parser):
    parser.getgroup("reporting").addoption('--alluredir',
                                           action="store",
                                           dest="allure_report_dir",
                                           metavar="DIR",
                                           default=None,
                                           help="Generate Allure report in the specified directory (may not exist)")

    parser.getgroup("reporting").addoption('--clean-alluredir',
                                           action="store_true",
                                           dest="clean_alluredir",
                                           help="Clean alluredir folder if it exists")

    parser.getgroup("reporting").addoption('--allure-no-capture',
                                           action="store_false",
                                           dest="attach_capture",
                                           help="Do not attach pytest captured logging/stdout/stderr to report")

    parser.getgroup("reporting").addoption('--inversion',
                                           action="store",
                                           dest="inversion",
                                           default=False,
                                           help="Run tests not in testplan")

    def label_type(type_name, legal_values=set()):
        def a_label_type(string):
            atoms = set(string.split(','))
            if type_name is LabelType.SEVERITY:
                if not atoms <= legal_values:
                    raise argparse.ArgumentTypeError('Illegal {} values: {}, only [{}] are allowed'.format(
                        type_name,
                        ', '.join(atoms - legal_values),
                        ', '.join(legal_values)
                    ))
                return set((type_name, allure.severity_level(atom)) for atom in atoms)
            return set((type_name, atom) for atom in atoms)
        return a_label_type

    severities = [x.value for x in list(allure.severity_level)]
    formatted_severities = ', '.join(severities)
    parser.getgroup("general").addoption('--allure-severities',
                                         action="store",
                                         dest="allure_severities",
                                         metavar="SEVERITIES_SET",
                                         default={},
                                         type=label_type(LabelType.SEVERITY, legal_values=set(severities)),
                                         help=f"""Comma-separated list of severity names.
                                         Tests only with these severities will be run.
                                         Possible values are: {formatted_severities}.""")

    parser.getgroup("general").addoption('--allure-epics',
                                         action="store",
                                         dest="allure_epics",
                                         metavar="EPICS_SET",
                                         default={},
                                         type=label_type(LabelType.EPIC),
                                         help="""Comma-separated list of epic names.
                                         Run tests that have at least one of the specified feature labels.""")

    parser.getgroup("general").addoption('--allure-features',
                                         action="store",
                                         dest="allure_features",
                                         metavar="FEATURES_SET",
                                         default={},
                                         type=label_type(LabelType.FEATURE),
                                         help="""Comma-separated list of feature names.
                                         Run tests that have at least one of the specified feature labels.""")

    parser.getgroup("general").addoption('--allure-stories',
                                         action="store",
                                         dest="allure_stories",
                                         metavar="STORIES_SET",
                                         default={},
                                         type=label_type(LabelType.STORY),
                                         help="""Comma-separated list of story names.
                                         Run tests that have at least one of the specified story labels.""")

    parser.getgroup("general").addoption('--allure-ids',
                                         action="store",
                                         dest="allure_ids",
                                         metavar="IDS_SET",
                                         default={},
                                         type=label_type(LabelType.ID),
                                         help="""Comma-separated list of IDs.
                                         Run tests that have at least one of the specified id labels.""")

    def cf_type(string):
        type_name, values = string.split("=", 1)
        atoms = set(values.split(","))
        return [(type_name, atom) for atom in atoms]

    parser.getgroup("general").addoption('--allure-label',
                                         action="append",
                                         dest="allure_labels",
                                         metavar="LABELS_SET",
                                         default=[],
                                         type=cf_type,
                                         help="""List of labels to run in format label_name=value1,value2.
                                         "Run tests that have at least one of the specified labels.""")

    def link_pattern(string):
        pattern = string.split(':', 1)
        if not pattern[0]:
            raise argparse.ArgumentTypeError('Link type is mandatory.')

        if len(pattern) != 2:
            raise argparse.ArgumentTypeError('Link pattern is mandatory')
        return pattern

    parser.getgroup("general").addoption('--allure-link-pattern',
                                         action="append",
                                         dest="allure_link_pattern",
                                         metavar="LINK_TYPE:LINK_PATTERN",
                                         default=[],
                                         type=link_pattern,
                                         help="""Url pattern for link type. Allows short links in test,
                                         like 'issue-1'. Text will be formatted to full url with python
                                         str.format().""")


def cleanup_factory(plugin):
    def clean_up():
        name = allure_commons.plugin_manager.get_name(plugin)
        allure_commons.plugin_manager.unregister(name=name)
    return clean_up


def pytest_addhooks(pluginmanager):
    # Need register title hooks before conftest init
    title_helper = AllureTitleHelper()
    allure_commons.plugin_manager.register(title_helper)


def pytest_configure(config):
    report_dir = config.option.allure_report_dir
    clean = False if config.option.collectonly else config.option.clean_alluredir

    test_helper = AllureTestHelper(config)
    allure_commons.plugin_manager.register(test_helper)
    config.add_cleanup(cleanup_factory(test_helper))

    if report_dir:
        report_dir = os.path.abspath(report_dir)
        test_listener = AllureListener(config)
        config.pluginmanager.register(test_listener, 'allure_listener')
        allure_commons.plugin_manager.register(test_listener)
        config.add_cleanup(cleanup_factory(test_listener))

        file_logger = AllureFileLogger(report_dir, clean)
        allure_commons.plugin_manager.register(file_logger)
        config.add_cleanup(cleanup_factory(file_logger))

    config.addinivalue_line("markers", f"{ALLURE_LABEL_MARK}: allure label marker")
    config.addinivalue_line("markers", f"{ALLURE_LINK_MARK}: allure link marker")
    config.addinivalue_line("markers", f"{ALLURE_DESCRIPTION_MARK}: allure description")
    config.addinivalue_line("markers", f"{ALLURE_DESCRIPTION_HTML_MARK}: allure description html")


def select_by_labels(items, config):
    arg_labels = set().union(
        config.option.allure_epics,
        config.option.allure_features,
        config.option.allure_stories,
        config.option.allure_ids,
        config.option.allure_severities,
        *config.option.allure_labels
    )
    if arg_labels:
        selected, deselected = [], []
        for item in items:
            test_labels = set(allure_labels(item))
            test_severity = allure_label(item, LabelType.SEVERITY)
            if not test_severity:
                test_labels.add((LabelType.SEVERITY, Severity.NORMAL))
            if arg_labels & test_labels:
                selected.append(item)
            else:
                deselected.append(item)
        return selected, deselected
    else:
        return items, []


def select_by_testcase(items, config):
    planned_tests = get_testplan()
    is_inversion = config.option.inversion

    if planned_tests:

        def is_planed(item):
            allure_ids = allure_label(item, LabelType.ID)
            allure_string_ids = list(map(str, allure_ids))
            for planed_item in planned_tests:
                planed_item_string_id = str(planed_item.get("id"))
                planed_item_selector = planed_item.get("selector")
                if (
                    planed_item_string_id in allure_string_ids
                    or planed_item_selector == allure_full_name(item)
                ):
                    return True if not is_inversion else False
            return False if not is_inversion else True

        selected, deselected = [], []
        for item in items:
            selected.append(item) if is_planed(item) else deselected.append(item)
        return selected, deselected
    else:
        return items, []


def pytest_collection_modifyitems(items, config):
    selected, deselected_by_testcase = select_by_testcase(items, config)
    selected, deselected_by_labels = select_by_labels(selected, config)

    items[:] = selected

    if deselected_by_testcase or deselected_by_labels:
        config.hook.pytest_deselected(items=[*deselected_by_testcase, *deselected_by_labels])
