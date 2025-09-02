import pytest
import doctest

import allure_commons
from allure_commons.utils import now
from allure_commons.utils import uuid4
from allure_commons.utils import represent
from allure_commons.utils import platform_label
from allure_commons.utils import host_tag, thread_tag
from allure_commons.utils import md5
from allure_commons.reporter import AllureReporter
from allure_commons.model2 import TestStepResult, TestResult, TestBeforeResult, TestAfterResult
from allure_commons.model2 import TestResultContainer
from allure_commons.model2 import StatusDetails
from allure_commons.model2 import Parameter
from allure_commons.model2 import Label, Link
from allure_commons.model2 import Status
from allure_commons.types import LabelType, AttachmentType, ParameterMode
from allure_pytest.utils import allure_description, allure_description_html
from allure_pytest.utils import allure_labels, allure_links, pytest_markers
from allure_pytest.utils import allure_full_name, allure_package, allure_name
from allure_pytest.utils import allure_title_path
from allure_pytest.utils import allure_suite_labels
from allure_pytest.utils import get_status, get_status_details
from allure_pytest.utils import get_outcome_status, get_outcome_status_details
from allure_pytest.utils import get_pytest_report_status
from allure_pytest.utils import format_allure_link
from allure_pytest.utils import get_history_id
from allure_pytest.compat import getfixturedefs


class AllureListener:

    SUITE_LABELS = {
        LabelType.PARENT_SUITE,
        LabelType.SUITE,
        LabelType.SUB_SUITE,
    }

    def __init__(self, config):
        self.config = config
        self.allure_logger = AllureReporter()
        self._cache = ItemCache()
        self._host = host_tag()
        self._thread = thread_tag()

    @allure_commons.hookimpl
    def start_step(self, uuid, title, params):
        parameters = [Parameter(name=name, value=value) for name, value in params.items()]
        step = TestStepResult(name=title, start=now(), parameters=parameters)
        self.allure_logger.start_step(None, uuid, step)

    @allure_commons.hookimpl
    def stop_step(self, uuid, exc_type, exc_val, exc_tb):
        self.allure_logger.stop_step(uuid,
                                     stop=now(),
                                     status=get_status(exc_val),
                                     statusDetails=get_status_details(exc_type, exc_val, exc_tb))

    @allure_commons.hookimpl
    def start_fixture(self, parent_uuid, uuid, name):
        after_fixture = TestAfterResult(name=name, start=now())
        self.allure_logger.start_after_fixture(parent_uuid, uuid, after_fixture)

    @allure_commons.hookimpl
    def stop_fixture(self, parent_uuid, uuid, name, exc_type, exc_val, exc_tb):
        self.allure_logger.stop_after_fixture(uuid,
                                              stop=now(),
                                              status=get_status(exc_val),
                                              statusDetails=get_status_details(exc_type, exc_val, exc_tb))

    def _update_fixtures_children(self, item):
        uuid = self._cache.get(item.nodeid)
        for fixturedef in _test_fixtures(item):
            group_uuid = self._cache.get(fixturedef)
            if group_uuid:
                group = self.allure_logger.get_item(group_uuid)
            else:
                group_uuid = self._cache.push(fixturedef)
                group = TestResultContainer(uuid=group_uuid)
                self.allure_logger.start_group(group_uuid, group)
            if uuid not in group.children:
                self.allure_logger.update_group(group_uuid, children=uuid)

    @pytest.hookimpl(hookwrapper=True, tryfirst=True)
    def pytest_runtest_protocol(self, item, nextitem):
        uuid = self._cache.push(item.nodeid)
        test_result = TestResult(name=item.name, uuid=uuid, start=now(), stop=now())
        self.allure_logger.schedule_test(uuid, test_result)
        yield
        uuid = self._cache.pop(item.nodeid)
        if uuid:
            test_result = self.allure_logger.get_test(uuid)
            if test_result.status is None:
                test_result.status = Status.SKIPPED
            self.allure_logger.close_test(uuid)

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_setup(self, item):
        if not self._cache.get(item.nodeid):
            uuid = self._cache.push(item.nodeid)
            test_result = TestResult(name=item.name, uuid=uuid, start=now(), stop=now())
            self.allure_logger.schedule_test(uuid, test_result)
        yield
        self._update_fixtures_children(item)
        uuid = self._cache.get(item.nodeid)
        test_result = self.allure_logger.get_test(uuid)
        params = self.__get_pytest_params(item)
        param_id = self.__get_pytest_param_id(item)
        test_result.name = allure_name(item, params, param_id)
        full_name = allure_full_name(item)
        test_result.fullName = full_name
        test_result.titlePath = [*allure_title_path(item)]
        test_result.testCaseId = md5(full_name)
        test_result.description = allure_description(item)
        test_result.descriptionHtml = allure_description_html(item)
        current_param_names = [param.name for param in test_result.parameters]
        test_result.parameters.extend([
            Parameter(name=name, value=represent(value))
            for name, value in params.items()
            if name not in current_param_names
        ])

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_call(self, item):
        uuid = self._cache.get(item.nodeid)
        test_result = self.allure_logger.get_test(uuid)
        if test_result:
            self.allure_logger.drop_test(uuid)
            self.allure_logger.schedule_test(uuid, test_result)
            test_result.start = now()
        yield
        self._update_fixtures_children(item)
        if test_result:
            test_result.stop = now()

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_teardown(self, item):
        yield
        uuid = self._cache.get(item.nodeid)
        test_result = self.allure_logger.get_test(uuid)
        test_result.historyId = get_history_id(
            test_result.fullName,
            test_result.parameters,
            original_values=self.__get_pytest_params(item)
        )
        test_result.labels.extend([Label(name=name, value=value) for name, value in allure_labels(item)])
        test_result.labels.extend([Label(name=LabelType.TAG, value=value) for value in pytest_markers(item)])
        self.__apply_default_suites(item, test_result)
        test_result.labels.append(Label(name=LabelType.HOST, value=self._host))
        test_result.labels.append(Label(name=LabelType.THREAD, value=self._thread))
        test_result.labels.append(Label(name=LabelType.FRAMEWORK, value='pytest'))
        test_result.labels.append(Label(name=LabelType.LANGUAGE, value=platform_label()))
        test_result.labels.append(Label(name='package', value=allure_package(item)))
        test_result.links.extend([Link(link_type, url, name) for link_type, url, name in allure_links(item)])

    @pytest.hookimpl(hookwrapper=True)
    def pytest_fixture_setup(self, fixturedef, request):
        fixture_name = getattr(fixturedef.func, '__allure_display_name__', fixturedef.argname)

        container_uuid = self._cache.get(fixturedef)

        if not container_uuid:
            container_uuid = self._cache.push(fixturedef)
            container = TestResultContainer(uuid=container_uuid)
            self.allure_logger.start_group(container_uuid, container)

        self.allure_logger.update_group(container_uuid, start=now())

        before_fixture_uuid = uuid4()
        before_fixture = TestBeforeResult(name=fixture_name, start=now())
        self.allure_logger.start_before_fixture(container_uuid, before_fixture_uuid, before_fixture)

        outcome = yield

        self.allure_logger.stop_before_fixture(before_fixture_uuid,
                                               stop=now(),
                                               status=get_outcome_status(outcome),
                                               statusDetails=get_outcome_status_details(outcome))

        finalizers = getattr(fixturedef, '_finalizers', [])
        for index, finalizer in enumerate(finalizers):
            finalizer_name = getattr(finalizer, "__name__", index)
            name = f'{fixture_name}::{finalizer_name}'
            finalizers[index] = allure_commons.fixture(finalizer, parent_uuid=container_uuid, name=name)

    @pytest.hookimpl(hookwrapper=True)
    def pytest_fixture_post_finalizer(self, fixturedef):
        yield
        if hasattr(fixturedef, 'cached_result') and self._cache.get(fixturedef):
            container_uuid = self._cache.pop(fixturedef)
            self.allure_logger.stop_group(container_uuid, stop=now())

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_makereport(self, item, call):
        uuid = self._cache.get(item.nodeid)

        report = (yield).get_result()

        test_result = self.allure_logger.get_test(uuid)
        status = get_pytest_report_status(report)
        status_details = None

        if call.excinfo:
            message = call.excinfo.exconly()
            if hasattr(report, 'wasxfail'):
                reason = report.wasxfail
                message = (f'XFAIL {reason}' if reason else 'XFAIL') + '\n\n' + message
            trace = report.longreprtext
            status_details = StatusDetails(
                message=message,
                trace=trace)

            exception = call.excinfo.value
            if (status != Status.SKIPPED and _exception_brokes_test(exception)):
                status = Status.BROKEN

        if status == Status.PASSED and hasattr(report, 'wasxfail'):
            reason = report.wasxfail
            message = f'XPASS {reason}' if reason else 'XPASS'
            status_details = StatusDetails(message=message)

        if report.when == 'setup':
            test_result.status = status
            test_result.statusDetails = status_details

        if report.when == 'call':
            if test_result.status == Status.PASSED:
                test_result.status = status
                test_result.statusDetails = status_details

        if report.when == 'teardown':
            if status in (Status.FAILED, Status.BROKEN) and test_result.status == Status.PASSED:
                test_result.status = status
                test_result.statusDetails = status_details

            if self.config.option.attach_capture:
                if report.caplog:
                    self.attach_data(report.caplog, "log", AttachmentType.TEXT, None)
                if report.capstdout:
                    self.attach_data(report.capstdout, "stdout", AttachmentType.TEXT, None)
                if report.capstderr:
                    self.attach_data(report.capstderr, "stderr", AttachmentType.TEXT, None)

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_logfinish(self, nodeid, location):
        yield
        uuid = self._cache.pop(nodeid)
        if uuid:
            self.allure_logger.close_test(uuid)

    @allure_commons.hookimpl
    def attach_data(self, body, name, attachment_type, extension):
        self.allure_logger.attach_data(uuid4(), body, name=name, attachment_type=attachment_type, extension=extension)

    @allure_commons.hookimpl
    def attach_file(self, source, name, attachment_type, extension):
        self.allure_logger.attach_file(uuid4(), source, name=name, attachment_type=attachment_type, extension=extension)

    @allure_commons.hookimpl
    def add_title(self, test_title):
        test_result = self.allure_logger.get_test(None)
        if test_result:
            test_result.name = test_title

    @allure_commons.hookimpl
    def add_description(self, test_description):
        test_result = self.allure_logger.get_test(None)
        if test_result:
            test_result.description = test_description

    @allure_commons.hookimpl
    def add_description_html(self, test_description_html):
        test_result = self.allure_logger.get_test(None)
        if test_result:
            test_result.descriptionHtml = test_description_html

    @allure_commons.hookimpl
    def add_link(self, url, link_type, name):
        test_result = self.allure_logger.get_test(None)
        if test_result:
            link_url = format_allure_link(self.config, url, link_type)
            new_link = Link(link_type, link_url, link_url if name is None else name)
            for link in test_result.links:
                if link.url == new_link.url:
                    return
            test_result.links.append(new_link)

    @allure_commons.hookimpl
    def add_label(self, label_type, labels):
        test_result = self.allure_logger.get_test(None)
        for label in labels if test_result else ():
            test_result.labels.append(Label(label_type, label))

    @allure_commons.hookimpl
    def add_parameter(self, name, value, excluded, mode: ParameterMode):
        test_result: TestResult = self.allure_logger.get_test(None)
        existing_param = next(filter(lambda x: x.name == name, test_result.parameters), None)
        if existing_param:
            existing_param.value = represent(value)
        else:
            test_result.parameters.append(
                Parameter(
                    name=name,
                    value=represent(value),
                    excluded=excluded or None,
                    mode=mode.value if mode else None
                )
            )

    @staticmethod
    def __get_pytest_params(item):
        return item.callspec.params if hasattr(item, 'callspec') else {}

    @staticmethod
    def __get_pytest_param_id(item):
        return item.callspec.id if hasattr(item, 'callspec') else None

    def __apply_default_suites(self, item, test_result):
        default_suites = allure_suite_labels(item)
        existing_suites = {
            label.name
            for label in test_result.labels
            if label.name in AllureListener.SUITE_LABELS
        }
        test_result.labels.extend(
            Label(name=name, value=value)
            for name, value in default_suites
            if name not in existing_suites
        )


class ItemCache:

    def __init__(self):
        self._items = dict()

    def get(self, _id):
        return self._items.get(id(_id))

    def push(self, _id):
        return self._items.setdefault(id(_id), uuid4())

    def pop(self, _id):
        return self._items.pop(id(_id), None)


def _test_fixtures(item):
    fixturemanager = item.session._fixturemanager
    fixturedefs = []

    if hasattr(item, "_request") and hasattr(item._request, "fixturenames"):
        for name in item._request.fixturenames:
            fixturedefs_pytest = getfixturedefs(fixturemanager, name, item)
            if fixturedefs_pytest:
                fixturedefs.extend(fixturedefs_pytest)

    return fixturedefs


def _exception_brokes_test(exception):
    return not isinstance(exception, (
        AssertionError,
        pytest.fail.Exception,
        doctest.DocTestFailure
    ))
