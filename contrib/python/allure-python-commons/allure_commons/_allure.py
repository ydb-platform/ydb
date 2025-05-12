from functools import wraps
from typing import Any, Callable, TypeVar, Union, overload

from allure_commons._core import plugin_manager
from allure_commons.types import LabelType, LinkType, ParameterMode
from allure_commons.utils import uuid4
from allure_commons.utils import func_parameters, represent

_TFunc = TypeVar("_TFunc", bound=Callable[..., Any])


def safely(result):
    if result:
        return result[0]
    else:
        def dummy(function):
            return function
        return dummy


def title(test_title):
    return safely(plugin_manager.hook.decorate_as_title(test_title=test_title))


def description(test_description):
    return safely(plugin_manager.hook.decorate_as_description(test_description=test_description))


def description_html(test_description_html):
    return safely(plugin_manager.hook.decorate_as_description_html(test_description_html=test_description_html))


def label(label_type, *labels):
    return safely(plugin_manager.hook.decorate_as_label(label_type=label_type, labels=labels))


def severity(severity_level):
    return label(LabelType.SEVERITY, severity_level)


def epic(*epics):
    return label(LabelType.EPIC, *epics)


def feature(*features):
    return label(LabelType.FEATURE, *features)


def story(*stories):
    return label(LabelType.STORY, *stories)


def suite(suite_name):
    return label(LabelType.SUITE, suite_name)


def parent_suite(parent_suite_name):
    return label(LabelType.PARENT_SUITE, parent_suite_name)


def sub_suite(sub_suite_name):
    return label(LabelType.SUB_SUITE, sub_suite_name)


def tag(*tags):
    return label(LabelType.TAG, *tags)


def id(id):  # noqa: A001,A002
    return label(LabelType.ID, id)


def manual(fn):
    return label(LabelType.MANUAL, True)(fn)


def link(url, link_type=LinkType.LINK, name=None):
    return safely(plugin_manager.hook.decorate_as_link(url=url, link_type=link_type, name=name))


def issue(url, name=None):
    return link(url, link_type=LinkType.ISSUE, name=name)


def testcase(url, name=None):
    return link(url, link_type=LinkType.TEST_CASE, name=name)


class Dynamic:

    @staticmethod
    def title(test_title):
        plugin_manager.hook.add_title(test_title=test_title)

    @staticmethod
    def description(test_description):
        plugin_manager.hook.add_description(test_description=test_description)

    @staticmethod
    def description_html(test_description_html):
        plugin_manager.hook.add_description_html(test_description_html=test_description_html)

    @staticmethod
    def label(label_type, *labels):
        plugin_manager.hook.add_label(label_type=label_type, labels=labels)

    @staticmethod
    def severity(severity_level):
        Dynamic.label(LabelType.SEVERITY, severity_level)

    @staticmethod
    def epic(*epics):
        Dynamic.label(LabelType.EPIC, *epics)

    @staticmethod
    def feature(*features):
        Dynamic.label(LabelType.FEATURE, *features)

    @staticmethod
    def story(*stories):
        Dynamic.label(LabelType.STORY, *stories)

    @staticmethod
    def tag(*tags):
        Dynamic.label(LabelType.TAG, *tags)

    @staticmethod
    def id(id):  # noqa: A003,A002
        Dynamic.label(LabelType.ID, id)

    @staticmethod
    def link(url, link_type=LinkType.LINK, name=None):
        plugin_manager.hook.add_link(url=url, link_type=link_type, name=name)

    @staticmethod
    def parameter(name, value, excluded=None, mode: Union[ParameterMode, None] = None):
        plugin_manager.hook.add_parameter(name=name, value=value, excluded=excluded, mode=mode)

    @staticmethod
    def issue(url, name=None):
        Dynamic.link(url, link_type=LinkType.ISSUE, name=name)

    @staticmethod
    def testcase(url, name=None):
        Dynamic.link(url, link_type=LinkType.TEST_CASE, name=name)

    @staticmethod
    def suite(suite_name):
        Dynamic.label(LabelType.SUITE, suite_name)

    @staticmethod
    def parent_suite(parent_suite_name):
        Dynamic.label(LabelType.PARENT_SUITE, parent_suite_name)

    @staticmethod
    def sub_suite(sub_suite_name):
        Dynamic.label(LabelType.SUB_SUITE, sub_suite_name)

    @staticmethod
    def manual():
        return Dynamic.label(LabelType.MANUAL, True)


@overload
def step(title: str) -> "StepContext":
    ...


@overload
def step(title: _TFunc) -> _TFunc:
    ...


def step(title):
    if callable(title):
        return StepContext(title.__name__, {})(title)
    else:
        return StepContext(title, {})


class StepContext:

    def __init__(self, title, params):
        self.title = title
        self.params = params
        self.uuid = uuid4()

    def __enter__(self):
        plugin_manager.hook.start_step(uuid=self.uuid, title=self.title, params=self.params)

    def __exit__(self, exc_type, exc_val, exc_tb):
        plugin_manager.hook.stop_step(uuid=self.uuid, title=self.title, exc_type=exc_type, exc_val=exc_val,
                                      exc_tb=exc_tb)

    def __call__(self, func: _TFunc) -> _TFunc:
        @wraps(func)
        def impl(*a, **kw):
            __tracebackhide__ = True
            params = func_parameters(func, *a, **kw)
            args = list(map(lambda x: represent(x), a))
            with StepContext(self.title.format(*args, **params), params):
                return func(*a, **kw)

        return impl  # type: ignore


class Attach:

    def __call__(self, body, name=None, attachment_type=None, extension=None):
        plugin_manager.hook.attach_data(body=body, name=name, attachment_type=attachment_type, extension=extension)

    def file(self, source, name=None, attachment_type=None, extension=None):
        plugin_manager.hook.attach_file(source=source, name=name, attachment_type=attachment_type, extension=extension)


attach = Attach()


class fixture:
    def __init__(self, fixture_function, parent_uuid=None, name=None):
        self._fixture_function = fixture_function
        self._parent_uuid = parent_uuid
        self._name = name if name else fixture_function.__name__
        self._uuid = uuid4()
        self.parameters = None

    def __call__(self, *args, **kwargs):
        self.parameters = func_parameters(self._fixture_function, *args, **kwargs)

        with self:
            return self._fixture_function(*args, **kwargs)

    def __enter__(self):
        plugin_manager.hook.start_fixture(parent_uuid=self._parent_uuid,
                                          uuid=self._uuid,
                                          name=self._name,
                                          parameters=self.parameters)

    def __exit__(self, exc_type, exc_val, exc_tb):
        plugin_manager.hook.stop_fixture(parent_uuid=self._parent_uuid,
                                         uuid=self._uuid,
                                         name=self._name,
                                         exc_type=exc_type,
                                         exc_val=exc_val,
                                         exc_tb=exc_tb)


class test:
    def __init__(self, _test, context):
        self._test = _test
        self._uuid = uuid4()
        self.context = context
        self.parameters = None

    def __call__(self, *args, **kwargs):
        self.parameters = func_parameters(self._test, *args, **kwargs)

        with self:
            return self._test(*args, **kwargs)

    def __enter__(self):
        plugin_manager.hook.start_test(parent_uuid=None,
                                       uuid=self._uuid,
                                       name=None,
                                       parameters=self.parameters,
                                       context=self.context)

    def __exit__(self, exc_type, exc_val, exc_tb):
        plugin_manager.hook.stop_test(parent_uuid=None,
                                      uuid=self._uuid,
                                      name=None,
                                      context=self.context,
                                      exc_type=exc_type,
                                      exc_val=exc_val,
                                      exc_tb=exc_tb)
