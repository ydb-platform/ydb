import uuid
import pickle
import pytest
import argparse

from collections import namedtuple
from six import text_type

from allure.common import AllureImpl, StepContext
from allure.constants import Status, AttachmentType, Severity, \
    FAILED_STATUSES, Label, SKIPPED_STATUSES
from allure.utils import parent_module, parent_down_from_module, labels_of, \
    all_of, get_exception_message, now, mangle_testnames, marker_of_node
from allure.structure import TestCase, TestStep, Attach, TestSuite, Failure, TestLabel


def pytest_addoption(parser):
    parser.getgroup("reporting").addoption('--alluredir',
                                           action="store",
                                           dest="allurereportdir",
                                           metavar="DIR",
                                           default=None,
                                           help="Generate Allure report in the specified directory (may not exist)")

    severities = [v for (_, v) in all_of(Severity)]

    def label_type(name, legal_values=set()):
        """
        argparse-type factory for labelish things.
        processed value is set of tuples (name, value).
        :param name: of label type (for future TestLabel things)
        :param legal_values: a `set` of values that are legal for this label, if any limit whatsoever
        :raises ArgumentTypeError: if `legal_values` are given and there are values that fall out of that
        """
        def a_label_type(string):
            atoms = set(string.split(','))
            if legal_values and not atoms < legal_values:
                raise argparse.ArgumentTypeError('Illegal {} values: {}, only [{}] are allowed'.format(name, ', '.join(atoms - legal_values), ', '.join(legal_values)))

            return set((name, v) for v in atoms)

        return a_label_type

    parser.getgroup("general").addoption('--allure_severities',
                                         action="store",
                                         dest="allureseverities",
                                         metavar="SEVERITIES_SET",
                                         default={},
                                         type=label_type(name=Label.SEVERITY, legal_values=set(severities)),
                                         help="""Comma-separated list of severity names.
                                         Tests only with these severities will be run.
                                         Possible values are:%s.""" % ', '.join(severities))

    parser.getgroup("general").addoption('--allure_features',
                                         action="store",
                                         dest="allurefeatures",
                                         metavar="FEATURES_SET",
                                         default={},
                                         type=label_type(name=Label.FEATURE),
                                         help="""Comma-separated list of feature names.
                                         Run tests that have at least one of the specified feature labels.""")

    parser.getgroup("general").addoption('--allure_stories',
                                         action="store",
                                         dest="allurestories",
                                         metavar="STORIES_SET",
                                         default={},
                                         type=label_type(name=Label.STORY),
                                         help="""Comma-separated list of story names.
                                         Run tests that have at least one of the specified story labels.""")


def pytest_configure(config):
    pytest.allure = MASTER_HELPER

    for label in (
            Label.FEATURE, Label.STORY,
            Label.SEVERITY, Label.ISSUE,
            Label.TESTCASE, Label.THREAD,
            Label.HOST, Label.FRAMEWORK,
            Label.LANGUAGE):
        config.addinivalue_line("markers", "%s.%s: this allure adaptor marker." % (Label.DEFAULT, label))

    reportdir = config.option.allurereportdir

    if reportdir:  # we actually record something
        allure_impl = AllureImpl(reportdir)

        testlistener = AllureTestListener(config)
        pytest.allure._allurelistener = testlistener
        config.pluginmanager.register(testlistener)

        if not hasattr(config, 'slaveinput'):
            # on xdist-master node do all the important stuff
            config.pluginmanager.register(AllureAgregatingListener(allure_impl, config))
            config.pluginmanager.register(AllureCollectionListener(allure_impl))


class AllureTestListener(object):
    """
    Per-test listener.
    Is responsible for recording in-test data and for attaching it to the test report thing.

    The per-test reports are handled by `AllureAgregatingListener` at the `pytest_runtest_logreport` hook.
    """

    def __init__(self, config):
        self.config = config
        self.environment = {}
        self.test = None

        # FIXME: that flag makes us pre-report failures in the makereport hook.
        # it is here to cope with xdist's begavior regarding -x.
        # see self.pytest_runtest_makereport and AllureAgregatingListener.pytest_sessionfinish

        self._magicaldoublereport = hasattr(self.config, 'slaveinput') and self.config.getvalue("maxfail")

    @pytest.mark.hookwrapper
    def pytest_runtest_protocol(self, item, nextitem):
        try:
            # for common items
            description = item.function.__doc__
        except AttributeError:
            # for doctests that has no `function` attribute
            description = item.reportinfo()[2]
        self.test = TestCase(name='.'.join(mangle_testnames([x.name for x in parent_down_from_module(item)])),
                             description=description,
                             start=now(),
                             attachments=[],
                             labels=labels_of(item),
                             status=None,
                             steps=[],
                             id=str(uuid.uuid4()))  # for later resolution in AllureAgregatingListener.pytest_sessionfinish

        self.stack = [self.test]

        yield

        self.test = None
        self.stack = []

    def attach(self, title, contents, attach_type):
        """
        Store attachment object in current state for later actual write in the `AllureAgregatingListener.write_attach`
        """
        attach = Attach(source=contents,  # we later re-save those, oh my...
                        title=title,
                        type=attach_type)
        self.stack[-1].attachments.append(attach)

    def dynamic_issue(self, *issues):
        """
        Attaches ``issues`` to the current active case
        """
        if self.test:
            self.test.labels.extend([TestLabel(name=Label.ISSUE, value=issue) for issue in issues])

    def description(self, description):
        """
        Sets description for the test
        """
        if self.test:
            self.test.description = description

    def start_step(self, name):
        """
        Starts an new :py:class:`allure.structure.TestStep` with given ``name``,
        pushes it to the ``self.stack`` and returns the step.
        """
        step = TestStep(name=name,
                        title=name,
                        start=now(),
                        attachments=[],
                        steps=[])
        self.stack[-1].steps.append(step)
        self.stack.append(step)
        return step

    def stop_step(self):
        """
        Stops the step at the top of ``self.stack``
        """
        step = self.stack.pop()
        step.stop = now()

    def _fill_case(self, report, call, pyteststatus, status):
        """
        Finalizes with important data
        :param report: py.test's `TestReport`
        :param call: py.test's `CallInfo`
        :param pyteststatus: the failed/xfailed/xpassed thing
        :param status: a :py:class:`allure.constants.Status` entry
        """
        [self.attach(name, contents, AttachmentType.TEXT) for (name, contents) in dict(report.sections).items()]

        self.test.stop = now()
        self.test.status = status

        if status in FAILED_STATUSES:
            self.test.failure = Failure(message=get_exception_message(call.excinfo, pyteststatus, report),
                                        trace=report.longrepr or hasattr(report, 'wasxfail') and report.wasxfail)
        elif status in SKIPPED_STATUSES:
            skip_message = type(report.longrepr) == tuple and report.longrepr[2] or report.wasxfail
            trim_msg_len = 89
            short_message = skip_message.split('\n')[0][:trim_msg_len]

            # FIXME: see pytest.runner.pytest_runtest_makereport
            self.test.failure = Failure(message=(short_message + '...' * (len(skip_message) > trim_msg_len)),
                                        trace=status == Status.PENDING and report.longrepr or short_message != skip_message and skip_message or '')

    def report_case(self, item, report):
        """
        Adds `self.test` to the `report` in a `AllureAggegatingListener`-understood way
        """
        parent = parent_module(item)
        # we attach a four-tuple: (test module ID, test module name, test module doc, environment, TestCase)
        report.__dict__.update(_allure_result=pickle.dumps((parent.nodeid,
                                                            parent.module.__name__,
                                                            parent.module.__doc__ or '',
                                                            self.environment,
                                                            self.test)))

    @pytest.mark.hookwrapper
    def pytest_runtest_makereport(self, item, call):
        """
        Decides when to actually report things.

        pytest runs this (naturally) three times -- with report.when being:
          setup     <--- fixtures are to be initialized in this one
          call      <--- when this finishes the main code has finished
          teardown  <--- tears down fixtures (that still possess important info)

        `setup` and `teardown` are always called, but `call` is called only if `setup` passes.

        See :py:func:`_pytest.runner.runtestprotocol` for proofs / ideas.

        The "other side" (AllureAggregatingListener) expects us to send EXACTLY ONE test report (it wont break, but it will duplicate cases in the report -- which is bad.

        So we work hard to decide exact moment when we call `_stop_case` to do that. This method may benefit from FSM (we keep track of what has already happened via self.test.status)

        Expected behavior is:
          FAILED when call fails and others OK
          BROKEN when either setup OR teardown are broken (and call may be anything)
          PENDING if skipped and xfailed
          SKIPPED if skipped and not xfailed
        """
        report = (yield).get_result()

        status = self.config.hook.pytest_report_teststatus(report=report, config=self.config)
        status = status and status[0]

        if report.when == 'call':
            if report.passed:
                self._fill_case(report, call, status, Status.PASSED)
            elif report.failed:
                self._fill_case(report, call, status, Status.FAILED)
                # FIXME: this is here only to work around xdist's stupid -x thing when in exits BEFORE THE TEARDOWN test log. Meh, i should file an issue to xdist
                if self._magicaldoublereport:
                    # to minimize ze impact
                    self.report_case(item, report)
            elif report.skipped:
                if hasattr(report, 'wasxfail'):
                    self._fill_case(report, call, status, Status.PENDING)
                else:
                    self._fill_case(report, call, status, Status.CANCELED)
        elif report.when == 'setup':  # setup / teardown
            if report.failed:
                self._fill_case(report, call, status, Status.BROKEN)
            elif report.skipped:
                if hasattr(report, 'wasxfail'):
                    self._fill_case(report, call, status, Status.PENDING)
                else:
                    self._fill_case(report, call, status, Status.CANCELED)
        elif report.when == 'teardown':
            # as teardown is always called for testitem -- report our status here
            if not report.passed:
                if self.test.status not in FAILED_STATUSES:
                    # if test was OK but failed at teardown => broken
                    self._fill_case(report, call, status, Status.BROKEN)
                else:
                    # mark it broken so, well, someone has idea of teardown failure
                    # still, that's no big deal -- test has already failed
                    # TODO: think about that once again
                    self.test.status = Status.BROKEN
            # if a test isn't marked as "unreported" or it has failed, add it to the report.
            if not marker_of_node(item, "unreported") or self.test.status in FAILED_STATUSES:
                self.report_case(item, report)


def pytest_runtest_setup(item):
    item_labels = set((l.name, l.value) for l in labels_of(item))  # see label_type

    arg_labels = set().union(item.config.option.allurefeatures,
                             item.config.option.allurestories,
                             item.config.option.allureseverities)

    if arg_labels and not item_labels & arg_labels:
        pytest.skip('Not suitable with selected labels: %s.' % ', '.join(text_type(l) for l in sorted(arg_labels)))


class LazyInitStepContext(StepContext):

    """
    This is a step context used for decorated steps.
    It provides a possibility to create step decorators, being initiated before pytest_configure, when no AllureListener initiated yet.
    """

    def __init__(self, allure_helper, title):
        self.allure_helper = allure_helper
        self.title = title
        self.step = None

    @property
    def allure(self):
        listener = self.allure_helper.get_listener()

        # if listener has `stack` we are inside a test
        # record steps only when that
        # FIXME: this breaks encapsulation a lot
        if hasattr(listener, 'stack'):
            return listener


class AllureHelper(object):

    """
    This object holds various utility methods used from ``pytest.allure`` namespace, like ``pytest.allure.attach``
    """

    def __init__(self):
        self._allurelistener = None  # FIXME: this gets injected elsewhere, like in the pytest_configure

    def get_listener(self):
        return self._allurelistener

    def attach(self, name, contents, type=AttachmentType.TEXT):  # @ReservedAssignment
        """
        Attaches ``contents`` to a current context with given ``name`` and ``type``.
        """
        if self._allurelistener:
            self._allurelistener.attach(name, contents, type)

    def label(self, name, *value):
        """
        A decorator factory that returns ``pytest.mark`` for a given label.
        """
        allure_label = getattr(pytest.mark, '%s.%s' % (Label.DEFAULT, name))
        return allure_label(*value)

    def severity(self, severity):
        """
        A decorator factory that returns ``pytest.mark`` for a given allure ``level``.
        """
        return self.label(Label.SEVERITY, severity)

    def feature(self, *features):
        """
        A decorator factory that returns ``pytest.mark`` for a given features.
        """
        return self.label(Label.FEATURE, *features)

    def story(self, *stories):
        """
        A decorator factory that returns ``pytest.mark`` for a given stories.
        """

        return self.label(Label.STORY, *stories)

    def issue(self, *issues):
        """
        A decorator factory that returns ``pytest.mark`` for a given issues.
        """
        return self.label(Label.ISSUE, *issues)

    def dynamic_issue(self, *issues):
        """
        Mark test ``issues`` from inside.
        """
        if self._allurelistener:
            self._allurelistener.dynamic_issue(*issues)

    def description(self, description):
        """
        Sets description for the test
        """
        if self._allurelistener:
            self._allurelistener.description(description)

    def testcase(self, *testcases):
        """
        A decorator factory that returns ``pytest.mark`` for a given testcases.
        """
        return self.label(Label.TESTCASE, *testcases)

    def step(self, title):
        """
        A contextmanager/decorator for steps.

        TODO: when moving to python 3, rework this with ``contextlib.ContextDecorator``.

        Usage examples::

          import pytest

          def test_foo():
             with pytest.allure.step('mystep'):
                 assert False

          @pytest.allure.step('make test data')
          def make_test_data_bar():
              raise ValueError('No data today')

          def test_bar():
              assert make_test_data_bar()

          @pytest.allure.step
          def make_test_data_baz():
              raise ValueError('No data today')

          def test_baz():
              assert make_test_data_baz()

          @pytest.fixture()
          @pytest.allure.step('test fixture')
          def steppy_fixture():
              return 1

          def test_baz(steppy_fixture):
              assert steppy_fixture
        """
        if callable(title):
            return LazyInitStepContext(self, title.__name__)(title)
        else:
            return LazyInitStepContext(self, title)

    def single_step(self, text):
        """
        Writes single line to report.
        """
        if self._allurelistener:
            with self.step(text):
                pass

    def environment(self, **env_dict):
        if self._allurelistener:
            self._allurelistener.environment.update(env_dict)

    @property
    def attach_type(self):
        return AttachmentType

    @property
    def severity_level(self):
        return Severity

    def __getattr__(self, attr):
        """
        Provides fancy shortcuts for severity::

            # these are the same
            pytest.allure.CRITICAL
            pytest.allure.severity(pytest.allure.severity_level.CRITICAL)

        """
        if attr in dir(Severity) and not attr.startswith('_'):
            return self.severity(getattr(Severity, attr))
        else:
            raise AttributeError


MASTER_HELPER = AllureHelper()


class AllureAgregatingListener(object):

    """
    Listens to pytest hooks to generate reports for common tests.
    """

    def __init__(self, impl, config):
        self.impl = impl

        # module's nodeid => TestSuite object
        self.suites = {}

    def pytest_sessionfinish(self):
        """
        We are done and have all the results in `self.suites`
        Lets write em down.

        But first we kinda-unify the test cases.

        We expect cases to come from AllureTestListener -- and the have ._id field to manifest their identity.

        Of all the test cases in suite.testcases we leave LAST with the same ID -- becase logreport can be sent MORE THAN ONE TIME
        (namely, if the test fails and then gets broken -- to cope with the xdist's -x behavior we have to have tests even at CALL failures)

        TODO: do it in a better, more efficient way
        """

        for s in self.suites.values():
            if s.tests:  # nobody likes empty suites
                s.stop = max(case.stop for case in s.tests)

                known_ids = set()
                refined_tests = []
                for t in s.tests[::-1]:
                    if t.id not in known_ids:
                        known_ids.add(t.id)
                        refined_tests.append(t)
                s.tests = refined_tests[::-1]

                with self.impl._reportfile('%s-testsuite.xml' % uuid.uuid4()) as f:
                    self.impl._write_xml(f, s)

        self.impl.store_environment()

    def write_attach(self, attachment):
        """
        Writes attachment object from the `AllureTestListener` to the FS, fixing it fields

        :param attachment: a :py:class:`allure.structure.Attach` object
        """

        # OMG, that is bad
        attachment.source = self.impl._save_attach(attachment.source, attachment.type)
        attachment.type = attachment.type.mime_type

    def pytest_runtest_logreport(self, report):
        if hasattr(report, '_allure_result'):
            module_id, module_name, module_doc, environment, testcase = pickle.loads(report._allure_result)

            report._allure_result = None  # so actual pickled data is garbage-collected, see https://github.com/allure-framework/allure-python/issues/98

            self.impl.environment.update(environment)

            for a in testcase.iter_attachments():
                self.write_attach(a)

            self.suites.setdefault(module_id, TestSuite(name=module_name,
                                                        description=module_doc,
                                                        tests=[],
                                                        labels=[],
                                                        start=testcase.start,  # first case starts the suite!
                                                        stop=None)).tests.append(testcase)


CollectFail = namedtuple('CollectFail', 'name status message trace')


class AllureCollectionListener(object):

    """
    Listens to pytest collection-related hooks
    to generate reports for modules that failed to collect.
    """

    def __init__(self, impl):
        self.impl = impl
        self.fails = []

    def pytest_collectreport(self, report):
        if not report.passed:
            if report.failed:
                status = Status.BROKEN
            else:
                status = Status.CANCELED

            self.fails.append(CollectFail(name=mangle_testnames(report.nodeid.split("::"))[-1],
                                          status=status,
                                          message=get_exception_message(None, None, report),
                                          trace=report.longrepr))

    def pytest_sessionfinish(self):
        """
        Creates a testsuite with collection failures if there were any.
        """

        if self.fails:
            self.impl.start_suite(name='test_collection_phase',
                                  title='Collection phase',
                                  description='This is the tests collection phase. Failures are modules that failed to collect.')
            for fail in self.fails:
                self.impl.start_case(name=fail.name.split(".")[-1])
                self.impl.stop_case(status=fail.status, message=fail.message, trace=fail.trace)
            self.impl.stop_suite()
