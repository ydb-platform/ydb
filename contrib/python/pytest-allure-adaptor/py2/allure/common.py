# -*- coding: utf-8 -*-
"""
Created on Feb 23, 2014

@author: pupssman
"""
import os
import uuid
from contextlib import contextmanager
from distutils.version import StrictVersion
from functools import wraps

import py
from _pytest import __version__ as pytest_version
from lxml import etree
from six import text_type, iteritems

from allure.constants import AttachmentType, Status
from allure.structure import Attach, TestStep, TestCase, TestSuite, Failure, Environment, EnvParameter
from allure.utils import now

if StrictVersion(pytest_version) >= StrictVersion("3.2.0"):
    from _pytest.outcomes import Skipped, XFailed
else:
    from _pytest.runner import Skipped
    from _pytest.skipping import XFailed


class StepContext:
    def __init__(self, allure, title):
        self.allure = allure
        self.title = title
        self.step = None

    def __enter__(self):
        if self.allure:
            self.step = self.allure.start_step(self.title)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.allure:
            if exc_type is not None:
                if exc_type == Skipped:
                    self.step.status = Status.CANCELED
                elif exc_type == XFailed:
                    self.step.status = Status.PENDING
                else:
                    self.step.status = Status.FAILED
            else:
                self.step.status = Status.PASSED
            self.allure.stop_step()

    def __call__(self, func):
        """
        Pretend that we are a decorator -- wrap the ``func`` with self.
        FIXME: may fail if evil dude will try to reuse ``pytest.allure.step`` instance.
        """

        @wraps(func)
        def impl(*a, **kw):
            with StepContext(self.allure, self.title.format(*a, **kw)):
                return func(*a, **kw)

        return impl


class AllureImpl(object):
    """
    Allure test-flow implementation that handles test data creation.
    Devised to be used as base layer for allure adaptation to arbitrary test frameworks.

    Has a SAX-like API -- methods to start or stop a ``TestSuite``, ``TestCase``, ``TestStep`` or to create an ``Attachment``.

    Warning::
      Violation of the call order may result in ill-formed allure XML that wont pass schema validation and render into a report.

    Calls should conform to a rule: A *suite* holds *cases*, a *case* holds *steps*, a *step* holds other *steps*.
    *case* and *step* can have as much *attachments* as they want.

    Therefore, basic call order is as follows::
      allure = AllureImpl('./reports')  # this clears ./reports

      allure.start_suite('demo')
      allure.start_case('test_one')
      allure.stop_case(Status.PASSED)
      allure.start_case('test_two')
      allure.start_step('a demo step')
      allure.attach('some file', 'a quick brown fox..', AttachmentType.TEXT)
      allure.stop_step()
      allure.stop_case(Status.FAILED, 'failed for demo', 'stack trace goes here')
      allure.stop_suite()  # this writes XML into ./reports

    """

    def __init__(self, logdir):
        self.logdir = os.path.normpath(
            os.path.abspath(os.path.expanduser(os.path.expandvars(logdir))))

        # Delete all files in report directory
        if not os.path.exists(self.logdir):
            os.makedirs(self.logdir)
        else:
            for f in os.listdir(self.logdir):
                f = os.path.join(self.logdir, f)
                if os.path.isfile(f):
                    os.unlink(f)

        # That's the state stack. It can contain TestCases or TestSteps.
        # Attaches and steps go to the object at top of the stack.
        self.stack = []

        self.testsuite = None
        self.environment = {}

    def attach(self, title, contents, attach_type):
        """
        Attaches ``contents`` with ``title`` and ``attach_type`` to the current active thing
        """
        attach = Attach(
            source=self._save_attach(contents, attach_type=attach_type),
            title=title,
            type=attach_type.mime_type)
        self.stack[-1].attachments.append(attach)

    def start_step(self, name):
        """
        Starts an new :py:class:`allure.structure.TestStep` with given ``name``,
        pushes it to the ``self.stack`` and returns the step.
        """
        step = TestStep(
            name=name, title=name, start=now(), attachments=[], steps=[])
        self.stack[-1].steps.append(step)
        self.stack.append(step)
        return step

    def stop_step(self):
        """
        Stops the step at the top of ``self.stack``
        """
        step = self.stack.pop()
        step.stop = now()

    def start_case(self, name, description=None, labels=None):
        """
        Starts a new :py:class:`allure.structure.TestCase`
        """
        test = TestCase(
            name=name,
            description=description,
            start=now(),
            attachments=[],
            labels=labels or [],
            steps=[])
        self.stack.append(test)

    def stop_case(self, status, message=None, trace=None):
        """
        :arg status: one of :py:class:`allure.constants.Status`
        :arg message: error message from the test
        :arg trace: error trace from the test

        Finalizes with important data the test at the top of ``self.stack`` and returns it

        If either ``message`` or ``trace`` are given adds a ``Failure`` object to the test with them.
        """
        test = self.stack[-1]
        test.status = status
        test.stop = now()

        if message or trace:
            test.failure = Failure(message=message, trace=trace or '')

        self.testsuite.tests.append(test)

        return test

    def start_suite(self, name, description=None, title=None, labels=None):
        """
        Starts a new Suite with given ``name`` and ``description``
        """
        self.testsuite = TestSuite(
            name=name,
            title=title,
            description=description,
            tests=[],
            labels=labels or [],
            start=now())

    def stop_suite(self):
        """
        Stops current test suite and writes it to the file in the report directory
        """
        self.testsuite.stop = now()

        with self._reportfile('%s-testsuite.xml' % uuid.uuid4()) as f:
            self._write_xml(f, self.testsuite)

    def store_environment(self):
        if not self.environment:
            return

        environment = Environment(
            id=uuid.uuid4(),
            name="Allure environment parameters",
            parameters=[])
        for key, value in iteritems(self.environment):
            environment.parameters.append(
                EnvParameter(name=key, key=key, value=value))

        with self._reportfile('environment.xml') as f:
            self._write_xml(f, environment)

    def _save_attach(self, body, attach_type=AttachmentType.TEXT):
        """
        Saves attachment to the report folder and returns file name

        :arg body: str or unicode with contents. str is written as-is in byte stream, unicode is written as utf-8 (what do you expect else?)
        """
        with self._attachfile("%s-attachment.%s" %
                              (uuid.uuid4(), attach_type.extension)) as f:
            if isinstance(body, text_type):
                f.write(body.encode('utf-8'))
            else:
                f.write(body)
            return os.path.basename(f.name)

    @contextmanager
    def _attachfile(self, filename):
        """
        Yields open file object in the report directory with given name
        """
        reportpath = os.path.join(self.logdir, filename)

        with open(reportpath, 'wb') as f:
            yield f

    @contextmanager
    def _reportfile(self, filename):
        """
        Yields open file object in the report directory with given name
        """
        reportpath = os.path.join(self.logdir, filename)
        encoding = 'utf-8'

        logfile = py.std.codecs.open(reportpath, 'w', encoding=encoding)

        try:
            yield logfile
        finally:
            logfile.close()

    def _write_xml(self, logfile, xmlfied):
        logfile.write(
            etree.tostring(
                xmlfied.toxml(),
                pretty_print=True,
                xml_declaration=False,
                encoding=text_type))
