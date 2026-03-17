"""
The flavor based on Jenkins xunit plugin:
https://github.com/jenkinsci/xunit-plugin/blob/xunit-2.3.2/src/main/resources/org/jenkinsci/plugins/xunit/types/model/xsd/junit-10.xsd

According to the internet, the schema is compatible with:

- Pytest (as default, though it also supports a "legacy" xunit1 flavor)
- Erlang/OTP
- Maven Surefire
- CppTest

There may be many others that I'm not aware of.
"""

import itertools
from typing import List, Type, TypeVar
from . import junitparser


class StackTrace(junitparser.System):
    _tag = "stackTrace"


class InterimResult(junitparser.Result):
    """Base class for intermediate (rerun and flaky) test result (in contrast to JUnit FinalResult)."""

    _tag = None

    @property
    def stack_trace(self):
        """<stackTrace>"""
        elem = self.child(StackTrace)
        if elem is not None:
            return elem.text
        return None

    @stack_trace.setter
    def stack_trace(self, value: str):
        """<stackTrace>"""
        trace = self.child(StackTrace)
        if trace is not None:
            trace.text = value
        else:
            trace = StackTrace(value)
            self.append(trace)

    @property
    def system_out(self):
        """<system-out>"""
        elem = self.child(junitparser.SystemOut)
        if elem is not None:
            return elem.text
        return None

    @system_out.setter
    def system_out(self, value: str):
        """<system-out>"""
        out = self.child(junitparser.SystemOut)
        if out is not None:
            out.text = value
        else:
            out = junitparser.SystemOut(value)
            self.append(out)

    @property
    def system_err(self):
        """<system-err>"""
        elem = self.child(junitparser.SystemErr)
        if elem is not None:
            return elem.text
        return None

    @system_err.setter
    def system_err(self, value: str):
        """<system-err>"""
        err = self.child(junitparser.SystemErr)
        if err is not None:
            err.text = value
        else:
            err = junitparser.SystemErr(value)
            self.append(err)


class RerunFailure(InterimResult):
    _tag = "rerunFailure"


class RerunError(InterimResult):
    _tag = "rerunError"


class FlakyFailure(InterimResult):
    _tag = "flakyFailure"


class FlakyError(InterimResult):
    _tag = "flakyError"


R = TypeVar("R", bound=InterimResult)


class TestCase(junitparser.TestCase):
    group = junitparser.Attr()

    # XUnit2 TestCase children are JUnit children and intermediate results
    ITER_TYPES = {
        t._tag: t
        for t in itertools.chain(
            junitparser.TestCase.ITER_TYPES.values(),
            (RerunFailure, RerunError, FlakyFailure, FlakyError),
        )
    }

    def _interim_results(self, _type: Type[R]) -> List[R]:
        return [entry for entry in self if isinstance(entry, _type)]

    @property
    def interim_result(self) -> List[InterimResult]:
        """
        A list of interim results: :class:`RerunFailure`, :class:`RerunError`,
        :class:`FlakyFailure`, or :class:`FlakyError` objects.
        This is complementary to the result property returning final results.
        """
        return self._interim_results(InterimResult)

    def rerun_failures(self) -> List[RerunFailure]:
        """<rerunFailure>"""
        return self._interim_results(RerunFailure)

    def rerun_errors(self) -> List[RerunError]:
        """<rerunError>"""
        return self._interim_results(RerunError)

    def flaky_failures(self) -> List[FlakyFailure]:
        """<flakyFailure>"""
        return self._interim_results(FlakyFailure)

    def flaky_errors(self) -> List[FlakyError]:
        """<flakyError>"""
        return self._interim_results(FlakyError)

    @property
    def is_rerun(self) -> bool:
        """Whether this testcase is rerun, i.e., there are rerun failures or errors."""
        return any(self.rerun_failures()) or any(self.rerun_errors())

    @property
    def is_flaky(self) -> bool:
        """Whether this testcase is flaky, i.e., there are flaky failures or errors."""
        return any(self.flaky_failures()) or any(self.flaky_errors())

    def add_interim_result(self, result: InterimResult):
        """Append an interim (rerun or flaky) result to the testcase. A testcase can have multiple interim results."""
        self.append(result)


class TestSuite(junitparser.TestSuite):
    """TestSuite for Pytest, with some different attributes."""

    group = junitparser.Attr()
    id = junitparser.Attr()
    package = junitparser.Attr()
    file = junitparser.Attr()
    log = junitparser.Attr()
    url = junitparser.Attr()
    version = junitparser.Attr()

    testcase = TestCase

    def __init__(self, name=None):
        super().__init__(name)
        self.root = JUnitXml

    @property
    def system_out(self):
        """<system-out>"""
        elem = self.child(junitparser.SystemOut)
        if elem is not None:
            return elem.text
        return None

    @system_out.setter
    def system_out(self, value: str):
        """<system-out>"""
        out = self.child(junitparser.SystemOut)
        if out is not None:
            out.text = value
        else:
            out = junitparser.SystemOut(value)
            self.append(out)

    @property
    def system_err(self):
        """<system-err>"""
        elem = self.child(junitparser.SystemErr)
        if elem is not None:
            return elem.text
        return None

    @system_err.setter
    def system_err(self, value: str):
        """<system-err>"""
        err = self.child(junitparser.SystemErr)
        if err is not None:
            err.text = value
        else:
            err = junitparser.SystemErr(value)
            self.append(err)


class JUnitXml(junitparser.JUnitXml):
    # Pytest and xunit schema doesn't have "skipped" in testsuites
    skipped = None

    testsuite = TestSuite

    def update_statistics(self):
        """Update test count, time, etc."""
        time = 0
        tests = failures = errors = 0
        for suite in self:
            suite.update_statistics()
            tests += suite.tests
            failures += suite.failures
            errors += suite.errors
            time += suite.time
        self.tests = tests
        self.failures = failures
        self.errors = errors
        self.time = round(time, 3)
