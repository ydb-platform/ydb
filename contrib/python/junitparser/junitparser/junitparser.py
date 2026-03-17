"""
junitparser is a JUnit/xUnit Result XML Parser. Use it to parse and manipulate
existing Result XML files, or create new JUnit/xUnit result XMLs from scratch.

Reference schema: https://github.com/windyroad/JUnit-Schema/blob/master/JUnit.xsd

This, according to the document, is Apache Ant's JUnit output.

See the documentation for other supported schemas.
"""
import io
import itertools
from copy import deepcopy
from pathlib import Path
from typing import List, Union, Iterator, IO, Optional

try:
    from lxml import etree
except ImportError:
    from xml.etree import ElementTree as etree


def write_xml(obj, file_or_filename: Optional[Union[str, IO, Path]] = None, *, pretty: bool = False):
    tree = etree.ElementTree(obj._elem)
    if file_or_filename is None:
        file_or_filename = obj.filepath
    if file_or_filename is None:
        raise JUnitXmlError("Missing file argument.")

    if pretty:
        from xml.dom.minidom import parseString

        text = etree.tostring(obj._elem)
        xml = parseString(text)  # nosec
        content = xml.toprettyxml(encoding="utf-8")
        if isinstance(file_or_filename, Path):
            file_or_filename = str(file_or_filename)

        if isinstance(file_or_filename, str):
            with open(file_or_filename, mode="wb") as xmlfile:
                xmlfile.write(content)
        else:
            if isinstance(file_or_filename, io.TextIOWrapper):
                if file_or_filename.encoding is not None and file_or_filename.encoding.lower() != "utf-8":
                    raise ValueError(f"Only utf-8 encoding is supported: {file_or_filename.encoding}")
                file_or_filename.buffer.write(content)
            else:
                file_or_filename.write(content)
    else:
        if isinstance(file_or_filename, io.TextIOWrapper):
            if file_or_filename.encoding is not None and file_or_filename.encoding.lower() != "utf-8":
                raise ValueError(f"Only utf-8 encoding is supported: {file_or_filename.encoding}")
            tree.write(file_or_filename.buffer, encoding="utf-8", xml_declaration=True)
        else:
            tree.write(file_or_filename, encoding="utf-8", xml_declaration=True)


class JUnitXmlError(Exception):
    """Exception for JUnit XML related errors."""


class Attr(object):
    """An attribute for an XML element.

    By default they are all string values. To support different value types,
    inherit this class and define your own methods.

    Also see: :class:`IntAttr`, :class:`FloatAttr`.
    """

    def __init__(self, name: str = None):
        self.name = name

    def __get__(self, instance, cls):
        """Get value from attribute, return ``None`` if attribute doesn't exist."""
        return instance._elem.attrib.get(self.name)

    def __set__(self, instance, value: str):
        """Sets XML element attribute."""
        if value is not None:
            instance._elem.attrib[self.name] = str(value)


class IntAttr(Attr):
    """An integer attribute for an XML element.

    This class is used internally for counting testcases, but you could use
    it for any specific purpose.
    """

    def __get__(self, instance, cls):
        result = super().__get__(instance, cls)
        if result is None and isinstance(instance, (JUnitXml, TestSuite)):
            instance.update_statistics()
            result = super().__get__(instance, cls)
        return int(result) if result else None

    def __set__(self, instance, value: int):
        if not isinstance(value, int):
            raise TypeError("Expected integer value.")
        super().__set__(instance, value)


class FloatAttr(Attr):
    """A float attribute for an XML element.

    This class is used internally for counting test durations, but you could
    use it for any specific purpose.
    """

    def __get__(self, instance, cls):
        result = super().__get__(instance, cls)
        if result is None and isinstance(instance, (JUnitXml, TestSuite)):
            instance.update_statistics()
            result = super().__get__(instance, cls)
        return float(result.replace(",", "")) if result else None

    def __set__(self, instance, value: float):
        if not (isinstance(value, float) or isinstance(value, int)):
            raise TypeError("Expected float value.")
        super().__set__(instance, value)


def attributed(cls):
    """Decorator to read XML element attribute name from class attribute."""
    for key, value in vars(cls).items():
        if isinstance(value, Attr):
            value.name = key
    return cls


class junitxml(type):
    """Metaclass to decorate the XML class."""

    def __new__(meta, name, bases, methods):
        cls = super(junitxml, meta).__new__(meta, name, bases, methods)
        cls = attributed(cls)
        return cls


class Element(metaclass=junitxml):
    """Base class for all JUnit XML elements."""

    def __init__(self, name: str = None):
        if not name:
            name = self._tag
        self._elem = etree.Element(name)

    def __hash__(self):
        return hash(etree.tostring(self._elem))

    def __repr__(self):
        tag = self._elem.tag
        keys = sorted(self._elem.attrib.keys())
        if keys:
            attrs_str = " ".join(
                '%s="%s"' % (key, self._elem.attrib[key]) for key in keys
            )
            return """<Element '%s' %s>""" % (tag, attrs_str)

        return """<Element '%s'>""" % tag

    def append(self, sub_elem):
        """Add the element subelement to the end of this elements internal
        list of subelements.
        """
        self._elem.append(sub_elem._elem)

    def extend(self, sub_elems):
        """Add elements subelement to the end of this elements internal
        list of subelements.
        """
        self._elem.extend((sub_elem._elem for sub_elem in sub_elems))

    @classmethod
    def fromstring(cls, text: str):
        """Construct JUnit object *cls* from XML string *test*."""
        instance = cls()
        instance._elem = etree.fromstring(text)  # nosec
        return instance

    @classmethod
    def fromelem(cls, elem):
        """Construct JUnit objects from an ElementTree element *elem*."""
        if elem is None:
            return
        instance = cls()
        if isinstance(elem, Element):
            instance._elem = elem._elem
        else:
            instance._elem = elem
        return instance

    def iterchildren(self, Child):
        """Iterate through specified *Child* type elements."""
        elems = self._elem.iterfind(Child._tag)
        for elem in elems:
            yield Child.fromelem(elem)

    def child(self, Child):
        """Find a single child of specified *Child* type."""
        elem = self._elem.find(Child._tag)
        return Child.fromelem(elem)

    def remove(self, sub_elem):
        """Remove subelement *sub_elem*."""
        for elem in self._elem.iterfind(sub_elem._tag):
            child = sub_elem.__class__.fromelem(elem)
            if child == sub_elem:
                self._elem.remove(child._elem)

    def tostring(self):
        """Convert element to XML string."""
        return etree.tostring(self._elem, encoding="utf-8")


class Result(Element):
    """Base class for test result.

    Attributes:
        message: Result as message string.
        type: Message type.
    """

    _tag = None
    message = Attr()
    type = Attr()

    def __init__(self, message: str = None, type_: str = None):
        super(Result, self).__init__(self._tag)
        if message:
            self.message = message
        if type_:
            self.type = type_

    def __eq__(self, other):
        return (
            self._tag == other._tag
            and self.type == other.type
            and self.message == other.message
        )

    @property
    def text(self):
        return self._elem.text

    @text.setter
    def text(self, value: str):
        self._elem.text = value


class FinalResult(Result):
    """Base class for final test result (in contrast to XUnit2 InterimResult)."""

    _tag = None


class Skipped(FinalResult):
    """Test result when the case is skipped."""

    _tag = "skipped"

    def __eq__(self, other):
        return super().__eq__(other)


class Failure(FinalResult):
    """Test result when the case failed."""

    _tag = "failure"

    def __eq__(self, other):
        return super().__eq__(other)


class Error(FinalResult):
    """Test result when the case has errors during execution."""

    _tag = "error"

    def __eq__(self, other):
        return super().__eq__(other)


class System(Element):
    """Parent class for :class:`SystemOut` and :class:`SystemErr`.

    Attributes:
        text: The output message.
    """

    _tag = ""

    def __init__(self, content: str = None):
        super().__init__(self._tag)
        self.text = content

    @property
    def text(self):
        return self._elem.text

    @text.setter
    def text(self, value: str):
        self._elem.text = value


class SystemOut(System):
    _tag = "system-out"


class SystemErr(System):
    _tag = "system-err"


class TestCase(Element):
    """Object to store a testcase and its result.

    Attributes:
        name: Name of the testcase.
        classname: The parent class of the testcase.
        time: The time consumed by the testcase.
    """

    _tag = "testcase"
    name = Attr()
    classname = Attr()
    time = FloatAttr()
    __test__ = False

    # JUnit TestCase children are final results, SystemOut and SystemErr
    ITER_TYPES = {t._tag: t for t in (Failure, Error, Skipped, SystemOut, SystemErr)}

    def __init__(self, name: str = None, classname: str = None, time: float = None):
        super().__init__(self._tag)
        if name is not None:
            self.name = name
        if classname is not None:
            self.classname = classname
        if time is not None:
            self.time = float(time)

    def __hash__(self):
        return super().__hash__()

    def __iter__(self) -> Iterator[Union[Result, System]]:
        for elem in self._elem.iter():
            if elem.tag in self.ITER_TYPES:
                yield self.ITER_TYPES[elem.tag].fromelem(elem)

    def __eq__(self, other):
        # TODO: May not work correctly if unreliable hash method is used.
        return hash(self) == hash(other)

    @property
    def is_passed(self):
        """Whether this testcase was a success (i.e. if it isn't skipped, failed, or errored)."""
        return not self.result

    @property
    def is_failure(self):
        """Whether this testcase failed."""
        return any(isinstance(r, Failure) for r in self.result)

    @property
    def is_error(self):
        """Whether this testcase errored."""
        return any(isinstance(r, Error) for r in self.result)

    @property
    def is_skipped(self):
        """Whether this testcase was skipped."""
        return any(isinstance(r, Skipped) for r in self.result)

    @property
    def result(self) -> List[FinalResult]:
        """A list of :class:`Failure`, :class:`Skipped`, or :class:`Error` objects."""
        return [entry for entry in self if isinstance(entry, FinalResult)]

    @result.setter
    def result(self, value: Union[FinalResult, List[FinalResult]]):
        # Check typing
        if not (isinstance(value, FinalResult) or
                isinstance(value, list) and all(isinstance(item, FinalResult) for item in value)):
            raise ValueError("Value must be either FinalResult or list of FinalResult")

        # First remove all existing results
        for entry in self.result:
            self.remove(entry)
        if isinstance(value, FinalResult):
            self.append(value)
        else:
            for entry in value:
                self.append(entry)

    @property
    def system_out(self):
        """stdout."""
        elem = self.child(SystemOut)
        if elem is not None:
            return elem.text
        return None

    @system_out.setter
    def system_out(self, value: str):
        out = self.child(SystemOut)
        if out is not None:
            out.text = value
        else:
            out = SystemOut(value)
            self.append(out)

    @property
    def system_err(self):
        """stderr."""
        elem = self.child(SystemErr)
        if elem is not None:
            return elem.text
        return None

    @system_err.setter
    def system_err(self, value: str):
        err = self.child(SystemErr)
        if err is not None:
            err.text = value
        else:
            err = SystemErr(value)
            self.append(err)


class Property(Element):
    """A key/value pare that's stored in the testsuite.

    Use it to store anything you find interesting or useful.

    Attributes:
        name: The property name.
        value: The property value.
    """

    _tag = "property"
    name = Attr()
    value = Attr()

    def __init__(self, name: str = None, value: str = None):
        super().__init__(self._tag)
        self.name = name
        self.value = value

    def __eq__(self, other):
        return self.name == other.name and self.value == other.value

    def __ne__(self, other):
        return not self == other

    def __lt__(self, other):
        """Supports sort() for properties."""
        return self.name > other.name


class Properties(Element):
    """A list of properties inside a testsuite.

    See :class:`Property`
    """

    _tag = "properties"

    def __init__(self):
        super().__init__(self._tag)

    def add_property(self, property_: Property):
        self.append(property_)

    def __iter__(self) -> Iterator[Property]:
        return super().iterchildren(Property)

    def __eq__(self, other):
        p1 = list(self)
        p2 = list(other)
        p1.sort()
        p2.sort()
        if len(p1) != len(p2):
            return False
        for e1, e2 in zip(p1, p2):
            if e1 != e2:
                return False
        return True


class TestSuite(Element):
    """The <testsuite> object.

    Attributes:
        name: The name of the testsuite.
        hostname: Name of the test machine.
        time: Time consumed by the testsuite.
        timestamp: When the test was run.
        tests: Total number of tests.
        failures: Number of failed tests.
        errors: Number of cases with errors.
        skipped: Number of skipped cases.
    """

    _tag = "testsuite"
    name = Attr()
    hostname = Attr()
    time = FloatAttr()
    timestamp = Attr()
    tests = IntAttr()
    failures = IntAttr()
    errors = IntAttr()
    skipped = IntAttr()
    __test__ = False

    testcase = TestCase

    def __init__(self, name=None):
        super().__init__(self._tag)
        self.name = name
        self.filepath = None
        self.root = JUnitXml

    def __iter__(self) -> Iterator[TestCase]:
        return itertools.chain(
            super().iterchildren(self.testcase),
            (case for suite in super().iterchildren(type(self)) for case in suite),
        )

    def __len__(self):
        return len(list(self.__iter__()))

    def __eq__(self, other):
        def props_eq(props1, props2):
            props1 = list(props1)
            props2 = list(props2)
            if len(props1) != len(props2):
                return False
            props1.sort(key=lambda x: x.name)
            props2.sort(key=lambda x: x.name)
            zipped = zip(props1, props2)
            return all(x == y for x, y in zipped)

        return (
            self.name == other.name
            and self.hostname == other.hostname
            and self.timestamp == other.timestamp
        ) and props_eq(self.properties(), other.properties())

    def __add__(self, other):
        if self == other:
            # Merge the two testsuites
            result = deepcopy(self)
            for case in other:
                result._add_testcase_no_update_stats(case)
            for suite in other.testsuites():
                result.add_testsuite(suite)
            result.update_statistics()
        else:
            # Create a new test result containing two testsuites
            result = self.root()
            result.add_testsuite(self)
            result.add_testsuite(other)
        return result

    def __iadd__(self, other):
        if self == other:
            for case in other:
                self._add_testcase_no_update_stats(case)
            for suite in other.testsuites():
                self.add_testsuite(suite)
            self.update_statistics()
            return self

        result = self.root()
        result.filepath = self.filepath
        result.add_testsuite(self)
        result.add_testsuite(other)
        return result

    def remove_testcase(self, testcase: TestCase):
        """Remove testcase *testcase* from the testsuite."""
        for case in self:
            if case == testcase:
                super().remove(case)
                self.update_statistics()

    def update_statistics(self):
        """Update test count and test time."""
        tests = errors = failures = skipped = 0
        time = 0
        for case in self:
            tests += 1
            if case.time is not None:
                time += case.time
            for entry in case.result:
                if isinstance(entry, Failure):
                    failures += 1
                elif isinstance(entry, Error):
                    errors += 1
                elif isinstance(entry, Skipped):
                    skipped += 1
        self.tests = tests
        self.errors = errors
        self.failures = failures
        self.skipped = skipped
        self.time = round(time, 3)

    def add_property(self, name: str, value: str):
        """Add a property *name* = *value* to the testsuite.

        See :class:`Property` and :class:`Properties`.
        """

        props = self.child(Properties)
        if props is None:
            props = Properties()
            self.append(props)
        prop = Property(name, value)
        props.add_property(prop)

    def add_testcase(self, testcase: TestCase):
        """Add a testcase *testcase* to the testsuite."""
        self.append(testcase)
        self.update_statistics()

    def add_testcases(self, testcases: List[TestCase]):
        """Add testcases *testcases* to the testsuite."""
        self.extend(testcases)
        self.update_statistics()

    def _add_testcase_no_update_stats(self, testcase: TestCase):
        """Add *testcase* to the testsuite (without updating statistics).

        For internal use only to avoid quadratic behaviour in merge.
        """
        self.append(testcase)

    def add_testsuite(self, suite):
        """Add a testsuite *suite* to the testsuite."""
        self.append(suite)

    def properties(self):
        """Iterate through all :class:`Property` elements in the testsuite."""
        props = self.child(Properties)
        if props is None:
            return
        for prop in props:
            yield prop

    def remove_property(self, property_: Property):
        """Remove property *property_* from the testsuite."""
        props = self.child(Properties)
        if props is None:
            return
        for prop in props:
            if prop == property_:
                props.remove(property_)

    def testsuites(self):
        """Iterate through all testsuites."""
        yield from self.iterchildren(type(self))

    def write(self, file_or_filename: Optional[Union[str, IO]] = None, *, pretty: bool = False):
        write_xml(self, file_or_filename=file_or_filename, pretty=pretty)


class JUnitXml(Element):
    """The JUnitXml root object.

    It may contain ``<TestSuites>`` or a ``<TestSuite>``.

    Attributes:
        name: Name of the testsuite if it only contains one testsuite.
        time: Time consumed by the testsuites.
        tests: Total number of tests.
        failures: Number of failed cases.
        errors: Number of cases with errors.
        skipped: Number of skipped cases.
    """

    _tag = "testsuites"
    name = Attr()
    time = FloatAttr()
    tests = IntAttr()
    failures = IntAttr()
    errors = IntAttr()
    skipped = IntAttr()

    testsuite = TestSuite

    def __init__(self, name=None):
        super().__init__(self._tag)
        self.filepath = None
        self.name = name

    def __iter__(self) -> Iterator[TestSuite]:
        return super().iterchildren(self.testsuite)

    def __len__(self):
        return len(list(self.__iter__()))

    def __add__(self, other):
        result = type(self)()
        for suite in self:
            result.add_testsuite(suite)
        for suite in other:
            result.add_testsuite(suite)
        return result

    def __iadd__(self, other):
        if other._elem.tag == "testsuites":
            for suite in other:
                self.add_testsuite(suite)
        elif other._elem.tag == "testsuite":
            suite = self.testsuite(name=other.name)
            for case in other:
                suite._add_testcase_no_update_stats(case)
            self.add_testsuite(suite)
            self.update_statistics()

        return self

    def add_testsuite(self, suite: TestSuite):
        """Add a testsuite."""
        for existing_suite in self:
            if existing_suite == suite:
                for case in suite:
                    existing_suite._add_testcase_no_update_stats(case)
                return
        self.append(suite)

    def update_statistics(self):
        """Update test count, time, etc."""
        time = 0
        tests = failures = errors = skipped = 0
        for suite in self:
            suite.update_statistics()
            tests += suite.tests
            failures += suite.failures
            errors += suite.errors
            skipped += suite.skipped
            time += suite.time
        self.tests = tests
        self.failures = failures
        self.errors = errors
        self.skipped = skipped
        self.time = round(time, 3)

    @classmethod
    def fromroot(cls, root_elem: Element) -> "JUnitXml":
        """Construct JUnit objects from an elementTree root element."""
        instance = cls()
        if root_elem.tag == "testsuite":
            testsuite_element = root_elem
            root_elem = testsuite_element.makeelement("testsuites", {})
            root_elem.append(testsuite_element)
        if not root_elem.tag == "testsuites":
            raise JUnitXmlError("Invalid format.")
        instance._elem = root_elem
        return instance

    @classmethod
    def fromstring(cls, text: Union[str, bytes]) -> "JUnitXml":
        """Construct JUnit objects from an XML string (str or bytes)."""
        root_elem = etree.fromstring(text)  # nosec
        return cls.fromroot(root_elem)

    @classmethod
    def fromfile(cls, file: Union[str, IO], parse_func=None) -> "JUnitXml":
        """
        Construct JUnit objects from an XML file.

        The ``file`` can be any of the following:

        - a file name/path
        - a file object
        - a file-like object
        - a URL using the HTTP or FTP protocol (with lxml only)
        """
        if parse_func is not None:
            tree = parse_func(file)
        else:
            tree = etree.parse(file)  # nosec
        root_elem = tree.getroot()
        instance = cls.fromroot(root_elem)
        instance.filepath = file if isinstance(file, str) else None
        return instance

    def write(self, file_or_filename: Optional[Union[str, IO]] = None, *, pretty: bool = False):
        """Write the object into a JUnit XML file.

        If `file_or_filename` is not specified, it will write to the original filename.
        If `pretty` is True, the result file will be more human friendly.
        """
        write_xml(self, file_or_filename=file_or_filename, pretty=pretty)
