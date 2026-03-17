#!/usr/bin/env python3

"""
Optional support classes for EmPy.
"""

#
# imports
#

import operator
import os
import platform
import sys

import em

#
# Filter ...
#

class Filter(em.Root):

    """An abstract filter."""

    # Meta methods.

    def follow(self):
        """Return the next filter/file-like object in the sequence, or
        None."""
        raise NotImplementedError

    def isAttached(self):
        """Is a filter/file already attached to this one?"""
        raise NotImplementedError

    def attach(self, filter):
        """Attach a filter to this one."""
        raise NotImplementedError

    def detach(self):
        """Detach a filter from its sink."""
        raise NotImplementedError

    def last(self):
        """Find the last filter in this chain."""
        this, last = self, None
        while this is not None:
            last = this
            this = this.follow()
        return last

    def _flush(self):
        """The _flush method should always flush the underlying sinks."""
        raise NotImplementedError

    # File-like methods.

    def write(self, data):
        """The write method must be implemented."""
        raise NotImplementedError

    def writelines(self, lines):
        """Write lines.  This should not need to be overriden
        for most filters."""
        for line in lines:
            self.write(line)

    def flush(self):
        """The flush method can be overridden."""
        self._flush()

    def close(self):
        """The close method must be implemented."""
        raise NotImplementedError


class NullFilter(Filter):

    """A filter that never sends any output to its sink."""

    def follow(self): return None
    def isAttached(self): return False
    def attach(self, filter): pass
    def detach(self): pass

    def write(self, data): pass
    def close(self): pass


class ConcreteFilter(Filter):

    """A concrete filter has a single sink."""

    def __init__(self):
        if self.__class__ is ConcreteFilter:
            raise NotImplementedError
        self.sink = None

    def follow(self):
        """Return the next filter/file-like object in the sequence, or None."""
        return self.sink

    def isAttached(self):
        """Is a filter/file already attached to this one?"""
        return self.sink is not None

    def attach(self, filter):
        """Attach a filter to this one."""
        if self.sink is not None:
            # If one's already attached, detach it first.
            self.detach()
        self.sink = filter

    def detach(self):
        """Detach a filter from its sink."""
        self.flush()
        self._flush() # do a guaranteed flush to just to be safe
        self.sink = None

    def last(self):
        """Find the last filter in this chain."""
        this, last = self, None
        while this is not None:
            last = this
            this = this.follow()
        return last

    def _flush(self):
        """This method should flush the concrete sink and should
        not be overridden."""
        self.sink.flush()

    def flush(self):
        self._flush()

    def close(self):
        """Close the filter.  Do an explicit flush first, then close the
        sink."""
        self.flush()
        self.sink.close()


class IdentityFilter(ConcreteFilter):

    """A filter which just sends any output straight through to its sink."""

    def write(self, data):
        self.sink.write(data)


class FunctionFilter(ConcreteFilter):

    """A filter that works simply by pumping its input through a
    function which maps strings into strings."""

    def __init__(self, function):
        super(FunctionFilter, self).__init__()
        self.function = function

    def write(self, data):
        self.sink.write(self.function(data))


class IndentFilter(ConcreteFilter):

    """Automatically indent a fixed number of spaces after every
    newline."""

    default = ' '

    def __init__(self, indent):
        super(IndentFilter, self).__init__()
        if indent is None:
            self.indent = self.default
        elif isinstance(indent, int):
            self.indent = self.default * indent
        else:
            self.indent = indent

    def write(self, data):
        self.sink.write(data.replace('\n', '\n' + self.indent))


class StringFilter(ConcreteFilter):

    """A filter that takes a lookup table (e.g., string, list, or
    dictionary) and filters any incoming data through it via
    `str.translate`."""

    def __init__(self, table):
        super(StringFilter, self).__init__()
        self.table = table

    def write(self, data):
        self.sink.write(data.translate(self.table))


class BufferedFilter(ConcreteFilter):

    """A buffered filter is one that doesn't modify the source data
    sent to the sink, but instead holds it for a time.  The standard
    variety only sends the data along when it receives a flush
    command."""

    def __init__(self):
        super(BufferedFilter, self).__init__()
        self.buffer = ''

    def write(self, data):
        self.buffer += data

    def flush(self):
        if self.buffer:
            self.sink.write(self.buffer)
        self._flush()


class SizeBufferedFilter(BufferedFilter):

    """A size-buffered filter only in fixed size chunks (excepting the
    final chunk)."""

    def __init__(self, bufferSize=em.Configuration.defaultBuffering):
        super(SizeBufferedFilter, self).__init__()
        assert bufferSize > 0
        self.bufferSize = bufferSize

    def write(self, data):
        BufferedFilter.write(self, data)
        while len(self.buffer) > self.bufferSize:
            chunk, self.buffer = self.buffer[:self.bufferSize], self.buffer[self.bufferSize:]
            self.sink.write(chunk)


class FullyBufferedFilter(BufferedFilter):

    """A maximally-buffered filter only lets its data through on the final
    close.  It ignores flushes."""

    def flush(self): pass

    def close(self):
        if self.buffer:
            BufferedFilter.flush(self)
            self.sink.close()


class DelimitedFilter(BufferedFilter):

    """A delimited filter only lets data through when it sees
    whole lines."""

    def __init__(self, delimiter):
        super(DelimitedFilter, self).__init__()
        self.delimiter = delimiter

    def write(self, data):
        BufferedFilter.write(self, data)
        chunks = self.buffer.split(self.delimiter)
        for chunk in chunks[:-1]:
            self.sink.write(chunk + self.delimiter)
        self.buffer = chunks[-1]


class LineDelimitedFilter(DelimitedFilter):

    """A line-delimited filter only lets data through when it sees
    whole lines."""

    def __init__(self, delimiter=em.NEWLINE_CHAR):
        super(LineDelimitedFilter, self).__init__(delimiter)

    def write(self, data):
        DelimitedFilter.write(self, data)
        chunks = self.buffer.split(self.delimiter)
        for chunk in chunks[:-1]:
            self.sink.write(chunk + self.delimiter)
        self.buffer = chunks[-1]


class DroppingFilter(ConcreteFilter):

    """A filter which drops any chunks that match the provided list of
    chunks to ignore."""

    def __init__(self, droppings):
        super(DroppingFilter, self).__init__()
        self.droppings = droppings

    def write(self, data):
        if data not in self.droppings:
            self.sink.write(data)


class SplittingFilter(Filter):

    """A filter that splits the output stream n ways."""

    def __init__(self, sinks):
        super(SplittingFilter, self).__init__()
        self.sinks = sinks[:]

    def _flush(self):
        for sink in self.sinks:
            sink._flush()

    def write(self, data):
        for sink in self.sinks:
            sink.write(data)

    def flush(self):
        for sink in self.sinks:
            sink.flush()

    def close(self):
        for sink in self.sinks:
            sink.close()

#
# Container
#

Container = em.Plugin # DEPRECATED

#
# AbstractHook, Hook ...
#

class AbstractHook(em.Plugin):

    """An abstract base class for implementing hooks.  All
    hook invocations are not present.  (Use this if you plan
    to systematically handle all calls with something like
    __getattr__.)"""

    pass


class Hook(AbstractHook):

    """The base class for implementing hooks.  All hook
    invocations are present and no-ops."""

    # Override these in a subclass.

    def test(self): pass # used in tests

    # Events

    def atInstallProxy(self, proxy, new): pass
    def atUninstallProxy(self, proxy, done): pass

    def atInstallFinder(self, finder): pass
    def atUninstallFinder(self, finder): pass

    def atStartup(self): pass
    def atReady(self): pass
    def atFinalize(self): pass
    def atShutdown(self): pass
    def atParse(self, scanner, locals): pass
    def atToken(self, token): pass
    def atHandle(self, info, fatal, contexts): pass
    def atInteract(self): pass

    # Contexts

    def pushContext(self, context): pass
    def popContext(self, context): pass
    def setContext(self, context): pass
    def restoreContext(self, context): pass

    # Tokens

    def preLineComment(self, comment): pass

    def preInlineComment(self, comment): pass

    def preWhitespace(self, whitespace): pass

    def preEnable(self, comment): pass
    def preDisable(self, comment): pass

    def prePrefix(self): pass

    def preString(self, string): pass
    def postString(self): pass

    def preBackquote(self, literal): pass
    def postBackquote(self, result): pass

    def preSignificator(self, key, value, stringized): pass
    def postSignificator(self): pass

    def preContextName(self, name): pass
    def postContextName(self, context): pass

    def preContextLine(self, line): pass
    def postContextLine(self, context): pass

    def preExpression(self, pairs, except_, locals): pass
    def postExpression(self, result): pass

    def preSimple(self, code, subtokens, locals): pass
    def postSimple(self, result): pass

    def preInPlace(self, code, locals): pass
    def postInPlace(self, result): pass

    def preStatement(self, code, locals): pass
    def postStatement(self): pass

    def preControl(self, type, rest, locals): pass
    def postControl(self): pass

    def preEscape(self, code): pass
    def postEscape(self): pass

    def preDiacritic(self, code): pass
    def postDiacritic(self): pass

    def preIcon(self, code): pass
    def postIcon(self): pass

    def preEmoji(self, name): pass
    def postEmoji(self): pass

    def preExtension(self, name, contents, depth, locals): pass
    def postExtension(self, result): pass

    def preCustom(self, contents): pass # DEPRECATED
    def postCustom(self, result): pass # DEPRECATED

    # Interpreter actions

    def beforeProcess(self, command, n): pass
    def afterProcess(self): pass

    def beforeInclude(self, file, locals, name): pass
    def afterInclude(self): pass

    def beforeExpand(self, string, locals, name, dispatcher): pass
    def afterExpand(self, result): pass

    def beforeFileLines(self, file, locals, dispatcher): pass
    def afterFileLines(self): pass

    def beforeFileChunks(self, file, bufferSize, locals, dispatcher): pass
    def afterFileChunks(self): pass

    def beforeFileFull(self, file, locals, dispatcher): pass
    def afterFileFull(self): pass

    def beforeImport(self, filename, module, locals, dispatcher): pass
    def afterImport(self): pass

    def beforeString(self, string, locals, dispatcher): pass
    def afterString(self): pass

    def beforeTokens(self, tokens, locals): pass
    def afterTokens(self, result): pass

    def beforeQuote(self, string): pass
    def afterQuote(self, result): pass

    def beforeEscape(self, string, more): pass
    def afterEscape(self, result): pass

    def beforeSignificate(self, key, value, locals): pass
    def afterSignificate(self): pass

    def beforeCallback(self, contents): pass # DEPRECATED
    def afterCallback(self, result): pass # DEPRECATED

    def beforeAtomic(self, name, value, locals): pass
    def afterAtomic(self): pass

    def beforeMulti(self, names, values, locals): pass
    def afterMulti(self): pass

    def beforeClause(self, catch, locals): pass
    def afterClause(self, exception, variable): pass

    def beforeDictionary(self, code, locals): pass
    def afterDictionary(self, result): pass

    def beforeSerialize(self, expression, locals): pass
    def afterSerialize(self): pass

    def beforeDefined(self, name, locals): pass
    def afterDefined(self, result): pass

    def beforeLiteral(self, text, locals): pass
    def afterLiteral(self, result): pass

    def beforeFunctional(self, code, lists, locals): pass
    def afterFunctional(self, result): pass

    def beforeEvaluate(self, expression, locals, replace): pass
    def afterEvaluate(self, result): pass

    def beforeExecute(self, statements, locals): pass
    def afterExecute(self): pass

    def beforeSingle(self, source, locals): pass
    def afterSingle(self, result): pass

    def beforeFinalizer(self, finalizer): pass
    def afterFinalizer(self): pass

#
# Callback
#

class Callback(em.Plugin):

    """A root class for any callback utilities."""

    pass

#
# Finalizer
#

class Finalizer(em.Plugin):

    """A root class for a finalizer."""

    pass

#
# Handler
#

class Handler(em.Plugin):

    """A root class for any error handler."""

    pass

#
# Document
#

class Document(em.Root):

    """A representation of an individual EmPy document, as used by a
    processor."""

    def __init__(self, ident, filename):
        self.ident = ident
        self.filename = filename
        self.significators = {}

#
# Processor
#

class Processor(em.Root):

    """An entity which is capable of processing a hierarchy of EmPy
    files and building a dictionary of document objects associated
    with them describing their significator contents."""

    defaultExtensions = ['.em']

    def __init__(self, config, factory=Document):
        self.config = config
        self.factory = factory
        self.documents = {}

    def identifier(self, pathname, filename): return filename

    def clear(self):
        self.documents = {}

    def scan(self, basename, extensions=None):
        if extensions is None:
            extensions = self.defaultExtensions
        if isinstance(extensions, em.strType):
            extensions = [extensions]
        def _noCriteria(x):
            return True
        def _extensionsCriteria(pathname, extensions=extensions):
            if extensions:
                for extension in extensions:
                    if pathname[-len(extension):] == extension:
                        return True
                return False
            else:
                return True
        self.directory(basename, _noCriteria, _extensionsCriteria, None)
        self.postprocess()

    def postprocess(self):
        pass

    def directory(self, basename, dirCriteria, fileCriteria, depth=None):
        if depth is not None:
            if depth <= 0:
                return
            else:
                depth -= 1
        filenames = os.listdir(basename)
        for filename in filenames:
            pathname = os.path.join(basename, filename)
            if os.path.isdir(pathname):
                if dirCriteria(pathname):
                    self.directory(pathname, dirCriteria, fileCriteria, depth)
            elif os.path.isfile(pathname):
                if fileCriteria(pathname):
                    documentID = self.identifier(pathname, filename)
                    document = self.factory(documentID, pathname)
                    self.file(document, open(pathname))
                    self.documents[documentID] = document

    def file(self, document, file):
        while True:
            line = file.readline()
            if not line:
                break
            self.line(document, line)

    def line(self, document, line):
        significatorRe = re.compile(self.config.significatorReString())
        match = significatorRe.search(line)
        if match:
            key, valueS = match.groups()
            valueS = valueS.strip()
            if valueS:
                value = eval(valueS)
            else:
                value = None
            document.significators[key] = value

#
# Requirement ...
#

class Requirement(em.Root):

    def check(self, details): raise NotImplementedError


class ConjunctiveRequirement(Requirement):

    def __init__(self, requirements):
        self.requirements = requirements

    def check(self, details):
        for requirement in self.requirements:
            if not requirement.check(details):
                return False
        return True


class DisjunctiveRequirement(Requirement):

    def __init__(self, requirements):
        self.requirements = requirements

    def check(self, details):
        for requirement in self.requirements:
            if requirement.check(details):
                return True
        return False


class VersionRequirement(Requirement):

    operators = {
        '=~': lambda x, y: y == x[:min(len(x), len(y))],
        '==': operator.eq,
        '!=': operator.ne,
        '>=': operator.ge,
        '>':  operator.gt,
        '<=': operator.le,
        '<':  operator.lt,
    }

    def __init__(self, operator, version):
        if isinstance(operator, em.strType):
            operator = self.operators[operator]
        self.operator = operator
        if isinstance(version, em.strType):
            version = Details.unpack(version)
        self.version = version

    def check(self, details):
        thisVersion = Details.unpack(details.getPythonVersion())
        return self.operator(thisVersion, self.version)


class ImplementationRequirement(Requirement):

    identity = lambda x: x
    operators = {
        '':  identity,
        '+': identity,
        '-': operator.not_,
    }

    def __init__(self, operator, implementation):
        if isinstance(operator, em.strType):
            operator = self.operators[operator]
        self.operator = operator
        self.implementation = implementation

    def check(self, details):
        thisImplementation = details.getPythonImplementation()
        return self.operator(self.implementation == thisImplementation)

#
# Details
#

class Details(em.Root):

    """Gather details on a running Python system for debugging
    purposes."""

    releaseFilenames = ['/etc/os-release', '/usr/lib/os-release']
    delimiter = '/'
    unsanitizableNames = set(['URL', 'ID'])

    @staticmethod
    def pack(tuple, delimiter='.'):
        """Pack a sequence of ints into a string."""
        return delimiter.join([str(x) for x in tuple])

    @staticmethod
    def unpack(string, delimiter='.'):
        """Unpack a string into a sequence of ints."""
        def convert(x):
            if x.isdigit():
                return int(x)
            else:
                return x
        return tuple([convert(x) for x in string.split(delimiter)])

    def __init__(self, useSanitization=False):
        self.useSanitization = useSanitization
        self.data = {}
        self._system = None
        self._os = None
        self._machine = None
        self._implementation = None
        self._version = None
        self._framework = None
        self._context = None

    def __contains__(self, key):
        return key in self.data

    def __getitem__(self, key):
        return self.data[key]

    def __iter__(self):
        items = list(self.items())
        items.sort()
        return iter(items)

    def empty(self):
        return len(self.data) == 0

    # System/OS information.

    def transformName(self, value, pairs, defaultFormat='[%s]', blank='?'):
        """Find the prefix of the value in the pairs and return
        the resulting name."""
        if not value:
            return blank
        for prefix, name in pairs:
            if value.startswith(prefix):
                return name
        return defaultFormat % value

    def getSystemName(self):
        """Get a nice name for the system this interpreter is
        running on.  Cached."""
        if self._system is None:
            self._system = platform.system()
        assert self._system is not None
        return self._system

    def getOSName(self):
        """Get a nice name for the OS this interpreter is
        running on.  Cached."""
        if self._os is None:
            PAIRS = [
                ('posix', 'POSIX'),
                ('nt', 'NT'),
                ('java', 'Java'),
            ]
            self._os = self.transformName(os.name, PAIRS)
        assert self._os is not None
        return self._os

    # Python information.

    def getPythonImplementation(self):
        """Get the name of this implementation.  Cached."""
        if self._implementation is None:
            DEFAULT = 'CPython'
            try:
                self._implementation = platform.python_implementation()
            except AttributeError:
                # If the module does not contain the function, then it's old
                # enough that we're surely the default.  Fall through.
                pass
            if not self._implementation:
                self._implementation = DEFAULT
        assert self._implementation is not None
        return self._implementation

    def checkPythonImplementation(self, choices):
        """Check the Python implementation against the sequence of
        choices."""
        return self.getPythonImplementation() in choices

    def getPythonVersion(self):
        """Get the version of Python.  Cached."""
        if self._version is None:
            try:
                self._version = platform.python_version()
            except AttributeError:
                # Stackless Python 2.4 raises an internal error when this is
                # called since sys.version is in an unexpected format.
                self._version = self.pack(sys.version_info[:3])
        assert self._version is not None
        return self._version

    def parsePythonVersion(self, version):
        """Parse a Python version into a tuple of ints."""
        if isinstance(version, em.strType):
            version = Details.unpack(version)
        return version

    def checkPythonVersion(self, minimum, maximum=None, closed=True):
        """Check whether or not this Python version is greater than or
        equal to the minimum, and if present, less than or equal to (or
        less than) the maximum."""
        version = sys.version_info[:3]
        minimum = self.parsePythonVersion(minimum)
        if version < minimum:
            return False
        if maximum is not None:
            maximum = self.parsePythonVersion(maximum)
            if len(version) > len(maximum):
                # Trim the version down so it's no more detailed than the
                # maximum.
                version = version[:len(maximum)]
            if closed:
                test = version > maximum
            else:
                test = version >= maximum
            if test:
                return False
        return True

    def getMachineType(self):
        """Get the machine name.  Cached."""
        if self._machine is None:
            self._machine = platform.machine()
            if not self._machine:
                self._machine = '?'
        assert self._machine is not None
        return self._machine

    def getFramework(self):
        """Get the framework and version this interpreter is
        running under as a 2-tuple, or None.  Cached."""
        if self._framework is None:
            implementation = self.getPythonImplementation()
            if implementation == 'PyPy':
                # The PyPy version is in the second line of the version string.
                lines = sys.version.split('\n')
                second = lines[1]
                if second.startswith('['):
                    second = second[1:]
                fields = second.split()
                self._framework = 'PyPy', fields[1]
            elif implementation == 'Jython':
                from java.lang import System
                self._framework = 'JDK', System.getProperty('java.version')
            elif implementation == 'IronPython':
                from System import Environment
                self._framework = '.NET', Environment.Version
            else:
                # CPython
                try:
                    fields = platform.python_compiler().split()
                    pair = fields[:2]
                    rest = fields[3:]
                    self._framework = tuple(pair)
                except AttributeError:
                    # Stackless Python 2.4 raises an internal error when this
                    # is called since sys.version is in an unexpected format.
                    lines = sys.version.splitlines()
                    compiler = lines[-1]
                    if compiler.startswith('[') and compiler.endswith(']'):
                        compiler = compiler[1:-1]
                    self._framework = tuple(compiler.split(' ', 1))
        assert self._framework is not None
        return self._framework

    def getContext(self):
        """Get the context that this interpreter is running in as a
        string.  Note: This is not always possible.  Cached."""
        release = platform.release().lower()
        if self._context is None:
            if 'pyrun_config' in sys.modules:
                self._context = 'PyRun'
            elif 'Stackless' in sys.version:
                fields = sys.version.split(None, 3)
                self._context = fields[1] + '/' + fields[2]
            elif 'ActiveState' in sys.copyright:
                self._context = 'ActiveState'
            elif ('microsoft' in release or 'wsl' in release or
                  'WSL_DISTRO_NAME' in os.environ):
                version = '?'
                # It's WSL.
                if 'wsl2' in release:
                    version = 2
                else:
                    version = 1
                self._context = 'WSL/' + str(version)
            else:
                self._context = ''
            assert self._context is not None
        return self._context

    # Details.

    def sanitize(self, suffix):
        """Normalize this key suffix if necessary."""
        if self.useSanitization:
            if '_' in suffix:
                words = suffix.split('_')
                for i, word in enumerate(words):
                    if i == 0:
                        words[i] = word.lower()
                    elif word in self.unsanitizableNames:
                        words[i] = word
                    else:
                        words[i] = word.capitalize()
                suffix = ''.join(words)
            if suffix.isupper():
                suffix = suffix.lower()
            if suffix[0].isupper():
                suffix = suffix[0].lower() + suffix[1:]
        return suffix

    def key(self, prefix, suffix):
        """Make a key out of this prefix and suffix."""
        if prefix and suffix:
            return prefix + self.delimiter + self.sanitize(suffix)
        elif prefix:
            return prefix
        else: # if suffix:
            return self.normalize(suffix)

    def has(self, prefix, suffix):
        """Is this key present?"""
        return self.key(prefix, suffix) in self.data

    def get(self, prefix, suffix, default=None):
        """Get the value for this key."""
        key = self.key(prefix, suffix)
        if key in self:
            return self.data[key]
        else:
            return default

    def set(self, prefix, suffix, value):
        """Set a key/value pair."""
        self.data[self.key(prefix, suffix)] = value

    def wrap(self, prefix, suffixes, func):
        """Replace an existing entry by passing it through a
        function."""
        if not isinstance(suffixes, tuple):
            suffixes = (suffixes,)
        for suffix in suffixes:
            key = self.key(prefix, suffix)
            if key in self.data:
                self.data[key] = func(self.data[key])

    def accumulate(self, prefix, suffixes, object, name,
                   args=None, attr=None, func=None, kwargs=None, delim='/'):
        """Accumulate the results of a method or attribute in a
        dictionary.  If the argument is None, just return the
        attribute; if not, treat it as a function and call it
        with the args and kwargs.  If attr is not None, access
        that named attribute on the result before returning.  If
        func is not None, pass the result through the function
        before storing."""
        assert name is not None
        if prefix is None:
            prefix = ''
        result = getattr(object, name, None)
        if result is None:
            return
        error = None
        if not isinstance(suffixes, tuple):
            suffixes = (suffixes,)
        try:
            if args is not None:
                # It's a function call.
                if kwargs is None:
                    kwargs = {}
                result = result(*args, **kwargs)
            if result is None:
                return
            if not isinstance(result, tuple):
                # If it's not a tuple, make it one for uniformity.
                result = (result,)
            if len(suffixes) == 1 and len(result) > 1:
                # If we have only one key but multiple values, then it's a
                # single key with a value that's a tuple.  Join up the tuple
                # with the delimiter.
                result = (delim.join(result),)
            for suffix, value in zip(suffixes, result):
                # Now iterate over the suffixes/values:
                if suffix is None:
                    # Skip suffixes that are None.
                    continue
                if attr is not None:
                    # We want an attribute of it.
                    value = getattr(value, attr)
                if func is not None:
                    # If there's a function wrapper, call it.
                    value = func(value)
                assert suffix not in self.data
                self.set(prefix, suffix, value)
        except:
            # Some weird things can happen when calling/accessing the result:
            #
            # - Jython 2.5 raises an internal AttributeError when accessing
            #   platform.architecture.
            # - IronPython 2.7 has not implemented platform.architecture.
            # - Some older versions of IronPython 2.7 raise an ImportError when
            #   calling platform.platform which is then bizarrely reported as a
            #   TypeError with no traceback by the executive.
            #
            # Set the appropriate values with a question mark and the name of
            # the exception which occurred to make it clear what happened.
            type, error, traceback = sys.exc_info()
            value = '?' + error.__class__.__name__
            for suffix in suffixes:
                self.set(prefix, suffix, value)

    # Requirements.

    def parseRequirement(self, string):
        DISJUNCTION = ';'
        VERSION_OPERATORS = '><=!~'
        IMPLEMENTATION_OPERATORS = '-'
        strings = string.split(';')
        results = []
        for string in strings:
            assert string
            if string[0] in VERSION_OPERATORS:
                # It's a version requirement.
                i = 0
                while i < len(string) and string[i] in VERSION_OPERATORS:
                    i += 1
                op, ver = string[:i], string[i:].strip()
                results.append(VersionRequirement(op, ver))
            else:
                # It's an implementation requirement.
                i = 0
                while i < len(string) and string[i] in IMPLEMENTATION_OPERATORS:
                    i += 1
                op, impl = string[:i], string[i:].strip()
                results.append(ImplementationRequirement(op, impl))
        if len(results) == 1:
            return results[0]
        else:
            return DisjunctiveRequirement(results)

    def parseRequirements(self, filename):
        requirements = []
        file = open(filename)
        try:
            for line in file.readlines():
                line = line.strip()
                if not line:
                    continue
                elif line.startswith('#'):
                    continue
                requirement = self.parseRequirement(line)
                requirements.append(requirement)
        finally:
            file.close()
        return requirements

    def checkRequirements(self, requirements):
        if isinstance(requirements, em.strType):
            requirements = self.parseRequirements(requirements)
        for requirement in requirements:
            if not requirement.check(self):
                return False
        return True

    # Details.

    def getBasicDetails(self):
        """Collect basic details."""
        self.accumulate('basic', 'system', self, 'getSystemName', ())
        self.accumulate('basic', 'os', self, 'getOSName', ())
        self.accumulate('basic', 'machine', self, 'getMachineType', ())
        self.accumulate('basic', 'implementation', self, 'getPythonImplementation', ())
        self.accumulate('basic', 'version', self, 'getPythonVersion', ())
        self.accumulate('basic/framework', ('name', 'version'), self, 'getFramework', ())
        self.accumulate('basic', 'context', self, 'getContext', ())

    def getPythonDetails(self):
        """Collect details about this interpreter.  Use the
        platform module if available when the flag is set to
        true."""
        self.accumulate('python', 'implementation', self, 'getPythonImplementation', ())
        self.accumulate('python', 'version', self, 'getPythonVersion', ())
        self.accumulate('python', 'branch', platform, 'python_branch', ())
        self.accumulate('python/build', ('name', 'date'), platform, 'python_build', ())
        self.accumulate('python', 'compiler', platform, 'python_compiler', ())
        self.accumulate('python', 'revision', platform, 'python_revision', ())

    def getSystemDetails(self):
        """Collect details about the system settings on this
        interpreter."""
        self.accumulate('system/api', 'flags', sys, 'abiflags')
        self.accumulate('system/api', 'version', sys, 'api_version')
        self.accumulate('system', 'byteorder', sys, 'byteorder')
        self.accumulate('system', 'copyright', sys, 'copyright', func=repr)
        self.accumulate('system/filesystem', 'encoding', sys, 'getfilesystemencoding', ())
        self.accumulate('system/filesystem', 'errors', sys, 'getfilesystemencodeerrors', ())
        self.accumulate('system/float', ('max', 'max_exp', None, 'min', 'min_exp', None, 'dig', 'mant_dig', 'epsilon', 'radix', 'rounds'), sys, 'float_info')
        self.accumulate('system/hash', ('width', 'modulus', 'inf', 'nan', 'imag', 'algorithm', 'bits', 'seed_bits'), sys, 'hash_info')
        self.wrap('system/hash', 'modulus', hex)
        self.accumulate('system/int', ('bits', 'sizeof', 'default_max', 'check_threshold'), sys, 'int_info')
        self.accumulate('system/path', 'executable', sys, 'executable')
        self.accumulate('system/path', 'library', sys, 'platlibdir')
        self.accumulate('system/path', 'prefix', sys, 'prefix')
        self.accumulate('system', 'platform', sys, 'platform')
        self.accumulate('system', 'size/max', sys, 'maxsize', func=hex)
        self.set('system', 'unicode/build', ['wide', 'narrow'][em.narrow])
        self.accumulate('system', 'unicode/max', sys, 'maxunicode', func=hex)
        self.accumulate('system/version', 'hex', sys, 'hexversion', func=hex)
        self.accumulate('system/version', 'str', sys, 'version', func=repr)

    def getPlatformDetails(self):
        """Collect details about this platform.  Use the
        platform module if available when the flag is set to true."""
        self.accumulate('platform/architecture', ('bits', 'linkage'), platform, 'architecture', ())
        self.accumulate('platform', 'machine', platform, 'machine', ())
        self.accumulate('platform', 'name', platform, 'platform', ())
        self.accumulate('platform', 'node', platform, 'node', ())
        self.accumulate('platform', 'processor', platform, 'processor', ())
        self.accumulate('platform', 'release', platform, 'release', ())
        self.accumulate('platform', 'system', platform, 'system', ())
        self.accumulate('platform/thread', ('implementation', 'lock', 'version'), sys, 'thread_info')
        self.accumulate('platform', 'version', platform, 'version', ())

    def getReleaseDetails(self, system=None):
        """Collect details about the given system release, or the
        running one if None."""
        if system is None:
            system = self.getSystemName()
        methodName = 'getReleaseDetails_' + system
        method = getattr(self, methodName, None)
        if method is not None:
            method()

    # Release details specializations.

    def getReleaseDetails_Linux(self):
        """Collect details about the Linux release."""
        PREFIX = 'release/linux'
        file = None
        for filename in self.releaseFilenames:
            try:
                file = open(filename, 'r')
                break
            except IOError:
                pass
        if file is None:
            return self.data
        try:
            while True:
                line = file.readline()
                if not line:
                    break
                if line.endswith('\n'):
                    line = line[:-1]
                key, value = line.split('=', 1)
                if value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                self.set(PREFIX, key, value)
        finally:
            file.close()

    def getReleaseDetails_Darwin(self):
        """Collect details about the Darwin release."""
        PREFIX = 'release/darwin'
        PROGRAM = 'sw_vers'
        FILENAME = '/tmp/sw_vers_%d.out' % os.getpid()
        waitStatus = os.system('%s > %s 2> /dev/null' % (PROGRAM, FILENAME))
        exitCode = waitStatus >> 8
        if exitCode != 0:
            return
        file = None
        try:
            try:
                file = open(FILENAME, 'r')
                for line in file:
                    if line.endswith('\n'):
                        line = line[:-1]
                    key, value = line.split(':', 1)
                    key = key.strip()
                    value = value.strip()
                    self.set(PREFIX, key, value)
            except OSError:
                pass
        finally:
            if file is not None:
                file.close()
            try:
                os.remove(FILENAME)
            except OSError:
                pass

    def getReleaseDetails_Windows(self):
        """Collect details about the Windows release."""
        PREFIX = 'release/windows'
        PRODUCT_TYPES = {
            1: 'VER_NT_WORKSTATION',
            2: 'VER_NT_DOMAIN_CONTROLLER',
            3: 'VER_NT_SERVER',
        }
        self.accumulate(PREFIX, 'dllhandle', sys, 'dllhandle', func=hex)
        self.accumulate(PREFIX, 'registry', sys, 'winver')
        self.accumulate(PREFIX, ('major', 'minor', 'build', 'platform', 'service_pack'), sys, 'getwindowsversion', ())
        self.accumulate(PREFIX, 'suite_mask', sys, 'getwindowsversion', (), attr='suite_mask', func=hex)
        self.accumulate(PREFIX, 'product_type', sys, 'getwindowsversion', (), attr='product_type', func=lambda x: PRODUCT_TYPES.get(x, x))
        self.accumulate(PREFIX, 'platform_version', sys, 'getwindowsversion', (), attr='platform_version', func=lambda x: '.'.join([em.nativeStr(e) for e in x]))

    # Summary.

    def classify(self, version):
        """Classify a release by version string."""
        PAIRS = [
            ('a', 'alpha'),
            ('b', 'beta'),
            ('d', 'development'),
            ('g', 'gamma'),
            ('h', 'adhoc'),
            ('r', 'revision'),
            ('RC', 'release candidate'),
            ('v', 'version'),
            ('x', 'development'),
        ]
        # First, normalize it.
        if '-' in version and '.' not in version:
            version = version.replace('-', '.')
        major = version.split('.')[0]
        if len(major) == 4 and major.isdigit():
            # If the major version looks like a year, it's a preview version.
            return 'preview'
        for key, name in PAIRS:
            if key in version:
                return name
        return None

    def collect(self, level):
        """Collect details."""
        if level >= em.Version.BASIC:
            self.getBasicDetails()
        if level >= em.Version.PYTHON:
            self.getPythonDetails()
        if level >= em.Version.SYSTEM:
            self.getSystemDetails()
        if level >= em.Version.PLATFORM:
            self.getPlatformDetails()
        if level >= em.Version.RELEASE:
            self.getReleaseDetails()

    def show(self, level=em.Version.INFO, prelim="", postlim="", file=None):
        """Show details."""
        if file is None:
            file = sys.stdout
        write = file.write
        if prelim:
            write(prelim)
        write("%s version %s" % (em.__project__, em.__version__))
        classification = self.classify(em.__version__)
        if classification:
            write(" [%s]" % classification)
        if level >= em.Version.INFO:
            write(", in %s/%s" % (
                self.getPythonImplementation(), self.getPythonVersion()))
            context = self.getContext()
            if context:
                write(", as %s" % context)
            write(", on %s (%s)" % (
                self.getSystemName(), self.getOSName()))
            write(", with %s" % self.getMachineType())
            framework = self.getFramework()
            if framework:
                write(", under %s/%s" % framework)
            if em.compat:
                write(" (%s)" % ', '.join(em.compat))
        if postlim:
            write(postlim)
        if self.empty():
            self.collect(level)
        if level >= em.Version.DATA:
            if self.data:
                write("Details:\n")
                items = list(self.data.items())
                items.sort()
                for key, value in items:
                    if value is None or value == '':
                        value = '--'
                    write("- %s: %s\n" % (key, value))
            else:
                write("(No details available.)\n")

#
# main
#

def main():
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        try:
            level = int(arg)
        except ValueError:
            level = getattr(em.Version, arg)
    else:
        level = em.Version.ALL
    details = Details()
    details.show(level, prelim="Welcome to ", postlim=".\n")

if __name__ == '__main__': main()
