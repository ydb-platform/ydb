#!/usr/bin/env python3

"""
A system for processing Python via markup embeded in text.
"""

__project__ = "EmPy"
__program__ = "empy"
__module__ = "em"
__version__ = "4.2.1"
__url__ = "http://www.alcyone.com/software/empy/"
__author__ = "Erik Max Francis <max@alcyone.com>"
__contact__ = "software@alcyone.com"
__copyright__ = "Copyright (C) 2002-2026 Erik Max Francis"
__license__ = "BSD"

#
# imports
#

import codecs
import copy
import getopt
import os
import platform
import re
import sys
import unicodedata

#
# compatibility
#

# Initializes the following global names based on Python 2.x vs. 3.x:
#
# - major             Detected major Python version
# - minor             Detected minor Python version
# - compat            List of Python backward-compatibility features applied
# - narrow            Was Python built with narrow Unicode (UTF-16 natively)?
# - modules           Is EmPy module support feasible?
# - nativeStr         The native str type (str in Python 3.x; str in Python 2.x)
# - str (_unicode)    The str type (str in Python 3.x; unicode in Python 2.x)
# - bytes (_str)      The bytes type (bytes in Python 3.x; str in Python 2.x)
# - strType           The str type (Python 3.x) or the bytes and str types (2.x)
# - chr               The chr function (unichr in Python 2.x)
# - input             The input function (raw_input in Python 2.x)
# - evalFunc          The eval function
# - execFunc          The exec function
# - binaryOpen        The codecs.open function, or open (Python 3.14+)
# - BaseException     The base exception class for all exceptions
# - FileNotFoundError FileNotFoundError (= IOError in Python < 3.3)
# - StringIO          The StringIO class
# - isIdentifier      Is this string a valid Python identifier?
# - uliteral          Return a version-specific wide Unicode literal ('\U...')
# - toString          Convert an arbitrary object to a Unicode-compatible string

# The major version of Python (2 or 3).
major = sys.version_info[0]
# The minor version of Python.
minor = sys.version_info[1]
# A list of Python backward-compatibility features which were applied.
compat = []
# The native str type/function.
nativeStr = str
# The eval function.
evalFunc = eval
if major == 2:
    # We're using Python 2.x!  Make sure there are Python 3.x-like names for
    # the basic types and functions; hereafter, use str for unicode and bytes
    # for str, respectively.
    bytes = _str = str
    str = _unicode = unicode
    strType = (bytes, str)
    chr = unichr
    input = raw_input
    # In Python 2.x, binaryOpen defers to codecs.open.
    binaryOpen = codecs.open
    # In Python 2.x, StringIO is contained in the cStringIO module.
    try:
        from cStringIO import StringIO
    except ImportError:
        # If cStringIO is not present for some reason, try to use the slower
        # StringIO module.
        from StringIO import StringIO
    if minor < 5:
        # Starting with Python 2.5, a new BaseException class serves as the
        # base class of all exceptions; prior to that, it was just Exception.
        # So create a name for it if necessary.
        BaseException = Exception
        compat.append('BaseException')
    # Python 2.x did not have a FileNotFoundError.
    FileNotFoundError = IOError
    compat.append('FileNotFoundError')
    # In Python 2.x, exec is a statement; in Python 3.x, it's a function.  Make
    # a new function that will simulate the Python 3.x form but work in Python
    # 2.x.
    def execFunc(code, globals=None, locals=None):
        if globals is None:
            exec("""exec code""")
        else:
            if locals is None:
                exec("""exec code in globals""")
            else:
                exec("""exec code in globals, locals""")
    def toString(value):
        """Convert value to a (Unicode) string."""
        if isinstance(value, _unicode):
            # Good to go.
            return value
        elif isinstance(value, _str):
            # It's already a str (bytes), convert it to a unicode (str).
            return _unicode(value)
        else:
            # In Python 2.x, __str__ returns a str, not a unicode.  Convert the
            # object to a str (bytes), then convert it to a unicode (str).
            return _unicode(_str(value))
    def isIdentifier(string, first=True):
        """Is this string a valid identifier?  If first is true, make
        sure the first character is a valid starting identifier character."""
        for char in string:
            if first:
                if not (char.isalpha() or char == '_'):
                    return False
                first = False
            else:
                if not (char.isalpha() or char.isdigit() or char == '_'):
                    return False
        return True
    def uliteral(i):
        """Return a wide Unicode string literal."""
        return r"u'\U%08x'" % i
elif major >= 3:
    # We're using Python 3.x!  Add Python 2.x-like names for the basic types
    # and functions.  The name duplication is so that there will always be a
    # definition of both str and types in the globals (as opposed to the
    # builtins).
    bytes = _str = bytes
    str = _unicode = str
    strType = str
    chr = chr
    input = input
    # In Python 3.x, the module containing StringIO is io.
    from io import StringIO
    # codecs.open is deprecated starting with Python 3.14.  Use open instead.
    if minor >= 14:
        binaryOpen = open
        compat.append('!codecs.open')
    else:
        binaryOpen = codecs.open
    # Python 3.x prior to 3.3 did not have a FileNotFoundError.
    if minor < 3:
        FileNotFoundError = IOError
        compat.append('FileNotFoundError')
    # The callable builtin was removed from Python 3.0 and reinstated in Python
    # 3.2, but we need it.
    if minor < 2:
        def callable(object):
            return getattr(object, '__call__', None) is not None
        compat.append('callable')
    # In Python 3.x, exec is a function, but attempting to reference it as such
    # in Python 2.x generates an error.  Since this needs to also compile in
    # Python 2.x, defer the evaluation past the parsing phase.
    try:
        execFunc = evalFunc('exec')
    except NameError:
        execFunc = evalFunc('__builtins__.exec')
    def toString(value):
        """Convert value to a (Unicode) string."""
        return str(value)
    def isIdentifier(string, first=True):
        """Is this string a valid identifier?  If first is true, make
        sure the first character is a valid starting identifier character."""
        if first:
            return string.isidentifier()
        else:
            return ('_' + string).isidentifier()
    def uliteral(i):
        """Return a wide Unicode string literal."""
        return r"'\U%08x'" % i
# Was this Python interpreter built with narrow Unicode?  That is, does it use
# a UTF-16 encoding (with surrgoate pairs) vs. UTF-32 internally?
if hasattr(sys, 'maxunicode'):
    narrow = sys.maxunicode < 0x10000
else:
    narrow = len(evalFunc(uliteral(0x10000))) > 1
if narrow:
    # Narrow Python builds will raise a ValueError when calling chr (unichr) on
    # a code point value outside of the Basic Multilingual Plane (U+0000
    # .. U+FFFF).  See if it needs to be replaced.
    _chr = chr
    if major == 2:
        if minor >= 6:
            # Versions 2.6 and up can use this struct/decode trick.
            compat.append('chr/decode')
            def chr(i, _chr=_chr):
                if i < 0x10000:
                    return _chr(i)
                else:
                    import struct
                    try:
                        return struct.pack('i', i).decode('utf-32')
                    except UnicodeDecodeError:
                        raise ValueError("chr() arg not in range")
        else:
            # Earlier versions (2.5 and below) need to evaluate a literal.
            compat.append('chr/uliteral')
            def chr(i, _chr=_chr):
                if i < 0x10000:
                    return _chr(i)
                else:
                    try:
                        return evalFunc(uliteral(i))
                    except (SyntaxError, UnicodeDecodeError):
                        raise ValueError("chr() arg not in range")
    compat.append('narrow')

# Is EmPy module support feasible on this interpreter?
modules = True
if (major, minor) < (3, 4):
    # importlib architecture didn't exist before Python 3.4.
    modules = False
if ('python_implementation' in platform.__dict__ and
    platform.python_implementation() == 'IronPython'):
    # importlib architecture doesn't work right in IronPython.
    modules = False
if not modules:
    compat.append('!modules')

#
# constants
#

# Character information.
UNDERSCORE_CHAR = '_'
DOT_CHAR = '.'
BACKSLASH_CHAR = '\\'
CARET_CHAR = '^'
STROKE_CHAR = '|'
OCTOTHORPE_CHAR = '#'
PLUS_CHAR = '+'
MINUS_CHAR = '-'
ASTERISK_CHAR = '*'
QUESTION_CHAR = '?'
EXCLAMATION_CHAR = '!'
PERCENT_CHAR = '%'
OPEN_PARENTHESIS_CHAR = '('
OPEN_BRACE_CHAR = '{'
OPEN_BRACKET_CHAR = '['
OPEN_ANGLE_CHAR = '<'
BACKQUOTE_CHAR = '`'
DOLLAR_CHAR = '$'
COLON_CHAR = ':'
WHITESPACE_CHARS = ' \t\v\f\r\n'
LITERAL_CHARS = '()[]{}<>\\\'\"?'
CARRIAGE_RETURN_CHAR = '\r'
NEWLINE_CHAR = '\n'
OPENING_CHARS = '([{<'
DUPLICATIVE_CHARS = '([{'
PHRASE_OPENING_CHARS = '(['
CLOSING_CHARS = ')]}>'
QUOTE_CHARS = '\'\"'
ENDING_CHAR_MAP = {'(': ')', '[': ']', '{': '}', '<': '>'}

# Environment variable names.
OPTIONS_ENV = 'EMPY_OPTIONS'
CONFIG_ENV = 'EMPY_CONFIG'
PREFIX_ENV = 'EMPY_PREFIX'
PSEUDO_ENV = 'EMPY_PSEUDO'
FLATTEN_ENV = 'EMPY_FLATTEN'
RAW_ERRORS_ENV = 'EMPY_RAW_ERRORS'
INTERACTIVE_ENV = 'EMPY_INTERACTIVE'
DELETE_ON_ERROR_ENV = 'EMPY_DELETE_ON_ERROR'
NO_PROXY_ENV = 'EMPY_NO_PROXY'
BUFFERING_ENV = 'EMPY_BUFFERING'
BINARY_ENV = 'EMPY_BINARY'
ENCODING_ENV = 'EMPY_ENCODING'
INPUT_ENCODING_ENV = 'EMPY_INPUT_ENCODING'
OUTPUT_ENCODING_ENV = 'EMPY_OUTPUT_ENCODING'
ERRORS_ENV = 'EMPY_ERRORS'
INPUT_ERRORS_ENV = 'EMPY_INPUT_ERRORS'
OUTPUT_ERRORS_ENV = 'EMPY_OUTPUT_ERRORS'

# A regular expression string suffix which will match singificators; prepend
# the interpreter prefix to make a full regular expression string.  A
# successful match will yield six groups, arranged in two clusters of three.
# Each cluster contains the following three named groups in index order:
#
# - string: A `!` to represent a stringized significator, or blank
# - key: The significator key
# - value: The significator value; can be blank
#
# The value should be stripped before being tested.  If it is blank and if the
# significator is not stringized, then the resulting significator value will be
# None.
#
# The first cluster (with group names ending in 2: string2, key2, value2)
# matches multiline significators; the second (with group names ending in 1:
# string1, key1, value1) matches singleline ones.  Only one of these clusters
# will be set.
#
# The regular expression should be compiled with the following flags:
# re.MULTILINE|re.DOTALL|re.VERBOSE.  To get a compiled regular expression
# object for a given config, call `config.significatorRe()`.
SIGNIFICATOR_RE_STRING_SUFFIX = r"""
                    # Flags: re.MULTILINE|re.DOTALL|re.VERBOSE
%                   # Opening `%`
(?:                 # Start non-grouping choice: Multiline
 %                  #  A second `%`
 (?P<string2>!?)    #  An optional `!` [string2]
 [ \t\v\f\r]*       #  Zero or more non-newline whitespace
 (?P<key2>\S+)      #  One or more non-whitespace [key2]
 \s*                #  Zero or more whitespace
 (?P<value2>[^%]*)  #  Zero or more non-`%` characters [value2]
 \s*                #  Zero or more whitespace
 %%                 #  Closing `%%`
|                   # Next non-grouping choice: Singleline
 (?P<string1>!?)    #  An optional `!` [string1]
 [ \t\v\f\r]*       #  Zero or more non-newline whitespace
 (?P<key1>\S+)      #  One or more non-whitespace [key1]
 [ \t\v\f\r]*       #  Zero or more non-newline whitespace
 (?P<value1>[^\n]*) #  Zero or more non-newline characters [value1]
 [ \t\v\f\r]*       #  Zero or more non-newline whitespace
)                   # End choice
$                   # End string
"""

#
# Error ...
#

class Error(Exception):

    def __init__(self, *args, **kwargs):
        # super does not work here in Python 2.4.
        Exception.__init__(self, *args)
        self.__dict__.update(kwargs)

class ConsistencyError(Error): pass
class ProxyError(ConsistencyError): pass
class DiversionError(Error): pass
class FilterError(Error): pass
class CoreError(Error): pass
class ExtensionError(Error): pass
class StackUnderflowError(Error, IndexError): pass
class UnknownEmojiError(Error, KeyError): pass
class StringError(Error): pass
class InvocationError(Error): pass

class ConfigurationError(Error): pass
class CompatibilityError(ConfigurationError): pass
class ConfigurationFileNotFoundError(ConfigurationError, FileNotFoundError): pass

class ParseError(Error): pass
class TransientParseError(ParseError): pass

#
# Flow ...
#

class Flow(Exception): pass
class ContinueFlow(Flow): pass
class BreakFlow(Flow): pass

#
# Root
#

class Root(object):

    """The root class of all EmPy class hierarchies.  It defines a default
    __repr__ method which will work appropriately whether or not the subclass
    defines a __str__ method.  Very old versions of Python 2.x won't
    print the proper __str__ form, but so be it."""

    def __repr__(self):
        if (hasattr(self, '__str__') and hasattr(type(self), '__str__') and
            getattr(type(self), '__str__') is not getattr(Root, '__str__')):
            return '%s(%s)' % (self.__class__.__name__, toString(self))
        else:
            return '<%s @ 0x%x>' % (self.__class__.__name__, id(self))

#
# EmojiModuleInfo
#

class EmojiModuleInfo(Root):

    """The abstraction of an emoji module that may or may not be available
    for usage."""

    def __init__(self, name, attribute, format, capitalization, delimiters,
                 *extra):
        self.name = name
        self.attribute = attribute
        self.format = format
        self.capitalization = capitalization
        self.delimiters = delimiters
        self.extra = extra
        self.ok = False
        self.initialize()

    def __str__(self):
        return self.name

    def initialize(self):
        """Attempt to initialize this emoji module.  Set ok to true if
        successful."""
        try:
            self.module = __import__(self.name)
            self.function = getattr(self.module, self.attribute, None)
        except ImportError:
            self.module = self.function = None
        if self.function is not None:
            self.ok = True

    def substitute(self, text):
        """Substitute text using this module.  Return None if
        unsuccessful."""
        assert self.ok
        wrapped = self.format % text
        try:
            result = self.function(wrapped)
        except KeyError:
            return None
        if wrapped == result:
            return None
        else:
            return result

Module = EmojiModuleInfo # DEPRECATED

#
# Configuration
#

class Configuration(Root):

    """The configuration encapsulates all the defaults and parameterized
    behavior of an interpreter.  When created, an interpreter is assigned
    a configuration; multiple configurations can be shared between different
    interpreters.  To override the defaults of an interpreter, create a
    Configuration instance and then modify its attributes."""

    # Constants.

    version = __version__
    bangpath = '#!'
    unknownScriptName = '<->'
    significatorReStringSuffix = SIGNIFICATOR_RE_STRING_SUFFIX
    fullBuffering = -1
    noBuffering = 0
    lineBuffering = 1
    unwantedGlobalsKeys = [None, '__builtins__'] # None = pseudomodule name
    unflattenableGlobalsKeys = ['globals']
    priorityVariables = ['checkVariables']
    ignoredConstructorArguments = []
    emojiModuleInfos = [
        # module name, attribute name, wrapping format, capitaliztion, delimiters
        ('emoji', 'emojize', ':%s:', 'lowercase', 'underscores'),
        ('emojis', 'encode', ':%s:', 'lowercase', 'underscores'),
        ('emoji_data_python', 'replace_colons', ':%s:', 'lowercase', 'underscores'),
        ('unicodedata', 'lookup', '%s', 'both', 'spaces'),
    ]

    # Defaults.

    defaultName = 'default'
    defaultPrefix = '@'
    defaultPseudomoduleName = 'empy'
    defaultModuleExtension = '.em'
    defaultRoot = '<root>'
    defaultBuffering = 16384
    defaultContextFormat = '%(name)s:%(line)d:%(column)d'
    defaultNormalizationForm = 'NFKC'
    defaultErrors = 'strict'
    defaultConfigVariableName = '_'
    defaultStdout = sys.stdout
    defaultSuccessCode = 0
    defaultFailureCode = 1
    defaultUnknownCode = 2
    defaultSkipCode = 111
    defaultSignificatorDelimiters = ('__', '__')
    defaultEmptySignificator = None
    defaultAutoValidateIcons = True
    defaultEmojiModuleNames = [
        'emoji',
        'emojis',
        'emoji_data_python',
        'unicodedata',
    ]
    defaultNoEmojiModuleNames = [
        'unicodedata',
    ]

    # Statics.

    baseException = BaseException
    topLevelErrors = (ConfigurationError,)
    fallThroughErrors = (SyntaxError,)
    proxyWrapper = None
    useContextFormatMethod = None
    emojiModules = None
    iconsSignature = None
    verboseFile = sys.stderr
    factory = None
    ignorableErrorAttributes = [
        # Python atttributes
        'args', 'message', 'add_note', 'characters_written', 'with_traceback',
        # Jython attributes
        'addSuppressed', 'cause', 'class', 'equals', 'fillInStackTrace',
        'getCause', 'getClass', 'getLocalizedMessage', 'getMessage',
        'getStackTrace', 'getSuppressed', 'hashCode', 'initCause',
        'localizedMessage', 'notify', 'notifyAll', 'printStackTrace',
        'setStackTrace', 'stackTrace', 'suppressed', 'toString', 'wait',
    ]

    tokens = None # list of token factories; intialized below

    _initialized = False # overridden in instances
    tag = None # overridden in instances

    # Dictionaries.

    controls = {
        # C0 (ASCII, ISO 646, ECMA-6)
        'NUL':    (0x00, "null"),
        'SOH':    (0x01, "start of heading, transmission control one"),
        'TC1':    (0x01, "start of heading, transmission control one"),
        'STX':    (0x02, "start of text, transmission control two"),
        'TC2':    (0x02, "start of text, transmission control two"),
        'ETX':    (0x03, "end of text, transmission control three"),
        'TC3':    (0x03, "end of text, transmission control three"),
        'EOT':    (0x04, "end of transmission, transmission control four"),
        'TC4':    (0x04, "end of transmission, transmission control four"),
        'ENQ':    (0x05, "enquiry, transmission control five"),
        'TC5':    (0x05, "enquiry, transmission control five"),
        'ACK':    (0x06, "acknowledge, transmission control six"),
        'TC6':    (0x06, "acknowledge, transmission control six"),
        'BEL':    (0x07, "bell; alert"),
        'BS':     (0x08, "backspace, format effector zero"),
        'FE0':    (0x08, "backspace, format effector zero"),
        'HT':     (0x09, "horizontal tabulation, format effector one; tab"),
        'FE1':    (0x09, "horizontal tabulation, format effector one; tab"),
        'LF':     (0x0a, "linefeed, format effector two; newline (Unix)"),
        'NL':     (0x0a, "linefeed, format effector two; newline (Unix)"),
        'FE2':    (0x0a, "linefeed, format effector two; newline (Unix)"),
        'VT':     (0x0b, "line tabulation, format effector three; vertical tab"),
        'LT':     (0x0b, "line tabulation, format effector three; vertical tab"),
        'FE3':    (0x0b, "line tabulation, format effector three; vertical tab"),
        'FF':     (0x0c, "form feed, format effector four"),
        'FE4':    (0x0c, "form feed, format effector four"),
        'CR':     (0x0d, "carriage return, format effector five; enter"),
        'FE5':    (0x0d, "carriage return, format effector five; enter"),
        'SO':     (0x0e, "shift out, locking-shift one"),
        'LS1':    (0x0e, "shift out, locking-shift one"),
        'SI':     (0x0f, "shift in, locking-shirt zero"),
        'LS0':    (0x0f, "shift in, locking-shirt zero"),
        'DLE':    (0x10, "data link escape; transmission control seven"),
        'TC7':    (0x10, "data link escape; transmission control seven"),
        'XON':    (0x11, "device control one; xon"),
        'DC1':    (0x11, "device control one; xon"),
        'DC2':    (0x12, "device control two"),
        'XOFF':   (0x13, "device control three; xoff"),
        'DC3':    (0x13, "device control three; xoff"),
        'STOP':   (0x14, "device control four; stop"),
        'DC4':    (0x14, "device control four; stop"),
        'NAK':    (0x15, "negative acknowledge, transmission control eight"),
        'TC8':    (0x15, "negative acknowledge, transmission control eight"),
        'SYN':    (0x16, "synchronous idle, transmission control nine"),
        'TC9':    (0x16, "synchronous idle, transmission control nine"),
        'ETB':    (0x17, "end of transmission block, transmission control ten"),
        'TC10':   (0x17, "end of transmission block, transmission control ten"),
        'CAN':    (0x18, "cancel"),
        'EM':     (0x19, "end of medium"),
        'SUB':    (0x1a, "substitute; end of file (DOS)"),
        'ESC':    (0x1b, "escape"),
        'FS':     (0x1c, "file separator, information separator four"),
        'IS4':    (0x1c, "file separator, information separator four"),
        'GS':     (0x1d, "group separator, information separator three"),
        'IS3':    (0x1d, "group separator, information separator three"),
        'RS':     (0x1e, "record separator, information separator two"),
        'IS2':    (0x1e, "record separator, information separator two"),
        'US':     (0x1f, "unit separator, information separator one"),
        'IS1':    (0x1f, "unit separator, information separator one"),
        'SP':     (0x20, "space"),
        'DEL':    (0x7f, "delete"),
        # C1 (ANSI X3.64, ISO 6429, ECMA-48)
        'PAD':    (0x80, "padding character"),
        'HOP':    (0x81, "high octet preset"),
        'BPH':    (0x82, "break permitted here"),
        'NBH':    (0x83, "no break here"),
        'IND':    (0x84, "index"),
        'NEL':    (0x85, "next line"),
        'SSA':    (0x86, "start of selected area"),
        'ESA':    (0x87, "end of selected area"),
        'HTS':    (0x88, "horizontal/character tabulation set"),
        'HTJ':    (0x89, "horizontal/character tabulation with justification"),
        'VTS':    (0x8a, "vertical/line tabulation set"),
        'PLD':    (0x8b, "partial line down/forward"),
        'PLU':    (0x8c, "partial line up/backward"),
        'RI':     (0x8d, "reverse index, reverse line feed"),
        'SS2':    (0x8e, "single shift two"),
        'SS3':    (0x8f, "single shift three"),
        'DCS':    (0x90, "device control string"),
        'PU1':    (0x91, "private use one"),
        'PU2':    (0x92, "private use two"),
        'STS':    (0x93, "set transmission state"),
        'CHC':    (0x94, "cancel character"),
        'MW':     (0x95, "message waiting"),
        'SPA':    (0x96, "start of protected/guarded area"),
        'EPA':    (0x97, "end of protected/guarded area"),
        'SOS':    (0x98, "start of string"),
        'SGCI':   (0x99, "single graphic character introducer, unassigned"),
        'SCI':    (0x9a, "single character introducer"),
        'CSI':    (0x9b, "control sequence introducer"),
        'ST':     (0x9c, "string terminator"),
        'OSC':    (0x9d, "operating system command"),
        'PM':     (0x9e, "privacy message"),
        'APC':    (0x9f, "application program command"),
        # ISO 8859
        'NBSP':   (0xa0, "no-break space"),
        'SHY':    (0xad, "soft hyphen, discretionary hyphen"),
        # Unicode, general punctuation
        'NQSP':   (0x2000, "en quad"),
        'MQSP':   (0x2001, "em quad; mutton quad"),
        'ENSP':   (0x2002, "en space; nut"),
        'EMSP':   (0x2003, "em space; mutton"),
        '3MSP':   (0x2004, "three-per-em space; thick space"),
        '4MSP':   (0x2005, "four-per-em space; mid space"),
        '6MSP':   (0x2006, "six-per-em space"),
        'FSP':    (0x2007, "figure space"),
        'PSP':    (0x2008, "punctuation space"),
        'THSP':   (0x2009, "thin space"),
        'HSP':    (0x200a, "hair space"),
        'ZWSP':   (0x200b, "zero width space"),
        'ZWNJ':   (0x200c, "zero width non-joiner"),
        'ZWJ':    (0x200d, "zero width joiner"),
        'LRM':    (0x200e, "left-to-right mark"),
        'RLM':    (0x200f, "right-to-left mark"),
        'NBHY':   (0x2011, "non-breaking hyphen"),
        'LS':     (0x2028, "line separator"),
        'LSEP':   (0x2028, "line separator"),
        'PS':     (0x2029, "paragraph separator"),
        'PSEP':   (0x2029, "paragraph separator"),
        'LRE':    (0x202a, "left-to-right encoding"),
        'RLE':    (0x202b, "right-to-left encoding"),
        'PDF':    (0x202c, "pop directional formatting"),
        'LRO':    (0x202d, "left-to-right override"),
        'RLO':    (0x202e, "right-to-left override"),
        'NNBSP':  (0x202f, "narrow no-break space"),
        'MMSP':   (0x205f, "medium mathematical space"),
        'WJ':     (0x2060, "word joiner"),
        'FA':     (0x2061, "function application (`f()`)"),
        'IT':     (0x2062, "invisible times (`x`)"),
        'IS':     (0x2063, "invisible separator (`,`)"),
        'IP':     (0x2064, "invisible plus (`+`)"),
        'LRI':    (0x2066, "left-to-right isolate"),
        'RLI':    (0x2067, "right-to-left isolate"),
        'FSI':    (0x2068, "first strong isolate"),
        'PDI':    (0x2069, "pop directional isolate"),
        'ISS':    (0x206a, "inhibit symmetric swapping"),
        'ASS':    (0x206b, "activate symmetric swapping"),
        'IAFS':   (0x206c, "inhibit arabic form shaping"),
        'AAFS':   (0x206d, "activate arabic form shaping"),
        'NADS':   (0x206e, "national digit shapes"),
        'NODS':   (0x206f, "nominal digit shapes"),
        # Geometric shapes (some circles)
        'WC':     (0x25cb, "white circle"),
        'DC':     (0x25cc, "dotted circle"),
        'CWVF':   (0x25cc, "circle with vertical fill"),
        'BE':     (0x25cc, "bullseye"),
        # Unicode, CJK symbols and punctuation
        'IDSP':   (0x3000, "ideographic space"),
        'IIM':    (0x3005, "ideographic iteration mark"),
        'ICM':    (0x3006, "ideographic closing mark"),
        'INZ':    (0x3007, "ideographic number zero"),
        'VIIM':   (0x303b, "vertical ideographic iteration mark"),
        'MASU':   (0x303c, "masu mark"),
        'PAM':    (0x303d, "part alternation mark"),
        'IVI':    (0x303e, "ideographic variation indicator"),
        'IHFSP':  (0x303f, "ideograhic half fill space"),
        # Combining diacritical marks
        'CGJ':    (0x034f, "combining grapheme joiner"),
        # Arabic leter forms
        'ANS':    (0x0600, "Arabic number sign"),
        'ASN':    (0x0601, "Arabic sign sanah"),
        'AFM':    (0x0602, "Arabic footnote marker"),
        'ASF':    (0x0603, "Arabic sign safha"),
        'ASM':    (0x0604, "Arabic sign samvat"),
        'ANMA':   (0x0605, "Arabic number mark above"),
        'ALM':    (0x061c, "Arabic letter mark"),
        # Unicode, variation selectors
        'VS1':    (0xfe00, "variation selector 1"),
        'VS2':    (0xfe01, "variation selector 2"),
        'VS3':    (0xfe02, "variation selector 3"),
        'VS4':    (0xfe03, "variation selector 4"),
        'VS5':    (0xfe04, "variation selector 5"),
        'VS6':    (0xfe05, "variation selector 6"),
        'VS7':    (0xfe06, "variation selector 7"),
        'VS8':    (0xfe07, "variation selector 8"),
        'VS9':    (0xfe08, "variation selector 9"),
        'VS10':   (0xfe09, "variation selector 10"),
        'VS11':   (0xfe0a, "variation selector 11"),
        'VS12':   (0xfe0b, "variation selector 12"),
        'VS13':   (0xfe0c, "variation selector 13"),
        'VS14':   (0xfe0d, "variation selector 14"),
        'VS15':   (0xfe0e, "variation selector 15; text display"),
        'TEXT':   (0xfe0e, "variation selector 15; text display"),
        'VS16':   (0xfe0f, "variation selector 16; emoji display"),
        'EMOJI':  (0xfe0f, "variation selector 16; emoji display"),
        # Unicode, Arabic presentation forms
        'ZWNBSP': (0xfeff, "zero width no-break space; byte order mark"),
        'BOM':    (0xfeff, "zero width no-break space; byte order mark"),
        # Unicode, specials
        'IAA':    (0xfff9, "interlinear annotation anchor"),
        'IAS':    (0xfffa, "interlinear annotation separator"),
        'IAT':    (0xfffb, "interlinear annotation terminator"),
        'ORC':    (0xfffc, "object replacement character"),
        'RC':     (0xfffd, "replacement character"),
        # Egyptian hieroglyph format controls
        'EHVJ':   (0x13430, "Egyptian hieroglyph vertical joiner"),
        'EHHJ':   (0x13431, "Egyptian hieroglyph horizontal joiner"),
        'EHITS':  (0x13432, "Egyptian hieroglyph insert at top start"),
        'EHIBS':  (0x13433, "Egyptian hieroglyph insert at bottom start"),
        'EHITE':  (0x13434, "Egyptian hieroglyph insert at top end"),
        'EHIBE':  (0x13435, "Egyptian hieroglyph insert at bottom end"),
        'EHOM':   (0x13436, "Egyptian hieroglyph overlay middle"),
        'EHBS':   (0x13437, "Egyptian hieroglyph begin segment"),
        'EHES':   (0x13437, "Egyptian hieroglyph end segment"),
        # Shorthand format controls
        'SFLO':   (0x1bca0, "shorthand format letter overlap"),
        'SFCO':   (0x1bca1, "shorthand format continuing overlap"),
        'SFDS':   (0x1bca2, "shorthand format down step"),
        'SFUS':   (0x1bca3, "shorthand format up step"),
        # step
        'TAG':    (0xe0001, "language tag"),
    }

    diacritics = {
        '`': (0x0300, "grave"),
        "'": (0x0301, "acute"),
        '^': (0x0302, "circumflex accent"),
        '~': (0x0303, "tilde"),
        '-': (0x0304, "macron"),
        '_': (0x0305, "overline"),
        '(': (0x0306, "breve"),
        '.': (0x0307, "dot"),
        ':': (0x0308, "diaeresis"),
        '?': (0x0309, "hook above"),
        'o': (0x030a, "ring above"),
        '"': (0x030b, "double acute accent"),
        'v': (0x030c, "caron"),
        's': (0x030d, "vertical line above"),
        'S': (0x030e, "double vertical line above"),
        '{': (0x030f, "double grave accent"),
        '@': (0x0310, "candrabinu"),
        ')': (0x0311, "inverted breve"),
        '1': (0x0312, "turned comma above"),
        '2': (0x0313, "comma above"),
        '3': (0x0314, "reversed comma above"),
        '4': (0x0315, "comma above right"),
        ']': (0x0316, "grave accent below"),
        '[': (0x0317, "acute accent below"),
        '<': (0x0318, "left tack below"),
        '>': (0x0319, "right tack below"),
        'A': (0x031a, "left angle above"),
        'h': (0x031b, "horn"),
        'r': (0x031c, "left half ring below"),
        'u': (0x031d, "up tack below"),
        'd': (0x031e, "down tack below"),
        '+': (0x031f, "plus sign below"),
        'm': (0x0320, "minus sign below"),
        'P': (0x0321, "palatalized hook below"),
        'R': (0x0322, "retroflex hook below"),
        'D': (0x0323, "dot below"),
        'E': (0x0324, "diaeresis below"),
        'O': (0x0325, "ring below"),
        'c': (0x0326, "comma below"),
        ',': (0x0327, "cedilla"),
        'K': (0x0328, "ogonek"),
        'V': (0x0329, "vertical line below"),
        '$': (0x032a, "bridge below"),
        'W': (0x032b, "inverted double arch below"),
        'H': (0x032c, "caron below"),
        'C': (0x032d, "circumflex accent below"),
        'B': (0x032e, "breve below"),
        'N': (0x032f, "inverted breve below"),
        'T': (0x0330, "tilde below"),
        'M': (0x0331, "macron below"),
        'l': (0x0332, "low line"),
        'L': (0x0333, "double low line"),
        '&': (0x0334, "tilde overlay"),
        '!': (0x0335, "short stroke overlay"),
        '|': (0x0336, "long stroke overlay"),
        '%': (0x0337, "short solidays overlay"),
        '/': (0x0338, "long solidus overlay"),
        'g': (0x0339, "right half ring below"),
        '*': (0x033a, "inverted bridge below"),
        '#': (0x033b, "square below"),
        'G': (0x033c, "seagull below"),
        'x': (0x033d, "x above"),
        ';': (0x033e, "vertical tilde"),
        '=': (0x033f, "double overline"),
    }

    icons = {
        '!':    ([0x2757, 0xfe0f], "exclamation mark"),
        '#':    (0x1f6d1, "octagonal sign"),
        '$':    (0x1f4b2, "heavy dollar sign"),
        '%%':   (0x1f3b4, "flower playing cards"),
        '%':    None,
        '%c':   ([0x2663, 0xfe0f], "club suit"),
        '%d':   ([0x2666, 0xfe0f], "diamond suit"),
        '%e':   (0x1f9e7, "red gift envelope"),
        '%h':   ([0x2665, 0xfe0f], "heart suit"),
        '%j':   (0x1f0cf, "joker"),
        '%r':   (0x1f004, "Mahjong red dragon"),
        '%s':   ([0x2660, 0xfe0f], "spade suit"),
        '&!':   ([0x1f396, 0xfe0f], "military medal"),
        '&$':   (0x1f3c6, "trophy"),
        '&':    None,
        '&0':   (0x1f3c5, "sports medal"),
        '&1':   (0x1f947, "first place medal"),
        '&2':   (0x1f948, "second place medal"),
        '&3':   (0x1f949, "third place medal"),
        '*':    ([0x2a, 0xfe0f], "asterisk"),
        '+':    (0x1f53a, "red triangle pointed up"),
        ',':    None,
        ',+':   (0x1f44d, "thumbs up"),
        ',-':   (0x1f44e, "thumbs down"),
        ',a':   ([0x261d, 0xfe0f], "point above"),
        ',d':   (0x1f447, "point down"),
        ',f':   (0x1f44a, "oncoming fist"),
        ',l':   (0x1f448, "point left"),
        ',o':   (0x1faf5, "point out"),
        ',r':   (0x1f449, "point right"),
        ',s':   (0x1f91d, "handshake"),
        ',u':   (0x1f446, "point up"),
        '-':    (0x1f53b, "red triangle pointed down"),
        '.':    None,
        '.d':   ([0x2b07, 0xfe0f], "down arrow"),
        '.l':   ([0x2b05, 0xfe0f], "left arrow"),
        '.r':   ([0x27a1, 0xfe0f], "right arrow"),
        '.u':   ([0x2b06, 0xfe0f], "up arrow"),
        '/':    ([0x2714, 0xfe0f], "check mark"),
        ':$':   (0x1f911, "money-mouth face"),
        ':':    None,
        ':(':   (0x1f61e, "disappointed face"),
        ':)':   (0x1f600, "grinning face"),
        ':*':   (0x1f618, "face blowing a kiss"),
        ':/':   (0x1f60f, "smirking face"),
        ':0':   (0x1f636, "face without mouth"),
        ':1':   (0x1f914, "thinking face"),
        ':2':   (0x1f92b, "shushing face"),
        ':3':   (0x1f617, "kissing face"),
        ':4':   (0x1f605, "grinning face with sweat"),
        ':5':   (0x1f972, "smiling face with tear"),
        ':6':   (0x1f602, "face with tears of joy"),
        ':7':   (0x1f917, "smiling face with open hands"),
        ':8':   (0x1f910, "zipper-mouth face"),
        ':9':   (0x1f923, "rolling on the floor laughing"),
        ':<':   None,
        ':<3':  ([0x2764, 0xfe0f], "red heart"),
        ':D':   (0x1f601, "beaming face with smiling eyes"),
        ':O':   (0x1f62f, "hushed face"),
        ':P':   (0x1f61b, "face with tongue"),
        ':S':   (0x1fae1, "saluting face"),
        ':T':   (0x1f62b, "tired face"),
        ':Y':   (0x1f971, "yawning face"),
        ':Z':   (0x1f634, "sleeping face"),
        ':[':   (0x1f641, "frowning face"),
        ':\\':  (0x1f615, "confused face"),
        ':]':   ([0x263a, 0xfe0f], "smiling face"),
        ':|':   (0x1f610, "neutral face"),
        ';':    None,
        ';)':   (0x1f609, "winking face"),
        '<':    (0x23ea, "black left-pointing double triangle"),
        '=':    None,
        '=*':   ([0x2716, 0xfe0f], "heavy multiplication sign"),
        '=+':   ([0x2795, 0xfe0f], "heavy plus sign"),
        '=-':   ([0x2796, 0xfe0f], "heavy minus sign"),
        '=/':   ([0x2797, 0xfe0f], "heavy division sign"),
        '>':    (0x23e9, "black right-pointing double triangle"),
        '?':    ([0x2753, 0xfe0f], "question mark"),
        'B':    None,
        'B)':   (0x1f60e, "smiling face with sunglasses"),
        'E':    (0x2130, "script capital E"),
        'F':    (0x2131, "script capital F"),
        'M':    (0x2133, "script capital M"),
        '\"':   None,
        '\"(':  (0x201c, "left double quotation mark"),
        '\")':  (0x201d, "right double quotation mark"),
        '\"\"': (0x22, "quotation mark"),
        '\'':   None,
        '\'(':  (0x2018, "left single quotation mark"),
        '\')':  (0x2019, "right single quotation mark"),
        '\'/':  (0xb4, "acute accent"),
        '\'\'': (0x27, "apostrophe"),
        '\'\\': (0x60, "grave accent"),
        '\\':   ([0x274c, 0xfe0f], "cross mark"),
        '^':    ([0x26a0, 0xfe0f], "warning sign"),
        '{!!':  None,
        '{!!}': ([0x203c, 0xfe0f], "double exclamation mark"),
        '{!':   None,
        '{!?':  None,
        '{!?}': ([0x2049, 0xfe0f], "exclamation question mark"),
        '{':    None,
        '{(':   None,
        '{()':  None,
        '{()}': (0x1f534, "red circle"),
        '{[':   None,
        '{[]':  None,
        '{[]}': (0x1f7e5, "red square"),
        '{{':   None,
        '{{}':  None,
        '{{}}': ([0x2b55, 0xfe0f], "hollow red circle"),
        '|':    (0x1f346, "aubergine"),
        '~':    ([0x3030, 0xfe0f], "wavy dash"),
    }

    emojis = {}

    def __init__(self, **kwargs):
        self._initialized = False
        # Meta variables.
        self._names = []
        self._specs = {}
        self._initials = {}
        self._descriptions = {}
        self._nones = {}
        self._functions = {}
        # Initialize.
        self.initialize()
        # Mark initialized.
        self._initialized = True
        # Update with any keyword arguments, if specified.
        self.update(**kwargs)

    def __setattr__(self, name, value):
        self.set(name, value)

    def __contains__(self, name):
        return name in self.__dict__

    def __bool__(self): return self._initialized # 3.x
    def __nonzero__(self): return self._initialized # 2.x

    def __iter__(self):
        return iter(self._names)

    def __str__(self):
        results = []
        for name in self._names:
            results.append("%s=%r" % (name, self.get(name)))
        return ', '.join(results)

    # Initialization.

    def initialize(self):
        """Setup the declarations and definitions for the defined
        attributes."""
        self.define('name', strType, self.defaultName, "The name of this configuration (optional)")
        self.define('notes', None, None, "Notes for this configuration (optional)")
        self.define('prefix', strType, self.defaultPrefix, "The prefix", none=True, env=PREFIX_ENV)
        self.define('pseudomoduleName', strType, self.defaultPseudomoduleName, "The pseudomodule name", env=PSEUDO_ENV)
        self.define('verbose', bool, False, "Verbose processing (for debugging)?")
        self.define('rawErrors', bool, None, "Print Python stacktraces on error?", env=RAW_ERRORS_ENV)
        self.define('verboseErrors', bool, True, "Show attributes in error messages?")
        self.define('exitOnError', bool, True, "Exit after an error?")
        self.define('ignoreErrors', bool, False, "Ignore errors?")
        self.define('contextFormat', strType, self.defaultContextFormat, "Context format")
        self.define('goInteractive', bool, None, "Go interactive after done processing?", env=INTERACTIVE_ENV)
        self.define('deleteOnError', bool, None, "Delete output file on error?", env=DELETE_ON_ERROR_ENV)
        self.define('doFlatten', bool, None, "Flatten pseudomodule members at start?", env=FLATTEN_ENV)
        self.define('useProxy', bool, None, "Install a stdout proxy?", env=NO_PROXY_ENV, invert=True)
        self.define('relativePath', bool, False, "Add EmPy script path to sys.path?")
        self.define('buffering', int, self.defaultBuffering, "Specify buffering strategy for files:\n0 (none), 1 (line), -1 (full), or N", env=BUFFERING_ENV, func=self.setBuffering)
        self.define('replaceNewlines', bool, False, "Replace newlines with spaces in expressions?")
        self.define('ignoreBangpaths', bool, True, "Treat bangpaths as comments?")
        self.define('noneSymbol', strType, None, "String to write when expanding None", none=True)
        self.define('missingConfigIsError', bool, True, "Is a missing configuration file an error?")
        self.define('pauseAtEnd', bool, False, "Prompt at the end of processing?")
        self.define('startingLine', int, 1, "Line number to start with")
        self.define('startingColumn', int, 1, "Column number to start with")
        self.define('significatorDelimiters', tuple, self.defaultSignificatorDelimiters, "Significator variable delimiters")
        self.define('emptySignificator', object, self.defaultEmptySignificator, "Value to use for empty significators", none=True)
        self.define('autoValidateIcons', bool, self.defaultAutoValidateIcons, "Automatically validate icons before each use?")
        self.define('emojiModuleNames', list, self.defaultEmojiModuleNames, "List of emoji modules to try to use\n", none=True)
        self.define('emojiNotFoundIsError', bool, True, "Is an unknown emoji an error?")
        self.define('useBinary', bool, False, "Open files as binary (Python 2.x Unicode)?", env=BINARY_ENV)
        defaultEncoding = self.environment(ENCODING_ENV, self.getDefaultEncoding())
        self.define('inputEncoding', strType, defaultEncoding, "Set input Unicode encoding", none=True, env=INPUT_ENCODING_ENV, helpFunction=lambda x: x == 'utf_8' and 'utf-8' or x)
        self.define('outputEncoding', strType, defaultEncoding, "Set output Unicode encoding", none=True, env=OUTPUT_ENCODING_ENV, helpFunction=lambda x: x == 'utf_8' and 'utf-8' or x)
        defaultErrors = self.environment(ERRORS_ENV, self.defaultErrors)
        self.define('inputErrors', strType, defaultErrors, "Set input Unicode error handler", none=True, env=INPUT_ERRORS_ENV)
        self.define('outputErrors', strType, defaultErrors, "Set output Unicode error handler", none=True, env=OUTPUT_ERRORS_ENV)
        self.define('normalizationForm', strType, self.defaultNormalizationForm, "Specify Unicode normalization form", none=True)
        self.define('autoPlayDiversions', bool, True, "Auto-play diversions on exit?")
        self.define('expandUserConstructions', bool, True, "Expand ~ and ~user constructions")
        self.define('configVariableName', strType, self.defaultConfigVariableName, "Configuration variable name while loading")
        self.define('successCode', int, self.defaultSuccessCode, "Exit code to return on script success")
        self.define('failureCode', int, self.defaultFailureCode, "Exit code to return on script failure")
        self.define('unknownCode', int, self.defaultUnknownCode, "Exit code to return on bad configuration")
        self.define('skipCode', int, self.defaultSkipCode, "Exit code to return on requirements failure (testing")
        self.define('checkVariables', bool, True, "Check configuration variables on assignment?")
        self.define('pathSeparator', strType, sys.platform.startswith('win') and ';' or ':', "Path separator for configuration file paths")
        self.define('supportModules', bool, True, "Support EmPy modules?")
        self.define('moduleExtension', strType, self.defaultModuleExtension, "Filename extension for EmPy modules")
        self.define('moduleFinderIndex', int, 0, "Index of module finder in meta path")
        self.define('enableImportOutput', bool, True, "Disable output during import?")
        self.define('duplicativeFirsts', list, list(DUPLICATIVE_CHARS), "List of duplicative first characters")
        self.define('openFunc', None, None, "The open function to use (None for automatic)")

        # Redefine static configuration variables so they're in the help.
        self.define('controls', dict, self.controls, "Controls dictionary")
        self.define('diacritics', dict, self.diacritics, "Diacritics dictionary")
        self.define('icons', dict, self.icons, "Icons dictionary")
        self.define('emojis', dict, self.emojis, "Emojis dictionary")
        # If the encoding or error handling has changed, enable binary
        # implicitly, just as if it had been specified on the command line.
        if (self.inputEncoding != defaultEncoding or
            self.outputEncoding != defaultEncoding or
            self.inputErrors != defaultErrors or
            self.outputErrors != defaultErrors):
            self.enableBinary(major, minor)

    def isInitialized(self):
        """Is this configuration initialized and ready for use?"""
        return self._initialized

    def check(self, inputFilename, outputFilename):
        """Do a sanity check on the configuration settings."""
        if self.prefix == '' or self.prefix == 'none' or self.prefix == 'None':
            self.prefix = None
        if self.buffering is None:
            self.buffering = self.defaultBuffering
        if isinstance(self.prefix, strType) and len(self.prefix) != 1:
            raise ConfigurationError("prefix must be single-character string")
        if not self.pseudomoduleName:
            raise ConfigurationError("pseudomodule name must be non-empty string")
        if self.deleteOnError and outputFilename is None:
            raise ConfigurationError("-d only makes sense with -o, -a, -O or -A arguments")
        if self.hasNoBuffering() and not self.hasBinary():
            raise ConfigurationError("no buffering requires file open in binary mode; try adding -u option")

    # Access.

    def declare(self, name, specs, initial, description,
                none=False, helpFunction=None):
        """Declare the configuration attribute."""
        self._names.append(name)
        self._specs[name] = specs
        self._initials[name] = initial
        self._descriptions[name] = description
        self._nones[name] = none
        self._functions[name] = helpFunction

    def define(self, name, specs, value, description, none=False,
               env=None, func=None, blank=None, invert=False, helpFunction=None):
        """Define a configuration attribute with the given name, type
        specification, initial value, and description.  If none is true, None
        is a legal value.  Also, allow an optional corresponding environment
        variable, and, if present, an optional blank variable to set the
        value to if the environment variable is defined but is blank.  If
        both env and func are present, call the function to set the
        environment variable.  Finally, if invert is true, invert the
        (bool) environment variable.  Additionally, if the type specification
        is or contains toString, convert the value to a proper string."""
        if isinstance(specs, tuple):
            isString = str in specs or bytes in specs
        else:
            isString = str is specs or bytes is specs
        if value is not None and specs is not None:
            assert isinstance(value, specs), (value, specs)
        if env is not None:
            if func is not None:
                if self.hasEnvironment(env):
                    value = func(self.environment(env, value))
            elif isString:
                value = self.environment(env, value, blank)
            elif specs is bool:
                value = self.hasEnvironment(env)
                if invert:
                    value = not value
            elif not isinstance(specs, tuple):
                value = specs(self.environment(env, value, blank))
            else:
                assert False, "environment attribute type must be bool or str: `%s`" % name
        if isString and value is not None:
            value = toString(value)
        self.declare(name, specs, value, description, none, helpFunction)
        self.set(name, value)

    def has(self, name):
        """Is this name a valid configuration attribute?"""
        return name in self.__dict__ or name in self.__class__.__dict__

    def get(self, name, default=None):
        """Get this (valid, existing) configuration attribute."""
        return getattr(self, name, default)

    def set(self, name, value):
        """Set the (valid) configuration attribute, checking its type
        if necessary."""
        if self._initialized and self.checkVariables:
            if not name.startswith('_'):
                # If this attribute is not already present, it's invalid.
                if not self.has(name):
                    raise ConfigurationError("unknown configuration attribute: `%s`" % name)
                # If there's a registered type for this attribute, check it.
                specs = self._specs.get(name)
                if specs is not None:
                    if value is None:
                        if not self._nones[name]:
                            raise ConfigurationError("configuration value cannot be None: `%s`: `%s`; `%s`" % (name, value, specs))
                    elif not isinstance(value, specs):
                        raise ConfigurationError("configuration value has invalid type: `%s`: `%s`; `%s`" % (name, value, specs))
        self.__dict__[name] = value

    def update(self, **kwargs):
        """Update the configuration by keyword arguments."""
        for name, value in kwargs.items():
            if not self.has(name):
                raise ConfigurationError("unknown configuration attribute: `%s`" % name)
            self.set(name, value)

    def clone(self, deep=False):
        """Create a distinct copy of this configuration.  Make it deep if
        desired."""
        if deep:
            copyMethod = copy.deepcopy
        else:
            copyMethod = copy.copy
        return copyMethod(self)

    # Configuration file loading.

    def run(self, statements):
        """Run some configuration variable assignment statements."""
        locals = {self.configVariableName: self}
        execFunc(statements, globals(), locals)
        keys = list(locals.keys())
        # Put the priority variables first.
        for priority in self.priorityVariables:
            if priority in locals:
                keys.remove(priority)
                keys.insert(0, priority)
        for key in keys:
            if not key.startswith('_') and key != self.configVariableName:
                setattr(self, key, locals[key])
        return True

    def load(self, filename, required=None):
        """Update the configuration by loading from a resource
        file.  If required is true, raise an exception; if false,
        ignore; if None, use the default in this configuration.
        Return whether or not the load succeeded."""
        if required is None:
            required = self.missingConfigIsError
        try:
            file = self.open(filename, 'r')
        except IOError:
            if required:
                raise ConfigurationFileNotFoundError("cannot open configuration file: %s" % filename, filename)
            return False
        try:
            contents = file.read()
            return self.run(contents)
        finally:
            file.close()

    def path(self, path, required=None):
        """Attempt to load several resource paths, either as a
        list of paths or a delimited string of paths.  If required
        is true, raise an exception; if false, ignore; if None,
        use the default in this configuration."""
        if isinstance(path, list):
            filenames = path
        else:
            filenames = path.split(self.pathSeparator)
        for filename in filenames:
            self.load(filename, required)

    # Environment.

    def hasEnvironment(self, name):
        """Is the current environment variable defined in the environment?
        The value does not matter."""
        return name in os.environ

    def environment(self, name, default=None, blank=None):
        """Get the value of the given environment variable, or default
        if it is not set.  If a variable is set to an empty value and
        blank is non-None, return blank instead."""
        if name in os.environ:
            value = os.environ[name]
            if not value and blank is not None:
                return blank
            else:
                return value
        else:
            return default

    # Convenience.

    def recode(self, result, encoding=None):
        """Convert a lookup table entry into a string.  A value can be a
        string itself, an integer corresponding to a code point, or a
        2-tuple, the first value of which is one of the above (the
        second is a description).  If the encoding is provided, use that
        to convert bytes objects; otherwise, use the output encoding."""
        assert result is not None
        # First, if it's a tuple, then use the first element; the remaining
        # elements are a description.
        if isinstance(result, tuple):
            result = result[0]
        # Check the type of the value:
        if isinstance(result, list):
            # If it's a list, then it's a sequence of some the above.
            # Turn them all into strings and then concatenate them.
            fragments = []
            for elem in result:
                fragments.append(self.recode(elem))
            result = ''.join(fragments)
        elif isinstance(result, str):
            # It's already a string, so do nothing.
            pass
        elif isinstance(result, bytes):
            # If it's a bytes, decode it.
            if encoding is None:
                encoding = self.outputEncoding
            result = result.decode(encoding)
        elif isinstance(result, int):
            # If it's an int, then it's a character code.
            result = chr(result)
        elif callable(result):
            # If it's callable, then call it.
            result = self.recode(result())
        else:
            # Otherwise, it's something convertible to a string.
            result = str(result)
        return result

    def escaped(self, ord, prefix='\\'):
        """Write a valid Python string escape sequence for the given
        character ordinal."""
        if ord <= 0xff:
            return '%sx%02x' % (prefix, ord)
        elif ord <= 0xffff:
            return '%su%04x' % (prefix, ord)
        else:
            return '%sU%08x' % (prefix, ord)

    def hasDefaultPrefix(self):
        """Is this configuration's prefix the default or
        non-existent?"""
        return self.prefix is None or self.prefix == self.defaultPrefix

    # Buffering.

    def hasFullBuffering(self): return self.buffering <= self.fullBuffering
    def hasNoBuffering(self): return self.buffering == self.noBuffering
    def hasLineBuffering(self): return self.buffering == self.lineBuffering
    def hasFixedBuffering(self):
        return self.buffering is not None and self.buffering > self.lineBuffering

    def setBuffering(self, name):
        """Set the buffering by name or value."""
        if isinstance(name, int):
            self.buffering = name
        elif name == 'none':
            self.buffering = self.noBuffering
        elif name == 'line':
            self.buffering = self.lineBuffering
        elif name == 'full':
            self.buffering = self.fullBuffering
        elif name is None or name == 'default' or name == '':
            self.buffering = self.defaultBuffering
        else:
            try:
                self.buffering = int(name)
            except ValueError:
                raise ConfigurationError("invalid buffering name: `%s`" % name)
        if self.buffering < self.fullBuffering:
            self.buffering = self.fullBuffering
        return self.buffering

    # Token factories.

    def createFactory(self, tokens=None):
        """Create a token factory and return it."""
        if tokens is None:
            tokens = self.tokens
        return Factory(tokens)

    def adjustFactory(self):
        """Adjust the factory to take into account a non-default
        prefix."""
        self.factory.adjust(self)

    def getFactory(self, tokens=None, force=False):
        """Get a factory (creating one if one has not yet been
        created).  If force is true, always create a new one."""
        if self.factory is None or force:
            self.factory = self.createFactory(tokens)
            self.adjustFactory()
        return self.factory

    def resetFactory(self):
        """Clear the current factory."""
        self.factory = None

    def createExtensionToken(self, first, name, last=None):
        if last is None:
            last = ENDING_CHAR_MAP.get(first[0], first[0])
        newType = type('ExtensionToken:' + first[:1], (ExtensionToken,),
            {'first': first, 'last': last, 'name': name})
        return newType

    # Binary/Unicode.

    def hasBinary(self):
        """Is binary/Unicode file open support enabled?"""
        return self.useBinary

    def enableBinary(self, major=None, minor=None):
        """Enable binary/Unicode file open support.  This is needed in
        Python 2.x for Unicode support.  If major/minor is present, only
        enable it implicitly if this is in fact Python 2.x."""
        if major is None or major == 2:
            self.useBinary = True

    def disableBinary(self):
        """Disable binary/Unicode support for this configuration."""
        self.useBinary = False

    # File I/O.

    def isDefaultEncodingErrors(self, encoding=None, errors=None, asInput=True):
        """Are both of the encoding/errors combinations the default?
        If either passed value is None the value in this configuration
        is what is checked.  Check for input if asInput is true;
        otherwise check output."""
        if encoding is None:
            if asInput:
                encoding = self.inputEncoding
            else:
                encoding = self.outputEncoding
        if encoding is not None and encoding != self.getDefaultEncoding():
            return False
        if errors is None:
            if asInput:
                errors = self.inputErrors
            else:
                errors = self.outputErrors
        if errors is not None and errors != self.defaultErrors:
            return False
        return True

    def getDefaultEncoding(self, default='unknown'):
        """What is the default encoding?"""
        try:
            return sys.getdefaultencoding()
        except AttributeError:
            return default

    def determineOpenFunc(self, filename,
                          mode=None, buffering=-1, encoding=None, errors=None):
        """Determine which openFunc to use if it has not already been
        specified and return it."""
        if self.openFunc is None:
            if self.useBinary:
                # Use binary mode, so call binaryOpen.
                self.openFunc = binaryOpen
            else:
                if major >= 3:
                    # If it's Python 3.x, just use open.
                    self.openFunc = open
                else:
                    # For Python 2.x, open doesn't take encoding and error
                    # handler arguments.  Check to make sure non-default
                    # encodings and error handlers haven't been chosen, because
                    # we can't comply.
                    if not self.isDefaultEncodingErrors(encoding, errors):
                        raise ConfigurationError("cannot comply with non-default Unicode encoding/errors selected in Python 2.x; use -u option: `%s`/`%s`" % (encoding, errors))
                    self.openFunc = open
            assert self.openFunc is not None
        return self.openFunc

    def isModeBinary(self, mode):
        """Does this mode represent binary mode?"""
        return 'b' in mode

    def open(self, filename,
             mode=None, buffering=-1, encoding=None, errors=None,
             expand=None):
        """Open a new file, handling whether or not binary
        (Unicode) should be employed.  Raise if the selection
        cannot be complied with.  Arguments:

        - filename: The filename to open (required);
        - mode: The file open mode, None for read;
        - buffering: The buffering setting (int);
        - encoding: The encoding to use, None for default;
        - errors: The error handler to use, None for default;
        - expand: Expand user constructions? (~ and ~user)"""
        if expand is None:
            expand = self.expandUserConstructions
        if expand:
            filename = os.path.expanduser(filename)
        if mode is None:
            # Default to read.
            mode = 'r'
            if self.useBinary and not self.isModeBinary(mode):
                # Make it binary if it needs to be.
                mode += 'b'
        # Figure out the encoding and error handler.
        if 'w' in mode or 'a' in mode:
            if encoding is None:
                encoding = self.outputEncoding
            if errors is None:
                errors = self.outputErrors
        else:
            if encoding is None:
                encoding = self.inputEncoding
            if errors is None:
                errors = self.inputErrors
        func = self.determineOpenFunc(
            filename, mode, buffering, encoding, errors)
        try:
            return func(filename,
                        mode=mode,
                        buffering=buffering,
                        encoding=encoding,
                        errors=errors)
        except TypeError:
            # Some older versions of the open functions (e.g., Python 2.x's
            # open) do not accept the Unicode encoding and errors arguments.
            # Try again.
            return func(filename,
                        mode=mode,
                        buffering=buffering)

    def reconfigure(self, file,
                    buffering=-1, encoding=None, errors=None):
        """Reconfigure an existing file (e.g., sys.stdout)) with the same
        arguments as open."""
        try:
            if buffering == 1:
                file.reconfigure(encoding=encoding,
                                 errors=errors,
                                 line_buffering=True)
            else:
                file.reconfigure(encoding=encoding,
                                 errors=errors)
        except (AssertionError, AttributeError):
            raise InvocationError("non-default Unicode output encoding/errors selected with %s; use -o/-a option instead: %s/%s" % (file.name, encoding, errors))

    # Significators.

    def significatorReString(self):
        """Return a string that can be compiled into a regular
        expression representing a significator.  If multi is true,
        it will match multiline significators."""
        return self.prefix + self.significatorReStringSuffix

    def significatorRe(self, flags=re.MULTILINE|re.DOTALL,
                       baseFlags=re.VERBOSE):
        """Return a regular expression object with the given
        flags that is suitable for parsing significators."""
        return re.compile(self.significatorReString(), flags|baseFlags)

    def significatorFor(self, key):
        """Return the significator name for this key."""
        prefix, suffix = self.significatorDelimiters
        return prefix + toString(key) + suffix

    # Contexts.

    def setContextFormat(self, rawFormat):
        """Set the context format, auto-detecting which
        mechanism to use."""
        useFormatMethod = None
        format = rawFormat
        if format.startswith('format:'):
            useFormatMethod = True
            format = format.split(':', 1)[1]
        elif format.startswith('operator:'):
            useFormatMethod = False
            format = format.split(':', 1)[1]
        elif format.startswith('variable:'):
            useFormatMethod = False
            format = format.split(':', 1)[1]
            format = format.replace('$NAME', '%(name)s')
            format = format.replace('$LINE', '%(line)d')
            format = format.replace('$COLUMN', '%(column)d')
            format = format.replace('$CHARS', '%(chars)d')
        else:
            useFormatMethod = '%' not in format
        self.contextFormat = format
        self.useContextFormatMethod = useFormatMethod

    def renderContext(self, context):
        """Render the context as a string according to this
        configuration."""
        if self.useContextFormatMethod is None:
            self.setContextFormat(self.contextFormat)
        return context.render(self.contextFormat, self.useContextFormatMethod)

    # Icons.

    def calculateIconsSignature(self, icons=None):
        """Calculate a signature of the icons dict.  If the value
        changes, it's likely (but not certain) that the underlying
        dict has changed.  The signature will always differ from
        None."""
        if icons is None:
            icons = self.icons
        length = len(icons)
        try:
            # Include the size of the dictionary, if possible.  If it's not
            # available, that's okay.
            sizeof = sys.getsizeof(icons)
        except (AttributeError, TypeError):
            sizeof = -1
        return length, sizeof

    def signIcons(self, icons=None):
        """Sign the icons dict."""
        self.iconsSignature = self.calculateIconsSignature(icons)
        return self.iconsSignature

    def transmogrifyIcons(self, icons=None):
        """Process the icons and make sure any keys' prefixes are
        backfilled with Nones.  Call this method after modifying
        icons."""
        if icons is None:
            icons = self.icons
        additions = {}
        for parent in icons.keys():
            key = parent
            while len(key) > 1:
                key = key[:-1]
                if key in icons:
                    value = icons[key]
                    if value is None:
                        continue
                    else:
                        raise ConfigurationError("icon `%s` makes icon `%s` inaccessible" % (key, parent))
                else:
                    additions[key] = None
        icons.update(additions)
        return icons

    def validateIcons(self, icons=None):
        """If the icons have not been transmogrified yet, do so
        and store their signature for future reference."""
        if icons is None:
            icons = self.icons
        if icons is None:
            raise ConfigurationError("icons not configured")
        if not self.autoValidateIcons:
            return icons
        if self.iconsSignature is None:
            self.transmogrifyIcons(icons)
            self.signIcons(icons)
        return icons

    # Emojis.

    def initializeEmojiModules(self, moduleNames=None):
        """Initialize the emoji modules.  If moduleNames is not
        specified, check the defaults.  Idempotent."""
        if self.emojiModules is None:
            okNames = []
            # Use the config default if not specified.
            if moduleNames is None:
                moduleNames = self.emojiModuleNames
            # If it's still blank, specify no modules.
            if moduleNames is None:
                moduleNames = []
            # Create a map of module names for fast lookup.  (This would be a
            # set, but sets aren't available in early versions of Python 2.x.)
            nameMap = {}
            for moduleName in moduleNames:
                nameMap[moduleName] = None
            # Now iterate over each potential module.
            self.emojiModules = {}
            for info in self.emojiModuleInfos:
                moduleName = info[0]
                if moduleName in nameMap:
                    module = EmojiModuleInfo(*info)
                    if module.ok:
                        okNames.append(moduleName)
                        self.emojiModules[moduleName] = module
            # Finally, replace the requested module names with the ones
            # actually present.
            self.emojiModuleNames = okNames
            if not self.emojiModules and not self.emojis:
                raise ConfigurationError("no emoji lookup methods available; install modules and set emoji modules or set emojis dictionary in configuration")

    def substituteEmoji(self, text):
        """Substitute emojis from the provided string.  Return
        the resulting substitution or None."""
        text = text.replace('\n', ' ')
        self.initializeEmojiModules()
        for moduleName in self.emojiModuleNames:
            module = self.emojiModules[moduleName]
            result = module.substitute(text)
            if result is not None:
                return result

    # Exit codes and errors.

    def isSuccessCode(self, code):
        """Does this exit code indicate success?"""
        return code == self.successCode

    def isExitError(self, error):
        """Is this error a SystemExit?"""
        return isinstance(error, SystemExit)

    def errorToExitCode(self, error):
        """Determine the exit code (can be a string) from the
        error.  If the error is None, then it is success."""
        if error is None:
            return self.successCode
        isExit = self.isExitError(error)
        if isExit:
            if len(error.args) == 0:
                return self.successCode
            else:
                # This can be a string which is okay.
                return error.args[0]
        else:
            return self.failureCode

    def isNotAnError(self, error):
        """Is this error None (no error) or does it indicate a
        successful exit?"""
        if error is None:
            return True
        else:
            return self.isSuccessCode(self.errorToExitCode(error))

    def formatError(self, error, prefix=None, suffix=None):
        """Format an error into a string for printing."""
        parts = []
        if prefix is not None:
            parts.append(prefix)
        parts.append(error.__class__.__name__)
        if self.verboseErrors:
            # Find the error's arguments.  This needs special treatment due to
            # spurious Java exceptions leaking through under Jython.
            args = getattr(error, 'args', None)
            if args is None:
                # It might be an unwrapped Java exception.  Check for
                # getMessage.
                method = getattr(error, 'getMessage', None)
                if method is not None:
                    args = (method(),)
                else:
                    # Otherwise, not sure what this is; treat it as having no
                    # arguments.
                    args = ()
            # Check for arguments.
            if len(args) > 0:
                parts.append(": ")
                parts.append(", ".join([toString(x) for x in args]))
            # Check for keyword arguments.
            pairs = []
            for attrib in dir(error):
                if (attrib not in self.ignorableErrorAttributes and
                    not attrib.startswith('_')):
                    value = getattr(error, attrib, None)
                    if value is not None:
                        pairs.append((attrib, value))
            if pairs:
                parts.append("; ")
                pairs.sort()
                kwargs = []
                for key, value in pairs:
                    kwargs.append("%s=%s" % (key, value))
                parts.append(', '.join(kwargs))
        # Fold the arguments together.
        if suffix is not None:
            parts.append(suffix)
        return ''.join(parts)

    # Proxy.

    @staticmethod
    def proxy(object=sys):
        """Find the proxy for this Python interpreter session, or
        None."""
        return getattr(object, '_EmPy_proxy', None)

    @staticmethod
    def evocare(increment=0, ignore=True):
        """Try to call the EmPy special method on the proxy with the
        given increment argument and return the resulting count value.
        If the magic method is not present (no proxy installed) and
        ignore is true (default), return None; otherwise, raise.  Exodus
        is four as one!"""
        method = getattr(Configuration.proxy(), '_EmPy_evocare', None)
        if method is not None:
            try:
                return method(increment)
            except:
                raise ProxyError("proxy evocare method should not raise")
        else:
            if ignore:
                return None
            else:
                raise ProxyError("proxy evocare method not found")

    def installProxy(self, output):
        """Install a proxy if necessary around the given output,
        wrapped to be uncloseable.  Return the wrapped object (not the
        proxy)."""
        assert output is not None
        # Invoke the special method ...
        count = self.evocare(+1)
        proxy = self.proxy()
        if count is not None:
            # ... and if it's present, we've already created it.
            new = False
        else:
            if proxy is None:
                # If not, setup the proxy, and increment the reference count.
                proxy = sys._EmPy_proxy = ProxyFile(output, self.proxyWrapper)
                if self.useProxy:
                    # Replace sys.stdout with the proxy.
                    sys.stdout = proxy
                self.evocare(+1)
            else:
                # ... but if the count showed no proxy but there is one,
                # something went wrong.
                raise ProxyError("proxy conflict; no proxy registered but one found")
            new = True
        if not self.useProxy:
            output = UncloseableFile(output)
        return output

    def uninstallProxy(self):
        """Uninstall a proxy if necessary."""
        # Try decrementing the reference count; if it hits zero, it will
        # automatically remove itself and restore sys.stdout.
        try:
            proxy = self.proxy()
            done = not self.evocare(-1)
            if done:
                del sys._EmPy_proxy
        except AttributeError:
            if self.proxy() is not None:
                raise ProxyError("proxy lost")

    def checkProxy(self, abandonedIsError=True):
        """Check whether a proxy is installed.  Returns the
        current reference count (positive means one is
        installed), None (for no proxy installed), or 0 if the
        proxy has been abandoned.  Thus, true means a proxy is
        installed, false means one isn't.  If abandonIsError
        is true, raise instead of returning 0 on abandonment."""
        if not self.useProxy:
            return False
        count = self.evocare(0)
        if count is not None:
            if count == 0 and abandonedIsError:
                raise ProxyError("stdout proxy abandoned; proxy present but with zero reference count: %r" % sys.stdout)
            return count
        else:
            return None

    # Meta path finder (for module support).

    @staticmethod
    def finder(object=sys):
        """Find the meta path finder for this Python interpreter
        session, if there is one."""
        return getattr(object, '_EmPy_finder', None)

    def createFinder(self):
        """Create a new finder object, ready for installation."""
        # Use the importlib architecture to set up an EmPy path finder.
        import importlib
        import importlib.abc
        import importlib.util

        #
        # Loader
        #

        class Loader(importlib.abc.Loader):

            def __init__(self, filename):
                self.filename = filename

            def create_module(self, spec):
                return None # default

            def exec_module(self, module):
                interp = sys.stdout._EmPy_current()
                assert interp
                interp.import_(self.filename, module)

        #
        # Finder
        #

        class Finder(importlib.abc.PathEntryFinder):

            _EmPy_next = 1

            def __init__(self):
                self._EmPy_tag = self._EmPy_next
                self.__class__._EmPy_next += 1

            def __str__(self):
                return '%s [tag %d]' % (
                    self.__class__.__name__, self._EmPy_tag)

            def find_spec(self, fullname, path, target=None):
                # If the proxy is not installed, skip.
                method = getattr(sys.stdout, '_EmPy_current', None)
                if not method:
                    return None
                interp = method()
                # If there's no active interpreter, also skip.
                if not interp:
                    return None
                # If this interpreter has modules disable, also skip.
                if not interp.config.supportModules:
                    return None
                if not path:
                    path = sys.path
                import os
                name = fullname.replace('.', os.sep)
                for dirname in path:
                    filename = (os.path.join(dirname, name) +
                        interp.config.moduleExtension)
                    if os.path.isfile(filename):
                        return importlib.util.spec_from_file_location(
                                fullname, filename, loader=Loader(filename))
                return None

        return Finder()

    def installFinder(self, index=None, dryRun=False):
        """Install EmPy module support, if possible.  Mark a flag the
        first time this is called so it's only installed once, if ever.
        Idempotent."""
        if Configuration.finder():
            # A finder had already been installed; abort.
            return None
        if not self.supportModules or not self.moduleExtension:
            # This configuration does not want to support mnodules; abort.
            return None
        if not modules:
            # Modules are not supported by the underlying interpreter; abort.
            return None
        if dryRun:
            # This is a dry run for displaying details; abort.
            return None
        # Create the finder.
        finder = self.createFinder()
        # Register it with this configuration.
        self.tag = finder._EmPy_tag
        # And install it.
        if index is None:
            index = self.moduleFinderIndex
        if index < 0:
            sys.meta_path.append(finder)
        else:
            sys.meta_path.insert(index, finder)
        # Register it with the sys module.
        sys._EmPy_finder = finder
        return finder

    def uninstallFinder(self, tag=None):
        """Uninstall any module meta path finder for EmPy support,
        either by tag (if not None), or all.  Idempotent."""
        if sys.meta_path is not None:
            newMetaPath = []
            for finder in sys.meta_path:
                finderTag = getattr(finder, '_EmPy_tag', None)
                if tag is None:
                    # Delete any custom finder.
                    if finderTag is not None:
                        continue
                else:
                    # Delete only the matching finder.
                    if finderTag == tag:
                        continue
                # If we're still here, we're keeping this finder.
                newMetaPath.append(finder)
            sys.meta_path = newMetaPath

    # Debugging.

    def printTraceback(self, file=sys.stderr):
        import types, traceback
        tb = None
        depth = 0
        while True:
            try:
                frame = sys._getframe(depth)
                depth += 1
            except ValueError:
                break
            tb = types.TracebackType(tb, frame, frame.f_lasti, frame.f_lineno)
        traceback.print_tb(tb, file=file)

#
# Version
#

class Version(Root):

    """An enumerated type representing version detail levels."""

    NONE, VERSION, INFO, BASIC, PYTHON, SYSTEM, PLATFORM, RELEASE, ALL = range(9)
    DATA = BASIC

#
# Stack
#

class Stack(Root):

    """A simple stack that is implemented as a sequence."""

    def __init__(self, seq=None):
        if seq is None:
            seq = []
        self.data = seq

    def __bool__(self): return len(self.data) != 0 # 3.x
    def __nonzero__(self): return len(self.data) != 0 # 2.x

    def __len__(self): return len(self.data)
    def __getitem__(self, index): return self.data[-(index + 1)]

    def __str__(self):
        return '[%s]' % ', '.join([repr(x) for x in self.data])

    def top(self):
        """Access the top element on the stack."""
        if self.data:
            return self.data[-1]
        else:
            raise StackUnderflowError("stack is empty for top")

    def pop(self):
        """Pop the top element off the stack and return it."""
        if self.data:
            return self.data.pop()
        else:
            raise StackUnderflowError("stack is empty for pop")

    def push(self, object):
        """Push an element onto the top of the stack."""
        self.data.append(object)

    def replace(self, object):
        """Replace the top element of the stack with another one."""
        if self.data:
            self.data[-1] = object
        else:
            raise StackUnderflowError("stack is empty for replace")

    def filter(self, function):
        """Filter the elements of the stack through the function."""
        self.data = list(filter(function, self.data))

    def purge(self, function=None):
        """Purge the stack, calling an optional function on each element
        first from top to bottom."""
        if function is None:
            self.data = []
        else:
            while self.data:
                element = self.data.pop()
                function(element)

    def clone(self):
        """Create a duplicate of this stack."""
        return self.__class__(self.data[:])

#
# File ...
#

class File(Root):

    """An abstract filelike object."""

    def __enter__(self):
        pass

    def __exit__(self, *exc):
        self.close()

    def write(self, data): raise NotImplementedError
    def writelines(self, lines): raise NotImplementedError
    def flush(self): raise NotImplementedError
    def close(self): raise NotImplementedError


class NullFile(File):

    """A simple class that supports all the file-like object methods
    but simply does nothing at all."""

    def write(self, data): pass
    def writelines(self, lines): pass
    def flush(self): pass
    def close(self): pass


class DelegatingFile(File):

    """A simple class which wraps around a delegate file-like object
    and lets everything through."""

    def __init__(self, delegate):
        self.delegate = delegate

    def __repr__(self):
        return '<%s : %r @ 0x%x>' % (
            self.__class__.__name__,
            self.delegate,
            id(self))

    def write(self, data):
        self.delegate.write(data)

    def writelines(self, lines):
        self.delegate.writelines(lines)

    def flush(self):
        self.delegate.flush()

    def close(self):
        self.delegate.close()
        self.unlink()

    def unlink(self):
        """Unlink from the delegate."""
        self.delegate = None


class UncloseableFile(DelegatingFile):

    """A delegating file class that lets through everything except
    close calls, which it turns into a flush."""

    def close(self):
        self.flush()


class ProxyFile(File):

    """The proxy file object that is intended to take the place of
    sys.stdout.  The proxy can manage a stack of interpreters (and
    their file object streams) it is writing to, and an underlying
    bottom file object."""

    def __init__(self, bottom, wrapper=None):
        """Create a new proxy file object with bottom as the
        underlying stream.  If wrapper is not None (and is an instance
        of (a subclass of) a DelegatingFile), we should wrap and
        unwrap the bottom file with it for protection."""
        assert bottom is not None
        self._EmPy_original = sys.stdout
        self._EmPy_count = 0
        self._EmPy_stack = Stack()
        self._EmPy_bottom = bottom
        self._EmPy_wrapper = wrapper
        self._EmPy_wrap()

    def __del__(self):
        self.finalize()

    def __str__(self):
        return '%s [count %d, depth %d]'% (
            self.__class__.__name__,
            self._EmPy_count,
            len(self._EmPy_stack))

    def __repr__(self):
        return '<%s [count %d, depth %d] : %r @ 0x%x>' % (
            self.__class__.__name__,
            self._EmPy_count,
            len(self._EmPy_stack),
            self._EmPy_bottom,
            id(self))

    def __getattr__(self, name):
        return getattr(self._EmPy_top(), name)

    # Finalizer.

    def finalize(self): pass

    # File methods.

    def write(self, data):
        self._EmPy_top().write(data)

    def writelines(self, lines):
        self._EmPy_top().writelines(lines)

    def flush(self):
        top = self._EmPy_top()
        assert top is not None
        top.flush()

    def close(self):
        """Close the current file.  If the current file is the bottom,
        then flush it (don't close it) and dispose of it."""
        top = self.top()
        assert top is not None
        if top is self._EmPy_bottom:
            # If it's the bottom stream, flush it, don't close it, and mark
            # this proxy done.
            top.flush()
            self._EmPy_bottom = None
        else:
            top.close()

    # Stack management.

    def _EmPy_enabled(self):
        """Is the top interpreter enabled?"""
        if self._EmPy_stack:
            return self._EmPy_stack.top().enabled
        else:
            return True

    def _EmPy_current(self):
        """Get the current interpreter, or None."""
        if self._EmPy_stack:
            return self._EmPy_stack.top()
        else:
            return None

    def _EmPy_top(self):
        """Get the current stream (not interpreter) to write to."""
        if self._EmPy_stack:
            return self._EmPy_stack.top().top()
        else:
            return self._EmPy_bottom

    def _EmPy_push(self, interpreter):
        self._EmPy_stack.push(interpreter)

    def _EmPy_pop(self, interpreter):
        top = self._EmPy_stack.pop()
        if interpreter.error is not None and interpreter is not top:
            # Only check if an error is not in progress; otherwise, when there
            # are interpreters and subinterpreters and full dispatchers,
            # interpreters can get popped out of order.
            raise ConsistencyError("interpreter popped off of proxy stack out of order")

    def _EmPy_clear(self, interpreter):
        self._EmPy_stack.filter(lambda x, i=interpreter: x is not i)

    # Bottom file protection.

    def _EmPy_shouldWrap(self):
        """Should the bottom file be wrapped and unwrapped?"""
        return self._EmPy_wrapper is not None

    def _EmPy_wrap(self):
        """Wrap the bottom file in a delegate."""
        if self._EmPy_shouldWrap():
            assert issubclass(self._EmPy_wrapper, DelegatingFile), self._EmPy_wrapper
            self._EmPy_bottom = self._EmPy_wrapper(self._EmPy_bottom)

    def _EmPy_unwrap(self):
        """Unwrap the bottom file from the delegate, and unlink the
        delegate."""
        if self._EmPy_shouldWrap():
            wrapped = self._EmPy_bottom
            self._EmPy_bottom = wrapped.delegate
            wrapped.unlink()

    # Special.

    def _EmPy_evocare(self, increment):
        """Do the EmPy magic:  Increment or decrement the reference
        count.  Either way, return the current reference count.
        Beware of Exodus!"""
        if increment > 0:
            self._EmPy_count += increment
        elif increment < 0:
            self._EmPy_count += increment # note: adding a negative number
            if self._EmPy_count <= 0:
                assert self._EmPy_original is not None
                self._EmPy_unwrap()
                sys.stdout = self._EmPy_original
        return self._EmPy_count

#
# Diversion
#

class Diversion(File):

    """The representation of an active diversion.  Diversions act as
    (writable) file objects, and then can be recalled either as pure
    strings or (readable) file objects."""

    def __init__(self, name):
        self.name = name
        self.file = StringIO()

    def __str__(self):
        return self.name

    # These methods define the writable file-like interface for the diversion.

    def write(self, data):
        self.file.write(data)

    def writelines(self, lines):
        for line in lines:
            self.write(line)

    def flush(self):
        self.file.flush()

    def close(self):
        self.file.close()

    # These methods are specific to diversions.

    def preferFile(self):
        """Would this particular diversion prefer to be treated as a
        file (true) or a string (false)?  This allows future
        optimization of diversions into actual files if they get
        overly large."""
        return False

    def asString(self):
        """Return the diversion as a string."""
        return self.file.getvalue()

    def asFile(self):
        """Return the diversion as a file."""
        return StringIO(self.file.getvalue())

    def spool(self, sink, chunkSize=Configuration.defaultBuffering):
        """Spool the diversion to the given sink."""
        if self.preferFile():
            # Either write it a chunk at a time ...
            input = self.asFile()
            while True:
                chunk = input.read(chunkSize)
                if not chunk:
                    break
                sink.write(chunk)
        else:
            # ... or write it all at once.
            sink.write(self.asString())

#
# Stream
#

class Stream(File):

    """A wrapper around an (output) file object which supports
    diversions and filtering."""

    def __init__(self, interp, file, diversions):
        assert file is not None
        self.interp = interp
        self.file = file
        self.current = None
        self.diversions = diversions
        self.sink = file
        self.done = False

    def __repr__(self):
        return '<%s : %r @ 0x%x>' % (
            self.__class__.__name__,
            self.file,
            id(self))

    def __getattr__(self, name):
        return getattr(self.sink, name)

    # File methods.

    def write(self, data):
        if self.current is None:
            if self.interp.enabled:
                self.sink.write(data)
        else:
            self.diversions[self.current].write(data)

    def writelines(self, lines):
        if self.interp.enabled:
            for line in lines:
                self.write(line)

    def flush(self):
        self.sink.flush()

    def close(self):
        self.flush()
        if not self.done:
            self.sink.close()
            self.done = True

    # Filters.

    def count(self):
        """Count the number of filters."""
        thisFilter = self.sink
        result = 0
        while thisFilter is not None and thisFilter is not self.file:
            thisFilter = thisFilter.follow()
            result += 1
        return result

    def last(self):
        """Find the last filter in the current filter chain, or None if
        there are no filters installed."""
        if self.sink is None:
            return None
        thisFilter, lastFilter = self.sink, None
        while thisFilter is not None and thisFilter is not self.file:
            lastFilter = thisFilter
            thisFilter = thisFilter.follow()
        return lastFilter

    def install(self, filters=None):
        """Install a list of filters as a chain, replacing the current
        chain."""
        self.sink.flush()
        if filters is None:
            filters = []
        if len(filters) == 0:
            # An empty sequence means no filter.
            self.sink = self.file
        else:
            # If there's more than one filter provided, chain them together.
            lastFilter = None
            for filter in filters:
                if lastFilter is not None:
                    lastFilter.attach(filter)
                lastFilter = filter
            lastFilter.attach(self.file)
            self.sink = filters[0]

    def prepend(self, filter):
        """Attach a solitary filter (no sequences allowed here)
        at the beginning of the current filter chain."""
        self.sink.flush()
        firstFilter = self.sink
        if firstFilter is None:
            # Just install it from scratch if there is no active filter.
            self.install([filter])
        else:
            # Attach this filter to the current one, and set this as the main
            # filter.
            filter.attach(firstFilter)
            self.sink = filter

    def append(self, filter):
        """Attach a solitary filter (no sequences allowed here) at the
        end of the current filter chain."""
        self.sink.flush()
        lastFilter = self.last()
        if lastFilter is None:
            # Just install it from scratch if there is no active filter.
            self.install([filter])
        else:
            # Attach the last filter to this one, and this one to the file.
            lastFilter.attach(filter)
            filter.attach(self.file)

    # Diversions.

    def names(self):
        """Return a sorted sequence of diversion names."""
        keys = list(self.diversions.keys())
        keys.sort()
        return keys

    def has(self, name):
        """Does this stream have a diversion with the given name?"""
        return name in self.diversions

    def revert(self):
        """Reset any current diversions."""
        self.current = None

    def create(self, name):
        """Create a diversion if one does not already exist, but do not
        divert to it yet.  Return the diversion."""
        if name is None:
            raise DiversionError("diversion name must be non-None")
        diversion = None
        if not self.has(name):
            diversion = Diversion(name)
            self.diversions[name] = diversion
        return diversion

    def retrieve(self, name, *defaults):
        """Retrieve the given diversion.  If an additional argument is
        provided, return that instead of raising on a nonexistent
        diversion."""
        if name is None:
            raise DiversionError("diversion name must be non-None")
        if self.has(name):
            return self.diversions[name]
        else:
            if defaults:
                return defaults[0]
            else:
                raise DiversionError("nonexistent diversion: `%s`" % name)

    def divert(self, name):
        """Start diverting."""
        if name is None:
            raise DiversionError("diversion name must be non-None")
        self.create(name)
        self.current = name

    def undivert(self, name, dropAfterwards=False):
        """Undivert a particular diversion."""
        if name is None:
            raise DiversionError("diversion name must be non-None")
        if self.has(name):
            if self.interp.enabled:
                diversion = self.diversions[name]
                diversion.spool(self.sink)
            if dropAfterwards:
                self.drop(name)
        else:
            raise DiversionError("nonexistent diversion: `%s`" % name)

    def drop(self, name):
        """Drop the specified diversion."""
        if name is None:
            raise DiversionError("diversion name must be non-None")
        if self.has(name):
            del self.diversions[name]
            if self.current == name:
                self.current = None

    def undivertAll(self, dropAfterwards=True):
        """Undivert all pending diversions."""
        if self.diversions:
            self.revert() # revert before undiverting!
            for name in self.names():
                self.undivert(name, dropAfterwards)

    def dropAll(self):
        """Eliminate all existing diversions."""
        if self.diversions:
            self.diversions.clear()
        self.current = None

#
# Context
#

class Context(Root):

    """A simple interpreter context, storing only the current data.
    It is not intended to be modified."""

    format = Configuration.defaultContextFormat

    def __init__(self, name, line, column, chars=0,
                 startingLine=None, startingColumn=None):
        self.name = name
        self.line = line
        self.column = column
        self.chars = chars
        if startingLine is None:
            startingLine = line
        self.startingLine = startingLine
        if startingColumn is None:
            startingColumn = column
        self.startingColumn = startingColumn
        self.pendingLines = 0
        self.pendingColumns = 0
        self.pendingChars = 0

    def __str__(self):
        return self.render(self.format)

    def reset(self):
        """Reset the context to the start."""
        self.line = self.startingLine
        self.column = self.startingColumn
        self.chars = 0

    def track(self, string, start, end):
        """Track the information for the substring in [start, end) in
        the context and mark it pending."""
        assert end >= start, (start, end)
        if end > start:
            length = end - start
            self.pendingLines += string.count(NEWLINE_CHAR, start, end)
            loc = string.rfind(NEWLINE_CHAR, start, end)
            if loc == -1:
                self.pendingColumns += length
            else:
                self.pendingColumns = self.startingColumn + end - loc - 1
            self.pendingChars += length

    def accumulate(self):
        """Accumulate the pending information and incorporate it into
        the total."""
        self.line += self.pendingLines
        if self.pendingLines > 0:
            self.column = self.pendingColumns # replaced, not added
        else:
            self.column += self.pendingColumns
        self.chars += self.pendingChars
        self.pendingLines = 0
        self.pendingColumns = 0
        self.pendingChars = 0

    def save(self, strict=False):
        """Take a snapshot of the current context."""
        if strict and self.pendingChars == 0:
            raise ConsistencyError("context %s has pending chars" % toString(self))
        return Context(self.name, self.line, self.column, self.chars)

    def restore(self, other, strict=False):
        """Restore from another context."""
        if strict and self.pendingChars == 0:
            raise ConsistencyError("context %s has pending chars" % toString(self))
        self.name = other.name
        self.line = other.line
        self.column = other.column
        self.chars = other.chars

    def render(self, format, useFormatMethod=None):
        """Render the context with the given format.  If
        useFormatMethod is true, use the format method; if false, use
        the % operator.  If useFormatMethod is None, try to trivially
        auto-detect given the format."""
        record = {
            'name': self.name,
            'line': self.line,
            'column': self.column,
            'chars': self.chars,
        }
        if useFormatMethod is None:
            # If it contains a percent sign, it looks like it's not using the
            # format method.
            useFormatMethod = '%' not in format
        if useFormatMethod:
            return format.format(**record)
        else:
            return format % record

    def identify(self):
        return self.name, self.line, self.column, self.chars

#
# Token ...
#

class Token(Root):

    """An element resulting from parsing."""

    def __init__(self, current):
        self.current = current

    def __str__(self):
       return self.string()

    def string(self):
        raise NotImplementedError

    def run(self, interp, locals):
        raise NotImplementedError


class TextToken(Token):

    """A chunk of text not containing markups."""

    def __init__(self, current, data):
        super(TextToken, self).__init__(current)
        self.data = data

    def string(self):
        return self.data

    def run(self, interp, locals):
        interp.write(self.data)


class ExpansionToken(Token):

    """A token that involves an expansion."""

    last = None # subclasses should always define a first class attribute

    def __init__(self, current, config, first):
        super(ExpansionToken, self).__init__(current)
        self.config = config
        # Only record a local copy of first/last if it's ambiguous.
        if self.first is None:
            self.first = first
        elif len(self.first) != 1:
            first = first[:1]
            self.first = first
            self.last = ENDING_CHAR_MAP.get(first, first)

    def scan(self, scanner):
        pass

    def run(self, interp, locals):
        pass


class CommentToken(ExpansionToken):

    """The abstract base class for the comment tokens."""

    pass


class LineCommentToken(CommentToken):

    """A line comment markup: ``@# ... NL``"""

    first = OCTOTHORPE_CHAR

    def scan(self, scanner):
        loc = scanner.find(NEWLINE_CHAR)
        if loc >= 0:
            self.comment = scanner.chop(loc, 1)
        else:
            raise TransientParseError("comment expects newline")

    def string(self):
        return '%s%s%s\n' % (self.config.prefix, self.first, self.comment)

    def run(self, interp, locals):
        interp.invoke('preLineComment', comment=self.comment)


class InlineCommentToken(CommentToken):

    """An inline comment markup: ``@* ... *``"""

    first = ASTERISK_CHAR
    last = ASTERISK_CHAR

    def scan(self, scanner):
        # Find the run of starting characters to match.
        self.count = 1
        start = scanner.last(self.first)
        self.count += start
        scanner.advance(start)
        # Then match them with the same number of closing characters.
        loc = scanner.find(self.last * self.count)
        if loc >= 0:
            self.comment = scanner.chop(loc, self.count)
        else:
            raise TransientParseError("inline comment expects asterisk")

    def string(self):
        return '%s%s%s%s' % (
            self.config.prefix,
            self.first * self.count,
            self.comment,
            self.last * self.count)

    def run(self, interp, locals):
        interp.invoke('preInlineComment', comment=self.comment)


class LiteralToken(ExpansionToken):

    """The abstract base class of the literal tokens.  If used as a
    concrete token class, it will expand to the first character."""

    def string(self):
        return '%s%s' % (self.config.prefix, self.first)

    def run(self, interp, locals):
        interp.write(self.first)


class WhitespaceToken(LiteralToken):

    """A whitespace markup: ``@ WS``"""

    first = '<whitespace>'

    def string(self):
        return '%s%s' % (self.config.prefix, self.first)

    def run(self, interp, locals):
        interp.invoke('preWhitespace', whitespace=self.first)


class SwitchToken(CommentToken):

    """Base class for the enable/disable tokens."""

    def scan(self, scanner):
        loc = scanner.find(NEWLINE_CHAR)
        if loc >= 0:
            self.comment = scanner.chop(loc, 1)
        else:
            raise TransientParseError("switch expects newline")

    def string(self):
        return '%s%s%s\n' % (self.config.prefix, self.first, self.comment)


class EnableToken(SwitchToken):

    """An enable output markup: ``@+ ... NL``"""

    first = PLUS_CHAR

    def run(self, interp, locals):
        interp.invoke('preEnable', comment=self.comment)
        interp.enable()


class DisableToken(SwitchToken):

    """An disable output markup: ``@- ... NL``"""

    first = MINUS_CHAR

    def run(self, interp, locals):
        interp.invoke('preDisable', comment=self.comment)
        interp.disable()


class PrefixToken(LiteralToken):

    """A prefix markup: ``@@``"""

    first = None # prefix

    def string(self):
        return self.config.prefix * 2

    def run(self, interp, locals):
        if interp.invoke('prePrefix'):
            return
        interp.write(self.config.prefix)


class StringToken(LiteralToken):

    """A string token markup: ``@'...'``, ``@'''...'''``, ``@"..."``,
    ``@\"\"\"...\"\"\"``"""

    first = list(QUOTE_CHARS)

    def scan(self, scanner):
        scanner.retreat()
        assert scanner[0] == self.first, (scanner[0], self.first)
        i = scanner.quote()
        self.literal = scanner.chop(i)

    def string(self):
        return '%s%s' % (self.config.prefix, self.literal)

    def run(self, interp, locals):
        if interp.invoke('preString', string=self.literal):
            return
        interp.literal(self.literal, locals)
        interp.invoke('postString')


class BackquoteToken(LiteralToken):

    """A backquote markup: ``@`...```"""

    first = BACKQUOTE_CHAR

    def scan(self, scanner):
        # Find the run of starting characters to match.
        self.count = 1
        start = scanner.last(self.first)
        self.count += start
        scanner.advance(start)
        # Then match them with the same number of closing characters.
        loc = scanner.find(self.first * self.count)
        if loc >= 0:
            self.literal = scanner.chop(loc, self.count)
        else:
            raise TransientParseError("backquote markup expects %d backquotes" % self.count)

    def string(self):
        return '%s%s%s%s' % (
            self.config.prefix,
            self.first * self.count,
            self.literal,
            self.first * self.count)

    def run(self, interp, locals):
        if interp.invoke('preBackquote', literal=self.literal):
            return
        interp.write(self.literal)
        interp.invoke('postBackquote', result=self.literal)


class SimpleToken(ExpansionToken):

    """An abstract base class for simple tokens which consist of nothing but
    the prefix and expand to either the results a function call in the
    interpreter globals (if args is not specified or is a tuple) or the value
    of a variable name named function (if args is None)."""

    def string(self):
        return self.config.prefix + self.first

    def run(self, interp, locals):
        args = getattr(self, 'args', ())
        if args is None:
            result = interp.lookup(self.function)
        else:
            callable = interp.lookup(self.function)
            result = callable(*args)
        interp.write(result)


class ExecutionToken(ExpansionToken):

    """The abstract base class for execution tokens (expressions,
    statements, controls, etc.)"""

    pass


class ExpressionToken(ExecutionToken):

    """An expression markup: ``@(...)``"""

    first = OPEN_PARENTHESIS_CHAR
    last = ENDING_CHAR_MAP[first]

    def scan(self, scanner):
        start = 0
        end = scanner.complex(self.first, self.last)
        try:
            except_ = scanner.next('$', start, end, True)
        except ParseError:
            except_ = end
        # Build up a list of relevant indices (separating start/if/then/else
        # clauses)
        indices = [start - 1] # start, if, then, [if, then]..., [else]
        while True:
            try:
                first = scanner.next('?', start, except_, True)
                indices.append(first)
                try:
                    last = scanner.next('!', first, except_, True)
                    indices.append(last)
                except ParseError:
                    last = except_
                    break
            except ParseError:
                first = last = except_
                break
            start = last + 1
        indices.append(except_)
        code = scanner.chop(end, 1)
        # Now build up the ifThenCodes pair chain.
        self.ifThenCodes = []
        prev = None
        pair = None
        for index in indices:
            if prev is not None:
                # pair can either be None or a 2-list, possibly with None as
                # the second element.
                if pair is None:
                    pair = [code[prev + 1:index], None]
                else:
                    pair[1] = code[prev + 1:index]
                    self.ifThenCodes.append(pair)
                    pair = None
            prev = index
        # If there's a half-constructed pair not yet added to the chain, add it
        # now.
        if pair is not None:
            self.ifThenCodes.append(pair)
        self.exceptCode = code[except_ + 1:end]

    def string(self):
        fragments = []
        sep = None
        for ifCode, thenCode in self.ifThenCodes:
            if sep:
                fragments.append(sep)
            fragments.append(ifCode)
            if thenCode is not None:
                fragments.append('?')
                fragments.append(thenCode)
            sep = '!'
        if self.exceptCode:
            fragments.append('$' + self.exceptCode)
        return '%s%s%s%s' % (
            self.config.prefix, self.first,
            ''.join(fragments), self.last)

    def run(self, interp, locals):
        # ifThenCodes is a list of 2-lists.  A list of one sublist whose second
        # subelement is None is a simple expression to evaluate; no
        # if-then-else logic.  A list of more than one sublist is a chained
        # if-then-if-then-...-else clause with each sublist containing an if
        # and a then subelement.  If the second subelement is None, then the
        # first element is not an if but an else.
        if interp.invoke('preExpression', pairs=self.ifThenCodes,
                         except_=self.exceptCode, locals=locals):
            return
        result = None
        try:
            for ifCode, thenCode in self.ifThenCodes:
                # An if or an else clause; evaluate it.
                result = interp.evaluate(ifCode, locals)
                if thenCode is None:
                    # If there's no then clause, then this is an isolated if or
                    # a final else, so we're done.
                    break
                else:
                    if result:
                        # With a then clause with a true if clause, evaluate
                        # it.
                        result = interp.evaluate(thenCode, locals)
                        # If it's true, we're done.
                        if result:
                            break
                    else:
                        # Otherwise, go again.  If there no remaining pairs,
                        # return None (no expansion).
                        result = None
        except self.config.fallThroughErrors:
            # Don't catch these errors; let them fall through.
            raise
        except:
            if self.exceptCode:
                result = interp.evaluate(self.exceptCode, locals)
            else:
                raise
        interp.serialize(result)
        interp.invoke('postExpression', result=result)


class SimpleExpressionToken(ExecutionToken):

    """A simple expression markup: ``@x``, ``@x.y``, ``@x(y)``, ``@x[y]``,
    ``@f{...}``"""

    first = '<identifier>'

    def top(self):
        return self.subtokens[-1]

    def new(self):
        self.subtokens.append([])

    def append(self, token):
        self.subtokens[-1].append(token)

    def scan(self, scanner, begin='{', end='}'):
        i = scanner.simple()
        self.code = self.first + scanner.chop(i)
        # Now scan ahead for functional expressions.
        self.subtokens = []
        scanner.acquire()
        try:
            while True:
                if not scanner:
                    raise TransientParseError("need more context for end of simple expression")
                if scanner.read() != begin:
                    break
                self.new()
                count = None
                while True:
                    peek = scanner.read()
                    if peek == begin:
                        # The start of an argument.
                        if count is None:
                            count = scanner.last(begin)
                            scanner.chop(count)
                            current = self.config.renderContext(scanner.context)
                            scanner.currents.replace(current)
                    elif peek == end and count is not None:
                        # The possible end of an argument.
                        if scanner.read(0, count) == end * count:
                            scanner.chop(count)
                            current = self.config.renderContext(scanner.context)
                            scanner.currents.replace(current)
                            break
                    token = scanner.one([end * count])
                    self.append(token)
        finally:
            scanner.release()

    def string(self):
        results = ['%s%s' % (self.config.prefix, self.code)]
        for tokens in self.subtokens:
            results.append('{%s}' % ''.join(map(str, tokens)))
        return ''.join(results)

    def run(self, interp, locals):
        if interp.invoke('preSimple', code=self.code,
                         subtokens=self.subtokens, locals=locals):
            return
        result = None
        if self.subtokens:
            result = interp.functional(self.code, self.subtokens, locals)
        else:
            result = interp.evaluate(self.code, locals)
        interp.serialize(result)
        interp.invoke('postSimple', result=result)


class InPlaceToken(ExecutionToken):

    """An in-place markup: ``@$...$...$``"""

    first = DOLLAR_CHAR

    def scan(self, scanner):
        i = scanner.next(self.first)
        j = scanner.next(self.first, i + 1)
        self.code = scanner.chop(i, j - i + 1)

    def string(self):
        return '%s%s%s%s%s' % (
            self.config.prefix, self.first, self.code, self.first, self.first)

    def run(self, interp, locals):
        if interp.invoke('preInPlace', code=self.code, locals=locals):
            return
        result = None
        interp.write("%s%s%s%s" % (
            self.config.prefix, self.first,
            self.code, self.first))
        try:
            result = interp.evaluate(self.code, locals)
            interp.serialize(result)
        finally:
            interp.write(self.first)
        interp.invoke('postInPlace', result=result)


class StatementToken(ExecutionToken):

    """A statement markup: ``@{...}``"""

    first = OPEN_BRACE_CHAR
    last = ENDING_CHAR_MAP[first]

    def scan(self, scanner):
        i = scanner.complex(self.first, self.last)
        self.code = scanner.chop(i, 1)

    def string(self):
        return '%s%s%s%s' % (
            self.config.prefix, self.first, self.code, self.last)

    def run(self, interp, locals):
        if interp.invoke('preStatement', code=self.code, locals=locals):
            return
        interp.execute(self.code, locals)
        interp.invoke('postStatement')


class ControlToken(ExecutionToken):

    """A control markup: ``@[...]``"""

    first = OPEN_BRACKET_CHAR
    last = ENDING_CHAR_MAP[first]

    class Chain(Root):

        """A chain of tokens with a starting token and the rest
        that follow."""

        def __init__(self, head, tail):
            self.head = head
            self.tail = tail

        def __str__(self):
            return '(%s, %s)' % (self.head, self.tail)

        def getType(self):
            return self.head.type

        def hasType(self, name):
            return self.head.type == name

    PRIMARY = ['if', 'for', 'while', 'dowhile', 'try', 'with', 'match', 'defined', 'def']
    SECONDARY = ['elif', 'else', 'except', 'finally', 'case']
    TERTIARY = ['continue', 'break']
    GREEDY = [
        'if', 'elif', 'for', 'while', 'dowhile', 'with', 'match', 'defined', 'def', 'end'
    ]
    CLEAN = ['try', 'else', 'except', 'finally', 'case', 'continue', 'break', 'end']
    END = ['end']

    ALLOWED = {
        'if': ['elif', 'else'],
        'for': ['else'],
        'while': ['else'],
        'dowhile': ['else'],
        'try': ['except', 'else', 'finally'],
        'with': [],
        'match': ['case', 'else'],
        'defined': ['else'],
        'def': None,
        'continue': None,
        'break': None,
    }

    IN_RE = re.compile(r"\bin\b")
    AS_RE = re.compile(r"\bas\b")

    runPrefix = 'run_'
    elseCase = '_'

    def scan(self, scanner):
        scanner.acquire()
        try:
            i = scanner.complex(self.first, self.last)
            self.contents = scanner.chop(i, 1)
            fields = self.contents.strip().split(None, 1)
            if len(fields) > 1:
                self.type, self.rest = fields
                # If this is a "clean" control, remove anything that looks like
                # a comment.
                if self.type in self.CLEAN and '#' in self.rest:
                    self.rest = self.rest.split('#', 1)[0].strip()
            else:
                self.type = fields[0]
                self.rest = None
            self.subtokens = []
            if self.type in self.GREEDY and self.rest is None:
                raise ParseError("control `%s` needs arguments" % self.type)
            if self.type in self.PRIMARY:
                self.subscan(scanner, self.type)
                self.kind = 'primary'
            elif self.type in self.SECONDARY:
                self.kind = 'secondary'
            elif self.type in self.TERTIARY:
                self.kind = 'tertiary'
            elif self.type in self.END:
                self.kind = 'end'
            else:
                raise ParseError("unknown control markup: `%s`" % self.type)
        finally:
            scanner.release()

    def subscan(self, scanner, primary):
        """Do a subscan for contained tokens."""
        while True:
            token = scanner.one()
            if token is None:
                raise TransientParseError("control `%s` needs more tokens" % primary)
            if isinstance(token, ControlToken) and token.type in self.END:
                if token.rest != primary:
                    raise ParseError("control must end with `end %s`" % primary)
                break
            self.subtokens.append(token)

    def build(self, allowed):
        """Process the list of subtokens and divide it up into a list
        of chains, returning that list.  Allowed specifies a list of
        the only secondary markup types which are allowed."""
        result = []
        current = []
        result.append(self.Chain(self, current))
        for subtoken in self.subtokens:
            if (isinstance(subtoken, ControlToken) and
                subtoken.kind == 'secondary'):
                if subtoken.type not in allowed:
                    raise ParseError("control unexpected secondary: `%s`" % subtoken.type)
                current = []
                result.append(self.Chain(subtoken, current))
            else:
                current.append(subtoken)
        return result

    def subrun(self, tokens, interp, locals):
        """Execute a list of tokens."""
        interp.runSeveral(tokens, locals)

    def substring(self):
        return ''.join([toString(x) for x in self.subtokens])

    def string(self):
        if self.kind == 'primary':
            return ('%s[%s]%s%s[end %s]' %
                    (self.config.prefix, self.contents, self.substring(),
                     self.config.prefix, self.type))
        else:
            return '%s[%s]' % (self.config.prefix, self.contents)

    def run(self, interp, locals):
        if interp.invoke('preControl', type=self.type,
                         rest=self.rest, locals=locals):
            return
        try:
            allowed = self.ALLOWED[self.type]
        except KeyError:
            raise ParseError("control `%s` cannot be at this level" % self.type)
        if allowed is not None:
            chains = self.build(allowed)
        else:
            chains = None
        try:
            method = getattr(self, self.runPrefix + self.type)
        except AttributeError:
            raise ConsistencyError("unknown handler for control type `%s`" % self.type)
        method(chains, interp, locals)
        interp.invoke('postControl')

    # Type handlers.

    def run_if(self, chains, interp, locals):
        # @[if E]...@[end if]
        # @[if E]...@[else]...@[end if]
        # @[if E]...@[elif E2]...@[end if]
        # @[if E]...@[elif E2]...@[else]...@[end if]
        # @[if E]...@[elif E2]... ... @[else]...@[end if]
        if chains[-1].hasType('else'):
            elseChain = chains.pop()
        else:
            elseChain = None
        first = True
        for chain in chains:
            if first:
                if not chain.hasType('if'):
                    raise ParseError("control `if` expected: `%s`" % chain.head.type)
                first = False
            else:
                if not chain.hasType('elif'):
                    raise ParseError("control `elif` expected: `%s`" % chain.head.type)
            if interp.evaluate(chain.head.rest, locals):
                self.subrun(chain.tail, interp, locals)
                break
        else:
            if elseChain:
                self.subrun(elseChain.tail, interp, locals)

    def run_for(self, chains, interp, locals):
        # @[for N in E]...@[end for]
        # @[for N in E]...@[else]...@[end for]
        sides = self.IN_RE.split(self.rest, 1)
        if len(sides) != 2:
            raise ParseError("control `for` expected `for x in ...`")
        iterator, iterableCode = sides
        forChain = chains[0]
        assert forChain.hasType('for'), forChain.getType()
        elseChain = None
        if chains[-1].hasType('else'):
            elseChain = chains.pop()
        if len(chains) != 1:
            raise ParseError("control `for` expects at most one `else`")
        iterable = interp.evaluate(iterableCode, locals)
        for element in iterable:
            try:
                interp.assign(iterator, element, locals)
                self.subrun(forChain.tail, interp, locals)
            except ContinueFlow:
                continue
            except BreakFlow:
                break
        else:
            if elseChain:
                self.subrun(elseChain.tail, interp, locals)

    def run_while(self, chains, interp, locals):
        # @[while E]...@[end while]
        # @[while E]...@[else]...@[end while]
        testCode = self.rest
        whileChain = chains[0]
        assert whileChain.hasType('while'), whileChain.getType()
        elseChain = None
        if chains[-1].hasType('else'):
            elseChain = chains.pop()
        if len(chains) != 1:
            raise ParseError("control `while` expects at most one `else`")
        exitedNormally = False
        while True:
            try:
                if not interp.evaluate(testCode, locals):
                    exitedNormally = True
                    break
                self.subrun(whileChain.tail, interp, locals)
            except ContinueFlow:
                continue
            except BreakFlow:
                break
        if exitedNormally and elseChain:
            self.subrun(elseChain.tail, interp, locals)

    def run_dowhile(self, chains, interp, locals):
        # @[dowhile E]...@[end dowhile]
        # @[dowhile E]...@[else]...@[end dowhile]
        testCode = self.rest
        doWhileChain = chains[0]
        assert doWhileChain.hasType('dowhile'), doWhileChain.getType()
        elseChain = None
        if chains[-1].hasType('else'):
            elseChain = chains.pop()
        if len(chains) != 1:
            raise ParseError("control `dowhile` expects at most one `else`")
        exitedNormally = False
        while True:
            try:
                self.subrun(doWhileChain.tail, interp, locals)
                if not interp.evaluate(testCode, locals):
                    exitedNormally = True
                    break
            except ContinueFlow:
                continue
            except BreakFlow:
                break
        if exitedNormally and elseChain:
            self.subrun(elseChain.tail, interp, locals)

    def run_try(self, chains, interp, locals):
        # @[try]...@[except]...@[end try]
        # @[try]...@[except C]...@[end try]
        # @[try]...@[except C as N]...@[end try]
        # @[try]...@[except C, N]...@[end try]
        # @[try]...@[except (C1, C2, ...) as N]...@[end try]
        # @[try]...@[except C1]...@[except C2]...@[end try]
        # @[try]...@[except C1]...@[except C2]... ... @[end try]
        # @[try]...@[finally]...@[end try]
        # @[try]...@[except ...]...@[finally]...@[end try]
        # @[try]...@[except ...]...@[else]...@[end try]
        # @[try]...@[except ...]...@[else]...@[finally]...@[end try]
        if len(chains) == 1:
            raise ParseError("control `try` expects at least one `except` or `finally`")
        tryChain = None
        exceptChains = []
        elseChain = None
        finallyChain = None
        # Process the chains and verify them.
        for chain in chains:
            if chain.hasType('try'):
                if exceptChains or elseChain or finallyChain:
                    raise ParseError("control `try` must be first")
                tryChain = chain
            elif chain.hasType('except'):
                if elseChain:
                    raise ParseError("control `try` cannot have `except` following `else`")
                elif finallyChain:
                    raise ParseError("control `try` cannot have `except` following `finally`")
                exceptChains.append(chain)
            elif chain.hasType('else'):
                if not exceptChains:
                    raise ParseError("control `try` cannot have `else` with no preceding `except`")
                elif elseChain:
                    raise ParseError("control `try` cannot have more than one `else`")
                elif finallyChain:
                    raise ParseError("control `try` cannot have `else` following `finally`")
                elseChain = chain
            elif chain.hasType('finally'):
                if finallyChain:
                    raise ParseError("control `try` cannot have more than one `finally`")
                finallyChain = chain
            else:
                assert False, chain
        try:
            try:
                self.subrun(tryChain.tail, interp, locals)
                if elseChain:
                    self.subrun(elseChain.tail, interp, locals)
            except Flow:
                raise
            except self.config.baseException:
                type, error, traceback = sys.exc_info()
                for chain in exceptChains:
                    exception, variable = interp.clause(chain.head.rest)
                    if isinstance(error, exception):
                        if variable is not None:
                            interp.assign(variable, error, locals)
                        self.subrun(chain.tail, interp, locals)
                        break
                else:
                    raise
        finally:
            if finallyChain:
                self.subrun(finallyChain.tail, interp, locals)

    def run_with(self, chains, interp, locals):
        # @[with E as N]...@[end with]
        # @[with N]...@[end with]
        # @[with E]...@[end with]
        fields = self.AS_RE.split(self.rest, 1)
        if len(fields) == 1:
            expression = fields[0].strip()
            variable = None
        else: # len(fields) == 2
            expression, variable = fields
            variable = variable.strip()
        manager = interp.evaluate(expression, locals)
        resource = manager
        if variable is not None:
            interp.assign(variable, resource, locals)
        if len(chains) != 1:
            raise ParseError("control `with` must be simple")
        withChain = chains[0]
        assert withChain.hasType('with'), withChain.getType()
        # As per Python's compound statement reference documentation.
        enter = manager.__enter__
        exit = manager.__exit__
        resource = enter()
        oops = False
        try:
            try:
                self.subrun(withChain.tail, interp, locals)
            except:
                oops = True
                if not exit(*sys.exc_info()):
                    raise
        finally:
            if not oops:
                exit(None, None, None)

    def run_match(self, chains, interp, locals):
        # @[match E]...@[case C]...@[end match]
        # @[match E]...@[case C1]...@[case C2]...@[end match]
        # @[match E]...@[case C1]...@[case C2]...@[else]...@[end match]
        expression = self.rest.strip()
        if not expression:
            raise ParseError("control `match` expects an expression")
        matchChain = chains[0]
        self.subrun(matchChain.tail, interp, locals)
        assert matchChain.hasType('match'), matchChain.getType()
        if chains[-1].hasType('else'):
            elseChain = chains[-1]
            chains = chains[:-1]
        else:
            elseChain = None
        if len(chains) + bool(elseChain) <= 1:
            raise ParseError("control `match` expects at least one `case` or `else`")
        cases = []
        for chain in chains[1:]:
            if not chain.hasType('case'):
                raise ParseError("contol `match` expects only `case` and at most one `else`")
            cases.append(Core.Case(chain.head.rest, chain.tail))
        if elseChain:
            cases.append(Core.Case(self.elseCase, elseChain.tail))
        interp.core.match(expression, cases, locals)

    def run_defined(self, chains, interp, locals):
        # @[defined N]...@[end defined]
        # @[defined N]...@[else]...@[end defined]
        testName = self.rest.strip()
        if not testName:
            raise ParseError("control `defined` expects an argument")
        definedChain = chains[0]
        assert definedChain.hasType('defined'), definedChain.getType()
        if len(chains) > 3:
            raise ParseError("control `defined` expects at most one `else`")
        if len(chains) == 2:
            elseChain = chains[1]
        else:
            elseChain = None
        if interp.defined(testName, locals):
            self.subrun(definedChain.tail, interp, locals)
        elif elseChain:
            self.subrun(elseChain.tail, interp, locals)

    def run_def(self, chains, interp, locals):
        # @[def F(...)]...@[end def]
        assert chains is None, chains
        signature = self.rest
        definition = self.substring()
        interp.core.define(signature, definition, locals)

    def run_continue(self, chains, interp, locals):
        # @[continue]
        assert chains is None, chains
        raise ContinueFlow("control `continue` encountered without loop control")

    def run_break(self, chains, interp, locals):
        # @[break]
        assert chains is None, chains
        raise BreakFlow("control `break` encountered without loop control")


class CodedToken(ExpansionToken):

    """The abstract base class for a token that supports codings."""

    def recode(self, result):
        return self.config.recode(result)


class EscapeToken(CodedToken):

    """An escape markup: ``@\\...``"""

    first = BACKSLASH_CHAR

    def scan(self, scanner):
        try:
            code = scanner.chop(1)
            result = None
            if code in LITERAL_CHARS: # literals
                result = code
            elif code == '0': # NUL, null
                result = 0x00
            elif code == 'a': # BEL, bell
                result = 0x07
            elif code == 'b': # BS, backspace
                result = 0x08
            elif code == 'B': # freeform binary code
                binaryCode = scanner.enclosure()
                result = int(binaryCode, 2)
            elif code == 'd': # three-digit decimal code
                decimalCode = scanner.chop(3)
                result = int(decimalCode, 10)
            elif code == 'D': # freeform decimal code
                decimalCode = scanner.enclosure()
                result = int(decimalCode, 10)
            elif code == 'e': # ESC, escape
                result = 0x1b
            elif code == 'f': # FF, form feed
                result = 0x0c
            elif code == 'h': # DEL, delete
                result = 0x7f
            elif code == 'k': # ACK, acknowledge
                result = 0x06
            elif code == 'K': # NAK, negative acknowledge
                result = 0x15
            elif code == 'n': # LF, linefeed; newline
                result = 0x0a
            elif code == 'N': # Unicode character name
                if unicodedata is None:
                    raise ConfigurationError("unicodedata module not available; cannot use @\\N{...} markup")
                name = scanner.enclosure()
                try:
                    name = name.replace('\n', ' ')
                    result = unicodedata.lookup(name)
                except AttributeError:
                    raise ConfigurationError("unicodedata.lookup function not available; cannot use @\\N{...} markup")
                except KeyError:
                    raise ParseError("unknown Unicode character name: `%s`" % name)
            elif code == 'o': # three-digit octal code
                octalCode = scanner.chop(3)
                result = int(octalCode, 8)
            elif code == 'O': # freeform octal code
                octalCode = scanner.enclosure()
                result = int(octalCode, 8)
            elif code == 'q': # four-digit quaternary code
                quaternaryCode = scanner.chop(4)
                result = int(quaternaryCode, 4)
            elif code == 'Q': # freeform quaternary code
                quaternaryCode = scanner.enclosure()
                result = int(quaternaryCode, 4)
            elif code == 'r': # CR, carriage return
                result = 0x0d
            elif code == 's' or code in WHITESPACE_CHARS: # SP, space
                result = ' '
            elif code == 'S': # NBSP, no-break space
                result = 0xa0
            elif code == 't': # HT, horizontal tab
                result = 0x09
            elif code == 'u': # 16-bit (four-digit) hexadecimal Unicode
                hexCode = scanner.chop(4)
                result = int(hexCode, 16)
            elif code == 'U': # 32-bit (eight-digit) hexadecimal Unicode
                hexCode = scanner.chop(8)
                result = int(hexCode, 16)
            elif code == 'v': # VT, vertical tab
                result = 0x0b
            elif code == 'V': # variation selector
                name = scanner.enclosure()
                try:
                    selector = int(name)
                except ValueError:
                    raise ParseError("variation selector must be int: `%s`" % name)
                if selector < 1 or selector > 256:
                    raise ParseError("variation selector must be between 1 and 256 inclusive: %d" % selector)
                if selector <= 16:
                    result = 0xfe00 + selector - 1
                else:
                    result = 0xe0100 + (selector - 17)
            elif code == 'w': # variation selector 15; text display
                result = 0xfe0e
            elif code == 'W': # variation selector 16; emoji display
                result = 0xfe0f
            elif code == 'x': # 8-bit (two-digit) hexadecimal code
                hexCode = scanner.chop(2)
                result = int(hexCode, 16)
            elif code == 'X': # freeform hexadecimal code
                hexCode = scanner.enclosure()
                result = int(hexCode, 16)
            elif code == 'y': # SUB, substitution
                result = 0x1a
            elif code == 'Y': # RC, replacement character
                result = 0xfffd
            elif code == 'z': # EOT, end of transmission
                result = 0x04
            elif code == 'Z': # ZWNBSP/BOM, zero-width no-break space/byte order mark
                result = 0xfeff
            elif code == ',': # THSP, thin space
                result = 0x2009
            elif code == '^': # control character
                controlCode = scanner.chop(1).upper()
                if controlCode == '{':
                    if self.config.controls is None:
                        raise ConfigurationError("controls not configured")
                    name = scanner.grab('}')
                    try:
                        result = self.config.controls[name.upper()]
                    except KeyError:
                        raise ParseError("unknown control character name: `%s`" % name)
                elif controlCode >= '@' and controlCode <= '`':
                    result = ord(controlCode) - ord('@')
                elif controlCode == '?':
                    result = 0x7f
                else:
                    raise ParseError("invalid escape control code")
            else:
                raise ParseError("unrecognized escape code: `%s`" % code)
            self.code = self.recode(result)
        except ValueError:
            raise ParseError("invalid numeric escape code")

    def string(self):
        """Return a general hexadecimal escape sequence rather than
        the exact one that was input."""
        return self.config.escaped(ord(self.code),
                                   self.config.prefix + self.first)

    def run(self, interp, locals):
        if interp.invoke('preEscape', code=self.code):
            return
        interp.serialize(self.code)
        interp.invoke('postEscape')


class DiacriticToken(CodedToken):

    """A diacritic markup: ``@^...``"""

    first = CARET_CHAR

    def scan(self, scanner):
        if self.config.diacritics is None:
            raise ConfigurationError("diacritics not configured")
        character = scanner.chop(1)
        diacritics = scanner.chop(1)
        if diacritics == '{':
            diacritics = scanner.grab('}')
        codes = []
        try:
            for diacritic in diacritics:
                code = self.config.diacritics[diacritic]
                code = self.recode(code)
                codes.append(code)
        except KeyError:
            raise ParseError("unknown diacritical mark: `%s`" % diacritic)
        self.character = character
        self.codes = codes
        self.diacritics = diacritics

    def string(self):
        if len(self.diacritics) > 1:
            return '%s%s%s{%s}' % (
                self.config.prefix, self.first, self.character, self.diacritics)
        else:
            return '%s%s%s%s' % (
                self.config.prefix, self.first, self.character, self.diacritics)

    def run(self, interp, locals):
        combiners = ''.join([interp.core.serialize(x) for x in self.codes])
        result = self.character + combiners
        if interp.invoke('preDiacritic', code=result):
            return
        try:
            if self.config.normalizationForm:
                result = unicodedata.normalize(
                    self.config.normalizationForm, result)
        except AttributeError:
            raise ConfigurationError("unicodedata.normalize function not available; cannot use normalization form (must be blank or None)")
        except ValueError:
            pass
        interp.serialize(result)
        interp.invoke('postDiacritic')


class IconToken(CodedToken):

    """An icon markup: ``@|...``"""

    first = STROKE_CHAR

    def scan(self, scanner):
        self.config.validateIcons()
        key = ''
        while True:
            key += scanner.chop(1)
            try:
                result = self.config.icons[key]
            except KeyError:
                raise ParseError("unknown icon sequence: `%s`" % key)
            if result is None:
                continue
            else:
                break
        self.key = key
        self.code = self.recode(result)

    def string(self):
        return '%s%s%s' % (self.config.prefix, self.first, self.key)

    def run(self, interp, locals):
        if interp.invoke('preIcon', code=self.code):
            return
        interp.serialize(self.code)
        interp.invoke('postIcon')


class EmojiToken(CodedToken):

    """An emoji markup: ``@:...:``"""

    first = COLON_CHAR

    def scan(self, scanner):
        i = scanner.next(self.first)
        self.name = scanner.chop(i, 1)
        if not self.name:
            raise ParseError("emoji cannot be blank")

    def string(self):
        return '%s%s%s%s' % (
            self.config.prefix, self.first, self.name, self.first)

    def run(self, interp, locals):
        if interp.invoke('preEmoji', name=self.name):
            return
        if (self.config.emojis is not None and
            self.name in self.config.emojis):
            code = self.config.emojis[self.name]
        else:
            code = self.config.substituteEmoji(self.name)
            if code is None:
                if self.config.emojiNotFoundIsError:
                    raise UnknownEmojiError("emoji not found: `%s`" % self.name)
                else:
                    code = '%s%s%s' % (self.first, self.name, self.first)
        code = self.recode(code)
        interp.serialize(code)
        interp.invoke('postEmoji')


class SignificatorToken(ExpansionToken):

    """A significator markup: ``@%... ... NL``, ``@%!... ... NL``,
    ``@%%... ... %% NL``, ``@%%!... ... %% NL``"""

    first = PERCENT_CHAR

    def ending(self, multiline):
        if multiline:
            return (self.first * 2) + NEWLINE_CHAR
        else:
            return NEWLINE_CHAR

    def scan(self, scanner):
        self.multiline = self.stringized = False
        peek = scanner.read()
        if peek == self.first:
            self.multiline = True
            scanner.advance(1)
            peek = scanner.read()
        if peek == '!':
            self.stringized = True
            scanner.advance(1)
        loc = scanner.find(self.ending(self.multiline))
        if loc >= 0:
            contents = scanner.chop(loc, len(self.ending(self.multiline)))
            if not contents:
                raise ParseError("significator must have nonblank key")
            contents = contents.strip()
            # Work around a subtle CPython-Jython difference by stripping the
            # string before splitting it: 'a '.split(None, 1) has two elements
            # in Jython 2.1).
            fields = contents.strip().split(None, 1)
            self.key = fields[0]
            if len(fields) > 1:
                self.value = fields[1].strip()
            else:
                self.value = ''
            if not self.value and not self.stringized:
                self.value = self.config.emptySignificator
        else:
            if self.multiline:
                raise TransientParseError("significator expects %s and then newline" % (self.first * 2))
            else:
                raise TransientParseError("significator expects newline")

    def string(self):
        if self.value is None:
            return '%s%s%s%s\n' % (
                self.config.prefix, self.first,
                ['', '!'][self.stringized],
                self.key)
        else:
            if self.multiline:
                return '%s%s%s%s %s%s\n' % (
                    self.config.prefix, self.first * 2,
                    ['', '!'][self.stringized],
                    self.key, self.value,
                    self.first * 2)
            else:
                return '%s%s%s%s %s\n' % (
                    self.config.prefix, self.first,
                    ['', '!'][self.stringized],
                    self.key, self.value)

    def run(self, interp, locals):
        if interp.invoke('preSignificator', key=self.key,
                         value=self.value, stringized=self.stringized):
            return
        value = self.value
        if not self.stringized:
            if value is not None and value != self.config.emptySignificator:
                value = interp.evaluate(value, locals, replace=False)
        interp.significate(self.key, value, locals)
        interp.invoke('postSignificator')


class ContextToken(ExpansionToken):

    """A base class for the context tokens."""

    pass


class ContextNameToken(ContextToken):

    """A context name change markup: ``@?...``"""

    first = QUESTION_CHAR

    def scan(self, scanner):
        loc = scanner.find(NEWLINE_CHAR)
        if loc >= 0:
            self.name = scanner.chop(loc, 1).strip()
        else:
            raise TransientParseError("context name expects newline")

    def string(self):
        return '%s%s%s\n' % (self.config.prefix, self.first, self.name)

    def run(self, interp, locals):
        if interp.invoke('preContextName', name=self.name):
            return
        context = interp.getContext()
        context.name = self.name
        interp.invoke('postContextName', context=context)


class ContextLineToken(ContextToken):

    """A context line change markup: ``@!...``"""

    first = EXCLAMATION_CHAR

    def scan(self, scanner):
        loc = scanner.find(NEWLINE_CHAR)
        if loc >= 0:
            try:
                self.line = int(scanner.chop(loc, 1))
            except ValueError:
                raise ParseError("context line requires integer")
        else:
            raise TransientParseError("context line expects newline")

    def string(self):
        return '%s%s%d\n' % (self.config.prefix, self.first, self.line)

    def run(self, interp, locals):
        if interp.invoke('preContextLine', line=self.line):
            return
        context = interp.getContext()
        context.line = self.line
        interp.invoke('postContextLine', context=context)


class ExtensionToken(ExpansionToken):

    """An extension markup, used for all customizable markup:
    ``@((...))``, ``@[[...]]``, ``@{{...}}``, ``@<...>``, etc."""

    def scan(self, scanner):
        # Find the run of starting characters to match.
        self.depth = 1
        start = scanner.last(self.first)
        self.depth += start
        scanner.advance(start)
        # Then match them with the same number of closing characters.
        loc = scanner.find(self.last * self.depth)
        if loc >= 0:
            self.contents = scanner.chop(loc, self.depth)
        else:
            raise TransientParseError("custom markup (%s) expects %d closing characters (%s)" % (self.first, self.depth, self.last))

    def string(self):
        return '%s%s%s%s' % (
            self.config.prefix,
            self.first * self.depth,
            self.contents,
            self.last * self.depth)

    def run(self, interp, locals):
        if interp.callback is not None:
            # Legacy custom callback behavior.
            if interp.invoke('preCustom', contents=self.contents):
                return
            result = interp.callExtension(
                self.name, self.contents, self.depth, locals)
            interp.invoke('postCustom', result=result)
        else:
            # New extension behavior.
            if interp.invoke('preExtension', name=self.name,
                             contents=self.contents, depth=self.depth,
                             locals=locals):
                return
            result = interp.callExtension(
                self.name, self.contents, self.depth, locals)
            interp.invoke('postExtension', result=result)


Configuration.tokens = [
    LineCommentToken,
    InlineCommentToken,
    WhitespaceToken,
    DisableToken,
    EnableToken,
    PrefixToken,
    StringToken,
    BackquoteToken,
    ExpressionToken,
    SimpleExpressionToken,
    InPlaceToken,
    StatementToken,
    ControlToken,
    EscapeToken,
    DiacriticToken,
    IconToken,
    EmojiToken,
    SignificatorToken,
    ContextNameToken,
    ContextLineToken,
]

#
# Factory
#

class Factory(Root):

    """Turn a first character sequence into a token class.  Token
    classes have a first attribute which is either None to indicate
    whatever the current prefix is; a string in angle brackets to
    indicate a special test; or a character sequence; or a list of
    character sequences.  Token classes are then retrieved by lookup
    table, or special test.  Initialize this meta-factory with a list of
    factory classes and it will automatically setup the lookup tables
    based on their first attributes."""

    addenda = {
        ')':  "; the `@)` markup has been removed, just use `)` instead",
        ']':  "; the `@]` markup has been removed, just use `]` instead",
        '}':  "; the `@}` markup has been removed, just use `}` instead",
        '((': "; extension markup `@((...))` invoked with no installed extension",
        '[[': "; extension markup `@[[...]]` invoked with no installed extension",
        '{{': "; extension markup `@{{...}}` invoked with no installed extension",
        '<':  "; extension markup `@<...>` invoked with no installed extension or callback",
    }

    def __init__(self, tokens):
        self.byChar = {}
        self.identifier = None
        self.whitespace = None
        for token in tokens:
            self.addToken(token)
        assert self.identifier is not None
        assert self.whitespace is not None

    def __contains__(self, first):
        return first in self.byChar

    def __getitem__(self, first):
        return self.byChar[first]

    def __call__(self, first):
        if first in self.byChar:
            return self.byChar[first]
        else:
            if first.isspace():
                return self.whitespace
            elif isIdentifier(first):
                return self.identifier
        return None

    def addToken(self, token, protect=False):
        """Add another token class to the factory."""
        first = token.first
        if first is None:
            # A prefix.
            if None in self.byChar and protect:
                raise ConfigurationError("will not replace prefix token; set protect to true")
            assert None not in self.byChar
            self.byChar[None] = token
        elif (isinstance(first, strType) and
              first.startswith('<') and first.endswith('>')):
            # A special case.
            if first == '<identifier>':
                assert self.identifier is None, self.identifier
                self.identifier = token
            elif first == '<whitespace>':
                assert self.whitespace is None, self.whitespace
                self.whitespace = token
            else:
                raise ConsistencyError("unknown special token case: `%s`" % first)
        else:
            # A character sequence or list of them.
            if not isinstance(first, list):
                first = [first]
            for char in first:
                if char in self.byChar and protect:
                    raise ConfigurationError("will not replace token with first `%s`; set protect to true" % char)
                self.byChar[char] = token

    def removeToken(self, first):
        """Remove token(s) from the mapping by first, which can be a string
        first or a list of strings firsts."""
        if first.startswith('<') and first.endswith('>'):
            if first == '<identifier>':
                self.identifier = None
            elif first == '<whitespace>':
                self.whitespace = None
            else:
                raise ConsistencyError("unknown special token case: `%s`" % first)
        else:
            del self.byChar[first]

    def removeTokens(self, firsts):
        for first in firsts:
            self.removeToken(first)

    def adjust(self, config):
        """Adjust this factory to swap the markup for a non-default
        prefix, if necessary."""
        if not config.hasDefaultPrefix() and config.prefix in self:
            oldFactory = self[config.prefix]
            self.byChar[config.defaultPrefix] = oldFactory
            oldFactory._first = oldFactory.first
            oldFactory.first = None # config.defaultPrefix

    def addendum(self, first):
        """An optional addendum about unsupported markup sequence (for
        compatibility or future notes)."""
        return self.addenda.get(first, '')

#
# Scanner
#

class Scanner(Root):

    """A scanner holds a buffer for lookahead parsing and has the
    ability to scan for special symbols and indicators in that
    buffer."""

    def __init__(self, config, context, currents, data=''):
        self.config = config
        self.context = context
        self.currents = currents
        self.head = 0
        self.pointer = 0
        self.buffer = data
        self.lock = 0
        self.factory = config.getFactory()

    def __bool__(self):
        return self.head + self.pointer < len(self.buffer) # 3.x
    def __nonzero__(self):
        return self.head + self.pointer < len(self.buffer) # 2.x

    def __len__(self): return len(self.buffer) - self.pointer - self.head

    if major >= 3:
        def __getitem__(self, index):
            if isinstance(index, slice):
                assert index.step is None or index.step == 1, index.step
                return self.__getslice__(index.start, index.stop)
            else:
                return self.buffer[self.head + self.pointer + index]
    else:
        def __getitem__(self, index):
            return self.buffer[self.head + self.pointer + index]

    def __getslice__(self, start, stop):
        if start is None:
            start = 0
        if stop is None:
            stop = len(self)
        if stop > len(self):
            stop = len(self)
        return self.buffer[self.head + self.pointer + start:
                           self.head + self.pointer + stop]

    # Meta.

    def advance(self, count=1):
        """Advance the pointer count characters."""
        self.pointer += count

    def retreat(self, count=1):
        self.pointer -= count
        if self.pointer < 0:
            raise ParseError("cannot retreat back over synced out chars")

    def set(self, data):
        """Start the scanner digesting a new batch of data; start the pointer
        over from scratch."""
        self.head = 0
        self.pointer = 0
        self.buffer = data

    def feed(self, data):
        """Feed some more data to the scanner."""
        self.rectify()
        if self.buffer:
            self.buffer += data
        else:
            self.buffer = data

    def acquire(self):
        """Lock the scanner so it doesn't destroy data on sync."""
        self.lock += 1

    def release(self):
        """Unlock the scanner."""
        self.lock -= 1

    def track(self):
        """Accumulate the moved pointer into the context."""
        if self.pointer > 0:
            self.context.track(self.buffer,
                               self.head, self.head + self.pointer)

    def accumulate(self):
        """Update the accumulated context into the actual context."""
        self.context.accumulate()

    def rectify(self):
        """Reset the read head and trim down the buffer."""
        if self.head + self.pointer > 0:
            self.buffer = self.buffer[self.head + self.pointer:]
        self.head = 0
        self.pointer = 0

    def sync(self):
        """Sync up the buffer with the read head."""
        if self.lock == 0 and self.pointer > 0:
            self.track()
            self.head += self.pointer
            self.pointer = 0

    def unsync(self):
        """Undo changes; reset the read head."""
        if self.lock == 0:
            self.pointer = 0

    def rest(self):
        """Get the remainder of the buffer."""
        return self[:]

    # Active.

    def chop(self, count=None, slop=0):
        """Chop the first count + slop characters off the front, and
        return the first count, advancing the pointer past them.  If
        count is not specified, then return everything."""
        if count is None:
            assert slop == 0, slop
            count = len(self)
        if count > len(self):
            raise TransientParseError("not enough data to read")
        result = self[:count]
        self.advance(count + slop)
        return result

    def enclosure(self, begin='{', end='}'):
        """Consume and return the next enclosure (text wrapped
        in the given delimiters).  The delimiters can be repeated."""
        count = self.last(begin)
        self.advance(count)
        if count == 0:
            raise ParseError("enclosure must start with %s" % begin)
        loc = self.find(end * count)
        if loc < 0:
            raise TransientParseError("enclosure must end with %s" % (end * count))
        return self.chop(loc, count)

    def read(self, start=0, count=1):
        """Read count chars starting from start; raise a transient
        error if there aren't enough characters remaining."""
        if len(self) < start + count:
            raise TransientParseError("need more data to read")
        else:
            return self[start:start + count]

    def find(self, sub, start=0, end=None):
        """Find the next occurrence of the substring, or return -1."""
        if end is None:
            end = len(self)
        return self.rest().find(sub, start, end)

    def trivial(self, sub, start=0, end=None):
        """Find the first occurrence of the substring where the
        previous character is _not_ the escape character `\\`)."""
        head = start
        while True:
            i = self.find(sub, head, end)
            if i == -1:
                raise TransientParseError("substring not found: `%s`" % sub)
            if i > 0 and self[i - 1] == BACKSLASH_CHAR:
                head = i + len(sub)
                continue
            return i

    def last(self, chars, start=0, end=None):
        """Find the first character that is _not_ one of the specified
        characters."""
        if end is None:
            end = len(self)
        i = start
        while i < end:
            if self[i] not in chars:
                return i
            i += 1
        else:
            raise TransientParseError("expecting other than %s" % chars)

    def grab(self, sub, start=0, end=None):
        """Find the next occurrence of the substring and chop the
        intervening characters, disposing the substring found."""
        i = self.find(sub, start, end)
        if i >= 0:
            return self.chop(i, len(sub))
        else:
            raise TransientParseError("delimiter not found: `%s`" % sub)

    def next(self, target, start=0, end=None, mandatory=False):
        """Scan for the next occurrence of one of the characters in
        the target string; optionally, make the scan mandatory."""
        if mandatory:
            assert end is not None
        quote = None
        if end is None:
            end = len(self)
        i = start
        while i < end:
            newQuote = self.check(i, quote)
            if newQuote:
                if newQuote == quote:
                    quote = None
                else:
                    quote = newQuote
                i += len(newQuote)
            else:
                c = self[i]
                if quote:
                    if c == '\\':
                        i += 1
                else:
                    if c in target:
                        return i
                i += 1
        else:
            if mandatory:
                raise ParseError("expecting %s, not found" % target)
            else:
                raise TransientParseError("expecting ending character")

    def check(self, start=0, archetype=None):
        """Scan for the next single or triple quote, optionally with
        the specified archetype.  Return the found quote or None."""
        quote = None
        i = start
        if len(self) - i <= 1:
            raise TransientParseError("need to scan for rest of quote")
        if self[i] in QUOTE_CHARS:
            quote = self[i]
            if self[i + 1] == quote:
                if len(self) - i <= 2:
                    raise TransientParseError("need more context to complete quote")
                if self[i + 2] == quote:
                    quote *= 3
        if quote is not None:
            if archetype is None:
                return quote
            else:
                if archetype == quote:
                    return archetype
                elif len(archetype) < len(quote) and archetype[0] == quote[0]:
                    return archetype
                else:
                    return None
        else:
            return None

    def quote(self, start=0, end=None, mandatory=False):
        """Scan for the end of the next quote."""
        assert self[start] in QUOTE_CHARS, self[start]
        quote = self.check(start)
        if end is None:
            end = len(self)
        i = start + len(quote)
        while i < end:
            newQuote = self.check(i, quote)
            if newQuote:
                i += len(newQuote)
                if newQuote == quote:
                    return i
            else:
                c = self[i]
                if c == '\\':
                    i += 1
                i += 1
        else:
            if mandatory:
                raise ParseError("expecting end of string literal")
            else:
                raise TransientParseError("expecting end of string literal")

    def nested(self, enter, exit, start=0, end=None):
        """Scan from start for an ending sequence, respecting entries and exits
        only."""
        depth = 0
        if end is None:
            end = len(self)
        i = start
        while i < end:
            c = self[i]
            if c == enter:
                depth += 1
            elif c == exit:
                depth -= 1
                if depth < 0:
                    return i
            i += 1
        else:
            raise TransientParseError("expecting end of complex expression")

    def complex(self, enter, exit, comment=OCTOTHORPE_CHAR,
                start=0, end=None, skip=None):
        """Scan from start for an ending sequence, respecting quotes,
        entries and exits."""
        quote = None
        depth = 0
        if end is None:
            end = len(self)
        lastNonQuote = None
        commented = False
        i = start
        while i < end:
            if commented:
                c = self[i]
                if c == '\n':
                    commented = False
                elif c == exit:
                    return i
                i += 1
            else:
                newQuote = self.check(i, quote)
                if newQuote:
                    if newQuote == quote:
                        quote = None
                    else:
                        quote = newQuote
                    i += len(newQuote)
                else:
                    c = self[i]
                    if quote:
                        if c == '\\':
                            i += 1
                    else:
                        if skip is None or lastNonQuote != skip:
                            if c == enter:
                                depth += 1
                            elif c == exit:
                                depth -= 1
                                if depth < 0:
                                    return i
                        if c == comment and depth == 0:
                            commented = True
                    lastNonQuote = c
                    i += 1
        else:
            raise TransientParseError("expecting end of complex expression")

    def word(self, start=0, additional='._'):
        """Scan from start for a simple word."""
        length = len(self)
        i = start
        while i < length:
            if not (self[i].isalnum() or self[i] in additional):
                return i
            i += 1
        else:
            raise TransientParseError("expecting end of word")

    def phrase(self, start=0):
        """Scan from start for a phrase (e.g., 'word', 'f(a, b, c)',
        'a[i]', or combinations like 'x[i](a)'."""
        # Find the word.
        i = self.word(start)
        while i < len(self) and self[i] in PHRASE_OPENING_CHARS:
            enter = self[i]
            exit = ENDING_CHAR_MAP[enter]
            i = self.complex(enter, exit, None, i + 1) + 1
        return i

    def simple(self, start=0):
        """Scan from start for a simple expression, which consists of
        one more phrases separated by dots.  Return a tuple giving the
        end of the expression and a list of tuple pairs consisting of
        the simple expression extensions found, if any."""
        i = self.phrase(start)
        length = len(self)
        while i < length and self[i] == DOT_CHAR:
            i = self.phrase(i)
        # Make sure we don't end with a trailing dot.
        while i > 0 and self[i - 1] == DOT_CHAR:
            i -= 1
        return i

    def one(self, firebreaks=None):
        """Parse, scan, and return one token, or None if the scanner
        is empty.  If the firebreaks argument is supplied, chop up
        text tokens before a character in that string."""
        if not self:
            return None
        if not self.config.prefix:
            loc = -1
        else:
            loc = self.find(self.config.prefix)
        if loc < 0:
            # If there's no prefix in the buffer, then set the location to the
            # end so the whole thing gets processed.
            loc = len(self)
        if loc == 0:
            # If there's a prefix at the beginning of the buffer, process
            # an expansion.
            prefix = self.chop(1)
            assert prefix == self.config.prefix, prefix
            try:
                first = self.chop(1)
                if first == self.config.prefix:
                    first = None
                elif first in self.config.duplicativeFirsts:
                    # If the first character is duplicative, there might be
                    # more han one.  Check if there is a second; if so, use
                    # that as the first.
                    if self.read() == first:
                        first *= 2
                tokenClass = self.factory(first)
                if tokenClass is None:
                    raise ParseError("unknown markup sequence: `%s%s`%s" % (self.config.prefix, first, self.factory.addendum(first)))
                current = self.config.renderContext(self.context)
                self.currents.replace(current)
                token = tokenClass(current, self.config, first)
                token.scan(self)
            except TransientParseError:
                # If a transient parse error occurs, reset the buffer pointer
                # so we can (conceivably) try again later.
                self.unsync()
                raise
        else:
            # Process everything up to loc as a text token, unless there are
            # intervening firebreaks before loc.
            if firebreaks:
                for firebreak in firebreaks:
                    i = self.find(firebreak, 0, loc)
                    if i >= 0 and i < loc:
                        loc = i
            data = self.chop(loc)
            current = self.config.renderContext(self.context)
            self.currents.replace(current)
            token = TextToken(current, data)
        self.sync()
        return token

    def all(self):
        """Yield a sequence of all tokens."""
        while True:
            token = self.one()
            if token:
                yield token
            else:
                break

#
# Command ...
#

class Command(Root):

    """A generic high-level processing command."""

    def __init__(self, noun):
        self.noun = noun

    def __str__(self):
        return self.noun

    def cleanup(self):
        pass

    def process(self, interp, n):
        """Run the command."""
        raise NotImplementedError


class ImportCommand(Command):

    """Import a Python module."""

    def process(self, interp, n):
        name = '<import:%s>' % n
        context = interp.newContext(name)
        interp.pushContext(context)
        # Expand shortcuts.
        self.noun = self.noun.replace('+', ' ')
        self.noun = self.noun.replace('=', ' as ')
        method = interp.string
        if ':' in self.noun:
            first, second = self.noun.split(':', 1)
            target = '%s{from %s import %s}' % (
                interp.config.prefix, first, second)
        else:
            target = '%s{import %s}' % (interp.config.prefix, self.noun)
        interp.protect(name, method, target)


class DefineCommand(Command):

    """Define a Python variable."""

    def process(self, interp, n):
        name = '<define:%s>' % n
        context = interp.newContext(name)
        interp.pushContext(context)
        if '=' in self.noun:
            interp.execute(self.noun)
        else:
            interp.atomic(self.noun.strip(), None)
        interp.popContext()


class StringCommand(Command):

    """Define a Python string variable."""

    def process(self, interp, n):
        if '=' in self.noun:
            key, value = self.noun.split('=', 1)
            key = key.strip()
            value = value.strip()
        else:
            key = self.noun.strip()
            value = ''
        interp.atomic(key, value)


class DocumentCommand(Command):

    """Read and execute an EmPy document."""

    def process(self, interp, n):
        name = self.noun
        method = interp.file
        self.target = None
        self.target = interp.config.open(self.noun, 'r')
        interp.protect(name, method, self.target)

    def cleanup(self):
        if self.target is not None:
            self.target.close()


class ExecuteCommand(Command):

    """Execute a Python statement."""

    def process(self, interp, n):
        name = '<execute:%s>' % n
        context = interp.newContext(name)
        interp.pushContext(context)
        interp.execute(self.noun)
        interp.popContext()

ExecCommand = ExecuteCommand # DEPRECATED


class FileCommand(Command):

    """Load an execute a Python file."""

    def process(self, interp, n):
        name = '<file:%s>' % n
        context = interp.newContext(name)
        interp.pushContext(context)
        try:
            file = interp.config.open(self.noun, 'r')
            try:
                data = file.read()
            finally:
                file.close()
            interp.execute(data)
        finally:
            interp.popContext()


class ExpandCommand(Command):

    """Execute a Python statement."""

    def process(self, interp, n):
        name = '<expand:%s>' % n
        context = interp.newContext(name)
        interp.pushContext(context)
        try:
            interp.string(self.noun)
        finally:
            interp.popContext()

#
# Plugin
#

class Plugin(Root):

    """A plugin is an object associated with an interpreter that has a
    back-reference to it."""

    def __init__(self, interp=None):
        if interp is not None:
            self.attach(interp)

    def attach(self, interp):
        """Attach this plugin to an interpreter.  This needs to be a
        separate step since some methods require access to the
        interpreter."""
        self.interp = interp

    def detach(self, interp=None):
        """Detach this plugin from an interpreter, or any interpreter if
        not specified.  This breaks any cyclical links between the
        interpreter and the plugin."""
        if interp is not None and interp is not self.interp:
            raise em.ConsistencyError("plugin not associated with this interpeter")
        self.interp = None

    def push(self):
        self.interp.push()

    def pop(self):
        self.interp.pop()

    # DEPRECATED:

    def register(self, interp):
        self.attach(interp)

    def deregister(self, interp=None):
        self.detach(interp)

#
# Core
#

class Core(Plugin):

    """A core encapsulates the functionality of the underlying language
    (Python by default).  To create an object where this these are
    native methods to that class, derive a class from Core but do not
    call its constructor."""

    casesVariable = '_EmPy_pairs'

    class Case:

        def __init__(self, expression, tokens):
            self.expression = expression
            self.tokens = tokens

        def __getitem__(self, index):
            if index == 0:
                return self.expression
            elif index == 1:
                return self.tokens
            else:
                raise IndexError

        def __len__(self):
            return 2

        def __iter__(self):
            yield self.expression
            yield self.tokens

    def __init__(self, **kwargs):
        evaluate = extract(kwargs, 'evaluate', None)
        if evaluate is not None:
            self.evaluate = evaluate
        execute = extract(kwargs, 'execute', None)
        if execute is not None:
            self.execute = execute
        serialize = extract(kwargs, 'serialize', None)
        if serialize is not None:
            self.serialize = serialize
        define = extract(kwargs, 'define', None)
        if define is not None:
            self.define = define
        match = extract(kwargs, 'match', None)
        if match is not None:
            self.match = match
        interp = extract(kwargs, 'interp', None)
        if interp is not None:
            interp.insertCore(self)

    def quote(self, code):
        """Find the right quote for this code."""
        if '"""' in code and "'''" in code:
            raise CoreError("cannot find proper quotes for code; code cannot contain both \'\'\' and \"\"\": %r" % code, code=code)
        if '"""' in code:
            return "'''"
        else:
            return '"""'

    # Implementation (override these)

    def evaluate(self, code, globals, locals=None):
        """Evaluate an expression and return it."""
        if locals is None:
            return evalFunc(code, globals)
        else:
            return evalFunc(code, globals, locals)

    def execute(self, code, globals, locals=None):
        """Execute a statement(s); return value ignored."""
        if locals is None:
            execFunc(code, globals)
        else:
            execFunc(code, globals, locals)

    def serialize(self, thing):
        """Return the string representation of an object."""
        return toString(thing)

    def define(self, signature, definition, locals=None):
        """Implement @[def ...] markup; return value ignored."""
        quote = self.quote(definition)
        quoted = quote + definition + quote
        code = ('def %s:\n'
                '\tr%s\n'
                '\treturn %s.expand(r%s, locals())\n' %
                (signature, quoted,
                 self.interp.config.pseudomoduleName, quoted))
        self.execute(code, self.interp.globals, locals)

    def match(self, expression, cases, locals=None):
        """Implement @[match ...] markup; return value ignored."""
        if sys.version_info < (3, 10):
            raise CompatibilityError("`match` control requires `match` control structure (Python 3.10 and later)")
        if locals is None:
            locals = {}
        if cases and isinstance(cases[0], tuple):
            # Old-style form is that cases is a list of tuples.  Transform them
            # into Cases.
            cases = [Core.Case(*x) for x in cases]
        locals[self.casesVariable] = cases
        lines = []
        lines.append('match %s:\n' % expression)
        for i, case in enumerate(cases):
            lines.append('\tcase %s:\n' % case.expression)
            lines.append('\t\t%s.runSeveral(%s[%d].tokens, locals())\n' % (
                self.interp.config.pseudomoduleName, self.casesVariable, i))
        self.execute(''.join(lines), self.interp.globals, locals)

#
# Extension
#

class Extension(Plugin):

    """An extension plugin that the interpreter will defer to for
    non-standard markup."""

    mapping = {
        '((': 'parentheses',
        '[[': 'square_brackets',
        '{{': 'curly_braces',
        '<':  'angle_brackets',
    }

    def __init__(self, mapping=None, **kwargs):
        if mapping is None:
            # The default class attribute will suffice.
            pass
        elif isinstance(mapping, dict):
            # A dict should be used directly.
            self.mapping = mapping
        elif isinstance(mapping, (list, set)):
            # A list of 2-tuples should be updated on top of the default.
            self.mapping = self.mapping.copy()
            self.mapping.update(mapping)
        else:
            raise ExtensionError("unknown mapping type (must be dict, list or None): %r" % mapping)
        for name, method in kwargs.items():
            self.__dict__[name] = method

#
# Interpreter
#

class Interpreter(Root):

    """An interpreter can process chunks of EmPy code."""

    # Compatibility.

    version = __version__
    compat = compat

    # Constants.

    ASSIGN_TOKEN_RE = re.compile(r"[_a-zA-Z][_a-zA-Z0-9]*|\(|\)|,")
    AS_RE = re.compile(r"\bas\b")

    # Construction, initialization, destruction.

    def __init__(self, **kwargs):
        """Accept keyword arguments only, so users will never have to
        worry about the ordering of arguments."""
        self.ok = None # is the interpreter initialized?
        self.shuttingDown = False # is the interpreter shutting down?
        config = extract(kwargs, 'config', None)
        if config is None:
            config = Configuration()
        core = extract(kwargs, 'core', None)
        if core is None:
            # Specifying the ...Func callbacks separately from the core is now
            # DEPRECATED.
            core = Core(
                evaluate=extract(kwargs, 'evalFunc', None),
                execute=extract(kwargs, 'execFunc', None),
                serialize=extract(kwargs, 'serializerFunc', None),
                define=extract(kwargs, 'definerFunc', None),
                match=extract(kwargs, 'matcherFunc', None),
            )
        extension = extract(kwargs, 'extension', None)
        args = (
            config,
            core,
            extension,
            extract(kwargs, 'ident', None),
            extract(kwargs, 'globals', None),
            extract(kwargs, 'output', None),
            extract(kwargs, 'executable', '?'),
            extract(kwargs, 'argv', None),
            extract(kwargs, 'filespec', None),
            extract(kwargs, 'hooks', None),
            extract(kwargs, 'finalizers', None),
            extract(kwargs, 'filters', None),
            extract(kwargs, 'callback', None),
            extract(kwargs, 'dispatcher', True),
            extract(kwargs, 'handler', None),
            extract(kwargs, 'input', sys.stdin),
            extract(kwargs, 'root', None),
            extract(kwargs, 'origin', False),
            extract(kwargs, 'immediately', True),
        )
        if kwargs:
            # Any remaining keyword arguments are a mistake: either simple
            # typos, or an old-style specification of local variables in an
            # `expand` call.
            badKeys = []
            for key in kwargs.keys():
                if key not in config.ignoredConstructorArguments:
                    badKeys.append(key)
            if badKeys:
                badKeys.sort()
                raise CompatibilityError("unrecognized Interpreter constructor keyword arguments; when calling expand, use locals dictionary instead of keywords: %s" % badKeys, keys=badKeys)
        self._initialize(*args)

    def __del__(self):
        self.shutdown()

    def __repr__(self):
        details = []
        if self.ident:
            details.append(' "%s"' % self.ident)
        if self.config and self.config.name:
            details.append(' ("%s")' % self.config.name)
        return '<%s pseudomodule/interpreter object%s @ 0x%x>' % (
            self.config.pseudomoduleName, ''.join(details), id(self))

    def __bool__(self): return self.ok # 3.x
    def __nonzero__(self): return self.ok # 2.x

    def __enter__(self):
        self.check()
        return self

    def __exit__(self, *exc):
        self.shutdown()

    def _initialize(self, config=None, core=None, extension=None,
                   ident=None, globals=None, output=None,
                   executable=None, argv=None, filespec=None,
                   hooks=None, finalizers=None, filters=None, callback=None,
                   dispatcher=True, handler=None, input=sys.stdin, root=None,
                   origin=False, immediately=True):
        """Initialize the interpreter with the given arguments (all of
        which have defaults).  The number and order of arguments here
        is subject to change."""
        self.ident = ident
        self.error = None # last error that occurred or None
        # Set up the configuration.
        if config is None:
            config = Configuration()
        self.config = config
        self.filespec = filespec
        self.globals = globals
        # Handle the executable and arguments.
        self.executable = executable
        if argv is None:
            argv = [None]
        if argv[0] is None:
            argv[0] = config.unknownScriptName
        self.argv = argv
        # The interpreter stacks.
        self.enabled = True
        self.contexts = Stack()
        self.streams = Stack()
        self.currents = Stack()
        # Initialize hooks.
        if hooks is None:
            hooks = []
        self.hooks = []
        self.hooksEnabled = None
        for hook in hooks:
            self.addHook(hook)
        # Initialize finalizers:
        if finalizers is None:
            finalizers = []
        self.finalizers = []
        self.setFinalizers(finalizers)
        # Initialize dispatcher.
        if dispatcher is True:
            dispatcher = self.dispatch
        elif dispatcher is False:
            dispatcher = self.reraise
        elif dispatcher is None:
            raise ConfigurationError("dispatcher cannot be None")
        self.dispatcher = dispatcher
        # Initialize handler.
        self.handler = None
        if handler is not None:
            self.setHandler(handler)
        # Install a proxy stdout if one hasn't been already.
        self.output = self.bottom(output)
        self.install(self.output)
        # Setup the execution core.
        self.insertCore(core)
        # Setup any extension.
        self.extension = None
        if extension is not None:
            self.installExtension(extension)
        # Initialize callback.
        self.callback = None
        if callback is not None:
            self.registerCallback(callback)
        # Setup the input file.
        self.input = input
        # Setup the root context.
        if root is None:
            root = self.config.defaultRoot
        self.root = root
        # Is this a top-level interpreter?
        self.origin = origin
        # Now declare that we've started up.
        self.ok = True
        self.invoke('atStartup')
        # Reset the state.
        self.reset(True)
        # Initialize filters (needs to be done after stacks are up).
        if filters:
            self.setFilterChain(filters)
        # Declare the interpreter ready.
        if immediately and not self.shuttingDown:
            self.ready()

    def _deinitialize(self):
        """Deinitialize by detaching all plugins.  Called at the end of
        shutdown."""
        self.clearHooks()
        self._deregisterCallback()
        self.clearFinalizers()
        self.resetHandler()
        self.uninstallExtension()
        self.ejectCore()

    def reset(self, clearStacks=False):
        """Completely reset the interpreter state.  If clearStacks is
        true, wipe the call stacks.  If immediately is true, declare
        the interpreter ready."""
        self.ok = False
        self.error = None
        self.enabled = True
        # None is a special sentinel meaning "false until added."
        self.hooksEnabled = len(self.hooks) > 0 and True or None
        # Set up a diversions dictionary.
        self.diversions = {}
        # Significators.
        self.significators = {}
        # Now set up the globals.
        self.fixGlobals()
        self.globalsHistory = Stack()
        # Reset the command counter.
        self.command = 0
        # Now, clear the state of all the stacks.
        if clearStacks:
            self.clear()
        self.current = None
        # Done.  Now declare that we've started up.
        self.ok = True

    def ready(self):
        """Declare the interpreter ready for normal operations."""
        self.invoke('atReady')

    def finalize(self):
        """Execute any remaining final routines."""
        if self.finalizers:
            self.push()
            self.invoke('atFinalize')
            try:
                # Pop them off one at a time so they get executed in reverse
                # order and we remove them as they're executed in case
                # something bad happens.
                while self.finalizers:
                    finalizer = self.finalizers.pop()
                    if self.invoke('beforeFinalizer', finalizer=finalizer):
                        continue
                    finalizer()
                    self.invoke('afterFinalizer')
                    self.detach(finalizer)
            finally:
                self.pop()

    def install(self, output):
        """Given the desired output files, install any global
        apparatus."""
        self.installProxy(output)
        if self.config.evocare() == 1:
            self.installFinder()

    def uninstall(self):
        """Uninstall any global apparatus.  The apparatus should be
        installed."""
        self.uninstallProxy()
        if self.config.evocare() is None:
            self.uninstallFinder()

    def succeeded(self):
        """Did the interpreter succeed?  That is, is the logged
        error not an error?"""
        return self.config.isNotAnError(self.error)

    def pause(self):
        """Pause (at the end of processing)."""
        try:
            self.input.readline()
        except EOFError:
            pass

    def shutdown(self):
        """Declare this interpreting session over; close all the
        stream file objects, and if this is the last interpreter,
        uninstall the proxy and/or finder.  This method is idempotent."""
        if self.ok and not self.shuttingDown:
            self.shuttingDown = True
            # Finally, if we're supposed to go interactive afterwards, do it.
            if self.config.goInteractive:
                self.interact()
            # Wrap things up.
            self.ok = False
            succeeded = self.succeeded()
            try:
                self.finalize()
                self.invoke('atShutdown')
                while self.streams:
                    stream = self.streams.pop()
                    if self.streams:
                        stream.close()
                    else:
                        # Don't close the bottom stream; auto-play diversions
                        # and just flush it.
                        if self.config.autoPlayDiversions and succeeded:
                            stream.undivertAll()
                        stream.flush()
                self.clear()
            finally:
                self.uninstall()
                if self.origin and self.evocare() is not None:
                    raise ProxyError("proxy persists; did you not call shutdown?")
            # Deinitialize (detach all plugins).
            self._deinitialize()
            # Do a final flush of all the streams.
            self.flushAll()
            # Finally, pause if desired.
            if self.config.pauseAtEnd:
                self.pause()

    def check(self):
        """Check the verify this interpreter is still alive."""
        if not self.ok:
            raise ConsistencyError("interpreter has already been shutdown")

    def failed(self):
        """Has this interpreter had an error (which we should not
        ignore)?"""
        return self.error and self.config.exitOnError

    # Installation delegates.

    def proxy(self, *args, **kwargs):
        return self.config.proxy(*args, **kwargs)

    def evocare(self, *args, **kwargs):
        return self.config.evocare(*args, **kwargs)

    def installProxy(self, *args, **kwargs):
        before = self.config.evocare()
        output = self.config.installProxy(*args, **kwargs)
        proxy = self.config.proxy()
        if proxy is not None:
            self.invoke('atInstallProxy', proxy=proxy, new=(before is None))
        return output

    def uninstallProxy(self):
        proxy = self.config.proxy()
        self.config.uninstallProxy()
        after = self.config.evocare()
        self.invoke('atUninstallProxy', proxy=proxy, done=(after is None))

    def checkProxy(self, *args, **kwargs):
        return self.config.checkProxy(*args, **kwargs)

    def installFinder(self, *args, **kwargs):
        self.config.installFinder(*args, **kwargs)
        finder = self.config.finder()
        self.invoke('atInstallFinder', finder=finder)

    def uninstallFinder(self):
        finder = self.config.finder()
        self.invoke('atUninstallFinder', finder=finder)
        self.config.uninstallFinder()

    # Writeable file-like methods.

    def enable(self, value=True):
        self.enabled = value

    def disable(self, value=False):
        self.enabled = value

    def write(self, data):
        stream = self.top()
        assert stream is not None
        stream.write(data)

    def writelines(self, lines):
        stream = self.top()
        assert stream is not None
        stream.writelines(lines)

    def flush(self):
        stream = self.top()
        assert stream is not None
        stream.flush()

    def flushAll(self):
        for stream in self.streams:
            stream.flush()

    def close(self):
        self.shutdown()

    def serialize(self, thing):
        """Output the string version of an object, or a special token if
        it is None."""
        if thing is None:
            if self.config.noneSymbol is not None:
                self.write(self.config.noneSymbol)
        else:
            self.write(self.core.serialize(thing))

    def bottom(self, output):
        """Get the underlying bottom file."""
        # If there's no output, check the bottom file in the proxy first.
        if output is None:
            output = getattr(self.config.proxy(), '_EmPy_bottom', None)
        # Otherwise, default to the config's stdout.
        if output is None:
            output = self.config.defaultStdout
        return output

    # Stream stack-related activity.

    def top(self):
        """Get the top stream."""
        return self.streams.top()

    def push(self):
        if self.config.useProxy and self.ok:
            try:
                sys.stdout._EmPy_push(self)
            except AttributeError:
                raise ProxyError("proxy lost; cannot push stream")

    def pop(self):
        if self.config.useProxy and self.ok:
            try:
                sys.stdout._EmPy_pop(self)
            except AttributeError:
                raise ProxyError("proxy lost; cannot pop stream")

    def clear(self):
        self.streams.purge()
        self.streams.push(Stream(self, self.output, self.diversions))
        self.contexts.purge()
        context = self.newContext(self.root)
        self.contexts.push(context)
        self.currents.purge()
        self.currents.push(self.config.renderContext(context))

    # Entry-level processing.

    def include(self, fileOrFilename, locals=None, name=None):
        """Do an include pass on a file or filename."""
        close = False
        if isinstance(fileOrFilename, strType):
            # Either it's a string representing a filename ...
            filename = fileOrFilename
            if not name:
                name = filename
            file = self.config.open(filename, 'r')
            close = True
        else:
            # ... or a file object.
            file = fileOrFilename
            if not name:
                name = '<%s>' % toString(file.__class__.__name__)
        try:
            if self.invoke('beforeInclude', file=file, locals=locals, name=name):
                return
            if name:
                context = self.newContext(name)
                self.pushContext(context)
            self.file(file, locals)
            if name:
                self.popContext()
        finally:
            if close:
                file.close()
        self.invoke('afterInclude')

    def expand(self, data, locals=None, name='<expand>', dispatcher=False):
        """Do an explicit expansion on a subordinate stream in a
        new context.  If dispatch is true, dispatch any exception
        through the interpreter; otherwise just reraise."""
        if dispatcher is None:
            dispatcher = self.dispatcher
        elif dispatcher is True:
            dispatcher = self.dispatch
        elif dispatcher is False:
            dispatcher = self.reraise
        outFile = StringIO()
        stream = Stream(self, outFile, self.diversions)
        if self.invoke('beforeExpand', string=data, locals=locals, name=name,
                       dispatcher=dispatcher):
            return
        self.push()
        self.streams.push(stream)
        try:
            if name:
                context = self.newContext(name)
                self.pushContext(context)
            try:
                self.string(data, locals, dispatcher)
            finally:
                if name:
                    self.popContext()
            try:
                stream.flush()
                result = outFile.getvalue()
            except ValueError:
                # Premature termination will result in the file being closed;
                # ignore it.
                result = None
            self.invoke('afterExpand', result=result)
            return result
        finally:
            self.streams.pop()
            self.pop()

    # High-level processing.

    def go(self, inputFilename, inputMode,
           preprocessing=None, postprocessing=None):
        """Execute an interpreter stack at a high level."""
        # Execute any preprocessing commands.
        if preprocessing is None:
            preprocessing = []
        if postprocessing is None:
            postprocessing = []
        self.processAll(preprocessing)
        # Ready!
        if not self.shuttingDown:
            self.ready()
        # Now process the primary file.
        method = self.file
        if inputFilename is None:
            # We'll be using stdin, so check the encoding.
            if not self.config.isDefaultEncodingErrors(asInput=True):
                self.config.reconfigure(sys.stdin,
                                        self.config.buffering,
                                        self.config.inputEncoding,
                                        self.config.inputErrors)
            self.config.goInteractive = True
        else:
            if inputFilename == '-':
                file = sys.stdin
                name = '<stdin>'
            else:
                file = self.config.open(inputFilename, inputMode, self.config.buffering)
                name = inputFilename
                if self.config.relativePath:
                    dirname = os.path.split(inputFilename)[0]
                    sys.path.insert(0, dirname)
            try:
                self.protect(name, method, file)
            finally:
                if file is not sys.stdin:
                    file.close()
        # Finally, execute any postprocessing commands.
        self.processAll(postprocessing)

    def protect(self, name, callable, *args, **kwargs):
        """Wrap around an application of a callable in a new
        context (named name)."""
        if name:
            context = self.newContext(name)
            self.pushContext(context)
        try:
            if kwargs is None:
                kwargs = {}
            callable(*args, **kwargs)
        finally:
            if name and self.contexts:
                self.popContext()

    def interact(self):
        """Perform interaction."""
        self.invoke('atInteract')
        self.protect('<interact>', self.fileLines, self.input)

    def file(self, file, locals=None, dispatcher=None):
        """Parse a file according to the current buffering strategy."""
        config = self.config
        if config.hasNoBuffering() or config.hasLineBuffering():
            self.fileLines(file, locals, dispatcher)
        elif config.hasFullBuffering():
            self.fileFull(file, locals, dispatcher)
        else: # if self.hasFixedBuffering()
            self.fileChunks(file, config.buffering, locals, dispatcher)

    def fileLines(self, file, locals=None, dispatcher=None):
        """Parse the entire contents of a file-like object, line by line."""
        if self.invoke('beforeFileLines', file=file, locals=locals,
                       dispatcher=dispatcher):
            return
        scanner = Scanner(self.config, self.getContext(), self.currents)
        done = False
        first = True
        while not done and not self.failed():
            line = file.readline()
            if first:
                if self.config.ignoreBangpaths and self.config.prefix:
                    if line.startswith(self.config.bangpath):
                        line = self.config.prefix + line
                first = False
            if line:
                scanner.feed(line)
            else:
                done = True
            while not self.safe(scanner, done, locals, dispatcher):
                pass
        self.invoke('afterFileLines')

    def fileChunks(self, file, bufferSize=None, locals=None, dispatcher=None):
        """Parse the entire contents of a file-like object, in
        buffered chunks."""
        if bufferSize is None:
            bufferSize = self.config.buffering
        assert bufferSize > 0, bufferSize
        if self.invoke('beforeFileChunks', file=file, bufferSize=bufferSize,
                       locals=locals, dispatcher=dispatcher):
            return
        scanner = Scanner(self.config, self.getContext(), self.currents)
        done = False
        first = True
        while not done and not self.failed():
            chunk = file.read(bufferSize)
            if first:
                if self.config.ignoreBangpaths and self.config.prefix:
                    if chunk.startswith(self.config.bangpath):
                        chunk = self.config.prefix + chunk
                first = False
            if chunk:
                scanner.feed(chunk)
            else:
                done = True
            while not self.safe(scanner, done, locals, dispatcher):
                pass
        self.invoke('afterFileChunks')

    def fileFull(self, file, locals=None, dispatcher=None):
        """Parse the entire contents of a file-like object, in one big
        chunk."""
        if self.invoke('beforeFileFull', file=file, locals=locals,
                       dispatcher=dispatcher):
            return
        scanner = Scanner(self.config, self.getContext(), self.currents)
        data = file.read()
        if self.config.ignoreBangpaths and self.config.prefix:
            if data.startswith(self.config.bangpath):
                data = self.config.prefix + data
        scanner.feed(data)
        while not self.safe(scanner, True, locals, dispatcher):
            pass
        self.invoke('afterFileFull')

    def string(self, string, locals=None, dispatcher=None):
        """Parse a string.  Cleans up after itself."""
        if self.invoke('beforeString', string=string, locals=locals,
                       dispatcher=dispatcher):
            return
        scanner = Scanner(self.config, self.getContext(), self.currents, string)
        while not self.safe(scanner, True, locals, dispatcher):
            pass
        self.invoke('afterString')

    def safe(self, scanner, final=False, locals=None, dispatcher=None):
        """Do a protected parse.  Catch transient parse errors; if
        final is true, then make a final pass with a terminator,
        otherwise ignore the transient parse error (more data is
        pending).  Return true if the scanner is exhausted or if an
        error has occurred."""
        if dispatcher is None:
            dispatcher = self.dispatcher
        try:
            return self.parse(scanner, locals)
        except TransientParseError:
            if final:
                buffer = scanner.rest()
                # If the buffer ends with a prefix, it's a real parse
                # error.
                if buffer and buffer.endswith(self.config.prefix):
                    raise
                # Try tacking on a dummy terminator to take into account
                # greedy tokens.
                scanner.feed(self.config.prefix + NEWLINE_CHAR)
                try:
                    # Any error thrown from here is a real parse error.
                    self.parse(scanner, locals)
                except:
                    if dispatcher():
                        return True
            return True
        except:
            if dispatcher():
                return True

    def import_(self, filename, module, locals=None, dispatcher=None):
        """Import an EmPy module."""
        if self.invoke('beforeImport', filename=filename, module=module,
                       locals=locals, dispatcher=dispatcher):
            return
        globals = self.globals
        self.globals = vars(module)
        self.fixGlobals()
        switch = self.enabled
        if not self.config.enableImportOutput:
            self.disable()
        context = self.newContext(filename)
        self.pushContext(context)
        try:
            self.push()
            file = self.config.open(filename, 'r')
            try:
                self.file(file, locals, dispatcher)
            finally:
                file.close()
                self.pop()
                self.globals = globals
                self.enabled = switch
            self.invoke('afterImport')
        finally:
            self.popContext()

    def parse(self, scanner, locals=None):
        """Parse and run as much from this scanner as possible.  Return
        true if the scanner ran out of tokens."""
        self.invoke('atParse', scanner=scanner, locals=locals)
        while True:
            token = scanner.one()
            if token is None:
                break
            self.invoke('atToken', token=token)
            self.run(token, locals)
            scanner.accumulate()
        return True

    def process(self, command):
        """Process a command."""
        if isinstance(command, list):
            # Backward compatibility: If process is called with a list, defer
            # to the explicit list version.
            self.processAll(command)
            return
        self.command += 1
        if self.invoke('beforeProcess', command=command, n=self.command):
            return
        try:
            command.process(self, self.command)
        finally:
            command.cleanup()
        self.invoke('afterProcess')

    def processAll(self, commands):
        """Process a sequence of commands."""
        for command in commands:
            self.process(command)

    # Medium-level processing.

    def tokens(self, tokens, locals=None):
        """Do an explicit result on a sequence of tokens.
        Cleans up after itself."""
        outFile = StringIO()
        stream = Stream(self, outFile, self.diversions)
        if self.invoke('beforeTokens', tokens=tokens, locals=locals):
            return
        self.streams.push(stream)
        try:
            self.runSeveral(tokens, locals)
            stream.flush()
            result = outFile.getvalue()
            self.invoke('afterTokens', result=result)
            return result
        finally:
            self.streams.pop()

    def quote(self, data):
        """Quote the given string so that if it were expanded it would
        evaluate to the original."""
        if self.invoke('beforeQuote', string=data):
            return
        scanner = Scanner(self.config, self.getContext(), self.currents, data)
        result = []
        i = 0
        try:
            j = scanner.next(self.config.prefix, i)
            result.append(data[i:j])
            result.append(self.config.prefix * 2)
            i = j + 1
        except TransientParseError:
            pass
        result.append(data[i:])
        result = ''.join(result)
        self.invoke('afterQuote', result=result)
        return result

    def escape(self, data, more=''):
        """Escape a string so that nonprintable or non-ASCII characters
        are replaced with compatible EmPy expansions.  Also treat
        characters in more as escapes."""
        if self.invoke('beforeEscape', string=data, more=more):
            return
        result = []
        for char in data:
            if char in more:
                result.append(self.config.prefix + '\\' + char)
            elif char < ' ' or char > '~':
                result.append(self.config.escaped(ord(char),
                                                  self.config.prefix + '\\'))
            else:
                result.append(char)
        result = ''.join(result)
        self.invoke('afterEscape', result=result)
        return result

    def tokenize(self, name):
        """Take an lvalue string and return a name or a (possibly recursive)
        list of names."""
        result = []
        stack = [result]
        for garbage in self.ASSIGN_TOKEN_RE.split(name):
            garbage = garbage.strip()
            if garbage:
                raise ParseError("unexpected assignment token: `%s`" % garbage)
        tokens = self.ASSIGN_TOKEN_RE.findall(name)
        # While processing, put a None token at the start of any list in which
        # commas actually appear.
        for token in tokens:
            if token == '(':
                stack.append([])
            elif token == ')':
                top = stack.pop()
                if len(top) == 1:
                    top = top[0] # no None token means that it's not a 1-tuple
                elif top[0] is None:
                    del top[0] # remove the None token for real tuples
                stack[-1].append(top)
            elif token == ',':
                if len(stack[-1]) == 1:
                    stack[-1].insert(0, None)
            else:
                stack[-1].append(token)
        # If it's a 1-tuple at the top level, turn it into a real subsequence.
        if result and result[0] is None:
            result = [result[1:]]
        if len(result) == 1:
            return result[0]
        else:
            return result

    def atomic(self, name, value, locals=None):
        """Do an atomic assignment."""
        if self.invoke('beforeAtomic', name=name, value=value, locals=locals):
            return
        if locals is None:
            self.globals[name] = value
        else:
            locals[name] = value
        self.invoke('afterAtomic')

    def multi(self, names, values, locals=None):
        """Do a (potentially recursive) assignment."""
        if self.invoke('beforeMulti', names=names, values=values, locals=locals):
            return
        values = tuple(values) # to force an exception if not a sequence
        if len(names) != len(values):
            raise ValueError("unpack tuple of wrong size")
        for name, value in zip(names, values):
            if isinstance(name, strType):
                self.atomic(name, value, locals)
            else:
                self.multi(name, value, locals)
        self.invoke('afterMulti')

    def assign(self, name, value, locals=None):
        """Do a potentially complex (including tuple unpacking) assignment."""
        left = self.tokenize(name)
        # The return value of tokenize can either be a string or a list of
        # (lists of) strings.
        if isinstance(left, strType):
            self.atomic(left, value, locals)
        else:
            self.multi(left, value, locals)

    def significate(self, key, value=None, locals=None):
        """Declare a significator."""
        if self.invoke('beforeSignificate', key=key, value=value, locals=locals):
            return
        name = self.config.significatorFor(key)
        self.atomic(name, value, locals)
        self.significators[key] = value
        self.invoke('afterSignificate')

    def clause(self, catch, locals=None):
        """Given the string representation of an except clause, turn
        it into a 2-tuple consisting of the class name or tuple of
        names, and either a variable name or None.  If the
        representation is None, then it's all exceptions and no name."""
        if self.invoke('beforeClause', catch=catch, locals=locals):
            return
        done = False
        if catch is None:
            exceptionCode, variable = None, None
            done = True
        if not done:
            match = self.AS_RE.search(catch)
            if match:
                exceptionCode, variable = self.AS_RE.split(catch.strip(), 1)
                exceptionCode = exceptionCode.strip()
                variable = variable.strip()
            else:
                comma = catch.rfind(',')
                if comma >= 0:
                    exceptionCode, variable = catch[:comma], catch[comma + 1:]
                    exceptionCode = exceptionCode.strip()
                    variable = variable.strip()
                else:
                    exceptionCode, variable = catch.strip(), None
        if not exceptionCode:
            exception = self.config.baseException
        else:
            exception = self.evaluate(exceptionCode, locals)
        self.invoke('afterClause', exception=exception, variable=variable)
        return exception, variable

    def dictionary(self, code, locals=None):
        """Given a string representing a key-value argument list, turn
        it into a dictionary."""
        code = code.strip()
        self.push()
        try:
            if self.invoke('beforeDictionary', code=code, locals=locals):
                return
            if code.strip():
                result = self.evaluate('{%s}' % code, locals)
            else:
                result = {}
            self.invoke('afterDictionary', result=result)
            return result
        finally:
            self.pop()

    def literal(self, text, locals=None):
        """Process a string literal."""
        if self.invoke('beforeLiteral', text=text, locals=locals):
            return
        result = self.evaluate(text, locals, replace=False)
        self.serialize(result)
        self.invoke('afterLiteral', result=result)

    def functional(self, code, tokensLists, locals=None):
        """Handle a functional expression like @f{x} and return the
        result.  code is the Python code to evaluate tokensLists is a
        list of list of tokens."""
        self.push()
        try:
            if self.invoke('beforeFunctional', code=code, lists=tokensLists,
                           locals=locals):
                return
            function = self.evaluate(code, locals)
            arguments = []
            for tokensSublist in tokensLists:
                arguments.append(self.tokens(tokensSublist, locals))
            result = function(*tuple(arguments))
            self.invoke('afterFunctional', result=result)
            return result
        finally:
            self.pop()

    # Low-level evaluation.

    def run(self, token, locals=None):
        """Run a token, tracking the current context."""
        token.run(self, locals)

    def runSeveral(self, tokens, locals=None):
        """Run a sequence of tokens."""
        for token in tokens:
            self.run(token, locals)

    def defined(self, name, locals=None):
        """Return a Boolean indicating whether or not the name is
        defined either in the locals or the globals."""
        if self.invoke('beforeDefined', name=name, locals=locals):
            return
        result = False
        if locals is not None and name in locals:
            result = True
        elif name in self.globals:
            result = True
        self.invoke('afterDefined', result=result)
        return result

    def lookup(self, variable, locals=None):
        """Lookup the value of a variable."""
        if locals is not None and variable in locals:
            return locals[variable]
        else:
            return self.globals[variable]

    def evaluate(self, expression, locals=None, replace=True):
        """Evaluate an expression.  If replace is true, replace
        newlines in the expression with spaces if that config
        variable is set; otherwise, don't do it regardless."""
        self.push()
        try:
            if self.invoke('beforeEvaluate',
                           expression=expression, locals=locals, replace=replace):
                return
            if replace and self.config.replaceNewlines:
                expression = expression.replace('\n', ' ')
            if locals is not None:
                result = self.core.evaluate(expression, self.globals, locals)
            else:
                result = self.core.evaluate(expression, self.globals)
            self.invoke('afterEvaluate', result=result)
            return result
        finally:
            self.pop()

    def execute(self, statements, locals=None):
        """Execute a statement(s)."""
        # If there are any carriage returns (as opposed to linefeeds/newlines)
        # in the statements code, then remove them.  Even on Windows platforms,
        # this will work in the Python interpreter.
        if CARRIAGE_RETURN_CHAR in statements:
            statements = statements.replace(CARRIAGE_RETURN_CHAR, '')
        # If there are no newlines in the statements code, then strip any
        # leading or trailing whitespace.
        if statements.find(NEWLINE_CHAR) < 0:
            statements = statements.strip()
        self.push()
        try:
            if self.invoke('beforeExecute',
                           statements=statements, locals=locals):
                return
            self.core.execute(statements, self.globals, locals)
            self.invoke('afterExecute')
        finally:
            self.pop()

    def single(self, source, locals=None):
        """Execute an expression or statement, just as if it were
        entered into the Python interactive interpreter."""
        self.push()
        try:
            if self.invoke('beforeSingle',
                           source=source, locals=locals):
                return
            code = compile(source, '<single>', 'single')
            result = self.core.execute(code, self.globals, locals)
            self.invoke('afterSingle', result=result)
            return result
        finally:
            self.pop()

    #
    # Pseudomodule routines.
    #

    # Identification.

    def identify(self):
        """Identify the topmost context with a tuple of the name and
        counters."""
        return self.getContext().identify()

    # Contexts.

    def getContext(self):
        """Get the top context."""
        return self.contexts.top()

    def newContext(self, name='<unnamed>', line=None, column=None, chars=None):
        """Create and return a new context."""
        if isinstance(name, tuple):
            # If name is a tuple, then use it as an argument.
            return self.newContext(*name)
        elif isinstance(name, Context):
            # If it's a Context, create a fresh clone of it.
            context = Context('<null>', 0, 0,
                startingLine=self.config.startingLine,
                startingColumn=self.config.startingColumn)
            context.restore(name)
            return context
        else:
            # Otherwise, build it up from scratch.
            if line is None:
                line = self.config.startingLine
            if column is None:
                column = self.config.startingColumn
            if chars is None:
                chars = 0
            return Context(name, line, column, chars)

    def pushContext(self, context):
        """Push a new context on the stack."""
        self.invoke('pushContext', context=context)
        self.contexts.push(context)
        self.currents.push(self.config.renderContext(context))

    def popContext(self):
        """Pop the top context."""
        context = self.contexts.pop()
        self.currents.pop()
        self.invoke('popContext', context=context)

    def setContext(self, context):
        """Replace the top context."""
        self.contexts.replace(context)
        self.currents.replace(self.config.renderContext(context))
        self.invoke('setContext', context=context)

    def setContextName(self, name):
        """Set the name of the topmost context."""
        context = self.getContext()
        context.name = name
        self.currents.replace(self.config.renderContext(context))
        self.invoke('setContext', context=context)

    def setContextLine(self, line):
        """Set the line number of the topmost context."""
        context = self.getContext()
        context.line = line
        self.currents.replace(self.config.renderContext(context))
        self.invoke('setContext', context=context)

    def setContextColumn(self, column):
        """Set the column number of the topmost context."""
        context = self.getContext()
        context.column = column
        self.currents.replace(self.config.renderContext(context))
        self.invoke('setContext', context=context)

    def setContextData(self, name=None, line=None, column=None, chars=None):
        """Set any of the name, line, or column of the topmost context."""
        context = self.getContext()
        if name is not None:
            context.name = name
        if line is not None:
            context.line = line
        if column is not None:
            context.column = column
        if chars is not None:
            context.chars = chars
        self.currents.replace(self.config.renderContext(context))
        self.invoke('setContext', context=context)

    def restoreContext(self, oldContext, strict=False):
        """Restore from an old context."""
        context = self.getContext()
        context.restore(oldContext, strict)
        self.currents.replace(self.config.renderContext(context))
        self.invoke('restoreContext', context=context)

    # Plugins.

    def attach(self, plugin):
        """Attach the plugin to this interpreter."""
        if hasattr(plugin, 'attach'):
            plugin.attach(self)

    def detach(self, plugin):
        """Detach the plugin from this interpreter."""
        if hasattr(plugin, 'detach'):
            plugin.detach(self)

    # Finalizers.

    def clearFinalizers(self):
        """Clear all finalizers."""
        for finalizer in self.finalizers:
            self.detach(finalizer)
        self.finalizers = []

    def appendFinalizer(self, finalizer):
        """Register a function to be called at exit."""
        self.attach(finalizer)
        self.finalizers.append(finalizer)

    def prependFinalizer(self, finalizer):
        """Register a function to be called at exit."""
        self.attach(finalizer)
        self.finalizers.insert(0, finalizer)

    def setFinalizers(self, finalizers):
        self.clearFinalizers()
        for finalizer in finalizers:
            self.attach(finalizer)
        self.finalizers = finalizers

    atExit = appendFinalizer # DEPRECATED

    # Globals.

    def fixGlobals(self):
        """Reset the globals, stamping in the pseudomodule."""
        if self.globals is None:
            self.globals = {}
        # Make sure that there is no collision between two interpreters'
        # globals.
        if self.config.pseudomoduleName in self.globals:
            if self.globals[self.config.pseudomoduleName] is not self:
                raise ConsistencyError("interpreter pseudomodule collision in globals")
        # And finally, flatten the namespaces if that option has been set.
        if self.config.doFlatten:
            self.flattenGlobals()
        self.globals[self.config.pseudomoduleName] = self

    def unfixGlobals(self):
        """Remove the pseudomodule (if present) from the globals."""
        for unwantedKey in self.config.unwantedGlobalsKeys:
            # None is a special sentinel that must be replaced with the name of
            # the pseudomodule.
            if unwantedKey is None:
                unwantedKey = self.config.pseudomoduleName
            if unwantedKey in self.globals:
                del self.globals[unwantedKey]

    def getGlobals(self):
        """Retrieve the globals."""
        return self.globals

    def setGlobals(self, globals):
        """Set the globals to the specified dictionary."""
        self.globals = globals
        self.fixGlobals()

    def updateGlobals(self, otherGlobals):
        """Merge another mapping object into this interpreter's globals."""
        self.globals.update(otherGlobals)
        self.fixGlobals()

    def clearGlobals(self):
        """Clear out the globals with a brand new dictionary."""
        self.globals = {}
        self.fixGlobals()

    def pushGlobals(self, globals):
        """Push a globals dictionary onto the history stack."""
        self.globalsHistory.push(globals)

    def popGlobals(self):
        """Pop a globals dictinoary off the history stack and return
        it."""
        return self.globalsHistory.pop()

    def saveGlobals(self, deep=True):
        """Save a copy of the globals off onto the history stack."""
        if deep:
            copyMethod = copy.deepcopy
        else:
            copyMethod = copy.copy
        self.unfixGlobals()
        self.globalsHistory.push(copyMethod(self.globals))
        self.fixGlobals()

    def restoreGlobals(self, destructive=True):
        """Restore the most recently saved copy of the globals."""
        if destructive:
            fetchMethod = self.globalsHistory.pop
        else:
            fetchMethod = self.globalsHistory.top
        self.unfixGlobals()
        self.globals = fetchMethod()
        self.fixGlobals()

    def flattenGlobals(self, skipKeys=None):
        """Flatten the contents of the pseudo-module into the globals
        namespace."""
        flattened = {}
        if skipKeys is None:
            skipKeys = self.config.unflattenableGlobalsKeys
        # The pseudomodule is really a class instance, so we need to fumble
        # using getattr instead of simply fumbling through the instance's
        # __dict__.
        for key in self.__dict__.keys():
            if key not in skipKeys and not key.startswith('_'):
                flattened[key] = getattr(self, key)
        for key in self.__class__.__dict__.keys():
            if key not in skipKeys and not key.startswith('_'):
                flattened[key] = getattr(self, key)
        # Stomp everything into the globals namespace.
        self.globals.update(flattened)

    # Prefix.

    def getPrefix(self):
        """Get the current prefix."""
        return self.config.prefix

    def setPrefix(self, prefix):
        """Set the prefix."""
        assert (prefix is None or
                (isinstance(prefix, strType) and len(prefix) == 1)), prefix
        self.config.prefix = prefix

    # Diversions.

    def stopDiverting(self):
        """Stop any diverting."""
        self.top().revert()

    def createDiversion(self, name):
        """Create a diversion (but do not divert to it) if it does not
        already exist."""
        self.top().create(name)

    def retrieveDiversion(self, name, *defaults):
        """Retrieve the diversion object associated with the name."""
        return self.top().retrieve(name, *defaults)

    def startDiversion(self, name):
        """Start diverting to the given diversion name."""
        self.top().divert(name)

    def playDiversion(self, name, drop=True):
        """Play the given diversion and then drop it."""
        self.top().undivert(name, drop)

    def replayDiversion(self, name, drop=False):
        """Replay the diversion without dropping it."""
        self.top().undivert(name, drop)

    def dropDiversion(self, name):
        """Eliminate the given diversion."""
        self.top().drop(name)

    def playAllDiversions(self):
        """Play all existing diversions and then drop them."""
        self.top().undivertAll(True)

    def replayAllDiversions(self):
        """Replay all existing diversions without dropping them."""
        self.top().undivertAll(False)

    def dropAllDiversions(self):
        """Drop all existing diversions."""
        self.top().dropAll()

    def getCurrentDiversionName(self):
        """Get the name of the current diversion."""
        return self.top().current

    def getAllDiversionNames(self):
        """Get the names of all existing diversions."""
        return self.top().names()

    def isExistingDiversionName(self, name):
        """Does a diversion with this name currently exist?"""
        return self.top().has(name)

    # Filters.

    def resetFilter(self):
        """Reset the filter stream so that it does no filtering."""
        self.top().install(None)

    def getFilter(self):
        """Get the top-level filter."""
        filter = self.top().sink
        if filter is self.top().file:
            return None
        else:
            return filter

    getFirstFilter = getFilter

    def getLastFilter(self):
        """Get the last filter in the current chain."""
        return self.top().last()

    def getFilterCount(self):
        """Get the number of chained filters; 0 means no active
        filters."""
        return self.top().count()

    def setFilter(self, *filters):
        """Set the filter."""
        self.top().install(filters)

    def prependFilter(self, filter):
        """Attach a single filter to the end of the current filter chain."""
        self.top().prepend(filter)

    def appendFilter(self, filter):
        """Attach a single filter to the end of the current filter chain."""
        self.top().append(filter)

    attachFilter = appendFilter # DEPRECATED

    def setFilterChain(self, filters):
        """Set the filter."""
        self.top().install(filters)

    # Core-related activity.

    def hasCore(self):
        """Does this interpreter have a core inserted?"""
        return self.core is not None

    def getCore(self):
        """Get this interpreter's core or None."""
        return self.core

    def insertCore(self, core=None):
        """Insert and attach the execution core."""
        if core is None:
            core = Core()
        self.attach(core)
        self.core = core

    def ejectCore(self):
        """Clear the execution core, breaking a potential cyclical link."""
        if self.core is not None:
            self.detach(self.core)
            self.core = None

    def resetCore(self):
        """Reset the execution core to the default core."""
        self.ejectCore()
        self.insertCore()

    # Extension-related activity.

    def hasExtension(self):
        """Does this interpreter have an extension installed?"""
        return self.extension is not None

    def installExtension(self, extension):
        """Install an extension."""
        if self.extension is not None:
            raise ExtensionError("cannot replace an installed extension")
        self.extension = extension
        self.attach(extension)
        # Now make sure there are token classes for them.
        factory = self.config.getFactory()
        for first, name in extension.mapping.items():
            factory.addToken(self.config.createExtensionToken(first, name))

    def uninstallExtension(self):
        """Uninstall any extension.  This should only be done by the
        interpreter itself at shutdown time."""
        if self.extension is not None:
            self.detach(self.extension)
            self.extension = None

    def callExtension(self, name, contents, depth, locals):
        if self.extension is None:
            raise ExtensionError("no extension installed")
        method = getattr(self.extension, name, None)
        if method is None:
            raise ExtensionError("extension name `%s` not defined; define method on extension object" % name)
        if self.callback is not None:
            # Legacy callback behavior: only pass the contents argument.
            result = method(contents)
        else:
            result = method(contents, depth, locals)
        self.serialize(result)
        return result

    # Hooks.

    def invokeHook(self, _name, **kwargs):
        """Invoke the hook(s) associated with the hook name, should they
        exist.  Stop and return on the first hook which returns a true
        result."""
        if self.config.verbose:
            self.config.verboseFile.write("%s: %r\n" % (_name, kwargs))
        if self.hooksEnabled:
            for hook in self.hooks:
                try:
                    method = getattr(hook, _name)
                    result = method(**kwargs)
                    if result:
                        return result
                finally:
                    pass
        return None

    invoke = invokeHook

    def areHooksEnabled(self):
        """Return whether or not hooks are presently enabled."""
        if self.hooksEnabled is None:
            # None is a special value indicate that hooks are enabled but none
            # have been added yet.  It is equivalent to true for testing but
            # can be optimized away upon invocation.
            return True
        else:
            return self.hooksEnabled

    def enableHooks(self):
        """Enable hooks."""
        self.hooksEnabled = True

    def disableHooks(self):
        """Disable hooks."""
        self.hooksEnabled = False

    def getHooks(self):
        """Get the current hooks."""
        return self.hooks

    def addHook(self, hook, prepend=False):
        """Add a new hook; optionally insert it rather than appending it."""
        self.attach(hook)
        if self.hooksEnabled is None:
            self.hooksEnabled = True
        if prepend:
            self.hooks.insert(0, hook)
        else:
            self.hooks.append(hook)

    def appendHook(self, hook):
        """Append the given hook."""
        self.addHook(hook, False)

    def prependHook(self, hook):
        """Prepend the given hook."""
        self.addHook(hook, True)

    def removeHook(self, hook):
        """Remove a preexisting hook."""
        self.detach(hook)
        self.hooks.remove(hook)

    def clearHooks(self):
        """Clear all hooks."""
        for hook in self.hooks:
            self.detach(hook)
        self.hooks = []
        self.hooksEnabled = None

    # Callbacks (DEPRECATED).

    def hasCallback(self):
        """Is there a custom callback registered?"""
        return self.callback is not None

    def getCallback(self):
        """Get the custom markup callback registered with this
        interpreter, or None."""
        return self.callback

    def registerCallback(self, callback, extensionFactory=Extension):
        """Register a custom markup callback with this interpreter."""
        if self.callback:
            raise ExtensionError("old-style callbacks cannot be reregistered; use extensions instead")
        if self.hasExtension():
            raise ExtensionError("old-style callbacks cannot be registered over existing extension; add `angle_brackets` method to extension instead")
        self.callback = callback
        self.attach(callback)
        extension = extensionFactory(angle_brackets=callback)
        self.installExtension(extension)

    def deregisterCallback(self):
        """Remove any previously registered custom markup callback
        with this interpreter."""
        raise ExtensionError("old-style callbacks cannot be deregistered; use extensions instead")

    def _deregisterCallback(self):
        """Remove any previously registered custom markup callback
        with this interpreter.  Internal use only."""
        if self.callback is not None:
            self.detach(self.callback)
            self.callback = None

    def invokeCallback(self, contents):
        """Call the custom markup callback."""
        if self.invoke('beforeCallback', contents=contents):
            return
        if self.callback is None:
            raise ConfigurationError("custom markup `@<...>` invoked with no defined callback")
        else:
            result = self.callback(contents)
        self.invoke('afterCallback', result=result)
        return result

    # Error handling.

    def getExitCode(self):
        """Get the exit code corresponding for the current error (if
        any)."""
        return self.config.errorToExitCode(self.error)

    def exit(self, exitCode=None):
        """Exit.  If exitCode is None, use the exit code from the
        current error (which may itself be None for no error)."""
        if exitCode is None:
            exitCode = self.getExitCode()
        # If we are supposed to delete the file on error, do it.
        if exitCode != self.config.successCode and self.config.deleteOnError:
            if self.filespec is not None:
                os.remove(self.filespec[0])

    def reraise(self, *args):
        """Reraise an exception."""
        raise

    def dispatch(self, triple=None):
        """Dispatch an exception."""
        if self.config.ignoreErrors:
            return False
        if triple is None:
            triple = sys.exc_info()
        type, error, traceback = triple
        # If error is None, then this is a old-style string exception.
        if error is None:
            error = StringError(type)
        # If it's a keyboard interrupt, quit immediately.
        if isinstance(type, KeyboardInterrupt):
            fatal = True
        else:
            fatal = False
        # Now handle the exception.
        self.handle((type, error, traceback), fatal)
        return self.error is not None and self.config.exitOnError

    def handle(self, info, fatal=False):
        """Handle an actual error that occurred."""
        self.invoke('atHandle', info=info, fatal=fatal, contexts=self.currents)
        type, self.error, traceback = info
        if self.config.isExitError(self.error):
            # No Python exception, but we're going to exit.
            fatal = True
        else:
            useDefault = True
            if self.handler is not None:
                # Call the customer handler.
                useDefault = self.handler(type, self.error, traceback)
            if useDefault and self.error is not None:
                # Call the default handler if there's still an error.
                self.defaultHandler(type, self.error, traceback)
            if self.config.rawErrors:
                raise
        if self.error is not None and (fatal or self.config.exitOnError):
            self.shutdown()
            self.exit()

    def defaultHandler(self, type, error, traceback):
        """Report an error."""
        first = True
        self.flush()
        sys.stderr.write('\n')
        for current in self.currents:
            if current is None:
                current = self.config.renderContext(self.getContext())
            if first:
                if error is not None:
                    description = "error: %s" % self.config.formatError(error)
                else:
                    description = "error"
            else:
                description = "from this context"
            first = False
            sys.stderr.write('%s: %s\n' % (current, description))
        sys.stderr.flush()

    def getHandler(self):
        """Get the current handler, or None for the default."""
        return self.handler

    def setHandler(self, handler, exitOnError=False):
        """Set the current handler.  Additionally, specify whether
        errors should exit (defaults to false with a custom
        handler)."""
        if self.handler is not None:
            self.detach(self.handler)
        self.attach(handler)
        self.handler = handler
        if exitOnError is not None:
            self.config.exitOnError = exitOnError

    def resetHandler(self, exitOnError=None):
        """Reset the current handler to the default."""
        if self.handler is not None:
            self.detach(self.handler)
        self.handler = None
        if exitOnError is not None:
            self.config.exitOnError = exitOnError

    def invokeHandler(self, *args):
        """Manually invoke the error handler with the given
        exception info 3-tuple or three arguments."""
        if len(args) == 1:
            self.handler(*args[0])
        else:
            self.handler(*args)

    # Emojis.

    def initializeEmojiModules(self, moduleNames=None):
        """Determine which emoji module to use.  If moduleNames is not
        specified, use the defaults."""
        return self.config.initializeEmojiModules(moduleNames)

    def getEmojiModule(self, moduleName):
        """Return an abstracted emoji module by name or return
        None."""
        return self.config.emojiModules.get(moduleName)

    def getEmojiModuleNames(self):
        """Return the emoji module names in usage in their proper
        order."""
        return self.config.emojiModuleNames

    def substituteEmoji(self, text):
        """Substitute an emoji text or return None."""
        return self.config.substituteEmoji(text)

#
# functions
#

def extract(dict, key, default):
    """Retrieve the value of the given key in this dictionary, but delete
    it first.  If the key is not present, use the given default."""
    if key in dict:
        value = dict.get(key)
        del dict[key]
    else:
        value = default
    return value

def details(level, config=None, prelim="Welcome to ", postlim=".\n",
            file=sys.stdout):
    """Write some details, using the details subsystem if available."""
    if config is None:
        config = Configuration()
    config.installFinder(dryRun=True)
    try:
        write = file.write
        details = None
        if level > Version.VERSION:
            try:
                import emlib
                details = emlib.Details(config)
            except ImportError:
                raise ConfigurationError("missing emlib module; details subsystem not available")
        if details is not None:
            try:
                details.show(level, prelim, postlim, file)
            except TypeError:
                raise
        else:
            write("%s%s version %s%s" % (prelim, __project__, __version__, postlim))
        sys.stdout.flush()
    finally:
        config.uninstallFinder()

def expand(data,
           _globals=None, _argv=None, _prefix=None, _pseudo=None, _options=None,
           **kwargs):
    """Do a self-contained expansion of the given source data,
    creating and shutting down an interpreter dedicated to the task.
    Expects the same keyword arguments as the Interpreter constructor.
    Additionally, 'name' will identify the expansion filename and
    'locals', if present, represents the locals dictionary to use.
    The sys.stdout object is saved off and then replaced before this
    function returns.  Any exception that occurs will be raised to the
    caller."""
    # For backward compatibility.  These arguments (starting with an
    # underscore) are now DEPRECATED.
    if _globals is not None:
        if 'globals' in kwargs:
            raise CompatibilityError("keyword arguments contain extra `globals` key; use keyword arguments")
        kwargs['globals'] = _globals
    if _argv is not None:
        if 'argv' in kwargs:
            raise CompatibilityError("keyword arguments contain extra `argv` key; use keyword arguments")
        kwargs['argv'] = _argv
    if _prefix is not None:
        raise CompatibilityError("_prefix argument to expand no longer supported; use prefix configuration variable")
    if _pseudo is not None:
        raise CompatibilityError("_pseudo argument to expand no longer supported; use pseudomoduleName configuration variable")
    if _options is not None:
        raise CompatibilityError("options dictionary is no longer supported; use configurations")
    # Keyword argument compatibility checks.
    for key in ['filters', 'handler', 'input', 'output']:
        if kwargs.get(key):
            raise ConfigurationError("argument does not make sense with an ephemeral interpreter; use a non-ephemeral interpreter instead: `%s`" % key, key=key)
    # Set up the changed defaults.
    if 'dispatcher' not in kwargs:
        kwargs['dispatcher'] = False
    # And then the local variables.
    name = extract(kwargs, 'name', '<expand>')
    locals = extract(kwargs, 'locals', None)
    if isinstance(locals, dict) and len(locals) == 0:
        # If there were no keyword arguments specified, don't use a locals
        # dictionary at all.
        locals = None
    interp = None
    result = None
    try:
        interp = Interpreter(**kwargs)
        result = interp.expand(data, locals, name, dispatcher=None)
    finally:
        if interp:
            interp.shutdown()
            interp.unfixGlobals() # remove pseudomodule to prevent clashes
    return result

def invoke(args, **kwargs):
    """Run a standalone instance of an EmPy interpreter with the given
    command line arguments.  See the Interpreter constructor for the
    keyword arguments."""
    # Get the defaults.
    config = extract(kwargs, 'config', None)
    errors = extract(kwargs, 'errors', ())
    globals = extract(kwargs, 'globals', {})
    hooks = extract(kwargs, 'hooks', [])
    output = extract(kwargs, 'output', None)
    for key in ['filespec', 'immediately']:
        if key in kwargs:
            raise ConfigurationError("argument cannot be specified with invoke: %s" % key, key=key)
    # Initialize the options.
    if config is None:
        config = Configuration()
    if errors is None:
        errors = config.topLevelErrors
    # Let's go!
    try:
        interp = None
        inputFilename = None
        inputMode = 'r'
        outputFilename = None
        outputMode = None
        nullFile = False
        preprocessing = []
        postprocessing = []
        preinitializers = []
        postinitializers = []
        configStatements = []
        configPaths = []
        immediately = False
        level = Version.NONE
        topics = None
        # Note any configuration files from the environment.
        configPath = config.environment(CONFIG_ENV)
        if configPath is not None:
            configPaths.append(configPath)
        # Get any extra arguments from the environment.
        extraArguments = config.environment(OPTIONS_ENV)
        if extraArguments is not None:
            extraArguments = extraArguments.split()
            args = extraArguments + args
        # Parse the arguments.
        try:
            SHORTS = 'VWZh?H:vp:qm:fkersidnc:Co:a:O:A:b:NLBP:Q:I:D:S:E:F:G:K:X:Y:wlux:y:z:gj'
            LONGS = ['version', 'info', 'details', 'help', 'topic=', 'topics=', 'extended-help=', 'verbose', 'prefix=', 'no-prefix', 'no-output', 'pseudomodule=', 'module=', 'flatten', 'keep-going', 'ignore-errors', 'raw-errors', 'brief-errors', 'verbose-errors', 'interactive', 'delete-on-error', 'no-proxy', 'no-override-stdout', 'config=', 'configuration=', 'config-file=', 'configuration-file=', 'config-variable=', 'configuration-variable=', 'ignore-missing-config', 'output=' 'append=', 'output-binary=', 'append-binary=', 'output-mode=', 'input-mode=', 'buffering=', 'default-buffering', 'no-buffering', 'line-buffering', 'full-buffering', 'preprocess=', 'postprocess=', 'import=', 'define=', 'string=', 'execute=', 'file=', 'postfile=', 'postexecute=', 'expand=', 'postexpand=', 'preinitializer=', 'postinitializer=', 'pause-at-end', 'relative-path', 'replace-newlines', 'no-replace-newlines', 'ignore-bangpaths', 'no-ignore-bangpaths', 'expand-user', 'no-expand-user', 'auto-validate-icons', 'no-auto-validate-icons', 'none-symbol=', 'no-none-symbol', 'starting-line=', 'starting-column=', 'emoji-modules=', 'no-emoji-modules', 'disable-emoji-modules', 'ignore-emoji-not-found', 'binary', 'input-binary', 'unicode', 'encoding=', 'unicode-encoding=', 'input-encoding=', 'unicode-input-encoding=', 'output-encoding=', 'unicode-output-encoding=', 'errors=', 'unicode-errors=', 'input-errors=', 'unicode-input-errors=', 'output-errors=', 'unicode-output-errors=', 'normalization-form=', 'unicode-normalization-form=', 'auto-play-diversions', 'no-auto-play-diversions', 'check-variables', 'no-check-variables', 'path-separator=', 'enable-modules', 'disable-modules', 'module-extension=', 'module-finder-index=', 'enable-import-output', 'disable-import-output', 'context-format=', 'success-code=', 'failure-code=', 'unknown-code=', 'null-hook', 'requirements=']
            pairs, argv = getopt.getopt(args, SHORTS, LONGS)
        except getopt.GetoptError:
            type, error, traceback = sys.exc_info()
            if error.args[1] in ['H', 'topic', 'topics', 'extended-help']:
                # A missing argument with -H should be interpreted as -H all.
                pairs = []
                topics = 'all'
            else:
                raise InvocationError(*error.args)
        for option, argument in pairs:
            if option in ['-V', '--version']:
                level += 1
            elif option in ['-W', '--info']:
                if level < Version.INFO:
                    level = Version.INFO
                else:
                    level += 1
            elif option in ['-Z', '--details']:
                level = Version.ALL
            elif option in ['-h', '-?', '--help']:
                if not topics:
                    topics = 'default'
                elif topics == 'default':
                    topics = 'more'
                else:
                    topics = 'all'
            elif option in ['-H', '--topic', '--topics', '--extended-help']:
                topics = argument
                if ',' in topics:
                    topics = topics.split(',')
                else:
                    topics = [topics]
            elif option in ['-v', '--verbose']:
                config.verbose = True
            elif option in ['-p', '--prefix']:
                config.prefix = argument
            elif option in ['--no-prefix']:
                config.prefix = None
            elif option in ['-q', '--no-output']:
                nullFile = True
            elif option in ['-m', '--pseudomodule', '--module']:
                config.pseudomoduleName = argument
            elif option in ['-f', '--flatten']:
                config.doFlatten = True
            elif option in ['-k', '--keep-going']:
                config.exitOnError = False
            elif option in ['-e', '--ignore-errors']:
                config.ignoreErrors = True
                config.exitOnError = False
            elif option in ['-r', '--raw-errors']:
                config.rawErrors = True
            elif option in ['-s', '--brief-errors']:
                config.verboseErrors = False
            elif option in ['--verbose-errors']:
                config.verboseErrors = True
            elif option in ['-i', '--interactive']:
                config.goInteractive = True
            elif option in ['-d', '--delete-on-error']:
                config.deleteOnError = True
            elif option in ['-n', '--no-proxy', '--no-override-stdout']:
                config.useProxy = False
            elif option in ['--config', '--configuration']:
                configStatements.append(argument)
            elif option in ['-c', '--config-file', '--configuration-file']:
                configPaths.append(argument)
            elif option in ['--config-variable', '--configuration-variable']:
                config.configVariableName = argument
            elif option in ['-C', '--ignore-missing-config']:
                config.missingConfigIsError = False
            elif option in ['-o', '--output']:
                outputFilename = argument
                outputMode = 'w'
            elif option in ['-a', '--append']:
                outputFilename = argument
                outputMode = 'a'
            elif option in ['-O', '--output-binary']:
                outputFilename = argument
                outputMode = 'wb'
            elif option in ['-A', '--append-binary']:
                outputFilename = argument
                outputMode = 'ab'
            elif option in ['--output-mode']:
                outputMode = argument
            elif option in ['--input-mode']:
                inputMode = argument
            elif option in ['-b', '--buffering']:
                config.setBuffering(argument)
            elif option in ['--default-buffering']:
                config.setBuffering(config.defaultBuffering)
            elif option in ['-N', '--no-buffering']:
                config.setBuffering(config.noBuffering)
            elif option in ['-L', '--line-buffering']:
                config.setBuffering(config.lineBuffering)
            elif option in ['-B', '--full-buffering']:
                config.setBuffering(config.fullBuffering)
            elif option in ['-I', '--import']:
                for module in argument.split(','):
                    module = module.strip()
                    preprocessing.append(ImportCommand(module))
            elif option in ['-D', '--define']:
                preprocessing.append(DefineCommand(argument))
            elif option in ['-S', '--string']:
                preprocessing.append(StringCommand(argument))
            elif option in ['-P', '--preprocess']:
                preprocessing.append(DocumentCommand(argument))
            elif option in ['-Q', '--postprocess']:
                postprocessing.append(DocumentCommand(argument))
            elif option in ['-E', '--execute']:
                preprocessing.append(ExecuteCommand(argument))
            elif option in ['-F', '--file']:
                preprocessing.append(FileCommand(argument))
            elif option in ['-G', '--postfile']:
                postprocessing.append(FileCommand(argument))
            elif option in ['-K', '--postexecute']:
                postprocessing.append(ExecuteCommand(argument))
            elif option in ['-X', '--expand']:
                preprocessing.append(ExpandCommand(argument))
            elif option in ['-Y', '--postexpand']:
                postprocessing.append(ExpandCommand(argument))
            elif option in ['--preinitializer']:
                preinitializers.append(argument)
            elif option in ['--postinitializer']:
                postinitializers.append(argument)
            elif option in ['-w', '--pause-at-end']:
                config.pauseAtEnd = True
            elif option in ['-l', '--relative-path']:
                config.relativePath = True
            elif option in ['--replace-newlines']:
                config.replaceNewlines = True
            elif option in ['--no-replace-newlines']:
                config.replaceNewlines = False
            elif option in ['--ignore-bangpaths']:
                config.ignoreBangpaths = True
            elif option in ['--no-ignore-bangpaths']:
                config.ignoreBangpaths = False
            elif option in ['--expand-user']:
                config.expandUserConstructions = True
            elif option in ['--no-expand-user']:
                config.expandUserConstructions = False
            elif option in ['--auto-validate-icons']:
                config.autoValidateIcons = True
            elif option in ['--no-auto-validate-icons']:
                config.autoValidateIcons = False
            elif option in ['--none-symbol']:
                config.noneSymbol = argument
            elif option in ['--no-none-symbol']:
                config.noneSymbol = None
            elif option in ['--starting-line']:
                config.startingLine = int(argument)
            elif option in ['--starting-column']:
                config.startingColumn = int(argument)
            elif option in ['--emoji-modules']:
                moduleNames = [x.strip() for x in argument.split(',')]
                if moduleNames == ['None'] or moduleNames == ['']:
                    moduleNames = None
                config.emojiModuleNames = moduleNames
            elif option in ['--no-emoji-modules']:
                config.emojiModuleNames = config.defaultNoEmojiModuleNames
            elif option in ['--disable-emoji-modules']:
                config.emojiModuleNames = None
            elif option in ['--ignore-emoji-not-found']:
                config.emojiNotFoundIsError = False
            elif option in ['-u', '--binary', '--input-binary', '--unicode']:
                config.enableBinary()
            elif option in ['-x', '--encoding', '--unicode-encoding']:
                config.enableBinary(major, minor)
                config.inputEncoding = config.outputEncoding = argument
            elif option in ['--input-encoding', '--unicode-input-encoding']:
                config.enableBinary(major, minor)
                config.inputEncoding = argument
            elif option in ['--output-encoding', '--unicode-output-encoding']:
                config.enableBinary(major, minor)
                config.outputEncoding = argument
            elif option in ['-y', '--errors', '--unicode-errors']:
                config.enableBinary(major, minor)
                config.inputErrors = config.outputErrors = argument
            elif option in ['--input-errors', '--unicode-input-errors']:
                config.enableBinary(major, minor)
                config.inputErrors = argument
            elif option in ['--output-errors', '--unicode-output-errors']:
                config.enableBinary(major, minor)
                config.outputErrors = argument
            elif option in ['-z', '--normalization-form', '--unicode-normalization-form']:
                if argument == 'none' or argument == 'None':
                    argument = ''
                config.normalizationForm = argument
            elif option in ['--auto-play-diversions']:
                config.autoPlayDiversions = True
            elif option in ['--no-auto-play-diversions']:
                config.autoPlayDiversions = False
            elif option in ['--check-variables']:
                config.checkVariables = True
            elif option in ['--no-check-variables']:
                config.checkVariables = False
            elif option in ['--path-separator']:
                config.pathSeparator = argument
            elif option in ['--enable-modules']:
                config.supportModules = True
            elif option in ['-g', '--disable-modules']:
                config.supportModules = False
                config.moduleExtension = argument
            elif option in ['--module-extension']:
                config.moduleExtension = argument
            elif option in ['--module-finder-index']:
                config.moduleFinderIndex = int(argument)
            elif option in ['--enable-import-output']:
                config.enableImportOutput = True
            elif option in ['-j', '--disable-import-output']:
                config.enableImportOutput = False
            elif option in ['--context-format']:
                config.setContextFormat(argument)
                Context.format = config.contextFormat
            elif option in ['--success-code']:
                config.successCode = int(argument)
            elif option in ['--failure-code']:
                config.failureCode = int(argument)
            elif option in ['--unknown-code']:
                config.unknownCode = int(argument)
            elif option in ['--null-hook']:
                try:
                    import emlib
                    hooks.append(emlib.Hook())
                except ImportError:
                    raise InvocationError("missing emlib module; --null-hook not available")
            elif option in ['--requirements']:
                try:
                    import emlib
                    det = emlib.Details()
                    if not det.checkRequirements(argument):
                        sys.exit(config.skipCode)
                except ImportError:
                    raise InvocationError("missing emlib module; cannot check requirements")
            else:
                assert False, "unhandled option: %s" % option
        # Show the details and exit if desired.
        if level > 0:
            details(level, config)
            return config.successCode
        # Load any configuration files.
        for configStatement in configStatements:
            config.run(configStatement)
        for configPath in configPaths:
            config.path(configPath)
        # Show the help if desired.
        if topics:
            try:
                import emhelp
                usage = emhelp.Usage(config)
                usage.hello()
                usage.show(topics)
            except ImportError:
                raise InvocationError("missing emhelp subsystem module; no help available")
            return config.successCode
        # Set up the main script filename and the arguments.
        if not argv:
            argv.append(None)
        else:
            inputFilename = argv[0]
        # Do sanity checks on the configuration.
        config.check(inputFilename, outputFilename)
        # Now initialize the output file.
        if nullFile:
            output = NullFile()
            filespec = None
        elif outputFilename is not None:
            if output is not None:
                raise InvocationError("cannot specify more than one output")
            filespec = outputFilename, outputMode, config.buffering
            output = config.open(*filespec)
        else:
            # So this is stdout.  Check the encoding.
            if not config.isDefaultEncodingErrors(asInput=False):
                config.reconfigure(sys.stdout,
                                   config.buffering,
                                   config.outputEncoding,
                                   config.outputErrors)
            filespec = None
        # Run any pre-initializers.
        for initializer in preinitializers:
            file = config.open(initializer, 'r')
            try:
                data = file.read()
                execFunc(data, globals, locals())
            finally:
                file.close()
        # Get ready!
        kwargs['argv'] = argv
        kwargs['config'] = config
        kwargs['filespec'] = filespec
        kwargs['globals'] = globals
        kwargs['hooks'] = hooks
        kwargs['immediately'] = immediately
        kwargs['output'] = output
        try:
            # Create the interpreter.
            interp = Interpreter(**kwargs)
            # Check it.
            interp.check()
            # Run it.
            interp.go(
                inputFilename, inputMode, preprocessing, postprocessing)
            exitCode = interp.getExitCode()
        finally:
            # Finally, handle any cleanup.
            if interp is not None:
                interp.shutdown()
        # Run any post-initializers.
        for initializer in postinitializers:
            try:
                file = config.open(initializer, 'r')
                data = file.read()
                execFunc(data, globals, locals())
            finally:
                file.close()
    except SystemExit:
        type, error, traceback = sys.exc_info()
        if len(error.args) > 0:
            exitCode = error.args[0] # okay even if a string
        else:
            exitCode = config.successCode
    except KeyboardInterrupt:
        if config.rawErrors:
            raise
        type, error, traceback = sys.exc_info()
        sys.stderr.write(config.formatError(error, "ERROR: ", "\n"))
        exitCode = config.failureCode
    except errors:
        if config.rawErrors:
            raise
        type, error, traceback = sys.exc_info()
        if not interp:
            sys.stderr.write(config.formatError(error, "ERROR: ", "\n"))
        exitCode = config.unknownCode
    except:
        if config.rawErrors:
            raise
        type, error, traceback = sys.exc_info()
        if not interp:
            sys.stderr.write(config.formatError(error, "ERROR: ", "\n"))
        exitCode = config.errorToExitCode(error)
    # Old versions of Python 3.x don't flush sys.__stdout__ when redirecting
    # stdout for some reason.
    if sys.__stdout__ is not None:
        sys.__stdout__.flush()
    return exitCode

#
# main
#

def main():
    exitCode = invoke(sys.argv[1:],
        executable=sys.argv[0], errors=None, origin=True)
    sys.exit(exitCode)

if __name__ == '__main__': main()
