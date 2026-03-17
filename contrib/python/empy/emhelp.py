#!/usr/bin/env python3

"""
Help subsystem for EmPy.
"""

#
# imports
#

import sys

import em

#
# Entry ...
#

class Entry(em.Root):

    """An entry in one of the usage tables.  This exists to allow
    optional annotations or processing for each entry."""

    def __init__(self, raw, right,
                 var=None, val=None, env=None, arg=None,
                 ord=None, ex=None, fun=None):
        """Create an entry for a help topic.  Arguments are:

        - raw: The left side raw value: str | list[str]
        - right: The right side base value: str
        - var: The configuration variable, if any: Optional[str]
        - val: The boolean value of the variable, if any: Optional[bool]
        - env: The environment variable name, if any: Optional[str]
        - arg: The command line option arguments, if any: Optional[str]
        - ord: The Unicode code point value(s), if any: Optional[int |
          list[int]]
        - ex: A practical example, if any: Optional[str]
        - fun: An optional function to format the resulting string:
          Optional[Callable]
        - sim: Whether or not the option entry is simple (-x): bool
        """
        self.raw = raw
        self.left = self.format(raw)
        self.right = right
        self.var = var
        self.val = val
        self.env = env
        self.arg = arg
        if isinstance(ord, int):
            ord = [ord]
        self.ords = ord
        self.ex = ex
        self.fun = fun
        if isinstance(self.raw, list):
            self.sim = self.raw[0] != '  '
        else:
            self.sim = False

    def __str__(self):
        if self.ords is None:
            ords = '--'
        else:
            ords = ' '.join([hex(x) for x in self.ords])
        return '%s [%s]' % (self.left, ords)

    def key(self):
        return (self.ords, self.left)

    def format(self, raw):
        """Format the raw list.  Override in subclasses."""
        if isinstance(raw, list):
            return ' '.join(raw)
        else:
            return raw


class NameEntry(Entry):

    """An entry tailored to a name."""

    def key(self):
        return self.left


class OptionEntry(Entry):

    """An entry tailored to an option."""

    def format(self, raw):
        if isinstance(raw, list):
            assert len(raw) > 0
            if raw[0].startswith('--'):
                raw.insert(0, '  ')
            return ' '.join(raw)
        else:
            return raw


class ControlEntry(Entry):

    """An entry tailored to a control (named escape)."""

    def key(self):
        name = self.left
        if name.isdigit():
            name = '_' + name
        return (self.ords, name)

#
# Section ...
#

class Section(em.Root):

    """A section in one of the usage tables."""

    def __init__(self, topic, description, header):
        self.topic = topic
        self.description = description
        self.header = header

    def __str__(self):
        return self.topic

    def ok(self):
        return True

    def show(self, usage, useSeparator=True):
        if not self.ok():
            return
        if useSeparator:
            usage.separator()
        usage.write("%s:\n" % self.topic.upper())
        usage.write(self.header)
        self.block(usage)

    def block(self, usage):
        pass


class TableSection(Section):

    """A section with a block that consists of a flat table of
    entries."""

    def __init__(self, topic, description, header, entries):
        super(TableSection, self).__init__(topic, description, header)
        self.entries = entries

    def ok(self):
        return bool(self.entries)

    def block(self, usage):
        usage.table(self.entries)


class ConfigSection(TableSection):

    """A section with a block that consists of each configuration
    variable."""

    def __init__(self, topic, description, header, config):
        super(ConfigSection, self).__init__(topic, description, header, [])
        for left in config._names:
            right = config._descriptions[left]
            fun = config._functions[left]
            entry = Entry(left, right, var=left, fun=fun)
            self.entries.append(entry)


class MappingSection(TableSection):

    """A section with a block that consists of a mapping type which is
    transformed into a table of entries."""

    def __init__(self, topic, description, header, mapping, factory=Entry):
        super(MappingSection, self).__init__(topic, description, header, [])
        for key, value in mapping.items():
            if value is None:
                continue
            # Value can be an ordinal, a string, or a 2-tuple of (ordinal,
            # name).
            if isinstance(value, tuple):
                ordinal, right = value
            else:
                ordinal = None
                right = value
            if isinstance(ordinal, (em.bytes, em.str)):
                ordinal = ord(ordinal)
            if isinstance(right, int):
                right = em.chr(right)
            elif isinstance(right, list):
                right = ''.join([em.chr(x) for x in right])
            entry = factory(key, right, ord=ordinal)
            self.entries.append(entry)
        self.entries.sort(key=lambda x: x.key())


class HelpSection(TableSection):

    """A section with a block that consists of the help topics."""

    def __init__(self, topic, description, header, usage):
        super(HelpSection, self).__init__(topic, description, header, [])
        self.usage = usage

    def refresh(self):
        """Refresh the list of topics.  At creation time it won't
        contain itself."""
        payload = self.usage.payload
        all = Usage.aliases['all']
        for topic in all:
            if topic in payload:
                section = payload[topic]
                if section.ok():
                    self.entries.append(
                        Entry(section.topic, section.description))

#
# Usage
#

class Usage(em.Root):

    """A utility class to print usage and extended help."""

    aliases = {
        # Groups
        'default': ['usage', 'options', 'markup', 'hints', 'topics'],
        'more': ['usage', 'options', 'markup', 'escapes', 'environ', 'hints',
                 'topics'],
        'all': ['usage', 'options', 'simple',
                'markup', 'escapes', 'environ', 'pseudo',
                'constructor', 'variables', 'methods', 'hooks', 'named',
                'diacritics', 'icons', 'emojis', 'hints', 'topics'],
        # Aliases
        'accents': ['diacritics'],
        'config': ['variables', 'methods'],
        'control': ['named'],
        'controls': ['named'],
        'ctor': ['constructor'],
        'diacritic': ['diacritics'],
        'emoji': ['emojis'],
        'escape': ['escapes'],
        'flags': ['options'],
        'hint': ['hints'],
        'hook': ['hooks'],
        'icon': ['icons'],
        'markups': ['markup'],
        'method': ['methods'],
        'option': ['options'],
        'optional': ['emojis'],
        'switches': ['options'],
        'topic': ['topics'],
        'variable': ['variables'],
    }

    defaultWidth = 6
    columns = None # 81
    dottedCircle = '\N{DOTTED CIRCLE}'

    def __init__(self, config=None, file=None):
        if config is None:
            config = em.Configuration()
        self.config = config
        if file is None:
            file = sys.stdout
        self.file = file
        self.payload = {}
        prepare(self)
        self.fix()

    def format(self, width):
        return '  %%-%ds  %%s\n' % max(width, Usage.defaultWidth)

    def transform(self, topics):
        """Transform a list of possible topics into a list of valid
        topics."""
        if topics is None:
            topics = 'default'
        results = []
        if not isinstance(topics, list):
            topics = [topics]
        for topic in topics:
            topic = topic.lower()
            if topic in Usage.aliases:
                results.extend(Usage.aliases[topic])
            else:
                results.append(topic)
        return results

    def add(self, section):
        """Add a section to the payload."""
        assert section.topic not in self.payload
        self.payload[section.topic] = section

    def fix(self):
        """Fix the payload."""
        self.payload['topics'].refresh()

    def separator(self):
        """Write a separator."""
        self.write("\n")

    def write(self, string):
        """Write a string (with the correct prefix substitution)."""
        if not self.config.hasDefaultPrefix():
            string = string.replace(self.config.defaultPrefix,
                                    self.config.prefix)
        self.file.write(string)

    def flush(self):
        """Flush the underlying stream."""
        self.file.flush()

    def scan(self, table):
        """Scan a table to find the minimum column width."""
        result = 0
        for entry in table:
            left = entry.left
            if len(left) > result:
                result = len(left)
        return result

    def preview(self, ords):
        """Return a preview of the (non-text) character ordinals,
        or None."""
        if ords is None:
            return
        if em.major < 3 or not sys.stdout.encoding.lower().startswith('utf'):
            # Don't bother if this isn't Python 3.x or the encoding isn't
            # UTF-8.
            return
        firstOrd = ords[0]
        chars = ''.join([chr(x) for x in ords])
        while not chars.isprintable():
            # Some emoji blocks report as unprintable in various versions of
            # Python.  (This is implemented as a while loop with breaks ending
            # in a return to make it easier to edit.)
            if firstOrd >= 0x2300 and firstOrd < 0x2400:
                break
            if firstOrd >= 0x2600 and firstOrd < 0x2c00:
                break
            if firstOrd >= 0x1f000 and firstOrd < 0x1fb00:
                break
            return
        if chars.isspace():
            return
        if firstOrd == 0x034f:
            # Combining grapheme joiner.
            return
        if firstOrd >= 0xfe00 and firstOrd < 0xfe10:
            # Variation selectors 1-16.
            return
        if firstOrd == 0xfffc:
            # Object replacement character.
            return
        if firstOrd >= 0xe0100 and firstOrd < 0xe01f0:
            # Variation selectors 17-256.
            return
        if firstOrd >= 0x0300 and firstOrd < 0x0370:
            # Combining diacritical marks.
            chars = self.dottedCircle + chars
        if firstOrd >= 0x1ab0 and firstOrd < 0x1b00:
            # Combining diacritical marks extended.
            chars = self.dottedCircle + chars
        if firstOrd >= 0x1dc0 and firstOrd < 0x1e00:
            # Combining diacritical marks supplemental.
            chars = self.dottedCircle + chars
        if firstOrd >= 0x20d0 and firstOrd < 0x2100:
            # Combining diacritical marks for symbols.
            chars = self.dottedCircle + chars
        return chars

    def entry(self, entry, format):
        """Print a table entry."""
        def _identity(x): return x
        left = entry.left
        lines = entry.right.split('\n')
        fun = entry.fun or _identity
        i = 0
        for line in lines:
            last = i == len(lines) - 1
            fragments = []
            if entry.ords is not None:
                first = True
                for x in entry.ords:
                    if first:
                        first = False
                    else:
                        fragments.append(' ')
                    fragments.append("U+%04X" % x)
                fragments.append("; ")
            fragments.append(line)
            if last:
                if entry.arg is not None:
                    fragments.append(": %s" % fun(entry.arg))
                if entry.var is not None and entry.val is None:
                    value = fun(getattr(self.config, entry.var))
                    if isinstance(value, dict):
                        value = "{%d}" % len(value)
                    fragments.append(" [%s]" % str(value))
                if entry.ex is not None:
                    fragments.append(" (e.g., %s)" % fun(entry.ex))
                preview = self.preview(entry.ords)
                if preview:
                    fragments.append(': ')
                    fragments.append(preview)
            right = ''.join(fragments).strip()
            line = format % (left, right)
            assert self.columns is None or len(line) < self.columns, line
            self.write(line)
            left = ''
            i += 1

    def table(self, table):
        """Print a table at the current level with an optional header."""
        width = self.scan(table)
        format = self.format(width)
        for entry in table:
            self.entry(entry, format)

    def hello(self):
        self.write("Welcome to %s version %s.\n" % (
            em.__project__, em.__version__))

    def show(self, topics=None, useSeparator=True):
        """Show usage."""
        if topics is None:
            topics = 'default'
        topics = self.transform(topics)
        flag = False
        for topic in topics:
            topic = topic.lower()
            if topic in self.payload:
                section = self.payload[topic]
                if section.ok():
                    section.show(self, useSeparator)
                    flag = True
            else:
                self.write("*** Unknown usage topic: %s\n" % topic)
        if not flag:
            self.write("*** No active topics found\n")

#
# functions
#

def prepare(usage, executable=None):
    """Lazily initialize the usage data and tables only if they're
    actually needed.  This is a standalone function so there's less
    misleading indentation.  Idempotent."""
    if executable is None:
        executable = sys.argv[0]
    payload = usage.payload
    if payload:
        return payload
    E = Entry
    OE = OptionEntry

    usage.add(Section(
        'usage',
        "Basic command line usage",
        """\
%s [<options>] [<filename, or `-` for stdin> [<argument>...]]
  - Options begin with `-` or `--`
  - Specify a filename (and arguments) to process that document as input
  - Specify `-` (and arguments) to process stdin with standard buffering
  - Specify no filename to enter interactive mode with line buffering
  - Specify `--` to stop processing options
""" % executable))

    usage.add(Section(
        'hints',
        "Usage hints",
        """\
Whitespace immediately inside parentheses of `@(...)` are ignored.  Whitespace
immediately inside braces of `@{...}` are ignored, unless ... spans multiple
lines.  Use `@{ ... }@` to suppress newline following second `@`.  Simple
expressions ignore trailing punctuation; `@x.` means `@(x).`, not a parse
error.  A `#!` at the start of a file is treated as a comment.  The full
documentation is available at <%s>.
""" % em.__url__))

    usage.add(TableSection(
        'options',
        "Command line options",
        """\
Valid command line options (defaults in brackets):
""", [
OE(["-V", "--version"], "Print version"),
OE(["-W", "--info"], "Print version and system information"),
OE(["-Z", "--details"], "Print extensive system and platform details"),
OE(["-h", "--help"], "Print help; more -h options for more help"),
OE(["-H", "--topics=TOPICS"], "Print usage for comma-separated topics"),
OE(["-v", "--verbose"], "Show verbose debugging while processing"),
OE(["-p", "--prefix=CHAR"], "Choose desired prefix", var='prefix', env=em.PREFIX_ENV),
OE(["--no-prefix"], "Do not do any markup processing at all"),
OE(["-q", "--no-output"], "Suppress all output"),
OE(["-m", "--pseudomodule=NAME"], "Change internal pseudomodule name", var='pseudomoduleName', env=em.PSEUDO_ENV),
OE(["-f", "--flatten"], "Flatten pseudomodule members at start", var='doFlatten', env=em.FLATTEN_ENV, val=True),
OE(["-k", "--keep-going"], "Don't exit on errors; continue", var='exitOnError', val=False),
OE(["-e", "--ignore-errors"], "Ignore errors completely", var='ignoreErrors', val=True),
OE(["-r", "--raw-errors"], "Show full tracebacks of Python errors", var='rawErrors', env=em.RAW_ERRORS_ENV, val=True),
OE(["-s", "--brief-errors"], "Don't show attributes when formatting errors", var='verboseErrors', val=False),
OE(["--verbose-errors"], "Show attributes when formatting errors (default)", var='verboseErrors', val=True),
OE(["-i", "--interactive"], "Enter interactive mode after processing", var='goInteractive', env=em.INTERACTIVE_ENV, val=True),
OE(["-d", "--delete-on-error"], "Delete output file on error\n"
                                "(-o or -a needed)", var='deleteOnError', env=em.DELETE_ON_ERROR_ENV),
OE(["-n", "--no-proxy"], "Do not override sys.stdout with proxy", var='useProxy', env=em.NO_PROXY_ENV),
OE(["--config=STATEMENTS"], "Do configuration variable assignments"),
OE(["-c", "--config-file=FILENAME"], "Load configuration resource path(s)", env=em.CONFIG_ENV),
OE(["--config-variable=NAME"], "Configuration variable name while loading", var='configVariableName'),
OE(["-C", "--ignore-missing-config"], "Don't raise for missing configuration files", var='missingConfigIsError', val=False),
OE(["-o", "--output=FILENAME"], "Specify file for output as write"),
OE(["-a", "--append=FILENAME"], "Specify file for output as append"),
OE(["-O", "--output-binary=FILENAME"], "Specify file for binary output as write"),
OE(["-A", "--append-binary=FILENAME"], "Specify file for binary output as append"),
OE(["--output-mode=MODE"], "Explicitly specify the mode for output"),
OE(["--input-mode=MODE"], "Explicitly specify the mode for input"),
OE(["-b", "--buffering"], "Specify buffering strategy for files:\n"
                          "none (= 0), line (= 1), full (= -1), default,\n"
                          "or another value for fixed", var='buffering', env=em.BUFFERING_ENV),
OE(["--default-buffering"], "Specify default buffering for files (default)"),
OE(["-N", "--no-buffering"], "Specify no buffering for reads on files"),
OE(["-L", "--line-buffering"], "Specify line buffering for files"),
OE(["-B", "--full-buffering"], "Specify full buffering for files"),
OE(["-I", "--import=MODULES"], "Import Python modules before processing"),
OE(["-D", "--define=DEFN"], "Execute Python assignment statement"),
OE(["-S", "--string=STR"], "Execute string literal assignment"),
OE(["-P", "--preprocess=FILENAME"], "Interpret EmPy file before main document"),
OE(["-Q", "--postprocess=FILENAME"], "Interpret EmPy file after main document"),
OE(["-E", "--execute=STATEMENT"], "Execute Python statement before main document"),
OE(["-K", "--postexecute=STATEMENT"], "Execute Python statement after main document"),
OE(["-F", "--file=FILENAME"], "Execute Python file before main document"),
OE(["-G", "--postfile=FILENAME"], "Execute Python file after main document"),
OE(["-X", "--expand=MARKUP"], "Expand EmPy markup before main document"),
OE(["-Y", "--postexpand=MARKUP"], "Expand EmPy markup after main document"),
OE(["--preinitializer=FILENAME"], "Execute Python file before interpreter"),
OE(["--postinitializer=FILENAME"], "Execute Python file after interpreter"),
OE(["-w", "--pause-at-end"], "Prompt at the end of processing", var='pauseAtEnd'),
OE(["-l", "--relative-path"], "Add path of EmPy script to sys.path", var='relativePath'),
OE(["--replace-newlines"], "Replace expression newlines with spaces (default)", var='replaceNewlines', val=True),
OE(["--no-replace-newlines"], "Don't replace expression newlines with spaces", var='replaceNewlines', val=False),
OE(["--ignore-bangpaths"], "Treat bangpaths as comments (default)", var='ignoreBangpaths', val=True),
OE(["--no-ignore-bangpaths"], "Don't treat bangpaths as comments", var='ignoreBangpaths', val=False),
OE(["--expand-user"], "Expand user constructions (~user) in paths (default)", var='expandUserConstructions', val=True),
OE(["--no-expand-user"], "Don't expand user constructions in paths", var='expandUserConstructions', val=False),
OE(["--auto-validate-icons"], "Auto-validate icons on first use (default)", var='autoValidateIcons', val=True),
OE(["--no-auto-validate-icons"], "Don't auto-validate icons on first use", var='autoValidateIcons', val=False),
OE(["--none-symbol"], "String to write when expanding None", var='noneSymbol'),
OE(["--no-none-symbol"], "Write nothing when expanding None (default)"),
OE(["--starting-line"], "Line number to start with", var='startingLine'),
OE(["--starting-column"], "Column number to start with", var='startingColumn'),
OE(["--emoji-modules"], "Comma-separated list of emoji modules to try\n", var='emojiModuleNames', fun=lambda x: ','.join(x)),
OE(["--no-emoji-modules"], "Only use unicodedata for emoji markup"),
OE(["--disable-emoji-modules"], "Disable emoji modules; use emojis dictionary"),
OE(["--ignore-emoji-not-found"], "Emoji not found is not an error", var='emojiNotFoundIsError', val=False),
OE(["-u", "--binary", "--unicode"], "Read input file as binary?\n"
                                    "(enables Unicode support in Python 2.x)", var='useBinary', env=em.BINARY_ENV),
OE(["-x", "--encoding=E"], "Set both input and output Unicode encodings"),
OE(["--input-encoding=E"], "Set input Unicode encoding", var='inputEncoding', env=em.INPUT_ENCODING_ENV, fun=lambda x: x == 'utf_8' and 'utf-8' or x),
OE(["--output-encoding=E"], "Set output Unicode encoding", var='outputEncoding', env=em.OUTPUT_ENCODING_ENV, fun=lambda x: x == 'utf_8' and 'utf-8' or x),
OE(["-y", "--errors=E"], "Set both input and output Unicode error handler"),
OE(["--input-errors=E"], "Set input Unicode error handler", var='inputErrors', env=em.INPUT_ERRORS_ENV),
OE(["--output-errors=E"], "Set output Unicode error handler", var='outputErrors', env=em.OUTPUT_ERRORS_ENV),
OE(["-z", "--normalization-form=F"], "Specify Unicode normalization form", var='normalizationForm'),
OE(["--auto-play-diversions"], "Autoplay diversions on exit (default)", var='autoPlayDiversions', val=True),
OE(["--no-auto-play-diversions"], "Don't autoplay diversions on exit", var='autoPlayDiversions', val=False),
OE(["--check-variables"], "Validate configuration variables (default)", var='checkVariables', val=True),
OE(["--no-check-variables"], "Don't validate configuration variables", var='checkVariables', val=False),
OE(["--path-separator"], "Path separator for configuration paths", var='pathSeparator'),
OE(["--enable-modules"], "Enable EmPy module support (default)", var='supportModules', val=True),
OE(["-g", "--disable-modules"], "Disable EmPy module support", var='supportModules', val=False),
OE(["--module-extension"], "Filename extension for EmPy modules", var='moduleExtension'),
OE(["--module-finder-index"], "Index of module finder in meta path", var='moduleFinderIndex'),
OE(["--enable-import-output"], "Enable output during import (default)", var='enableImportOutput', val=True),
OE(["-j", "--disable-import-output"], "Disable output during import", var='enableImportOutput', val=False),
OE(["--context-format"], "Context format", var='contextFormat'),
OE(["--success-code=N"], "Exit code to return on script success", var='successCode'),
OE(["--failure-code=N"], "Exit code to return on script failure", var='failureCode'),
OE(["--unknown-code=N"], "Exit code to return on bad configuration", var='unknownCode'),
]))

    usage.add(TableSection(
        'simple',
        "Simple (one-letter) command line options",
        """\
Short (single letter) command line options:
""",
        sorted([x for x in usage.payload['options'].entries if x.sim],
                           key=lambda x: x.raw[0].swapcase())))

    usage.add(TableSection(
        'markup',
        "Markup syntax",
        """\
The following markups are supported (NL means newline; WS means whitespace):
""", [
E("@# ... NL", "Line comment; consume everything including newline"),
E("@* ... *", "Inline comment; consume everything inside"),
E("@ WS", "Ignore whitespace; line continuation"),
E("@- ... NL", "Disable output; consume up to trailing newline"),
E("@+ ... NL", "Enable output; consume up to trailing newline"),
E("@@", "Literal @; @ is escaped (duplicated prefix)"),
E("@' STRING '", "Replace with string literal contents"),
E("@\" STRING \"", "Replace with string literal contents"),
E("@''' STRING ... '''", "Replace with multiline string literal contents"),
E("@\"\"\" STRING ... \"\"\"", "Replace with multiline string literal contents"),
E("@` LITERAL `", "Write everything inside literally (no expansion)"),
E("@( EXPRESSION )", "Evaluate expression and substitute with str"),
E("@( TEST ? THEN )", "If test, evaluate then, otherwise do nothing"),
E("@( TEST ? THEN ! ELSE )", "If test, evaluate then, otherwise evaluate else;\n"
                             "can be chained with repeated test/then/[else]"),
E("@( TRY $ EXCEPT )", "Evaluate try expression, or except if it raises"),
E("@ SIMPLE_EXPRESSION", "Evaluate simple expression and substitute\n", ex="@x, @x.y, @f(a, b), @l[i], @f{...}, etc."),
E("@$ EXPRESSION $ [DUMMY] $", "Evaluates to @$ EXPRESSION $ EXPANSION $"),
E("@{ STATEMENTS }", "Statements are executed for side effects"),
E("@[ CONTROL ]", "Control markups: if E; elif E; for N in E;\n"
                  "while E; dowhile E; try; except E as N; finally;\n"
                  "continue; break; else; with E as N; match E;\n"
                  "case E; defined N; def F(...); end X"),
E("@\\ ESCAPE_CODE", "A C-style escape sequence"),
E("@\\^{ NAMED_ESCAPE }", "A named escape sequence", ex="ESC for escape"),
E("@^ CHAR DIACRITIC(S)", "A two-part diacritic sequence\n", ex="e' for an e with acute accent"),
E("@| ICON", "A custom icon sequence\n", ex=":) for a smiley face emoji"),
E("@: EMOJI :", "Lookup emoji by name"),
E("@% KEY NL", "Significator form of __KEY__ = None"),
E("@% KEY WS VALUE NL", "Significator form of __KEY__ = VALUE"),
E("@%! KEY NL", "Significator form of __KEY__ = \"\""),
E("@%! KEY WS STRING NL", "Stringized significator form: __KEY__ = \"STRING\""),
E("@%% KEY WS VALUE %% NL", "Multiline significator form"),
E("@%%! KEY WS STRING %% NL", "Multiline stringized significator form"),
E("@? NAME NL", "Set the current context name"),
E("@! INTEGER NL", "Set the current context line number"),
E("@(( ... )), @[[ ... ]],", "Extension markup; implementation provided by user"),
E("@{{ ... }}, @< ... >", ""),
]))

    usage.add(TableSection(
        'escapes',
        "Escape sequences",
        """\
Valid escape sequences are:
""", [
E("@\\0", "NUL, null", ord=0x0),
E("@\\a", "BEL, bell", ord=0x7),
E("@\\b", "BS, backspace", ord=0x8),
E("@\\B{BIN}", "freeform binary code BIN", ex="{1000001} for A"),
E("@\\dDDD", "three-digit decimal code DDD", ex="065 for A"),
E("@\\D{DEC}", "freeform decimal code DEC", ex="{65} for A"),
E("@\\e", "ESC, escape", ord=0x1b),
E("@\\f", "FF, form feed", ord=0xc),
E("@\\h", "DEL, delete", ord=0x7f),
E("@\\k", "ACK, acknowledge", ord=0x6),
E("@\\K", "NAK, negative acknowledge", ord=0x15),
E("@\\n", "LF, linefeed; newline", ord=0xa),
E("@\\N{NAME}", "Unicode character named NAME", ex="LATIN CAPITAL LETTER A"),
E("@\\oOOO", "three-digit octal code OOO", ex="101 for A"),
E("@\\O{OCT}", "freeform octal code OCT", ex="{101} for A"),
E("@\\qQQQQ", "four-digit quaternary code QQQQ", ex="1001 for A"),
E("@\\Q{QUA}", "freeform quaternary code QUA", ex="{1001} for A"),
E("@\\r", "CR, carriage return", ord=0xd),
E("@\\s", "SP, space", ord=0x20),
E("@\\S", "NBSP, no-break space", ord=0xa0),
E("@\\t", "HT, horizontal tab", ord=0x9),
E("@\\uHHHH", "16-bit hexadecimal Unicode HHHH", ex="0041 for A"),
E("@\\UHHHHHHHH", "32-bit hexadecimal Unicode HHHHHHHH", ex="00000041 for A"),
E("@\\v", "VT, vertical tab", ord=0xb),
E("@\\V{VS}", "VS, variation selector (1 .. 256)", ex="16 for emoji display"),
E("@\\w", "VS15, variation selector 15; text display", ord=0xfe0e),
E("@\\W", "VS16, variation selector 16; emoji display", ord=0xfe0f),
E("@\\xHH", "8-bit hexadecimal code HH", ex="41 for A"),
E("@\\X{HEX}", "freeform hexadecimal code HEX", ex="{41} for A"),
E("@\\y", "SUB, substitution", ord=0x1a),
E("@\\Y", "RC, replacement character", ord=0xfffd),
E("@\\z", "EOT, end of transmission", ord=0x4),
E("@\\Z", "ZWNBSP/BOM, zero-width no-break space/byte order mark", ord=0xfeff),
E("@\\,", "THSP, thin space", ord=0x2009),
E("@\\^C", "Control character C", ex="[ for ESC"),
E("@\\^{NAME}", "Escape named NAME (control character)", ex="ESC"),
E("@\\(", "Literal (", ord=0x28),
E("@\\)", "Literal )", ord=0x29),
E("@\\[", "Literal [", ord=0x5b),
E("@\\]", "Literal ]", ord=0x5d),
E("@\\{", "Literal {", ord=0x7b),
E("@\\}", "Literal }", ord=0x7d),
E("@\\<", "Literal <", ord=0x3c),
E("@\\>", "Literal >", ord=0x3e),
E("@\\\\", "Literal \\", ord=0x5c),
E("@\\'", "Literal '", ord=0x27),
E("@\\\"", "Literal \"", ord=0x22),
E("@\\?", "Literal ?", ord=0x3f),
]))

    usage.add(TableSection(
        'environ',
        "Environment variables",
        """\
The following environment variables are recognized (with corresponding
command line arguments):
""", [
E(em.OPTIONS_ENV, "Specify additional options to be included"),
E(em.CONFIG_ENV, "Specify configuration file(s) to load", arg='-c PATHS'),
E(em.PREFIX_ENV, "Specify the default prefix", arg='-p PREFIX'),
E(em.PSEUDO_ENV, "Specify name of pseudomodule", arg='-m NAME'),
E(em.FLATTEN_ENV, "Flatten empy pseudomodule if defined", arg='-f'),
E(em.RAW_ERRORS_ENV, "Show raw errors if defined", arg='-r'),
E(em.INTERACTIVE_ENV, "Enter interactive mode if defined", arg='-i'),
E(em.DELETE_ON_ERROR_ENV, "Delete output file on error", arg='-d'),
E(em.NO_PROXY_ENV, "Do not install sys.stdout proxy if defined", arg='-n'),
E(em.BUFFERING_ENV, "Buffer size (-1, 0, 1, or n)", arg='-b VALUE'),
E(em.BINARY_ENV, "Open input file as binary (for Python 2.x Unicode)", arg='-u'),
E(em.ENCODING_ENV, "Unicode both input and output encodings"),
E(em.INPUT_ENCODING_ENV, "Unicode input encoding"),
E(em.OUTPUT_ENCODING_ENV, "Unicode output encoding"),
E(em.ERRORS_ENV, "Unicode both input and output error handlers"),
E(em.INPUT_ERRORS_ENV, "Unicode input error handler"),
E(em.OUTPUT_ERRORS_ENV, "Unicode output error handler"),
]))

    usage.add(TableSection(
        'pseudo',
        "Pseudomodule attributes and functions",
        """\
The %s pseudomodule contains the following attributes and methods:
""" % usage.config.pseudomoduleName, [
E("version", "String representing EmPy version"),
E("compat", "List of applied Python compatibility features"),
E("executable", "The EmPy executable"),
E("argv", "EmPy script name and command line arguments"),
E("config", "The configuration for this interpreter"),
E("ok", "Is the interpreter still active?"),
E("error", "The last error to occur, or None"),
E("__init__(**kwargs)", "The interpreter constructor"),
E("__enter__/__exit__(...)", "Context manager support"),
E("reset()", "Reset the interpreter state"),
E("ready()", "Declare the interpreter ready"),
E("shutdown()", "Shutdown the interpreter"),
E("write(data)", "Write data to stream"),
E("writelines(lines)", "Write a sequence of lines to stream"),
E("flush()", "Flush the stream"),
E("serialize(object)", "Write a string version of the object"),
E("top() -> Stream", "Get the top-level stream"),
E("push()", "Push this interpreter on the stream stack"),
E("pop()", "Pop this interpreter off the stream stack"),
E("clear()", "Clear the interpreter stacks"),
E("go(fname, mode, [pre, [post]])", "Process a file by filename"),
E("interact()", "Enter interactive mode"),
E("file(file, [loc])", "Process a file object"),
E("fileLine(file, [loc])", "Process a file object in lines"),
E("fileChunk(file, buf, [loc]])", "Process a file in chunks"),
E("fileFull(file, [loc]])", "Process a file as a single chunk"),
E("process(command)", "Process a command"),
E("processAll(commands)", "Process a sequence of commands"),
E("identify()", "Identify top context as name, line"),
E("getContext()", "Return the current context"),
E("newContext(...)", "Return a new context with name and counts"),
E("pushContext(context)", "Push a context"),
E("popContext()", "Pop the current context"),
E("setContext(context)", "Replace the current context"),
E("setContextName(name)", "Set the name of the current context"),
E("setContextLine(line)", "Set the line number of the current context"),
E("setContextColumn(column)", "Set the column number of the current context"),
E("setContextData(...)", "Set any of the name, line, column number"),
E("restoreContext(context)", "Replace the top context with an existing one"),
E("clearFinalizers()", "Clear all finalizers"),
E("appendFinalizer(finalizer)", "Append function to be called at shutdown"),
E("prependFinalizer(finalizer)", "Prepend function to be called at shutdown"),
E("setFinalizers(finalizers)", "Set functions to be called at shutdown"),
E("getGlobals()", "Retrieve this interpreter's globals"),
E("setGlobals(dict)", "Set this interpreter's globals"),
E("updateGlobals(dict)", "Merge dictionary into interpreter's globals"),
E("clearGlobals()", "Start globals over anew"),
E("saveGlobals([deep])", "Save a copy of the globals"),
E("restoreGlobals([pop])", "Restore the most recently saved globals"),
E("include(file, [loc])", "Include filename or file-like object"),
E("expand(string, [loc])", "Explicitly expand string and return"),
E("defined(name, [loc])", "Find if the name is defined"),
E("lookup(name, [loc])", "Lookup the variable name"),
E("evaluate(expression, [loc])", "Evaluate the expression (and write?)"),
E("execute(statements, [loc])", "Execute the statements"),
E("single(source, [loc])", "Execute the expression or statement"),
E("atomic(name, value, [loc])", "Perform an atomic assignment"),
E("assign(name, value, [loc])", "Perform an arbitrary assignment"),
E("significate(key, [value])", "Significate the given key, value pair"),
E("quote(string)", "Quote prefixes in provided string and return"),
E("escape(data)", "Escape EmPy markup in data and return"),
E("flatten([keys])", "Flatten module contents to globals namespace"),
E("getPrefix()", "Get current prefix"),
E("setPrefix(char)", "Set new prefix"),
E("stopDiverting()", "Stop diverting; data sent directly to output"),
E("createDiversion(name)", "Create a diversion but do not divert to it"),
E("retrieveDiversion(name, [def])", "Retrieve the actual named diversion object"),
E("startDiversion(name)", "Start diverting to given diversion"),
E("playDiversion(name)", "Recall diversion and then eliminate it"),
E("replayDiversion(name)", "Recall diversion but retain it"),
E("dropDiversion(name)", "Drop diversion"),
E("playAllDiversions()", "Stop diverting and play all diversions"),
E("replayAllDiversions()", "Stop diverting and replay all diversions"),
E("dropAllDiversions()", "Stop diverting and drop all diversions"),
E("getCurrentDiversionName()", "Get the name of the current diversion"),
E("getAllDiversionNames()", "Get a sorted sequence of diversion names"),
E("isExistingDiversionName(name)", "Is this the name of a diversion?"),
E("getFilter()", "Get the first filter in the current chain"),
E("getLastFilter()", "Get the last filter in the current chain"),
E("getFilterCount()", "Get the number of filters in current chain"),
E("resetFilter()", "Reset filter; no filtering"),
E("setFilter(filter...)", "Install new filter(s), replacing any chain"),
E("prependFilter(filter)", "Prepend filter to beginning of current chain"),
E("appendFilter(filter)", "Append a filter to end of current chain"),
E("setFilterChain(filters)", "Install a new filter chain"),
E("areHooksEnabled()", "Return whether or not hooks are enabled"),
E("enableHooks()", "Enable hooks (default)"),
E("disableHooks()", "Disable hook invocation"),
E("getHooks()", "Get all the hooks"),
E("appendHook(hook)", "Append the given hook"),
E("prependHook(hook)", "Prepend the given hook"),
E("removeHook(hook)", "Remove an already-registered hook"),
E("clearHooks()", "Clear all hooks"),
E("invokeHook(_name, ...)", "Manually invoke hook"),
E("hasCallback()", "Is there a custom callback?"),
E("getCallback()", "Get custom callback"),
E("registerCallback(callback)", "Register custom callback"),
E("deregisterCallback()", "Deregister custom callback"),
E("invokeCallback(contents)", "Invoke the custom callback directly"),
E("defaultHandler(t, e, tb)", "The default error handler"),
E("getHandler()", "Get the current error handler (or None)"),
E("setHandler(handler, [eoe])", "Set the error handler"),
E("resetHandler([eoe])", "Reset the error handler"),
E("invokeHandler(t, e, tb)", "Manually invoke the error handler"),
E("initializeEmojiModules(names)", "Initialize the emoji modules"),
E("getEmojiModule(name)", "Get an abstracted emoji module"),
E("getEmojiModuleNames()", "Return the list of emoji module names"),
E("substituteEmoji(text)", "Do an emoji substitution"),
]))

    usage.add(TableSection(
        'constructor',
        "Keyword arguments for the Interpreter constructor",
        """\
The following keyword arguments for the Interpreter constructor are supported
(defaults in brackets).  All arguments have meaningful defaults:
""", [
E("argv", "The system arguments to use ['<->']"),
E("callback", "A custom callback to register [None]"),
E("config", "The configuration object [default]"),
E("core", "The core to use for this interpreter [Core()]"),
E("dispatcher", "Dispatch errors or raise to caller? [True]"),
E("executable", "The path to the EmPy executable [\".../em.py\"]"),
E("extension", "The extension to automatically install [None]"),
E("filespec", "A 3-tuple of the input filename, mode, and buffering [None]"),
E("filters", "The list of filters to install [[]]"),
E("finalizers", "The list of finalizers to installed [[]]"),
E("globals", "The globals dictionary to use [{}]"),
E("handler", "The error handler to use [default]"),
E("hooks", "The list of hooks to install [[]]"),
E("ident", "The identifier for the interpreter [None]"),
E("immediately", "Declare the interpreter ready after initialization? [True]"),
E("input", "The input file to use for interactivity [sys.stdin]"),
E("output", "The output file to use [sys.stdout]"),
E("root", "The root interpreter context filename ['<root>']"),
]))

    usage.add(ConfigSection(
        'variables',
        "Configuration variable attributes",
        """\
The following configuration variable attributes are supported (defaults in
brackets, with dictionaries being summarized with their length):
""",
        usage.config))

    usage.add(TableSection(
        'methods',
        "Configuration methods",
        """\
The configuration instance contains the following methods:
""", [
E("initialize()", "Initialize the instance"),
E("shutdown()", "Shutdown the instance"),
E("isInitialize()", "Is this configuration initialized?"),
E("pauseIfRequired()", "Pause if required"),
E("check(in, out)", "Check file settings"),
E("has(name)", "Is this variable defined?"),
E("get(name[, default])", "Get this variable value"),
E("set(name, value)", "Set this variable"),
E("update(**kwargs)", "Update with dictionary"),
E("clone([deep])", "Clone (optionally deep) this configuration"),
E("run(statements)", "Run configuration commands"),
E("load(filename, [required])", "Load configuration file"),
E("path(filename, [required])", "Load configuration file(s) path"),
E("hasEnvironment(name)", "Is this environment variable defined?"),
E("environment(name, ...)", "Get the enviroment variable value"),
E("hasDefaultPrefix()", "Is the prefix the default?"),
E("hasFullBuffering()", "Is the buffering set to full?"),
E("hasNoBuffering()", "Is the buffering set to none?"),
E("hasLineBuffering()", "Is the buffering set to line?"),
E("hasFixedBuffering()", "Is the buffering set to fixed?"),
E("createFactory([tokens])", "Create token factory"),
E("adjustFactory()", "Adjust token factory for non-default prefix"),
E("getFactory([tokens], [force])", "Get a token factory"),
E("resetFactory()", "Clear the current token factory"),
E("hasBinary()", "Is binary (Unicode) support enabled?"),
E("enableBinary(...)", "Enable binary (Unicode) support"),
E("disableBinary()", "Disable binary (Unicode) support"),
E("isDefaultEncodingErrors()", "Is encoding/errors the default?"),
E("getDefaultEncoding()", "Get the default file encoding"),
E("open(filename, [mode], ...)", "Open a file"),
E("significatorReString()", "Regular expression string for significators"),
E("significatorRe()", "Regular expression pattern for significators"),
E("significatorFor(key)", "Significator variable name for key"),
E("setContextFormat(format)", "Set the context format"),
E("renderContext(context)", "Render context using format"),
E("calculateIconsSignature()", "Calculate icons signature"),
E("signIcons()", "Sign the icons dictionary"),
E("transmogrifyIcons()", "Process the icons dictionary"),
E("validateIcons()", "Process the icons dictionaray if necessary"),
E("intializeEmojiModules([names])", "Initialize emoji module support"),
E("substituteEmoji(text)", "Substitute emoji"),
E("isSuccessCode(code)", "Does this exit code indicate success?"),
E("isExitError(error)", "Is this exception instance an exit?"),
E("errorToExitCode(error)", "Return exit code for exception instance"),
E("isNotAnError(error)", "Is this exception instance not an error?"),
E("formatError(error[, p, s])", "Render an error string from instance"),
]))

    usage.add(TableSection(
        'hooks',
        "Hook methods",
        """\
The following hook methods are supported.  The return values are ignored except
for the `pre...` methods which, when they return a true value, signal that the
following token handling should be skipped:
""", [
E("atInstallProxy(proxy, new)", "Proxy being installed"),
E("atUninstallProxy(proxy, new)", "Proxy being uninstalled"),
E("atStartup()", "Interpreter started up"),
E("atReady()", "Interpreter ready"),
E("atFinalize()", "Interpreter finalizing"),
E("atShutdown()", "Interpreter shutting down"),
E("atParse(scanner, loc)", "Interpreter parsing"),
E("atToken(token)", "Interpreter expanding token"),
E("atHandle(info, fatal, contexts)", "Interpreter encountered error"),
E("atInteract()", "Interpreter going interactive"),
E("pushContext(context)", "Context being pushed"),
E("popContext(context)", "Context was popped"),
E("setContext(context)", "Context was set or modified"),
E("restoreContext(context)", "Context was restored"),
E("prePrefix()", "Pre prefix token"),
E("preWhitespace()", "Pre whitespace token"),
E("preString(string)", "Pre string literal token"),
E("preLineComment(comment)", "Pre line comment"),
E("postLineComment()", "Post line comment"),
E("preInlineComment(comment)", "Pre inline comment"),
E("postInlineComment()", "Post inline comment"),
E("preBackquote(literal)", "Pre backquote literal"),
E("postBackquote()", "Post backquote literal"),
E("preSignificator(key, value, s)", "Pre significator"),
E("postSignificator()", "post significator"),
E("preContextName(name)", "Pre context name"),
E("postContextName()", "Post context name"),
E("preContextLine(line)", "Pre context line"),
E("postContextLine()", "Post context line"),
E("preExpression(pairs, except, loc)", "Pre expression"),
E("postExpression(result)", "Post expression"),
E("preSimple(code, sub, loc)", "Pre simple expression"),
E("postSimple(result)", "Post simple expression"),
E("preInPlace(code, loc)", "Pre in-place expression"),
E("postInPlace(result)", "Post in-place expression"),
E("preStatement(code, loc)", "Pre statement"),
E("postStatement()", "Post statement"),
E("preControl(type, rest, loc)", "Pre control"),
E("postControl()", "Post control"),
E("preEscape(code)", "Pre escape"),
E("postEscape()", "Post escape"),
E("preDiacritic(code)", "Pre diacritic"),
E("postDiacritic()", "Post diacritic"),
E("preIcon(code)", "Pre icon"),
E("postIcon()", "Post icon"),
E("preEmoji(name)", "Pre emoji"),
E("postEmoji()", "Post emoji"),
E("preCustom(contents)", "Pre custom"),
E("postCustom()", "Post custom"),
E("beforeProcess(command, n)", "Before command processing"),
E("afterProcess()", "After command processing"),
E("beforeInclude(file, loc, name)", "Before file inclusion"),
E("afterInclude()", "After file inclusion"),
E("beforeExpand(string, loc, name)", "Before expand call"),
E("afterExpand(result)", "After expand call"),
E("beforeTokens(tokens, loc)", "Before processing tokens"),
E("afterTokens(result)", "After processing tokens"),
E("beforeFileLines(file, loc)", "Before reading file by lines"),
E("afterFileLines()", "After reading file by lines"),
E("beforeFileChunks(file, loc)", "Before reading file by chunks"),
E("afterFileChunks()", "After reading file by chunks"),
E("beforeFileFull(file, loc)", "Before reading file in full"),
E("afterFilFull()", "After reading file in full"),
E("beforeString(string, loc)", "Before processing string"),
E("afterString()", "After processing string"),
E("beforeQuote(string)", "Before quoting string"),
E("afterQuote()", "After quoting string"),
E("beforeEscape(string)", "Before escaping string"),
E("afterEscape()", "After escaping string"),
E("beforeSignificate(key, value, loc)", "Before significator"),
E("afterSignificate()", "After significator"),
E("beforeCallback(contents)", "Before custom callback"),
E("afterCallback()", "Before custom callback"),
E("beforeAtomic(name, value, loc)", "Before atomic assignment"),
E("afterAtomic()", "After atomic assignment"),
E("beforeMulti(names, values, loc)", "Before complex assignment"),
E("afterMulti()", "After complex assignment"),
E("beforeImport(name, loc)", "Before module import"),
E("afterImport()", "After module import"),
E("beforeFunctional(code, lists, loc)", "Before functional expression"),
E("afterFunctional(result)", "After functional expression"),
E("beforeEvaluate(code, loc, write)", "Before expression evaluation"),
E("afterEvaluate(result)", "After expression evaluation"),
E("beforeExecute(statements, loc)", "Before statement execution"),
E("afterExecute()", "After statement execution"),
E("beforeSingle(source, loc)", "Before single execution"),
E("afterSingle(result)", "After single execution"),
E("beforeFinalizer(final)", "Before finalizer processing"),
E("afterFinalizer()", "After finalizer processing"),
]))

    usage.add(MappingSection(
        'named',
        "Named escapes",
        "The following named escapes (control codes and control-like characters)\n(`@\\^{...}`) are supported:\n",
        usage.config.controls,
        ControlEntry))

    usage.add(MappingSection(
        'diacritics',
        "Diacritic combiners",
        "The following diacritic combining characters (`@^C...`) are supported:\n",
        usage.config.diacritics))

    usage.add(MappingSection(
        'icons',
        "Icons",
        "The following icon sequences (`@|...`) are supported:\n",
        usage.config.icons,
        NameEntry))

    usage.add(MappingSection(
        'emojis',
        "Custom emojis",
        "The following custom emojis have been made available:\n",
        usage.config.emojis,
        NameEntry))

    usage.add(HelpSection(
        'topics',
        "This list of topics",
        """\
Need more help?  Add more -h options (-hh, -hhh) for more help.  Use -H <topic>
for help on a specific topic, or specify a comma-separated list of topics.  Try
`default` (-h) for default help, `more` (-hh) for more common topics, `all`
(-hhh) for all help topics, or `topics` for this list.  Use -V for version
information, -W for version and system information, or -Z for all debug
details.  Available topics:
""",
        usage))

    return payload

#
# main
#

def main():
    topics = sys.argv[1:]
    if not topics:
        topics = ['default']
    usage = Usage()
    usage.show(topics)

if __name__ == '__main__': main()
