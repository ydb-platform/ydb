#!/usr/bin/env python3

"""
EmPy documentation generating system.  This module requires a modern
Python 3.x interpreter.
"""

#
# imports
#

import hashlib
import io
import os
import subprocess
import sys
import time

import em
import emhelp
import emlib

#
# constants
#

SOURCE = '\N{KEYBOARD}\ufe0f'
OUTPUT = '\N{DESKTOP COMPUTER}\ufe0f'

PATHS = ['../..', '.']

EMOJIS = {
    '...': ('\N{HORIZONTAL ELLIPSIS}', "horizontal ellipsis"),
    '<--': ('\N{LONG LEFTWARDS ARROW}', "long leftwards arrow"),
    '-->': ('\N{LONG RIGHTWARDS ARROW}', "long rightwards arrow"),
}

def admonish(text, emoji=None, admonition="important"):
    """Create an admonition."""
    if emoji is None:
        return ":::{{{admonition}}}\n{text}\n:::\n".format(
            admonition=admonition, text=text)
    else:
        return ":::{{{admonition}}}\n{emoji} {text}\n:::\n".format(
            admonition=admonition, emoji=emoji, text=text)

if not os.environ.get('EMPY_STRICT', None):
    # Only make the alerting admonitions available available if this is not
    # strict; if strict, this will result in an emoji error.
    EMOJIS.update({
        '!!!': (admonish("!!!", "\u2757\ufe0f"), "exclamation mark"),
        '???': (admonish("???", "\u2753\ufe0f"), "question mark"),
        '^^^': (admonish("^^^", "\u26a0\ufe0f"), "warning sign"),
        '///': (admonish("///", "\u2714\ufe0f"), "check mark"),
        '\\\\\\': (admonish("\\\\\\", "\u274c\ufe0f"), "cross mark"),
        '+++': (admonish("+++", "\U0001f53a"), "red triangle pointed up"),
        '---': (admonish("---", "\U0001f53b"), "red triangle pointed down"),
    })

#
# Error ...
#

class DocumentationError(em.Error): pass

#
# Identity
#

class Identity:

    """Dynamically access magic attributes on both the interpreter and
    underlying module."""

    def __init__(self, interp, module):
        self.interp = interp
        self.module = module

    def __str__(self):
        try:
            return self.__getattr__('project')
        except AttributeError:
            return self.__getattr__('program')

    def __getattr__(self, attr):
        attribute = '__{}__'.format(attr)
        if attribute in self.interp.globals:
            return self.interp.globals[attribute]
        elif attribute in self.module.__dict__:
            return self.module.__dict__[attribute]
        else:
            raise AttributeError("unknown attribute: {}".format(__attribute__))

    def tarball(self, version='latest'):
        if version is None:
            version = self.version
        return '{}{}-{}.tar.gz'.format(self.url, self.program, version)

    def path(self, suffix=None):
        if suffix is not None:
            return self.url + suffix
        else:
            return self.url

#
# Tee
#

class Tee:

    """A file-like object which can split output into multiple files."""

    temporary = '/tmp'

    def __init__(self, files):
        self.files = files

    def __call__(self, string):
        self.write(string)

    def write(self, string):
        for file in self.files:
            file.write(string)

    def writelines(self, lines):
        for line in lines:
            self.write(line)

#
# Information
#

class Information:

    """A helper information class to generate documentation."""

    timeFormat = '%Y-%m-%d %H:%M:%S'
    extensions = ['.md.em', '.md.pre', '.md', '']
    hashFactory = hashlib.sha1
    hashName = 'SHA1'
    encoding = 'utf-8'

    class Flag:

        """Encapsulate a flag:  a command line option, a configuration
        variable, and/or an environment variable."""

        def __init__(self, config, options,
                     var=None, env=None, val=None):
            self.config = config
            self.options = options
            self.var = var
            self.val = val
            self.env = env

        def __str__(self):
            return str((self.options, self.var, self.val, self.env))

        def render(self, file, key,
                   showDefault=True, default=None, verbose=False):
            isOption = key.startswith('-')
            options = '/'.join("{}".format(x) for x in self.options)
            environ = '{}'.format(self.env) if self.env else None
            if self.var is not None:
                if (verbose and not showDefault and
                    self.val is not None):
                    variable = '{}\N{NO-BREAK SPACE}=\N{NO-BREAK SPACE}{}'.format(
                        self.var, repr(self.val))
                else:
                    variable = '{}'.format(self.var)
            else:
                variable = None
            declarations = []
            fragments = []
            if isOption:
                assert options, (key, str(self))
                # It's an option, so list those first.
                declarations.append(options)
                if environ:
                   fragments.extend(["_environment variable:_ `{}`".format(
                       environ)])
                if variable:
                    fragments.extend(["_configuration variable:_ `{}`".format(
                        variable)])
            elif key.startswith('EMPY_'):
                assert environ, (key, str(self))
                # If it starts with EMPY_, it's an environment variable.
                declarations.append(environ)
                if options:
                    fragments.extend(["_command line option:_ `{}`".format(
                        options)])
                if variable:
                    fragments.extend(["_configuration variable:_ `{}`".format(
                        variable)])
            else:
                assert variable, (key, str(self))
                # Otherwise, it's a configuration variable.
                declarations.append(variable)
                types = self.config._specs[key]
                if types is not None and verbose:
                    if types == em.strType:
                        types = str
                    if types is list:
                        types = 'list[str]'
                    if not isinstance(types, tuple):
                        types = (types,)
                    optional = False
                    if types[-1] is None:
                        # If the last one is None, then the type is
                        # Optional.
                        optional = True
                        types = types[:-1]
                    types = ' | '.join(
                        x if isinstance(x, str) else x.__name__
                        for x in types)
                    if optional:
                        types = 'Optional[%s]' % types
                    declarations.append(": {}".format(types))
                if options:
                    fragments.extend(["_command line option:_ `{}`".format(options)])
                if environ:
                   fragments.extend(["_environment variable:_ `{}`".format(environ)])
            if showDefault:
                declarations.append("\N{NO-BREAK SPACE}=\N{NO-BREAK SPACE}{}".format(default))
            if fragments and verbose:
                file.write('`{}` ({})'.format(''.join(declarations),
                                              ', '.join(fragments)))
            else:
                file.write('`{}`'.format(''.join(declarations)))

    noLanguage = 'text'

    def __init__(self, interp, moduleName, file=sys.stdout):
        self.interp = interp
        self.moduleName = moduleName
        self.file = file
        self.module = __import__(moduleName)
        self.ident = Identity(interp, self.module)
        self.details = emlib.Details()
        self.config = em.Configuration()
        self.usage = emhelp.Usage(config=self.config)
        self.options = self.process()

    def process(self):
        options = {}
        section = self.usage.payload['options']
        for entry in section.entries:
            fullOptions = [x for x in entry.raw if not x.isspace()]
            theseOptions = [x.split('=', 1)[0] for x in fullOptions]
            flag = self.Flag(self.config, fullOptions,
                var=entry.var, val=entry.val, env=entry.env)
            for key in theseOptions:
                assert key not in options, key
                options[key] = flag
            if entry.var:
                options[entry.var] = flag
            if entry.env:
                options[entry.env.split(' ', 1)[0]] = flag
        for key, value in self.config.__dict__.items():
            if key in options:
                options[key].default = value
            else:
                flag = self.Flag(self.config, [], var=key)
                options[key] = flag
        return options

    def topic(self, topic, separator=False):
        self.file.write("```{}\n".format(self.noLanguage))
        self.usage.show([topic], separator)
        self.file.write("```\n")

    def option(self, key, verbose=False, showDefault=False):
        if verbose is None:
            self.file.write('`{}`'.format(key))
            return
        if showDefault and key in self.config:
            if key == 'pathSeparator':
                default = "';'` (Windows) or `':'` (others) ` "
            else:
                default = getattr(self.config, key)
                if isinstance(default, dict):
                    default = "{...}"
                else:
                    default = repr(default)
        else:
            default = None
        self.options[key].render(self.file, key, showDefault, default, verbose)

    def variable(self, variable):
        self.file.write("`{}`".format(variable))

    def source(self, filename):
        for extension in self.extensions:
            if os.path.exists(filename + extension):
                return filename + extension
        else:
            raise FileNotFoundError("source file not found: {}".format(filename))

    def filter(self, text, lines=None, blanks=None):
        if isinstance(text, bytes) and self.encoding:
            text = text.decode(self.encoding)
        text = (text
            .replace('&', '&amp;')
            .replace('<', '&lt;')
            .replace('>', '&gt;'))
        stopped = False
        chunks = []
        for line in text.splitlines():
            if line.startswith('...'):
                chunks.append("<i>{}</i>".format(line))
            else:
                chunks.append(line)
            if not line and blanks is not None:
                blanks -= 1
                if blanks == 0:
                    stopped = True
            if lines is not None and len(chunks) >= lines:
                stopped = True
            if stopped:
                break
        if stopped:
            chunks.append('...')
        return '\n'.join(chunks)

    def shell(self, command, output, prefix='% ', lines=None, blanks=None,
              class_='shell', exitCode=0):
        display = command
        if isinstance(display, list):
            words = ['"{}"'.format(x)
                if ' ' in x and not x.startswith('#')
                else x
                for x in command]
            display = ' '.join(words)
        self.file.write("<pre class=\"{}\">\n".format(class_))
        if display:
            self.file.write("<b><i>{}{}</i></b>\n".format(
                prefix, self.filter(display)))
        self.file.write(self.filter(output, lines, blanks))
        if exitCode != 0:
            self.file.write("<i>Exit code: {}</i>\n".format(exitCode))
        self.file.write("</pre>\n")

    def execute(self, command, prefix='% ', lines=None, blanks=None, check=True):
        oldPath = os.environ['PATH']
        try:
            # Make sure that if it's em.py, it's the local one.
            path = '.:' + oldPath
            os.environ['PATH'] = path
            result = subprocess.run(command, capture_output=True)
            output = result.stdout
            self.shell(command, output, prefix, lines, blanks,
                       exitCode=result.returncode if check else 0)
        finally:
            os.environ['PATH'] = oldPath

    def splice(self, file, name='<splice>'):
        context = self.interp.newContext(name)
        self.interp.pushContext(context)
        try:
            self.interp.fileFull(file)
        finally:
            self.interp.popContext()

    def load(self, filename, mode='r'):
        with open(filename, mode) as file:
            self.splice(file, filename)

    def clip(self, filename, heading, rename=None, stoplines=None, mode='r'):
        buffer = []
        on = False
        start = 0
        number = 1
        if stoplines is None:
            stoplines = []
        stoplines = [x if x.endswith('\n') else x + '\n' for x in stoplines]
        with open(filename, mode) as file:
            for line in file.readlines():
                if line.startswith('#'):
                    try:
                        prelim, title = line.split(' ', 1)
                    except ValueError:
                        raise DocumentationError("malformed header line: {}".format(line), line=line)
                    if ':' in title:
                        title, subtitle = title.split(':', 1)
                        subtitle = subtitle.strip()
                    title = title.strip()
                    level = prelim.count('#')
                    if title == heading:
                        if rename is not None:
                            line = ('#' * level) + ' ' + rename + '\n'
                        if not on:
                            start = number
                            on = True
                    else:
                        if on:
                            break
                if line in stoplines:
                    on = False
                if on:
                    buffer.append(line)
                number += 1
        if start:
            self.splice(io.StringIO(''.join(buffer)), (filename, start))
        else:
            raise DocumentationError("could not find heading '{}'".format(heading), heading=heading)

    def tee(self, filename):
        return Tee(filename)

    def summarize(self, filename='README'):
        filename = self.source(filename)
        hasher = self.hashFactory()
        with open(filename, 'rb') as f:
            data = f.read()
        length = len(data)
        hasher.update(data)
        hash = hasher.hexdigest()
        record = os.stat(filename)
        when = time.localtime(record.st_mtime)
        timeStamp = time.strftime(self.timeFormat, when)
        out = em.StringIO('w')
        self.details.show(file=out)
        self.file.write("""\
_This documentation for {} version {} was generated from {} ({} `{}`, {} bytes) at {} using {}._
""".format(
    self.ident, self.ident.version, filename,
    self.hashName, hash, length,
    timeStamp, out.getvalue()))
        self.done()

    def done(self):
        if self.config:
            self.interp.dropAllDiversions()
            self.config = None

#
# Extension
#

class Extension(em.Extension):

    """The EmPy documentation extension."""

    languages = {
        'empy': '' # to eliminate Pygments warning
    }

    sub = True

    def __init__(self):
        super(Extension, self).__init__()
        self.current = 0

    def next(self, amount=1):
        self.current += amount
        return self.current

    def expand(self, source, root='<root>'):
        with em.StringIO() as file:
            with em.Interpreter(output=file, root=root) as interp:
                interp.string(source)
            output = file.getvalue()
        return output

    def angle_brackets(self, source, depth, locals):
        caption = None
        if source.startswith('['):
            language = 'empy'
            caption, source = source[1:].split(']', 1)
            example = True
        else:
            if not source.startswith('\n'):
                language, source = source.split('\n', 1)
                language = language.strip()
            else:
                language = ''
            example = False
        if caption:
            suffix = ': ' + caption
        else:
            suffix = ''
        source = source.strip() + '\n'
        if example:
            number = self.next()
            self.interp.startDiversion(caption)
            self.interp.write('<a class="example" id="example-{}"></a>\n'.format(number))
            self.interp.write('\n')
            self.interp.write(':::{{admonition}} Example {}{}\n'.format(
                number, suffix))
            output = self.expand(source, '<example {}{}>'.format(
                number, ' "' +  caption + '"' if caption else ''))
            self.interp.write('_Source_: {}\n'.format(SOURCE))
            self.interp.write('<div class="source">\n\n')
            self.interp.write('``````{}\n'.format(self.languages.get(language, language)))
            self.interp.write(source)
            self.interp.write('``````\n')
            self.interp.write('\n</div>\n\n')
            self.interp.write('_Output_: {}\n'.format(OUTPUT))
            self.interp.write('<div class="output">\n\n')
            self.interp.write('``````\n')
            self.interp.write(output)
            self.interp.write('``````\n')
            self.interp.write('\n</div>\n\n')
            self.interp.write(':::\n')

            self.interp.stopDiverting()
            self.interp.replayDiversion(caption)
        else:
            self.interp.write('``````{}\n'.format(self.languages.get(language, language)))
            self.interp.write(source)
            self.interp.write('``````\n')

#
# Hook
#

class Hook(emlib.Hook):

    """The EmPy documentation hook."""

    delimiter = '`'

    def __init__(self, interp):
        self.interp = interp

    def preBackquote(self, literal):
        if self.delimiter in literal:
            for count in range(1, 10):
                if '`' * count not in literal:
                    count -= 1
                    break
        else:
            count = 0
        affix = self.delimiter * (count + 1)
        self.interp.write("{}{}{}".format(affix, literal, affix))
        return True

#
# init
#

def init(interp, moduleName, paths=None):
    """Initialize the information object."""
    if paths is None:
        paths = PATHS
    for path in paths:
        sys.path.insert(0, os.path.abspath(path))
    interp.config.emojis = EMOJIS
    interp.addHook(Hook(interp))
    interp.installExtension(Extension())
    return Information(interp, moduleName)
