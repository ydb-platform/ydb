# -*- coding: utf-8 -*-
import codecs
import os
import re

from .compat import PY2

DELIMITERS = ('{', '}', ';')
EXTERNAL_BUILDERS = {}
ESCAPE_SEQUENCES_RE = re.compile(r'(\\x[0-9a-f]{2}|\\[0-7]{1,3})')


def _escape(string):
    prev, char = '', ''
    for char in string:
        if prev == '\\' or prev + char == '${':
            prev += char
            yield prev
            continue
        if prev == '$':
            yield prev
        if char not in ('\\', '$'):
            yield char
        prev = char
    if char in ('\\', '$'):
        yield char


def _needs_quotes(string):
    if string == '':
        return True

    # lexer should throw an error when variable expansion syntax
    # is messed up, but just wrap it in quotes for now I guess
    chars = _escape(string)

    # arguments can't start with variable expansion syntax
    char = next(chars)
    if char.isspace() or char in ('{', '}', ';', '"', "'", '${'):
        return True

    expanding = False
    for char in chars:
        if char.isspace() or char in ('{', ';', '"', "'"):
            return True
        elif char == ('${' if expanding else '}'):
            return True
        elif char == ('}' if expanding else '${'):
            expanding = not expanding

    return char in ('\\', '$') or expanding


def _replace_escape_sequences(match):
    return match.group(1).decode('string-escape')


def _enquote(arg):
    if not _needs_quotes(arg):
        return arg

    if PY2:
        arg = codecs.encode(arg, 'utf-8') if isinstance(arg, unicode) else arg
        arg = codecs.decode(arg, 'raw-unicode-escape')
        arg = repr(arg).replace('\\\\', '\\').lstrip('u')
        arg = ESCAPE_SEQUENCES_RE.sub(_replace_escape_sequences, arg)
        arg = unicode(arg, 'utf-8')
    else:
        arg = repr(arg).replace('\\\\', '\\')

    return arg


def build(payload, indent=4, tabs=False, header=False):
    padding = '\t' if tabs else ' ' * indent

    head = ''
    if header:
        head += '# This config was built from JSON using NGINX crossplane.\n'
        head += '# If you encounter any bugs please report them here:\n'
        head += '# https://github.com/nginxinc/crossplane/issues\n'
        head += '\n'

    def _build_block(output, block, depth, last_line):
        margin = padding * depth

        for stmt in block:
            directive = _enquote(stmt['directive'])
            line = stmt.get('line', 0)

            if directive == '#' and line == last_line:
                output += ' #' + stmt['comment']
                continue
            elif directive == '#':
                built = '#' + stmt['comment']
            elif directive in EXTERNAL_BUILDERS:
                external_builder = EXTERNAL_BUILDERS[directive]
                built = external_builder(stmt, padding, indent, tabs)
            else:
                args = [_enquote(arg) for arg in stmt['args']]

                if directive == 'if':
                    built = 'if (' + ' '.join(args) + ')'
                elif args:
                    built = directive + ' ' + ' '.join(args)
                else:
                    built = directive

                if stmt.get('block') is None:
                    built += ';'
                else:
                    built += ' {'
                    built = _build_block(built, stmt['block'], depth+1, line)
                    built += '\n' + margin + '}'

            output += ('\n' if output else '') + margin + built
            last_line = line

        return output

    body = ''
    body = _build_block(body, payload, 0, 0)
    return head + body


def build_files(payload, dirname=None, indent=4, tabs=False, header=False):
    """
    Uses a full nginx config payload (output of crossplane.parse) to build
    config files, then writes those files to disk.
    """
    if dirname is None:
        dirname = os.getcwd()

    for config in payload['config']:
        path = config['file']
        if not os.path.isabs(path):
            path = os.path.join(dirname, path)

        # make directories that need to be made for the config to be built
        dirpath = os.path.dirname(path)
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)

        # build then create the nginx config file using the json payload
        parsed = config['parsed']
        output = build(parsed, indent=indent, tabs=tabs, header=header)
        output = output.rstrip() + '\n'
        with codecs.open(path, 'w', encoding='utf-8') as fp:
            fp.write(output)


def register_external_builder(builder, directives):
    for directive in directives:
        EXTERNAL_BUILDERS[directive] = builder
