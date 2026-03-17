import optparse
import sys
import tokenize
import warnings

# Polyfill stdin loading/reading lines
# https://gitlab.com/pycqa/flake8-polyfill/blob/1.0.1/src/flake8_polyfill/stdin.py#L52-57
try:
    from flake8.engine import pep8
    stdin_get_value = pep8.stdin_get_value
    readlines = pep8.readlines
except ImportError:
    from flake8 import utils
    import pycodestyle
    stdin_get_value = utils.stdin_get_value
    readlines = pycodestyle.readlines

from flake8_quotes.__about__ import __version__
from flake8_quotes.docstring_detection import get_docstring_tokens


_IS_PEP701 = sys.version_info[:2] >= (3, 12)


class QuoteChecker(object):
    name = __name__
    version = __version__

    INLINE_QUOTES = {
        # When user wants only single quotes
        "'": {
            'good_single': "'",
            'bad_single': '"',
            'single_error_message': 'Double quotes found but single quotes preferred',
        },
        # When user wants only double quotes
        '"': {
            'good_single': '"',
            'bad_single': "'",
            'single_error_message': 'Single quotes found but double quotes preferred',
        },
    }
    # Provide aliases for Windows CLI support
    #   https://github.com/zheller/flake8-quotes/issues/49
    INLINE_QUOTES['single'] = INLINE_QUOTES["'"]
    INLINE_QUOTES['double'] = INLINE_QUOTES['"']

    MULTILINE_QUOTES = {
        "'": {
            'good_multiline': "'''",
            'good_multiline_ending': '\'"""',
            'bad_multiline': '"""',
            'multiline_error_message': 'Double quote multiline found but single quotes preferred',
        },
        '"': {
            'good_multiline': '"""',
            'good_multiline_ending': '"\'\'\'',
            'bad_multiline': "'''",
            'multiline_error_message': 'Single quote multiline found but double quotes preferred',
        },
    }
    # Provide Windows CLI and multi-quote aliases
    MULTILINE_QUOTES['single'] = MULTILINE_QUOTES["'"]
    MULTILINE_QUOTES['double'] = MULTILINE_QUOTES['"']
    MULTILINE_QUOTES["'''"] = MULTILINE_QUOTES["'"]
    MULTILINE_QUOTES['"""'] = MULTILINE_QUOTES['"']

    DOCSTRING_QUOTES = {
        "'": {
            'good_docstring': "'''",
            'bad_docstring': '"""',
            'docstring_error_message': 'Double quote docstring found but single quotes preferred',
        },
        '"': {
            'good_docstring': '"""',
            'bad_docstring': "'''",
            'docstring_error_message': 'Single quote docstring found but double quotes preferred',
        },
    }
    # Provide Windows CLI and docstring-quote aliases
    DOCSTRING_QUOTES['single'] = DOCSTRING_QUOTES["'"]
    DOCSTRING_QUOTES['double'] = DOCSTRING_QUOTES['"']
    DOCSTRING_QUOTES["'''"] = DOCSTRING_QUOTES["'"]
    DOCSTRING_QUOTES['"""'] = DOCSTRING_QUOTES['"']

    def __init__(self, tree, lines=None, filename='(none)'):
        self.filename = filename
        self.lines = lines

    @staticmethod
    def _register_opt(parser, *args, **kwargs):
        """
        Handler to register an option for both Flake8 3.x and 2.x.

        This is based on:
        https://github.com/PyCQA/flake8/blob/3.0.0b2/docs/source/plugin-development/cross-compatibility.rst#option-handling-on-flake8-2-and-3

        It only supports `parse_from_config` from the original function and it
        uses the `Option` object returned to get the string.
        """
        try:
            # Flake8 3.x registration
            parser.add_option(*args, **kwargs)
        except (optparse.OptionError, TypeError):
            # Flake8 2.x registration
            parse_from_config = kwargs.pop('parse_from_config', False)
            option = parser.add_option(*args, **kwargs)
            if parse_from_config:
                parser.config_options.append(option.get_opt_string().lstrip('-'))

    @classmethod
    def add_options(cls, parser):
        cls._register_opt(parser, '--quotes', action='store',
                          parse_from_config=True,
                          choices=sorted(cls.INLINE_QUOTES.keys()),
                          help='Deprecated alias for `--inline-quotes`')
        cls._register_opt(parser, '--inline-quotes', default="'",
                          action='store', parse_from_config=True,
                          choices=sorted(cls.INLINE_QUOTES.keys()),
                          help="Quote to expect in all files (default: ')")
        cls._register_opt(parser, '--multiline-quotes', default=None, action='store',
                          parse_from_config=True,
                          choices=sorted(cls.MULTILINE_QUOTES.keys()),
                          help='Quote to expect in all files (default: """)')
        cls._register_opt(parser, '--docstring-quotes', default=None, action='store',
                          parse_from_config=True,
                          choices=sorted(cls.DOCSTRING_QUOTES.keys()),
                          help='Quote to expect in all files (default: """)')
        cls._register_opt(parser, '--avoid-escape', default=None, action='store_true',
                          parse_from_config=True,
                          help='Avoiding escaping same quotes in inline strings (enabled by default)')
        cls._register_opt(parser, '--no-avoid-escape', dest='avoid_escape', default=None, action='store_false',
                          parse_from_config=False,
                          help='Disable avoiding escaping same quotes in inline strings')
        cls._register_opt(parser, '--check-inside-f-strings',
                          dest='check_inside_f_strings', default=False, action='store_true',
                          parse_from_config=True,
                          help='Check strings inside f-strings, when PEP701 is active (Python 3.12+)')

    @classmethod
    def parse_options(cls, options):
        # Define our default config
        # cls.config = {good_single: ', good_multiline: ''', bad_single: ", bad_multiline: """}
        cls.config = {}
        cls.config.update(cls.INLINE_QUOTES["'"])
        cls.config.update(cls.MULTILINE_QUOTES['"""'])
        cls.config.update(cls.DOCSTRING_QUOTES['"""'])

        # If `options.quotes` was specified, then use it
        if hasattr(options, 'quotes') and options.quotes is not None:
            # https://docs.python.org/2/library/warnings.html#warnings.warn
            warnings.warn('flake8-quotes has deprecated `quotes` in favor of `inline-quotes`. '
                          'Please update your configuration')
            cls.config.update(cls.INLINE_QUOTES[options.quotes])
        # Otherwise, use the supported `inline_quotes`
        else:
            # cls.config = {good_single: ', good_multiline: """, bad_single: ", bad_multiline: '''}
            #   -> {good_single: ", good_multiline: """, bad_single: ', bad_multiline: '''}
            cls.config.update(cls.INLINE_QUOTES[options.inline_quotes])

        # If multiline quotes was specified, overload our config with those options
        if hasattr(options, 'multiline_quotes') and options.multiline_quotes is not None:
            # cls.config = {good_single: ', good_multiline: """, bad_single: ", bad_multiline: '''}
            #   -> {good_single: ', good_multiline: ''', bad_single: ", bad_multiline: """}
            cls.config.update(cls.MULTILINE_QUOTES[options.multiline_quotes])

        # If docstring quotes was specified, overload our config with those options
        if hasattr(options, 'docstring_quotes') and options.docstring_quotes is not None:
            cls.config.update(cls.DOCSTRING_QUOTES[options.docstring_quotes])

        # If avoid escaped specified, add to config
        if hasattr(options, 'avoid_escape') and options.avoid_escape is not None:
            cls.config.update({'avoid_escape': options.avoid_escape})
        else:
            cls.config.update({'avoid_escape': True})

        # If check inside f-strings specified, add to config
        if hasattr(options, 'check_inside_f_strings') and options.check_inside_f_strings is not None:
            cls.config.update({'check_inside_f_strings': options.check_inside_f_strings})
        else:
            cls.config.update({'check_inside_f_strings': False})

    def get_file_contents(self):
        if self.filename in ('stdin', '-', None):
            return stdin_get_value().splitlines(True)
        else:
            if self.lines:
                return self.lines
            else:
                return readlines(self.filename)

    def run(self):
        file_contents = self.get_file_contents()

        noqa_line_numbers = self.get_noqa_lines(file_contents)
        errors = self.get_quotes_errors(file_contents)

        for error in errors:
            if error.get('line') not in noqa_line_numbers:
                yield (error.get('line'), error.get('col'), error.get('message'), type(self))

    def get_noqa_lines(self, file_contents):
        tokens = [Token(t) for t in tokenize.generate_tokens(lambda L=iter(file_contents): next(L))]
        return [token.start_row
                for token in tokens
                if token.type == tokenize.COMMENT and token.string.endswith('noqa')]

    def get_quotes_errors(self, file_contents):
        tokens = [Token(t) for t in tokenize.generate_tokens(lambda L=iter(file_contents): next(L))]
        docstring_tokens = get_docstring_tokens(tokens)
        # when PEP701 is enabled, we track when the token stream
        # is passing over an f-string

        # the start of the current f-string (row, col)
        fstring_start = None

        # > 0 when we are inside an f-string token stream, since
        # f-string can be arbitrarily nested, we need a counter
        fstring_nesting = 0

        # the token.string part of all tokens inside the current
        # f-string
        fstring_buffer = []

        for token in tokens:
            is_docstring = token in docstring_tokens

            # non PEP701, we only check for STRING tokens
            if not _IS_PEP701:
                if token.type == tokenize.STRING:
                    yield from self._check_string(token.string, token.start, is_docstring)

                continue

            # otherwise, we track all tokens for the current f-string
            if token.type == tokenize.FSTRING_START:
                if fstring_nesting == 0:
                    fstring_start = token.start

                fstring_nesting += 1
                fstring_buffer.append(token.string)
            elif token.type == tokenize.FSTRING_END:
                fstring_nesting -= 1
                fstring_buffer.append(token.string)
            elif fstring_nesting > 0:
                fstring_buffer.append(token.string)

            # if we have reached the end of a top-level f-string, we check
            # it as if it was a single string (pre PEP701 semantics) when
            # check_inside_f_strings is false
            if token.type == tokenize.FSTRING_END and fstring_nesting == 0:
                token_string = ''.join(fstring_buffer)
                fstring_buffer[:] = []

                if not self.config['check_inside_f_strings']:
                    yield from self._check_string(token_string, fstring_start, is_docstring)
                    continue

            # otherwise, we check nested strings and f-strings, we don't
            # check FSTRING_END since it should be legal if tokenize.FSTRING_START succeeded
            if token.type in (tokenize.STRING, tokenize.FSTRING_START,):
                if fstring_nesting > 0:
                    if self.config['check_inside_f_strings']:
                        yield from self._check_string(token.string, token.start, is_docstring)
                else:
                    yield from self._check_string(token.string, token.start, is_docstring)

    def _check_string(self, token_string, token_start, is_docstring):
        # Remove any prefixes in strings like `u` from `u"foo"`
        # DEV: `last_quote_char` is 1 character, even for multiline strings
        #   `"foo"`   -> `"foo"`
        #   `b"foo"`  -> `"foo"`
        #   `br"foo"` -> `"foo"`
        #   `b"""foo"""` -> `"""foo"""`
        last_quote_char = token_string[-1]
        first_quote_index = token_string.index(last_quote_char)
        prefix = token_string[:first_quote_index].lower()
        unprefixed_string = token_string[first_quote_index:]

        # Determine if our string is multiline-based
        #   "foo"[0] * 3 = " * 3 = """
        #   "foo"[0:3] = "fo
        #   """foo"""[0:3] = """
        is_multiline_string = unprefixed_string[0] * 3 == unprefixed_string[0:3]
        start_row, start_col = token_start

        # If our string is a docstring
        # DEV: Docstring quotes must come before multiline quotes as it can as a multiline quote
        if is_docstring:
            if self.config['good_docstring'] in unprefixed_string:
                return

            yield {
                'message': 'Q002 ' + self.config['docstring_error_message'],
                'line': start_row,
                'col': start_col,
            }
        # Otherwise if our string is multiline
        elif is_multiline_string:
            # If our string is or containing a known good string, then ignore it
            #   (""")foo""" -> good (continue)
            #   '''foo(""")''' -> good (continue)
            #   (''')foo''' -> possibly bad
            if self.config['good_multiline'] in unprefixed_string:
                return

            # If our string ends with a known good ending, then ignore it
            #   '''foo("''') -> good (continue)
            #     Opposite, """foo"""", would break our parser (cannot handle """" ending)
            if unprefixed_string.endswith(self.config['good_multiline_ending']):
                return

            # Output our error
            yield {
                'message': 'Q001 ' + self.config['multiline_error_message'],
                'line': start_row,
                'col': start_col,
            }
        # Otherwise (string is inline quote)
        else:
            #   'This is a string'       -> Good
            #   'This is a "string"'     -> Good
            #   'This is a \"string\"'   -> Good
            #   'This is a \'string\''   -> Bad (Q003)  Escaped inner quotes
            #   '"This" is a \'string\'' -> Good        Changing outer quotes would not avoid escaping
            #   "This is a string"       -> Bad (Q000)
            #   "This is a 'string'"     -> Good        Avoids escaped inner quotes
            #   "This is a \"string\""   -> Bad (Q000)
            #   "\"This\" is a 'string'" -> Good

            string_contents = unprefixed_string[1:-1]
            # If string preferred type, check for escapes
            if last_quote_char == self.config['good_single']:
                if not self.config['avoid_escape'] or 'r' in prefix:
                    return
                if (self.config['good_single'] in string_contents and
                        not self.config['bad_single'] in string_contents):
                    yield {
                        'message': 'Q003 Change outer quotes to avoid escaping inner quotes',
                        'line': start_row,
                        'col': start_col,
                    }
                return

            # If not preferred type, only allow use to avoid escapes.
            if self.config['good_single'] not in string_contents:
                yield {
                    'message': 'Q000 ' + self.config['single_error_message'],
                    'line': start_row,
                    'col': start_col,
                }


class Token:
    """Python 2 and 3 compatible token"""
    def __init__(self, token):
        self.token = token

    @property
    def type(self):
        return self.token[0]

    @property
    def string(self):
        return self.token[1]

    @property
    def start(self):
        return self.token[2]

    @property
    def start_row(self):
        return self.token[2][0]

    @property
    def start_col(self):
        return self.token[2][1]
