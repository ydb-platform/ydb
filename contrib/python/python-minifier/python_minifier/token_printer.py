"""Tools for assembling python code from tokens."""

import re
import sys


class TokenTypes(object):
    NoToken = 0
    Identifier = 1
    Keyword = 2
    SoftKeyword = 3
    NumberLiteral = 4
    NonNumberLiteral = 5
    Delimiter = 6
    Operator = 7
    NewLine = 8
    EndStatement = 9


class Delimiter(object):
    def __init__(self, terminal_printer, delimiter=',', add_parens=False):
        """
        Delimited group printer

        A group of items that should be delimited by a delimiter character.
        Each call to new_item() will insert the delimiter character if necessary.

        When used as a context manager, the group will be enclosed by the start and end characters if the group has any items.

        >>> d = Delimiter(terminal_printer)
        ... d.new_item()
        ... terminal_printer.identifier('a')
        ... print(terminal_printer.code)
        a

        >>> d.new_item()
        ... terminal_printer.identifier('b')
        ... print(terminal_printer.code)
        a,b

        >>> with Delimiter(terminal_printer, add_parens=True) as d:
        ...     d.new_item()
        ...     terminal_printer.identifier('a')
        ... print(terminal_printer.code)
        (a)

        :param terminal_printer: The terminal printer to use.
        :param delimiter: The delimiter to use.
        :param add_parens: If the group should be enclosed by parentheses. Only used when used as a context manager.
        """

        self._terminal_printer = terminal_printer
        self._delimiter = delimiter
        self._add_parens = add_parens

        self._first = True

        self._context_manager = False

    def __enter__(self):
        """Open a delimited group."""
        self._context_manager = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close the delimited group."""
        if not self._first and self._add_parens:
            self._terminal_printer.delimiter(')')

    def new_item(self):
        """Add a new item to the delimited group."""
        if self._first:
            self._first = False
            if self._context_manager and self._add_parens:
                self._terminal_printer.delimiter('(')
        else:
            self._terminal_printer.delimiter(self._delimiter)


class TokenPrinter(object):
    """
    Concatenates terminal symbols of the python grammar
    """

    def __init__(self, prefer_single_line=False, allow_invalid_num_warnings=False):
        """
        :param prefer_single_line: If True, chooses to put as much code as possible on a single line.
        :param allow_invalid_num_warnings: If True, allows invalid number literals to be printe that may cause warnings.
        """

        self._prefer_single_line = prefer_single_line
        self._allow_invalid_num_warnings = allow_invalid_num_warnings

        # Initialize as unicode string on Python 2.7 to handle Unicode content
        if sys.version_info[0] < 3:
            self._code = u''
        else:
            self._code = ''
        self.indent = 0
        self.unicode_literals = False
        self.previous_token = TokenTypes.NoToken

    def __str__(self):
        """Return the output code."""
        return self._code

    def __unicode__(self):
        """Return the output code as unicode (for Python 2.7 compatibility)."""
        return self._code

    def identifier(self, name):
        """Add an identifier to the output code."""
        assert isinstance(name, str)

        if self.previous_token in [TokenTypes.Identifier, TokenTypes.Keyword, TokenTypes.SoftKeyword, TokenTypes.NumberLiteral]:
            self.delimiter(' ')

        self._code += name
        self.previous_token = TokenTypes.Identifier

    def keyword(self, kw):
        """Add a keyword to the output code."""
        assert kw in [
            'False', 'None', 'True', 'and', 'as',
            'assert', 'async', 'await', 'break',
            'class', 'continue', 'def', 'del',
            'elif', 'else', 'except', 'finally',
            'for', 'from', 'global', 'if', 'import',
            'in', 'is', 'lambda', 'nonlocal', 'not',
            'or', 'pass', 'raise', 'return',
            'try', 'while', 'with', 'yield', '_',
            'case', 'match', 'print', 'exec',
            'type'
        ]

        if self.previous_token in [TokenTypes.Identifier, TokenTypes.Keyword, TokenTypes.SoftKeyword, TokenTypes.NumberLiteral]:
            self.delimiter(' ')

        self._code += kw

        if kw in ['_', 'case', 'match', 'type']:
            self.previous_token = TokenTypes.SoftKeyword
        else:
            self.previous_token = TokenTypes.Keyword

    def stringliteral(self, value):
        """Add a string literal to the output code."""
        s = repr(value)

        if sys.version_info < (3, 0) and self.unicode_literals:
            if s[0] == 'u':
                # Remove the u prefix since literals are unicode by default
                s = s[1:]
            else:
                # Add a b prefix to indicate it is NOT unicode
                s = 'b' + s

        if len(s) > 0 and s[0].isalpha() and self.previous_token in [TokenTypes.Identifier, TokenTypes.Keyword, TokenTypes.SoftKeyword]:
            self.delimiter(' ')

        self._code += s
        self.previous_token = TokenTypes.NonNumberLiteral

    def bytesliteral(self, value):
        """Add a bytes literal to the output code."""
        s = repr(value)

        if len(s) > 0 and s[0].isalpha() and self.previous_token in [TokenTypes.Identifier, TokenTypes.Keyword, TokenTypes.SoftKeyword]:
            self.delimiter(' ')

        self._code += s
        self.previous_token = TokenTypes.NonNumberLiteral

    def fstring(self, s):
        """Add an f-string to the output code."""
        assert isinstance(s, str)

        if self.previous_token in [TokenTypes.Identifier, TokenTypes.Keyword, TokenTypes.SoftKeyword]:
            self.delimiter(' ')

        self._code += s
        self.previous_token = TokenTypes.NonNumberLiteral

    def tstring(self, s):
        """Add a template string (t-string) to the output code."""
        assert isinstance(s, str)

        if self.previous_token in [TokenTypes.Identifier, TokenTypes.Keyword, TokenTypes.SoftKeyword]:
            self.delimiter(' ')

        self._code += s
        self.previous_token = TokenTypes.NonNumberLiteral

    def delimiter(self, d):
        """Add a delimiter to the output code."""
        assert d in [
            '(', ')', '[', ']', '{', '}', ' ',
            ',', ':', '.', ';', '@', '=', '->',
            '+=', '-=', '*=', '/=', '//=', '%=', '@=',
            '&=', '|=', '^=', '>>=', '<<=', '**=', '|',
            '`'
        ]

        self._code += d
        self.previous_token = TokenTypes.Delimiter

    def operator(self, o):
        """Add an operator to the output code."""
        assert o in [
            '+', '-', '*', '**', '/', '//', '%', '@',
            '<<', '>>', '&', '|', '^', '~', ':=',
            '<', '>', '<=', '>=', '==', '!='
        ]

        self._code += o
        self.previous_token = TokenTypes.Operator

    def integer(self, v):
        """Add an integer to the output code."""

        s = repr(v)
        h = hex(v)

        if self.previous_token == TokenTypes.SoftKeyword:
            self.delimiter(' ')
        elif self.previous_token in [TokenTypes.Identifier, TokenTypes.Keyword]:
            self.delimiter(' ')

        self._code += h if len(h) < len(s) else s

        self.previous_token = TokenTypes.NumberLiteral

    def imagnumber(self, value):
        """Add a complex number to the output code."""
        assert isinstance(value, complex)

        s = repr(value)

        if s in ['infj', 'inf*j']:
            s = '1e999j'
        elif s in ['-infj', '-inf*j']:
            s = '-1e999j'

        if self.previous_token == TokenTypes.SoftKeyword:
            self.delimiter(' ')
        elif self.previous_token in [TokenTypes.Identifier, TokenTypes.Keyword]:
            self.delimiter(' ')

        self._code += s

        self.previous_token = TokenTypes.NumberLiteral

    def floatnumber(self, v):
        """Add a float to the output code."""
        assert isinstance(v, float)

        s = repr(v)

        s = s.replace('e+', 'e')

        add_e = re.match(r'^(\d+?)(0+).0$', s)
        if add_e:
            s = add_e.group(1) + 'e' + str(len(add_e.group(2)))

        if s == 'inf':
            s = '1e999'
        elif s == '-inf':
            s = '-1e999'
        elif s.startswith('0.'):
            s = s[1:]
        elif s.startswith('-0.'):
            s = '-' + s[2:]
        elif s.endswith('.0'):
            s = s[:-1]

        if self.previous_token == TokenTypes.SoftKeyword:
            self.delimiter(' ')
        elif self.previous_token in [TokenTypes.Identifier, TokenTypes.Keyword]:
            self.delimiter(' ')

        self._code += s

        self.previous_token = TokenTypes.NumberLiteral

    def newline(self):
        """ Add a newline to the code. """
        if self._code == '':
            return

        self._code = self._code.rstrip('\n\t;')
        self._code += '\n'
        self._code += '\t' * self.indent

        self.previous_token = TokenTypes.NewLine

    def enter_block(self):
        """Enter a new block, indenting the code."""
        self.indent += 1
        self.newline()

    def leave_block(self):
        """Leave a block, un-indenting the code."""
        self.indent -= 1
        self.newline()

    def end_statement(self):
        """ End a statement with a newline, or a semi-colon if it saves characters. """

        if self.indent == 0 and not self._prefer_single_line:
            self.newline()
        else:
            if self._code[-1] != ';':
                self._code += ';'

        self.previous_token = TokenTypes.EndStatement

    def append(self, code, token_type):
        """ Append arbitrary string to the output."""
        self._code += code
        self.previous_token = token_type
