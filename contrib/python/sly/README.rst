SLY (Sly Lex-Yacc)
==================

SLY is a 100% Python implementation of the lex and yacc tools
commonly used to write parsers and compilers.  Parsing is
based on the same LALR(1) algorithm used by many yacc tools.
Here are a few notable features:

-  SLY provides *very* extensive error reporting and diagnostic 
   information to assist in parser construction.  The original
   implementation was developed for instructional purposes.  As
   a result, the system tries to identify the most common types
   of errors made by novice users.  

-  SLY provides full support for empty productions, error recovery,
   precedence specifiers, and moderately ambiguous grammars.

-  SLY uses various Python metaprogramming features to specify
   lexers and parsers.  There are no generated files or extra
   steps involved. You simply write Python code and run it.

-  SLY can be used to build parsers for "real" programming languages.
   Although it is not ultra-fast due to its Python implementation,
   SLY can be used to parse grammars consisting of several hundred
   rules (as might be found for a language like C).  

SLY originates from the `PLY project <http://www.dabeaz.com/ply/index.html>`_.
However, it's been modernized a bit.  In fact, don't expect any code
previously written for PLY to work. That said, most of the things 
that were possible in PLY are also possible in SLY. 

SLY is a modern library for performing lexing and parsing. It
implements the LALR(1) parsing algorithm, commonly used for
parsing and compiling various programming languages. 

Important Notice : October 11, 2022
-----------------------------------
I'm officially retiring the SLY project.  There will be no further
releases.  Should you choose to still use it, the GitHub repo will
remain alive--feel free to vendor the code and report bugs as you see
fit.  I'd like to thank everyone who contributed to the
project. -- Dave

Requirements
------------

SLY requires the use of Python 3.6 or greater.  Older versions
of Python are not supported.

An Example
----------

SLY is probably best illustrated by an example.  Here's what it
looks like to write a parser that can evaluate simple arithmetic
expressions and store variables:

.. code:: python

    # -----------------------------------------------------------------------------
    # calc.py
    # -----------------------------------------------------------------------------

    from sly import Lexer, Parser

    class CalcLexer(Lexer):
        tokens = { NAME, NUMBER, PLUS, TIMES, MINUS, DIVIDE, ASSIGN, LPAREN, RPAREN }
        ignore = ' \t'

        # Tokens
        NAME = r'[a-zA-Z_][a-zA-Z0-9_]*'
        NUMBER = r'\d+'

        # Special symbols
        PLUS = r'\+'
        MINUS = r'-'
        TIMES = r'\*'
        DIVIDE = r'/'
        ASSIGN = r'='
        LPAREN = r'\('
        RPAREN = r'\)'

        # Ignored pattern
        ignore_newline = r'\n+'

        # Extra action for newlines
        def ignore_newline(self, t):
            self.lineno += t.value.count('\n')

        def error(self, t):
            print("Illegal character '%s'" % t.value[0])
            self.index += 1

    class CalcParser(Parser):
        tokens = CalcLexer.tokens

        precedence = (
            ('left', PLUS, MINUS),
            ('left', TIMES, DIVIDE),
            ('right', UMINUS),
            )

        def __init__(self):
            self.names = { }

        @_('NAME ASSIGN expr')
        def statement(self, p):
            self.names[p.NAME] = p.expr

        @_('expr')
        def statement(self, p):
            print(p.expr)

        @_('expr PLUS expr')
        def expr(self, p):
            return p.expr0 + p.expr1

        @_('expr MINUS expr')
        def expr(self, p):
            return p.expr0 - p.expr1

        @_('expr TIMES expr')
        def expr(self, p):
            return p.expr0 * p.expr1

        @_('expr DIVIDE expr')
        def expr(self, p):
            return p.expr0 / p.expr1

        @_('MINUS expr %prec UMINUS')
        def expr(self, p):
            return -p.expr

        @_('LPAREN expr RPAREN')
        def expr(self, p):
            return p.expr

        @_('NUMBER')
        def expr(self, p):
            return int(p.NUMBER)

        @_('NAME')
        def expr(self, p):
            try:
                return self.names[p.NAME]
            except LookupError:
                print(f'Undefined name {p.NAME!r}')
                return 0

    if __name__ == '__main__':
        lexer = CalcLexer()
        parser = CalcParser()
        while True:
            try:
                text = input('calc > ')
            except EOFError:
                break
            if text:
                parser.parse(lexer.tokenize(text))

Documentation
-------------

Further documentation can be found at `https://sly.readthedocs.io/en/latest <https://sly.readthedocs.io/en/latest>`_.

Talks
-----

* `Reinventing the Parser Generator <https://www.youtube.com/watch?v=zJ9z6Ge-vXs>`_, talk by David Beazley at PyCon 2018, Cleveland.

Resources
---------

For a detailed overview of parsing theory, consult the excellent
book "Compilers : Principles, Techniques, and Tools" by Aho, Sethi, and
Ullman.  The topics found in "Lex & Yacc" by Levine, Mason, and Brown
may also be useful.

The GitHub page for SLY can be found at:

     ``https://github.com/dabeaz/sly``

Please direct bug reports and pull requests to the GitHub page.
To contact me directly, send email to dave@dabeaz.com or contact
me on Twitter (@dabeaz).
 
-- Dave

P.S.
----

You should come take a `course <https://www.dabeaz.com/courses.html>`_!




