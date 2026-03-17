import sys
from sympy.external import import_module


from .AutolevParser import AutolevParser
from .AutolevLexer import AutolevLexer
from .AutolevListener import AutolevListener


def parse_autolev(autolev_code, include_numeric):
    antlr4 = import_module('antlr4', warn_not_installed=True)
    if not antlr4:
        raise ImportError("Autolev parsing requires the antlr4 python package,"
                          " provided by pip (antlr4-python2-runtime or"
                          " antlr4-python3-runtime) or"
                          " conda (antlr-python-runtime)")
    try:
        l = autolev_code.readlines()
        input_stream = antlr4.InputStream("".join(l))
    except Exception:
        input_stream = antlr4.InputStream(autolev_code)

    if AutolevListener:
        from ._listener_autolev_antlr import MyListener
        lexer = AutolevLexer(input_stream)
        token_stream = antlr4.CommonTokenStream(lexer)
        parser = AutolevParser(token_stream)
        tree = parser.prog()
        my_listener = MyListener(include_numeric)
        walker = antlr4.ParseTreeWalker()
        walker.walk(my_listener, tree)
        return "".join(my_listener.output_code)
