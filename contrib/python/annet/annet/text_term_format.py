from itertools import groupby
from operator import itemgetter
from typing import Dict, List

from pygments import lex
from pygments.formatter import Formatter
from pygments.lexer import RegexLexer
from pygments.lexers import DiffLexer, YamlLexer  # pylint: disable=no-name-in-module
from pygments.token import Token

from .output import TextArgs


# =====
Warning = Token.Warning  # pylint: disable=redefined-builtin
Error = Token.Error

YAML_TERMINAL_COLORS = {
    Token.Literal.String: "green",
    Token.Literal.Scalar.Plain: "darkblue",
    Token.Punctuation.Indicator: "darkblue",
}

DIFF_TERMINAL_COLORS = {
    Token.Generic.Inserted: "green",
    Token.Generic.Deleted: "red",
    Token.Generic.Heading: "cyan_blue",
}

SWITCH_OUTPUT_TERMINAL_COLORS = {
    Warning: "yellow",
    Error: "red",
}


class SwitchOutputLexer(RegexLexer):
    name = "SwitchOutputLexer"
    aliases = ["switch_output_lexer"]
    tokens = {
        "root": [
            (r"[wW]arning.*\n", Warning),
            (r"Info.*\n", Warning),
            (r"[eE]rror.*\n", Error),
            (r".*\n", Token.Text),
        ]
    }


class CursesFormatter(Formatter):
    def __init__(self, **options):
        self.colorscheme = options.pop("scheme")
        Formatter.__init__(self, **options)

    def format(self, tokensource, outfile="") -> Dict[int, List[TextArgs]]:
        res = {}
        tokensource = list(tokensource)
        tmp_res = {}
        line_no = 0
        for ttype, values in groupby(tokensource, itemgetter(0)):
            color = self.colorscheme.get(ttype)
            for value in values:
                if line_no not in tmp_res:
                    tmp_res[line_no] = []
                if value[1].endswith("\n"):
                    if len(value[1]) > 1:
                        tmp_res[line_no].append([color, value[1].rstrip()])
                    line_no += 1
                else:
                    tmp_res[line_no].append([color, value[1]])

        for line_no, color_values in tmp_res.items():
            res[line_no] = []
            for color, values in groupby(color_values, itemgetter(0)):
                str_values = "".join([v[1] for v in values])
                res[line_no].append(TextArgs(str_values, color))
        return res


def format_yaml(txt):
    return CursesFormatter(scheme=YAML_TERMINAL_COLORS).format(lex(txt, YamlLexer()))


def format_diff(txt):
    return CursesFormatter(scheme=DIFF_TERMINAL_COLORS).format(lex(txt, DiffLexer()))


LEXERS = {
    "diff": (DiffLexer, DIFF_TERMINAL_COLORS),
    "yaml": (YamlLexer, YAML_TERMINAL_COLORS),
    "switch_out": (SwitchOutputLexer, SWITCH_OUTPUT_TERMINAL_COLORS),
}


def curses_format(txt, lexer) -> Dict[int, List[TextArgs]]:
    return CursesFormatter(scheme=LEXERS[lexer][1]).format(lex(txt, LEXERS[lexer][0]()))
