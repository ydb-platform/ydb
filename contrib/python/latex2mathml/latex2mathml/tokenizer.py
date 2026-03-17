import re
from typing import Iterator

from latex2mathml import commands
from latex2mathml.symbols_parser import convert_symbol

UNITS = ("in", "mm", "cm", "pt", "em", "ex", "pc", "bp", "dd", "cc", "sp", "mu")

PATTERN = re.compile(
    rf"""
    (%[^\n]+) |                                 # comment
    (a-zA-Z) |                                  # letter
    ([_^])(\d) |                                # number succeeding an underscore or a caret
    (-?\d+(?:\.\d+)?\s*(?:{'|'.join(UNITS)})) | # dimension
    (\d+(?:\.\d+)?) |                           # integer/decimal
    (\.\d*) |                                   # dot (.) or decimal can start with just a dot
    (\\[\\\[\]{{}}\s!,:>;|_%#$&]) |             # escaped characters
    (\\(?:begin|end)\s*{{[a-zA-Z]+\*?}}) |      # begin or end
    (\\operatorname\s*{{[a-zA-Z\s*]+\*?\s*}}) | # operatorname
    #  color, fbox, href, hbox, mbox, style, text, textbf, textit, textrm, textsf, texttt
    (\\(?:color|fbox|hbox|href|mbox|style|text|textbf|textit|textrm|textsf|texttt))\s*{{([^}}]*)}} |
    (\\[cdt]?frac)\s*([.\d])\s*([.\d])? |       # fractions
    (\\math[a-z]+)({{)([a-zA-Z])(}}) |          # commands starting with math
    (\\[a-zA-Z]+) |                             # other commands
    (\S)                                        # non-space character
    """,
    re.VERBOSE,
)


def tokenize(latex_string: str, skip_comments: bool = True) -> Iterator[str]:
    """
    Converts Latex string into tokens.

    :param latex_string: Latex string.
    :param skip_comments: Flag to skip comments (default=True).
    """
    for match in PATTERN.finditer(latex_string):
        tokens = tuple(filter(lambda x: x is not None, match.groups()))
        if tokens[0].startswith(commands.MATH):
            full_math = "".join(tokens)
            symbol = convert_symbol(full_math)
            if symbol:
                yield f"&#x{symbol};"
                continue
        for captured in tokens:
            if skip_comments and captured.startswith("%"):
                break
            if captured.endswith(UNITS):
                yield captured.replace(" ", "")
                continue
            if captured.startswith((commands.BEGIN, commands.END, commands.OPERATORNAME)):
                yield "".join(captured.split(" "))
                continue
            yield captured
