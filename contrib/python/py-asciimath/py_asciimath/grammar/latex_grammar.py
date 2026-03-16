from itertools import islice

from ..utils.utils import alias_string
from ..translation.latex2asciimath import (
    left_parenthesis,
    right_parenthesis,
    smb,
    unary_functions,
    binary_functions,
)


latex_grammar = r"""
    %import common.WS
    %import common.LETTER
    %import common.NUMBER
    %ignore WS
    start: "\\[" exp "\\]" -> exp
        | "$$" exp "$$" -> exp
        | "$" exp "$" -> exp
        | exp -> exp
    exp: i exp* -> exp
    i: s -> exp_interm
        | s "_" s -> exp_under
        | s "^" s -> exp_super
        | s "_" s "^" s -> exp_under_super
    s: _l exp? _r -> exp_par
        | "\\left" (_l | /\./ | /\\vert/ | /\\mid/) start? "\\right" (_r | /\./ | /\\vert/ | /\\mid/) -> exp_par
        | "\\begin{{matrix}}" row_mat (/\\\\/ row_mat?)* "\\end{{matrix}}" -> exp_mat
        | /\\sqrt/ "[" i+ "]" "{{" exp "}}" -> exp_binary
        | "{{" i+ "}}" -> exp
        | _u "{{" exp "}}" -> exp_unary
        | _b "{{" exp "}}" "{{" exp "}}" -> exp_binary
        | _latex1 -> symbol
        | _latex2 -> symbol
        | _c -> const
    !_c: NUMBER
        | LETTER
    !row_mat: exp ("&" exp?)* -> row_mat
    !_l: {} // left parenthesis
    !_r: {} // right parenthesis
    !_b: {} // binary functions
    !_u: {} // unary functions
    !_latex1: {}
    !_latex2: {}
""".format(
    alias_string(left_parenthesis, alias=False, lang_from="latex"),
    alias_string(right_parenthesis, alias=False, lang_from="latex"),
    alias_string(binary_functions, alias=False, lang_from="latex"),
    alias_string(unary_functions, alias=False, lang_from="latex"),
    alias_string(
        dict(islice(smb.items(), len(smb) // 2)),
        alias=False,
        lang_from="latex",
    ),
    alias_string(
        dict(islice(smb.items(), len(smb) // 2, len(smb))),
        alias=False,
        lang_from="latex",
    ),
)
