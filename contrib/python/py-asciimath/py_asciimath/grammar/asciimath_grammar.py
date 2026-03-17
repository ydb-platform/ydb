from itertools import islice

from ..asciimath import get_symbols_for
from ..utils.utils import alias_string

unary_functions = get_symbols_for("unary_functions", None)
binary_functions = get_symbols_for("binary_functions", None)
left_parenthesis = get_symbols_for("left_parenthesis", None)
right_parenthesis = get_symbols_for("right_parenthesis", None)

smb = get_symbols_for("misc_symbols", None)
smb.update(get_symbols_for("colors", None))
smb.update(get_symbols_for("function_symbols", None))
smb.update(get_symbols_for("relation_symbols", None))
smb.update(get_symbols_for("logical_symbols", None))
smb.update(get_symbols_for("operation_symbols", None))
smb.update(get_symbols_for("greek_letters", None))
smb.update(get_symbols_for("arrows", None))
smb = dict(sorted(smb.items(), key=lambda x: (-len(x[0]), x[0])))

asciimath_grammar = r"""
    %import common.WS
    %import common.LETTER
    %import common.NUMBER
    %ignore WS
    start: i start* -> exp
    i: s -> exp_interm
        | s "/" s -> exp_frac
        | s "_" s -> exp_under
        | s "^" s -> exp_super
        | s "_" s "^" s -> exp_under_super
    s: _l start? _r -> exp_par
        | _u s -> exp_unary
        | _b s s -> exp_binary
        | _asciimath1 -> symbol
        | _asciimath2 -> symbol
        | _c -> const
        | QS -> q_str
    !_c: /d[A-Za-z]/
        | NUMBER
        | LETTER
    !_l: {} // left parenthesis
    !_r: {} // right parenthesis
    !_b: {} // binary functions
    !_u: {} // unary functions
    !_asciimath1: {}
    !_asciimath2: {}
    QS: "\"" /(?<=").+(?=")/ "\"" // Quoted String
""".format(
    alias_string(left_parenthesis, alias=False),
    alias_string(right_parenthesis, alias=False),
    alias_string(binary_functions, alias=False),
    alias_string(unary_functions, alias=False),
    alias_string(dict(islice(smb.items(), len(smb) // 2)), alias=False),
    alias_string(
        dict(islice(smb.items(), len(smb) // 2, len(smb))), alias=False
    ),
)
