from collections import OrderedDict, defaultdict
from typing import Optional

OPENING_BRACE = "{"
CLOSING_BRACE = "}"
BRACES = "{}"

OPENING_BRACKET = "["
CLOSING_BRACKET = "]"
BRACKETS = "[]"

OPENING_PARENTHESIS = "("
CLOSING_PARENTHESIS = ")"
PARENTHESES = "()"

SUBSUP = "_^"
SUBSCRIPT = "_"
SUPERSCRIPT = "^"
APOSTROPHE = "'"
PRIME = r"\prime"
DPRIME = r"\dprime"

LEFT = r"\left"
MIDDLE = r"\middle"
RIGHT = r"\right"

ABOVE = r"\above"
ABOVEWITHDELIMS = r"\abovewithdelims"
ATOP = r"\atop"
ATOPWITHDELIMS = r"\atopwithdelims"
BINOM = r"\binom"
BRACE = r"\brace"
BRACK = r"\brack"
CFRAC = r"\cfrac"
CHOOSE = r"\choose"
DBINOM = r"\dbinom"
DFRAC = r"\dfrac"
FRAC = r"\frac"
GENFRAC = r"\genfrac"
OVER = r"\over"
TBINOM = r"\tbinom"
TFRAC = r"\tfrac"

ROOT = r"\root"
SQRT = r"\sqrt"

OVERSET = r"\overset"
UNDERSET = r"\underset"

ACUTE = r"\acute"
BAR = r"\bar"
BREVE = r"\breve"
CHECK = r"\check"
DOT = r"\dot"
DDOT = r"\ddot"
DDDOT = r"\dddot"
DDDDOT = r"\ddddot"
GRAVE = r"\grave"
HAT = r"\hat"
MATHRING = r"\mathring"
OVERBRACE = r"\overbrace"
OVERLEFTARROW = r"\overleftarrow"
OVERLEFTRIGHTARROW = r"\overleftrightarrow"
OVERLINE = r"\overline"
OVERPAREN = r"\overparen"
OVERRIGHTARROW = r"\overrightarrow"
TILDE = r"\tilde"
UNDERBRACE = r"\underbrace"
UNDERLEFTARROW = r"\underleftarrow"
UNDERLINE = r"\underline"
UNDERPAREN = r"\underparen"
UNDERRIGHTARROW = r"\underrightarrow"
UNDERLEFTRIGHTARROW = r"\underleftrightarrow"
VEC = r"\vec"
WIDEHAT = r"\widehat"
WIDETILDE = r"\widetilde"
XLEFTARROW = r"\xleftarrow"
XRIGHTARROW = r"\xrightarrow"

HREF = r"\href"
TEXT = r"\text"
TEXTBF = r"\textbf"
TEXTIT = r"\textit"
TEXTRM = r"\textrm"
TEXTSF = r"\textsf"
TEXTTT = r"\texttt"

BEGIN = r"\begin"
END = r"\end"

LIMITS = r"\limits"
INTEGRAL = r"\int"
SUMMATION = r"\sum"
PRODUCT = r"\prod"
LIMIT = (r"\lim", r"\sup", r"\inf", r"\max", r"\min")

OPERATORNAME = r"\operatorname"

LBRACE = r"\{"

FUNCTIONS = (
    r"\arccos",
    r"\arcsin",
    r"\arctan",
    r"\cos",
    r"\cosh",
    r"\cot",
    r"\coth",
    r"\csc",
    r"\deg",
    r"\dim",
    r"\exp",
    r"\hom",
    r"\ker",
    r"\ln",
    r"\lg",
    r"\log",
    r"\sec",
    r"\sin",
    r"\sinh",
    r"\tan",
    r"\tanh",
)
DETERMINANT = r"\det"
GCD = r"\gcd"
INTOP = r"\intop"
INJLIM = r"\injlim"
LIMINF = r"\liminf"
LIMSUP = r"\limsup"
PR = r"\Pr"
PROJLIM = r"\projlim"
MOD = r"\mod"
PMOD = r"\pmod"
BMOD = r"\bmod"

HDASHLINE = r"\hdashline"
HLINE = r"\hline"
HFIL = r"\hfil"

CASES = r"\cases"
DISPLAYLINES = r"\displaylines"
SMALLMATRIX = r"\smallmatrix"
SUBSTACK = r"\substack"
SPLIT = r"\split"
ALIGN = r"\align*"
MATRICES = (
    r"\matrix",
    r"\matrix*",
    r"\pmatrix",
    r"\pmatrix*",
    r"\bmatrix",
    r"\bmatrix*",
    r"\Bmatrix",
    r"\Bmatrix*",
    r"\vmatrix",
    r"\vmatrix*",
    r"\Vmatrix",
    r"\Vmatrix*",
    r"\array",
    SUBSTACK,
    CASES,
    DISPLAYLINES,
    SMALLMATRIX,
    SPLIT,
    ALIGN,
)

BACKSLASH = "\\"
CARRIAGE_RETURN = r"\cr"

COLON = r"\:"
COMMA = r"\,"
DOUBLEBACKSLASH = r"\\"
ENSPACE = r"\enspace"
EXCLAMATION = r"\!"
GREATER_THAN = r"\>"
HSKIP = r"\hskip"
HSPACE = r"\hspace"
KERN = r"\kern"
MKERN = r"\mkern"
MSKIP = r"\mskip"
MSPACE = r"\mspace"
NEGTHINSPACE = r"\negthinspace"
NEGMEDSPACE = r"\negmedspace"
NEGTHICKSPACE = r"\negthickspace"
NOBREAKSPACE = r"\nobreakspace"
SPACE = r"\space"
THINSPACE = r"\thinspace"
QQUAD = r"\qquad"
QUAD = r"\quad"
SEMICOLON = r"\;"

BLACKBOARD_BOLD = r"\Bbb"
BOLD_SYMBOL = r"\boldsymbol"
MIT = r"\mit"
OLDSTYLE = r"\oldstyle"
SCR = r"\scr"
TT = r"\tt"

MATH = r"\math"
MATHBB = r"\mathbb"
MATHBF = r"\mathbf"
MATHCAL = r"\mathcal"
MATHFRAK = r"\mathfrak"
MATHIT = r"\mathit"
MATHRM = r"\mathrm"
MATHSCR = r"\mathscr"
MATHSF = r"\mathsf"
MATHTT = r"\mathtt"

BOXED = r"\boxed"
FBOX = r"\fbox"
HBOX = r"\hbox"
MBOX = r"\mbox"

COLOR = r"\color"
DISPLAYSTYLE = r"\displaystyle"
TEXTSTYLE = r"\textstyle"
SCRIPTSTYLE = r"\scriptstyle"
SCRIPTSCRIPTSTYLE = r"\scriptscriptstyle"
STYLE = r"\style"

HPHANTOM = r"\hphantom"
PHANTOM = r"\phantom"
VPHANTOM = r"\vphantom"

IDOTSINT = r"\idotsint"
LATEX = r"\LaTeX"
TEX = r"\TeX"

SIDESET = r"\sideset"

SKEW = r"\skew"
NOT = r"\not"


def font_factory(default: Optional[str], replacement: dict[str, Optional[str]]) -> defaultdict[str, Optional[str]]:
    fonts = defaultdict(lambda: default, replacement)
    return fonts


LOCAL_FONTS: dict[str, defaultdict[str, Optional[str]]] = {
    BLACKBOARD_BOLD: font_factory("double-struck", {"fence": None}),
    BOLD_SYMBOL: font_factory("bold", {"mi": "bold-italic", "mtext": None}),
    MATHBB: font_factory("double-struck", {"fence": None}),
    MATHBF: font_factory("bold", {"fence": None}),
    MATHCAL: font_factory("script", {"fence": None}),
    MATHFRAK: font_factory("fraktur", {"fence": None}),
    MATHIT: font_factory("italic", {"fence": None}),
    MATHRM: font_factory(None, {"mi": "normal"}),
    MATHSCR: font_factory("script", {"fence": None}),
    MATHSF: font_factory(None, {"mi": "sans-serif"}),
    MATHTT: font_factory("monospace", {"fence": None}),
    MIT: font_factory("italic", {"fence": None, "mi": None}),
    OLDSTYLE: font_factory("normal", {"fence": None}),
    SCR: font_factory("script", {"fence": None}),
    TT: font_factory("monospace", {"fence": None}),
}

OLD_STYLE_FONTS: dict[str, defaultdict[str, Optional[str]]] = {
    r"\rm": font_factory(None, {"mi": "normal"}),
    r"\bf": font_factory(None, {"mi": "bold"}),
    r"\it": font_factory(None, {"mi": "italic"}),
    r"\sf": font_factory(None, {"mi": "sans-serif"}),
    r"\tt": font_factory(None, {"mi": "monospace"}),
}

GLOBAL_FONTS = {
    **OLD_STYLE_FONTS,
    r"\cal": font_factory("script", {"fence": None}),
    r"\frak": font_factory("fraktur", {"fence": None}),
}

COMMANDS_WITH_ONE_PARAMETER = (
    ACUTE,
    BAR,
    BLACKBOARD_BOLD,
    BOLD_SYMBOL,
    BOXED,
    BREVE,
    CHECK,
    DOT,
    DDOT,
    DDDOT,
    DDDDOT,
    GRAVE,
    HAT,
    HPHANTOM,
    MATHRING,
    MIT,
    MOD,
    OLDSTYLE,
    OVERBRACE,
    OVERLEFTARROW,
    OVERLEFTRIGHTARROW,
    OVERLINE,
    OVERPAREN,
    OVERRIGHTARROW,
    PHANTOM,
    PMOD,
    SCR,
    TILDE,
    TT,
    UNDERBRACE,
    UNDERLEFTARROW,
    UNDERLINE,
    UNDERPAREN,
    UNDERRIGHTARROW,
    UNDERLEFTRIGHTARROW,
    VEC,
    VPHANTOM,
    WIDEHAT,
    WIDETILDE,
)
COMMANDS_WITH_TWO_PARAMETERS = (
    BINOM,
    CFRAC,
    DBINOM,
    DFRAC,
    FRAC,
    OVERSET,
    TBINOM,
    TFRAC,
    UNDERSET,
)

BIG: dict[str, tuple[str, dict]] = {
    # command: (mathml_equivalent, attributes)
    r"\Bigg": ("mo", OrderedDict([("minsize", "2.470em"), ("maxsize", "2.470em")])),
    r"\bigg": ("mo", OrderedDict([("minsize", "2.047em"), ("maxsize", "2.047em")])),
    r"\Big": ("mo", OrderedDict([("minsize", "1.623em"), ("maxsize", "1.623em")])),
    r"\big": ("mo", OrderedDict([("minsize", "1.2em"), ("maxsize", "1.2em")])),
}

BIG_OPEN_CLOSE = {
    command + postfix: (tag, OrderedDict([("stretchy", "true"), ("fence", "true"), *attrib.items()]))
    for command, (tag, attrib) in BIG.items()
    for postfix in "lmr"
}

MSTYLE_SIZES: dict[str, tuple[str, dict]] = {
    # command: (mathml_equivalent, attributes)
    r"\Huge": ("mstyle", {"mathsize": "2.49em"}),
    r"\huge": ("mstyle", {"mathsize": "2.07em"}),
    r"\LARGE": ("mstyle", {"mathsize": "1.73em"}),
    r"\Large": ("mstyle", {"mathsize": "1.44em"}),
    r"\large": ("mstyle", {"mathsize": "1.2em"}),
    r"\normalsize": ("mstyle", {"mathsize": "1em"}),
    r"\scriptsize": ("mstyle", {"mathsize": "0.7em"}),
    r"\small": ("mstyle", {"mathsize": "0.85em"}),
    r"\tiny": ("mstyle", {"mathsize": "0.5em"}),
    r"\Tiny": ("mstyle", {"mathsize": "0.6em"}),
}

STYLES: dict[str, tuple[str, dict]] = {
    DISPLAYSTYLE: ("mstyle", {"displaystyle": "true", "scriptlevel": "0"}),
    TEXTSTYLE: ("mstyle", {"displaystyle": "false", "scriptlevel": "0"}),
    SCRIPTSTYLE: ("mstyle", {"displaystyle": "false", "scriptlevel": "1"}),
    SCRIPTSCRIPTSTYLE: ("mstyle", {"displaystyle": "false", "scriptlevel": "2"}),
}

CONVERSION_MAP: dict[str, tuple[str, dict]] = {
    # command: (mathml_equivalent, attributes)
    # tables
    **{matrix: ("mtable", {}) for matrix in MATRICES},
    DISPLAYLINES: ("mtable", {"rowspacing": "0.5em", "columnspacing": "1em", "displaystyle": "true"}),
    SMALLMATRIX: ("mtable", {"rowspacing": "0.1em", "columnspacing": "0.2778em"}),
    SPLIT: (
        "mtable",
        {"displaystyle": "true", "columnspacing": "0em", "rowspacing": "3pt"},
    ),
    ALIGN: (
        "mtable",
        {"displaystyle": "true", "rowspacing": "3pt"},
    ),
    # subscripts/superscripts
    SUBSCRIPT: ("msub", {}),
    SUPERSCRIPT: ("msup", {}),
    SUBSUP: ("msubsup", {}),
    # fractions
    BINOM: ("mfrac", {"linethickness": "0"}),
    CFRAC: ("mfrac", {}),
    DBINOM: ("mfrac", {"linethickness": "0"}),
    DFRAC: ("mfrac", {}),
    FRAC: ("mfrac", {}),
    GENFRAC: ("mfrac", {}),
    TBINOM: ("mfrac", {"linethickness": "0"}),
    TFRAC: ("mfrac", {}),
    # over/under
    ACUTE: ("mover", {}),
    BAR: ("mover", {}),
    BREVE: ("mover", {}),
    CHECK: ("mover", {}),
    DOT: ("mover", {}),
    DDOT: ("mover", {}),
    DDDOT: ("mover", {}),
    DDDDOT: ("mover", {}),
    GRAVE: ("mover", {}),
    HAT: ("mover", {}),
    LIMITS: ("munderover", {}),
    MATHRING: ("mover", {}),
    OVERBRACE: ("mover", {}),
    OVERLEFTARROW: ("mover", {}),
    OVERLEFTRIGHTARROW: ("mover", {}),
    OVERLINE: ("mover", {}),
    OVERPAREN: ("mover", {}),
    OVERRIGHTARROW: ("mover", {}),
    TILDE: ("mover", {}),
    OVERSET: ("mover", {}),
    UNDERBRACE: ("munder", {}),
    UNDERLEFTARROW: ("munder", {}),
    UNDERLINE: ("munder", {}),
    UNDERPAREN: ("munder", {}),
    UNDERRIGHTARROW: ("munder", {}),
    UNDERLEFTRIGHTARROW: ("munder", {}),
    UNDERSET: ("munder", {}),
    VEC: ("mover", {}),
    WIDEHAT: ("mover", {}),
    WIDETILDE: ("mover", {}),
    # spaces
    COLON: ("mspace", {"width": "0.222em"}),
    COMMA: ("mspace", {"width": "0.167em"}),
    DOUBLEBACKSLASH: ("mspace", {"linebreak": "newline"}),
    ENSPACE: ("mspace", {"width": "0.5em"}),
    EXCLAMATION: ("mspace", {"width": "negativethinmathspace"}),
    GREATER_THAN: ("mspace", {"width": "0.222em"}),
    HSKIP: ("mspace", {}),
    HSPACE: ("mspace", {}),
    KERN: ("mspace", {}),
    MKERN: ("mspace", {}),
    MSKIP: ("mspace", {}),
    MSPACE: ("mspace", {}),
    NEGTHINSPACE: ("mspace", {"width": "negativethinmathspace"}),
    NEGMEDSPACE: ("mspace", {"width": "negativemediummathspace"}),
    NEGTHICKSPACE: ("mspace", {"width": "negativethickmathspace"}),
    THINSPACE: ("mspace", {"width": "thinmathspace"}),
    QQUAD: ("mspace", {"width": "2em"}),
    QUAD: ("mspace", {"width": "1em"}),
    SEMICOLON: ("mspace", {"width": "0.278em"}),
    # enclose
    BOXED: ("menclose", {"notation": "box"}),
    FBOX: ("menclose", {"notation": "box"}),
    # operators
    **BIG,
    **BIG_OPEN_CLOSE,
    **MSTYLE_SIZES,
    **{limit: ("mo", {}) for limit in LIMIT},
    LEFT: ("mo", OrderedDict([("stretchy", "true"), ("fence", "true"), ("form", "prefix")])),
    MIDDLE: ("mo", OrderedDict([("stretchy", "true"), ("fence", "true"), ("lspace", "0.05em"), ("rspace", "0.05em")])),
    RIGHT: ("mo", OrderedDict([("stretchy", "true"), ("fence", "true"), ("form", "postfix")])),
    # styles
    COLOR: ("mstyle", {}),
    **STYLES,
    # others
    SQRT: ("msqrt", {}),
    ROOT: ("mroot", {}),
    HREF: ("mtext", {}),
    TEXT: ("mtext", {}),
    TEXTBF: ("mtext", {"mathvariant": "bold"}),
    TEXTIT: ("mtext", {"mathvariant": "italic"}),
    TEXTRM: ("mtext", {}),
    TEXTSF: ("mtext", {"mathvariant": "sans-serif"}),
    TEXTTT: ("mtext", {"mathvariant": "monospace"}),
    HBOX: ("mtext", {}),
    MBOX: ("mtext", {}),
    HPHANTOM: ("mphantom", {}),
    PHANTOM: ("mphantom", {}),
    VPHANTOM: ("mphantom", {}),
    SIDESET: ("mrow", {}),
    SKEW: ("mrow", {}),
    MOD: ("mi", {}),
    PMOD: ("mi", {}),
    BMOD: ("mo", {}),
    XLEFTARROW: ("mover", {}),
    XRIGHTARROW: ("mover", {}),
}


DIACRITICS: dict[str, tuple[str, dict[str, str]]] = {
    ACUTE: ("&#x000B4;", {}),
    BAR: ("&#x000AF;", {"stretchy": "true"}),
    BREVE: ("&#x002D8;", {}),
    CHECK: ("&#x002C7;", {}),
    DOT: ("&#x002D9;", {}),
    DDOT: ("&#x000A8;", {}),
    DDDOT: ("&#x020DB;", {}),
    DDDDOT: ("&#x020DC;", {}),
    GRAVE: ("&#x00060;", {}),
    HAT: ("&#x0005E;", {"stretchy": "false"}),
    MATHRING: ("&#x002DA;", {}),
    OVERBRACE: ("&#x23DE;", {}),
    OVERLEFTARROW: ("&#x02190;", {}),
    OVERLEFTRIGHTARROW: ("&#x02194;", {}),
    OVERLINE: ("&#x02015;", {"accent": "true"}),
    OVERPAREN: ("&#x023DC;", {}),
    OVERRIGHTARROW: ("&#x02192;", {}),
    TILDE: ("&#x0007E;", {"stretchy": "false"}),
    UNDERBRACE: ("&#x23DF;", {}),
    UNDERLEFTARROW: ("&#x02190;", {}),
    UNDERLEFTRIGHTARROW: ("&#x02194;", {}),
    UNDERLINE: ("&#x02015;", {"accent": "true"}),
    UNDERPAREN: ("&#x023DD;", {}),
    UNDERRIGHTARROW: ("&#x02192;", {}),
    VEC: ("&#x02192;", {"stretchy": "true"}),
    WIDEHAT: ("&#x0005E;", {}),
    WIDETILDE: ("&#x0007E;", {}),
}
