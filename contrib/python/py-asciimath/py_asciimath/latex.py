import sys


def get_symbols_for(symbol_group, lang_to):  # pragma: no cover
    return {
        k: (
            None
            if lang_to is None
            else (
                v[lang_to]
                if not isinstance(v[lang_to], list)
                else v[lang_to][0]
            )
        )
        for k, v in getattr(sys.modules[__name__], symbol_group).items()
    }


binary_functions = {
    "\\frac": {"asciimath": "frac", "mathml": "<mfrac>{}{}</mfrac>"},
    # "\\sqrt": {"asciimath": "root", "mathml": "<mroot>{}{}</mroot>"},
    "\\stackrel": {"asciimath": "stackrel", "mathml": "<mover>{}{}</mover>"},
    "\\overset": {"asciimath": "overset", "mathml": "<mover>{}{}</mover>"},
    "\\underset": {
        "asciimath": "underset",
        "mathml": "<munder>{}{}</munder>",
    },
    "\\textcolor": {
        "asciimath": "color",
        "mathml": "<mstyle mathcolor='{}'>{}</mstyle>",
    },
}

unary_functions = {
    "\\sqrt": {"asciimath": "sqrt", "mathml": "<msqrt>{}</msqrt>"},
    "\\text": {"asciimath": "text", "mathml": "<mtext>{}</mtext>"},
    "\\textrm": {"asciimath": "text", "mathml": "<mtext>{}</mtext>"},
    "\\mathrm": {"asciimath": "text", "mathml": "<mtext>{}</mtext>"},
    "\\underbrace": {
        "asciimath": ["ubrace", "underbrace"],
        "mathml": "<munder>{}<mo>&#x23DF;</mo></munder>",
    },
    "\\overbrace": {
        "asciimath": ["obrace", "overbrace"],
        "mathml": "<mover>{}<mo>&#x23DF;</mo></mover>",
    },
    "\\cancel": {
        "asciimath": "cancel",
        "mathml": "<menclose notation='updiagonalstrike'>{}</menclose>",
    },
    "\\boldsymbol": {
        "asciimath": "bb",
        "mathml": "<mstyle mathvariant='bold'>{}</mstyle>",
    },
    "\\mathbb": {
        "asciimath": "bbb",
        "mathml": "<mstyle mathvariant='double-struck'>{}</mstyle>",
    },
    "\\mathcal": {
        "asciimath": "cc",
        "mathml": "<mstyle mathvariant='script'>{}</mstyle>",
    },
    "\\texttt": {
        "asciimath": "tt",
        "mathml": "<mstyle mathvariant='monospace'>{}</mstyle>",
    },
    "\\mathfrak": {
        "asciimath": "fr",
        "mathml": "<mstyle mathvariant='fraktur'>{}</mstyle>",
    },
    "\\textsf": {
        "asciimath": "sf",
        "mathml": "<mstyle mathvariant='sanf-serif'>{}</mstyle>",
    },
    "\\underline": {
        "asciimath": ["ul", "underline"],
        "mathml": "<munder>{}<mo>&#x332;</mo></munder>",
    },
    "\\overline": {
        "asciimath": ["bar", "overline"],
        "mathml": "<mover>{}<mo>&#x332;</mo></mover>",
    },
    "\\hat": {"asciimath": "hat", "mathml": "<mover>{}<mo>^</mo></mover>"},
    "\\vec": {
        "asciimath": "vec",
        "mathml": "<mover>{}<mo stretchy='false'>&#x2192;</mo></mover>",
    },
    "\\dot": {
        "asciimath": "dot",
        "mathml": "<mover>{}<mo stretchy='false'>.</mo></mover>",
    },
    "\\ddot": {
        "asciimath": "ddot",
        "mathml": "<mover>{}<mo stretchy='false'>..</mo></mover>",
    },
    "\\displaystyle": {
        "asciimath": "dstyle",
        "mathml": "<mstyle displaystyle='true'>{}</mstyle>",
    },
}

operation_symbols = {
    "+": {"asciimath": "+", "mathml": "+"},
    "\\cdot": {"asciimath": ["*", "cdot"], "mathml": "&sdot;"},
    "-": {"asciimath": "-", "mathml": "-"},
    "*": {"asciimath": ["**", "ast"], "mathml": "&ast;"},
    "\\ast": {"asciimath": ["**", "ast"], "mathml": "&ast;"},
    "\\star": {"asciimath": ["***", "star"], "mathml": "&Star;"},
    "/": {"asciimath": "//", "mathml": "/"},
    "\\setminus": {"asciimath": [r"\\", "setminus"], "mathml": "&setminus;"},
    "\\times": {"asciimath": ["xx", "times"], "mathml": "&times;"},
    "\\div": {"asciimath": ["-:", "div"], "mathml": "&div;"},
    "\\ltimes": {"asciimath": ["|><", "ltimes"], "mathml": "&ltimes;"},
    "\\rtimes": {"asciimath": ["><|", "rtimes"], "mathml": "&rtimes;"},
    "\\bowtie": {"asciimath": ["|><|", "bowtie"], "mathml": "&bowtie;"},
    "\\circ": {"asciimath": ["@", "circ"], "mathml": "&SmallCircle;"},
    "\\oplus": {"asciimath": ["o+", "oplus"], "mathml": "&oplus;"},
    "\\otimes": {"asciimath": ["ox", "otimes"], "mathml": "&times;"},
    "\\odot": {"asciimath": ["o.", "odot"], "mathml": "&odot;"},
    "\\sum": {"asciimath": "sum", "mathml": "&sum;"},
    "\\prod": {"asciimath": "prod", "mathml": "&prod;"},
    "\\wedge": {"asciimath": ["^^", "wedge"], "mathml": "&wedge;"},
    "\\bigwedge": {"asciimath": ["^^^", "bigwedge"], "mathml": "&bigwedge;"},
    "\\vee": {"asciimath": ["vv", "vee"], "mathml": "&vee;"},
    "\\bigvee": {"asciimath": ["vvv", "bigvee"], "mathml": "&bigvee;"},
    "\\cap": {"asciimath": ["nn", "cap"], "mathml": "&cap;"},
    "\\bigcap": {"asciimath": ["nnn", "bigcap"], "mathml": "&bigcap;"},
    "\\cup": {"asciimath": ["uu", "cup"], "mathml": "&cup;"},
    "\\bigcup": {"asciimath": ["uuu", "bigcup"], "mathml": "&bigcup;"},
}

logical_symbols = {
    "\\mathmr{and}": {"asciimath": "and", "mathml": "and"},
    "\\mathmr{or}": {"asciimath": "or", "mathml": "or"},
    "\\neg": {"asciimath": ["not", "neg"], "mathml": "&not;"},
    "\\implies": {"asciimath": ["=>", "implies"], "mathml": "&Implies;"},
    "\\mathmr{if}": {"asciimath": "if", "mathml": "if"},
    "\\iff": {"asciimath": ["<=>", "iff"], "mathml": "&iff;"},
    "\\forall": {"asciimath": ["AA", "forall"], "mathml": "&ForAll;"},
    "\\exists": {"asciimath": ["EE", "exists"], "mathml": "&Exists;"},
    "\\bot": {"asciimath": ["_|_", "bot"], "mathml": "&bot;"},
    "\\top": {"asciimath": ["TT", "top"], "mathml": "&top;"},
    "\\vdash": {"asciimath": ["|--", "vdash"], "mathml": "&RightTee;"},
    "\\models": {"asciimath": ["|==", "models"], "mathml": "&DoubleRightTee;"},
}

relation_symbols = {
    "=": {"asciimath": "=", "mathml": "="},
    "\\ne": {"asciimath": ["!=", "ne"], "mathml": "&NotEqual;"},
    "<": {"asciimath": ["<", "lt"], "mathml": "&lt;"},
    ">": {"asciimath": [">", "gt"], "mathml": "&gt;"},
    "\\le": {"asciimath": ["<=", "le"], "mathml": "&leq;"},
    "\\ge": {"asciimath": [">=", "ge"], "mathml": "&geq;"},
    "\\leqslant": {"asciimath": ["<=", "le"], "mathml": "&leq;"},
    "\\geqslant": {"asciimath": [">=", "ge"], "mathml": "&geq;"},
    "\\prec": {"asciimath": ["-<", "prec"], "mathml": "&Precedes;"},
    "\\preceq": {"asciimath": ["-<=", "preceq"], "mathml": "&PrecedesEqual;"},
    "\\succ": {"asciimath": [">-", "succ"], "mathml": "&Succeeds;"},
    "\\succeq": {"asciimath": [">-=", "succeq"], "mathml": "&SucceedsEqual;"},
    "\\in": {"asciimath": "in", "mathml": "&in;"},
    "\\notin": {"asciimath": ["!in", "notin"], "mathml": "&notin;"},
    "\\subset": {"asciimath": ["sub", "subset"], "mathml": "&subset;"},
    "\\supset": {"asciimath": ["sup", "supset"], "mathml": "&supset;"},
    "\\subseteq": {
        "asciimath": ["sube", "subseteq"],
        "mathml": "&SubsetEqual;",
    },
    "\\supseteq": {
        "asciimath": ["supe", "supseteq"],
        "mathml": "&SupersetEqual;",
    },
    "\\equiv": {"asciimath": ["-=", "equiv"], "mathml": "&equiv;"},
    "\\cong": {"asciimath": ["~=", "cong"], "mathml": "&cong;"},
    "\\approx": {"asciimath": ["~~", "approx"], "mathml": "&approx;"},
    "\\propto": {"asciimath": ["prop", "propto"], "mathml": "&prop;"},
}

function_symbols = {
    "\\sin": {"asciimath": "sin", "mathml": "sin"},
    "\\cos": {"asciimath": "cos", "mathml": "cos"},
    "\\tan": {"asciimath": "tan", "mathml": "tan"},
    "\\sec": {"asciimath": "sec", "mathml": "sec"},
    "\\csc": {"asciimath": "csc", "mathml": "csc"},
    "\\cot": {"asciimath": "cot", "mathml": "cot"},
    "\\arcsin": {"asciimath": "arcsin", "mathml": "arcsin"},
    "\\arccos": {"asciimath": "arccos", "mathml": "arccos"},
    "\\arctan": {"asciimath": "arctan", "mathml": "arctan"},
    "\\sinh": {"asciimath": "sinh", "mathml": "sinh"},
    "\\cosh": {"asciimath": "cosh", "mathml": "cosh"},
    "\\tanh": {"asciimath": "tanh", "mathml": "tanh"},
    "\\sech": {"asciimath": "sech", "mathml": "sech"},
    "\\csch": {"asciimath": "csch", "mathml": "csch"},
    "\\coth": {"asciimath": "coth", "mathml": "coth"},
    "\\exp": {"asciimath": "exp", "mathml": "exp"},
    "\\log": {"asciimath": "log", "mathml": "log"},
    "\\ln": {"asciimath": "ln", "mathml": "ln"},
    "\\lg": {"asciimath": "lg", "mathml": "lg"},
    "\\det": {"asciimath": "det", "mathml": "det"},
    "\\dim": {"asciimath": "dim", "mathml": "dim"},
    "\\mod": {"asciimath": "mod", "mathml": "mod"},
    "\\gcd": {"asciimath": "gcd", "mathml": "gcd"},
    "\\lcm": {"asciimath": "lcm", "mathml": "lcm"},
    "\\lub": {"asciimath": "lub", "mathml": "lub"},
    "\\glb": {"asciimath": "glb", "mathml": "glb"},
    "\\min": {"asciimath": "min", "mathml": "min"},
    "\\max": {"asciimath": "max", "mathml": "max"},
    "\\lim": {"asciimath": "lim", "mathml": "lim"},
    "f": {"asciimath": "f", "mathml": "f"},
    "g": {"asciimath": "g", "mathml": "g"},
}

greek_letters = {
    "\\alpha": {"asciimath": "alpha", "mathml": "&alpha;"},
    "\\beta": {"asciimath": "beta", "mathml": "&beta;"},
    "\\gamma": {"asciimath": "gamma", "mathml": "&gamma;"},
    "\\Gamma": {"asciimath": "Gamma", "mathml": "&Gamma;"},
    "\\delta": {"asciimath": "delta", "mathml": "&delta;"},
    "\\Delta": {"asciimath": "Delta", "mathml": "&Delta;"},
    "\\epsilon": {"asciimath": "epsilon", "mathml": "&epsiv;"},
    "\\varepsilon": {"asciimath": "varepsilon", "mathml": "&varepsilon;"},
    "\\zeta": {"asciimath": "zeta", "mathml": "&zeta;"},
    "\\eta": {"asciimath": "eta", "mathml": "&eta;"},
    "\\theta": {"asciimath": "theta", "mathml": "&theta;"},
    "\\Theta": {"asciimath": "Theta", "mathml": "&Theta;"},
    "\\vartheta": {"asciimath": "vartheta", "mathml": "&vartheta;"},
    "\\iota": {"asciimath": "iota", "mathml": "&iota;"},
    "\\kappa": {"asciimath": "kappa", "mathml": "&kappa;"},
    "\\lambda": {"asciimath": "lambda", "mathml": "&lambda;"},
    "\\Lambda": {"asciimath": "Lambda", "mathml": "&Lambda;"},
    "\\mu": {"asciimath": "mu", "mathml": "&mu;"},
    "\\nu": {"asciimath": "nu", "mathml": "&nu;"},
    "\\xi": {"asciimath": "xi", "mathml": "&xi;"},
    "\\Xi": {"asciimath": "Xi", "mathml": "&Xi;"},
    "\\pi": {"asciimath": "pi", "mathml": "&pi;"},
    "\\Pi": {"asciimath": "Pi", "mathml": "&Pi;"},
    "\\rho": {"asciimath": "rho", "mathml": "&rho;"},
    "\\sigma": {"asciimath": "sigma", "mathml": "&sigma;"},
    "\\Sigma": {"asciimath": "Sigma", "mathml": "&Sigma;"},
    "\\tau": {"asciimath": "tau", "mathml": "&tau;"},
    "\\upsilon": {"asciimath": "upsilon", "mathml": "&upsilon;"},
    "\\phi": {"asciimath": "phi", "mathml": "&phi;"},
    "\\Phi": {"asciimath": "Phi", "mathml": "&Phi;"},
    "\\varphi": {"asciimath": "varphi", "mathml": "&varphi;"},
    "\\chi": {"asciimath": "chi", "mathml": "&chi;"},
    "\\psi": {"asciimath": "psi", "mathml": "&psi;"},
    "\\Psi": {"asciimath": "Psi", "mathml": "&Psi;"},
    "\\omega": {"asciimath": "omega", "mathml": "&omega;"},
    "\\Omega": {"asciimath": "Omega", "mathml": "&Omega;"},
}

left_parenthesis = {
    "[": {"asciimath": "[", "mathml": "["},
    # ".": {"asciimath": "{:", "mathml": ""},
    "\\{": {"asciimath": "{", "mathml": "{"},
    # "\\vert": {"asciimath": "|:", "mathml": "&VerticalBar;"},
    "\\lVert": {"asciimath": "||:", "mathml": "&DoubleVerticalBar;"},
    "\\langle": {"asciimath": ["langle", "<<", "(:"], "mathml": "&langle;"},
}

right_parenthesis = {
    "]": {"asciimath": "]", "mathml": "]"},
    # ".": {"asciimath": ":}", "mathml": ""},
    "\\}": {"asciimath": "}", "mathml": "}"},
    # "\\vert": {"asciimath": ":|", "mathml": "&VerticalBar;"},
    "\\rVert": {"asciimath": ":||", "mathml": "&DoubleVerticalBar;"},
    "\\rangle": {"asciimath": ["rangle", ">>", ":)"], "mathml": "&rangle;"},
}

arrows = {
    "\\uparrow": {"asciimath": ["uarr", "uparrow"], "mathml": "&uarr;"},
    "\\downarrow": {"asciimath": ["darr", "downarrow"], "mathml": "&darr;"},
    "\\rightarrow": {
        "asciimath": ["rarr", "rArr", "rightarrow"],
        "mathml": "&rarr;",
    },
    "\\to": {"asciimath": ["->", "to"], "mathml": "&rightarrow;"},
    "\\rightarrowtail": {
        "asciimath": [">->", "rightarrowtail"],
        "mathml": "&rightarrowtail;",
    },
    "\\twoheadrightarrow": {
        "asciimath": ["->>", "twoheadrightarrow"],
        "mathml": "&twoheadrightarrow;",
    },
    "\\twoheadrightarrowtail": {
        "asciimath": [">->>", "twoheadrightarrowtail"],
        "mathml": "&Rarrtl;",
    },
    "\\mapsto": {"asciimath": ["|->", "mapsto"], "mathml": ""},
    "\\leftarrow": {"asciimath": ["larr", "leftarrow"], "mathml": "&larr;"},
    "\\leftrightarrow": {
        "asciimath": ["harr", "leftrightarrow"],
        "mathml": "&leftrightarrow;",
    },
    "\\Leftarrow": {"asciimath": ["lArr", "Leftarrow"], "mathml": "&lArr;"},
    "\\Leftrightarrow": {
        "asciimath": ["hArr", "Leftrightarrow"],
        "mathml": "&hArr;",
    },
}

colors = {
    "red": {"asciimath": "red", "mathml": "red"},
}

misc_symbols = {
    "(": {"asciimath": "(", "mathml": "("},
    ")": {"asciimath": ")", "mathml": ")"},
    "^": {"asciimath": "^", "mathml": "&#x5E;"},
    ",": {"asciimath": ",", "mathml": ","},
    ".": {"asciimath": ".", "mathml": "."},
    "_": {"asciimath": "_", "mathml": "_"},
    "'": {"asciimath": "'", "mathml": "'"},
    "|": {"asciimath": "|", "mathml": "|"},
    "\\vert": {"asciimath": "|", "mathml": "|"},
    "\\mid": {"asciimath": "|", "mathml": "|"},
    ":": {"asciimath": ":", "mathml": ":"},
    "\\int": {"asciimath": ["int", "integral"], "mathml": "&Integral;"},
    "\\oint": {"asciimath": "oint", "mathml": "&conint;"},
    "\\partial": {"asciimath": ["del", "partial"], "mathml": "&part;"},
    "\\nabla": {"asciimath": ["grad", "nabla"], "mathml": "&Del;"},
    "\\pm": {"asciimath": ["+-", "pm"], "mathml": "&PlusMinus;"},
    "\\emptyset": {"asciimath": ["O/", "emptyset"], "mathml": "&emptyset;"},
    "\\infty": {"asciimath": ["oo", "infty"], "mathml": "&infin;"},
    "\\aleph": {"asciimath": "aleph", "mathml": "&aleph;"},
    "\\therefore": {"asciimath": [":.", "therefore"], "mathml": "&therefore;"},
    "\\because": {"asciimath": [":'", "because"], "mathml": "&because;"},
    "\\ldots": {"asciimath": ["...", "ldots"], "mathml": "..."},
    "\\cdots": {"asciimath": "cdots", "mathml": "&ctdot;"},
    "\\vdots": {"asciimath": "vdots", "mathml": "&vellip;"},
    "\\ddots": {"asciimath": "ddots", "mathml": "&dtdot;"},
    "\\quad": {"asciimath": "quad", "mathml": "&nbsp;"},
    "\\angle": {"asciimath": ["/_", "angle"], "mathml": "&angle;"},
    "\\frown": {"asciimath": "frown", "mathml": "&frown;"},
    "\\triangle": {
        "asciimath": [r"/_\\", "triangle"],
        "mathml": "&bigtriangleup;",
    },
    "\\diamond": {"asciimath": "diamond", "mathml": "&diamond;"},
    "\\square": {"asciimath": "square", "mathml": "&square;"},
    "\\lfloor": {"asciimath": ["|__", "lfloor"], "mathml": "&lfloor;"},
    "\\rfloor": {"asciimath": ["__|", "rfloor"], "mathml": "&rfloor;"},
    "\\lceiling": {"asciimath": ["|~", "lceiling"], "mathml": "&lceil;"},
    "\\rceiling": {"asciimath": ["~|", "rceiling"], "mathml": "&rceil;"},
    "\\mathbb{C}": {"asciimath": "CC", "mathml": "&Copf;"},
    "\\mathbb{N}": {"asciimath": "NN", "mathml": "&Nopf;"},
    "\\mathbb{Q}": {"asciimath": "QQ", "mathml": "&Qopf;"},
    "\\mathbb{R}": {"asciimath": "RR", "mathml": "&Ropf;"},
    "\\mathbb{Z}": {"asciimath": "ZZ", "mathml": "&Zopf;"},
    "\\prime": {"asciimath": "'", "mathml": "&prime;"},
}

# matrix2par = {
#     "pmatrix": ["(", ")"],
#     "bmatrix": ["[", "]"],
#     "Bmatrix": ["\{", "\}"],
#     "vmatrix": ["|", "|"],
#     "Vmatrix": ["||", "||"],
# }
