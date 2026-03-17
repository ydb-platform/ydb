import logging
import re
from functools import wraps

from lark import Transformer

from ..translation.asciimath2latex import binary_functions as latex_bin
from ..translation.asciimath2latex import left_parenthesis as latex_left
from ..translation.asciimath2latex import right_parenthesis as latex_right
from ..translation.asciimath2latex import smb as latex_smb
from ..translation.asciimath2latex import unary_functions as latex_una
from ..translation.asciimath2mathml import binary_functions as mathml_bin
from ..translation.asciimath2mathml import colors
from ..translation.asciimath2mathml import left_parenthesis as mathml_left
from ..translation.asciimath2mathml import right_parenthesis as mathml_right
from ..translation.asciimath2mathml import smb as mathml_smb
from ..translation.asciimath2mathml import unary_functions as mathml_una
from ..translation.latex2asciimath import binary_functions as l2mml_bin
from ..translation.latex2asciimath import left_parenthesis as l2mml_left
from ..translation.latex2asciimath import right_parenthesis as l2mml_right
from ..translation.latex2asciimath import smb as l2mml_smb
from ..translation.latex2asciimath import unary_functions as l2mml_una
from ..utils.log import Log
from ..utils.utils import UtilsMat, encapsulate_mrow

# standard_library.install_aliases()
logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG)


# TODO: MathematicaTransformer
""" class MathematicaTransformer(Transformer):
    def __init__(self):
        pass """

# ["\\(", "\\(:", "\\[", "\\{", "\\{:"]
# ["\\)", ":\\)", "\\]", "\\}", ":\\}"]


class MathTransformer(Transformer):  # pragma: no cover
    def __init__(
        self,
        log=True,
        start_end_par_pattern="{}{}",
        left_par=[],
        right_par=[],
        visit_tokens=False,
    ):
        Transformer.__init__(self, visit_tokens=visit_tokens)
        formatted_left_parenthesis = "|".join(left_par)
        formatted_right_parenthesis = "|".join(right_par)
        self.start_end_par_pattern = re.compile(
            start_end_par_pattern.format(
                formatted_left_parenthesis, formatted_right_parenthesis,
            )
        )
        self._logger_func = logging.info
        if not log:
            self._logger_func = lambda x: x
        self._logger = Log(logger_func=self._logger_func)

    def remove_parenthesis(self, s):
        return re.sub(self.start_end_par_pattern, r"\2", s)

    @classmethod
    def log(cls, f):
        @wraps(f)
        def decorator(*args, **kwargs):
            self = args[0]
            return self._logger.__call__(f)(*args, **kwargs)

        return decorator

    def exp(self, items):
        raise NotImplementedError

    def exp_interm(self, items):
        raise NotImplementedError

    def exp_frac(self, items):
        raise NotImplementedError

    def exp_under(self, items):
        raise NotImplementedError

    def exp_super(self, items):
        raise NotImplementedError

    def exp_under_super(self, items):
        raise NotImplementedError

    def exp_par(self, items):
        raise NotImplementedError

    def exp_unary(self, items):
        raise NotImplementedError

    def exp_binary(self, items):
        raise NotImplementedError

    def symbol(self, items):
        raise NotImplementedError

    def const(self, items):
        raise NotImplementedError

    def q_str(self, items):
        raise NotImplementedError


class ASCIIMath2TexTransformer(MathTransformer):
    """Trasformer class, read `lark.Transformer`."""

    def __init__(self, log=True, visit_tokens=False):
        MathTransformer.__init__(
            self,
            log,
            r"^(?:\\left(?:(?:\\)?({})))(.*?)(?:\\right(?:(?:\\)?({})))$",
            ["\\(", "\\(:", "\\[", "\\{", "\\{:"],
            ["\\)", ":\\)", "\\]", "\\}", ":\\}"],
            visit_tokens,
        )

    @MathTransformer.log
    def exp(self, items):
        return " ".join(items)

    @MathTransformer.log
    def exp_interm(self, items):
        return items[0]

    @MathTransformer.log
    def exp_frac(self, items):
        items[0] = self.remove_parenthesis(items[0])
        items[1] = self.remove_parenthesis(items[1])
        return "\\frac{" + items[0] + "}{" + items[1] + "}"

    @MathTransformer.log
    def exp_under(self, items):
        items[1] = self.remove_parenthesis(items[1])
        return "{" + items[0] + "}_{" + items[1] + "}"

    @MathTransformer.log
    def exp_super(self, items):
        items[1] = self.remove_parenthesis(items[1])
        return "{" + items[0] + "}^{" + items[1] + "}"

    @MathTransformer.log
    def exp_under_super(self, items):
        items[1] = self.remove_parenthesis(items[1])
        items[2] = self.remove_parenthesis(items[2])
        return "{" + items[0] + "}_{" + items[1] + "}^{" + items[2] + "}"

    @MathTransformer.log
    def exp_par(self, items):
        yeah_mat = False
        s = ", ".join(items[1:-1])
        if s.startswith("\\left"):
            yeah_mat, row_par = UtilsMat.check_mat(s)
            if yeah_mat:
                s = UtilsMat.get_latex_mat(s, row_par)
        lpar = (
            "\\left"
            + latex_left[items[0]]
            + (" " if items[0] == "langle" else "")
        )
        rpar = "\\right" + latex_right[items[-1]]
        return (
            lpar
            + ("\\begin{matrix}" + s + "\\end{matrix}" if yeah_mat else s)
            + rpar
        )

    @MathTransformer.log
    def exp_unary(self, items):
        unary = latex_una[items[0]]
        items[1] = self.remove_parenthesis(items[1])
        if unary == "norm":
            return "\\left\\lVert " + items[1] + " \\right\\rVert"
        elif unary == "abs":
            return "\\left\\mid " + items[1] + " \\right\\mid"
        elif unary == "floor":
            return "\\left\\lfloor " + items[1] + " \\right\\rfloor"
        elif unary == "ceil":
            return "\\left\\lceil " + items[1] + " \\right\\rceil"
        else:
            return unary + "{" + items[1] + "}"

    @MathTransformer.log
    def exp_binary(self, items):
        binary = latex_bin[items[0]]
        items[1] = self.remove_parenthesis(items[1])
        items[2] = self.remove_parenthesis(items[2])
        if binary == "\\sqrt":
            return binary + "[" + items[1] + "]" + "{" + items[2] + "}"
        else:
            return binary + "{" + items[1] + "}" + "{" + items[2] + "}"

    @MathTransformer.log
    def symbol(self, items):
        if items[0] == "\\":
            return "\\setminus"
        elif items[0] == "/_\\":
            return "\\triangle"
        return latex_smb[items[0]]

    @MathTransformer.log
    def const(self, items):
        if len(items[0].value) == 2 and items[0].value[0] == "d":
            return "\\mathrm{" + items[0].value + "}"
        return items[0].value

    @MathTransformer.log
    def q_str(self, items):
        return "\\text{" + items[0].strip('"') + "}"


class ASCIIMath2MathMLTransformer(MathTransformer):
    """Trasformer class, read `lark.Transformer`."""

    def __init__(self, log=True, visit_tokens=False):
        MathTransformer.__init__(
            self,
            log,
            r"^(?:<mrow>)?"
            r"(?:<mo>(?:({}))</mo>)"
            r"(.*?)"
            r"(?:<mo>(?:({}))</mo>)"
            r"(?:</mrow>)?$",
            ["\\(", "\\(:", "\\[", "\\{", "\\{:"],
            ["\\)", ":\\)", "\\]", "\\}", ":\\}"],
            visit_tokens,
        )

    @MathTransformer.log
    def exp(self, items):
        return "".join(items)

    @MathTransformer.log
    def exp_interm(self, items):
        return items[0]

    @MathTransformer.log
    def exp_frac(self, items):
        items[0] = self.remove_parenthesis(items[0])
        items[1] = self.remove_parenthesis(items[1])
        return encapsulate_mrow(
            "<mfrac>"
            + encapsulate_mrow(items[0])
            + encapsulate_mrow(items[1])
            + "</mfrac>"
        )

    @MathTransformer.log
    def exp_under(self, items):
        items[1] = self.remove_parenthesis(items[1])
        return encapsulate_mrow(
            "<msub>"
            + encapsulate_mrow(items[0])
            + encapsulate_mrow(items[1])
            + "</msub>"
        )

    @MathTransformer.log
    def exp_super(self, items):
        items[1] = self.remove_parenthesis(items[1])
        return encapsulate_mrow(
            "<msup>"
            + encapsulate_mrow(items[0])
            + encapsulate_mrow(items[1])
            + "</msup>"
        )

    @MathTransformer.log
    def exp_under_super(self, items):
        items[1] = self.remove_parenthesis(items[1])
        items[2] = self.remove_parenthesis(items[2])
        return encapsulate_mrow(
            "<msubsup>"
            + encapsulate_mrow(items[0])
            + encapsulate_mrow(items[1])
            + encapsulate_mrow(items[2])
            + "</msubsup>"
        )

    @MathTransformer.log
    def exp_par(self, items):
        yeah_mat = False
        s = ", ".join(items[1:-1])
        if re.match(
            r"^<mrow><mo>(\[|\(|\{|\{:|\|:|\|\|:|<<|\(:|langle)</mo>", s
        ):
            yeah_mat, row_par = UtilsMat.check_mat(s)
            if yeah_mat:
                s = (
                    "<mtable>"
                    + UtilsMat.get_mathml_mat(s, row_par)
                    + "</mtable>"
                )
        lpar = mathml_left[items[0]]
        rpar = mathml_right[items[-1]]
        return encapsulate_mrow(
            "<mo>"
            + lpar
            + "</mo>"
            + (encapsulate_mrow(s) if not yeah_mat else s)
            + "<mo>"
            + rpar
            + "</mo>"
        )

    @MathTransformer.log
    def exp_unary(self, items):
        unary = mathml_una[items[0]]
        items[1] = self.remove_parenthesis(items[1])
        if items[0] == "text":
            return encapsulate_mrow(
                unary.format(re.sub(r"<.*?>", "", items[1]))
            )
        return encapsulate_mrow(unary.format(encapsulate_mrow(items[1])))

    @MathTransformer.log
    def exp_binary(self, items):
        binary = mathml_bin[items[0]]
        items[1] = self.remove_parenthesis(items[1])
        items[2] = self.remove_parenthesis(items[2])
        if items[1][6:-7] in colors:
            s = binary.format(items[1][6:-7], encapsulate_mrow(items[2]))
        elif items[0] != "root":
            s = binary.format(
                encapsulate_mrow(items[1]), encapsulate_mrow(items[2]),
            )
        else:
            s = binary.format(
                encapsulate_mrow(items[2]), encapsulate_mrow(items[1]),
            )
        return encapsulate_mrow(s)

    @MathTransformer.log
    def symbol(self, items):
        if items[0] in colors:
            return items[0]
        elif items[0] == "\\":
            return "<mo>&setminus;</mo>"
        else:
            return "<mo>" + mathml_smb[items[0]] + "</mo>"

    @MathTransformer.log
    def const(self, items):
        if items[0].isnumeric():
            return "<mn>" + items[0].value + "</mn>"
        else:
            return "<mi>" + items[0].value + "</mi>"

    @MathTransformer.log
    def q_str(self, items):
        return "<mtext>" + items[0].strip('"') + "</mtext>"


class Tex2ASCIIMathTransformer(MathTransformer):  # pragma: no cover
    def __init__(self, log=True, visit_tokens=False):
        MathTransformer.__init__(
            self,
            log,
            r"^(?:(?:(?:\\)?({})))(.*?)(?:(?:(?:\\)?({})))$",
            [r"\(", r"\[", r"\{", r"\\\{", r"\\langle", r"\\lVert"],
            [r"\)", r"\]", r"\}", r"\\\}", r"\\rangle", r"\\rVert"],
            visit_tokens=visit_tokens,
        )

    def log(f):
        @wraps(f)
        def decorator(*args, **kwargs):
            self = args[0]
            return self._logger.__call__(f)(*args, **kwargs)

        return decorator

    def _add_brackets(self, item):
        if item.isdigit() or len(item.split()) == 1:
            return item
        return "(" + item + ")"

    @log
    def exp(self, items):
        return " ".join(items)

    @log
    def exp_interm(self, items):
        return items[0]

    @log
    def exp_frac(self, items):
        items[0] = self.remove_parenthesis(items[0])
        items[1] = self.remove_parenthesis(items[1])
        return self._add_brackets(items[0]) + "/" + self._add_brackets(items[1])

    @log
    def exp_under(self, items):
        items[0] = self.remove_parenthesis(items[0])
        items[1] = self.remove_parenthesis(items[1])
        return self._add_brackets(items[0]) + "_" + self._add_brackets(items[1])

    @log
    def exp_super(self, items):
        items[0] = self.remove_parenthesis(items[0])
        items[1] = self.remove_parenthesis(items[1])
        return self._add_brackets(items[0]) + "^" + self._add_brackets(items[1])

    @log
    def exp_under_super(self, items):
        items[0] = self.remove_parenthesis(items[0])
        items[1] = self.remove_parenthesis(items[1])
        items[2] = self.remove_parenthesis(items[2])
        return self._add_brackets(items[0]) + "_" + self._add_brackets(items[1]) + "^" + self._add_brackets(items[2])

    @log
    def exp_par(self, items):
        left = items[0].value
        right = items[-1].value
        if left == ".":
            left = "{:"
        elif left in ["\\vert", "\\mid"]:
            left = "|:"
        elif left != "[":
            left = l2mml_left[items[0].value]
        if right == ".":
            right = ":}"
        elif right in ["\\vert", "\\mid"]:
            right = ":|"
        elif right != "]":
            right = l2mml_right[items[-1].value]
        return left + " ".join(items[1:-1]) + right

    @log
    def exp_unary(self, items):
        return l2mml_una[items[0]] + "(" + items[1] + ")"

    @log
    def exp_binary(self, items):
        if items[0].startswith("\\sqrt"):
            return "root(" + "".join(items[1:-1]) + ", " + items[-1] + ")"
        if items[0].startswith("\\frac"):
            return self._add_brackets(items[1]) + "/" + self._add_brackets(items[2])
        return l2mml_bin[items[0]] + "(" + items[1] + ")(" + items[2] + ")"

    @log
    def symbol(self, items):
        return l2mml_smb[items[0]]

    @log
    def const(self, items):
        return items[0]

    @log
    def q_str(self, items):
        return items

    def _get_row(self, items, sep="&", mat=False):
        s = ""
        for i in items:
            if i == sep:
                i = ","
            s = s + i
        if mat:
            return "{:" + s + ":}"
        else:
            return "[" + s + "]"

    @log
    def exp_mat(self, items):
        return self._get_row(items, sep="\\\\", mat=True)

    @log
    def row_mat(self, items):
        return self._get_row(items, sep="&")
