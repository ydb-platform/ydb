import logging
import re
import socket

try:
    import http.client as httplib
except ImportError:
    import httplib

try:
    import collections.abc as collections
except ImportError:
    import collections

logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG)


def alias_string(mapping, init=False, alias=True, prefix="", lang_from=None):
    mapping = list(mapping.items())
    s = (
        "|"
        if init
        else ""
        + (
            '"' + mapping[0][0] + '"'
            if lang_from != "latex"
            else (
                '/' + mapping[0][0].encode("unicode-escape").decode() + '/'
                if mapping[0][0].startswith("\\")
                else '"' + mapping[0][0] + '"'
            )
        )
        + (
            " -> " + (prefix + "_" if prefix != "" else "") + mapping[0][1]
            if alias
            else ""
        )
    )
    for k, v in mapping[1:]:
        s = (
            s
            + "\n\t| "
            + (
                '"' + k + '"'
                if lang_from != "latex"
                else (
                    '/' + k.encode("unicode-escape").decode() + '/'
                    if k.startswith("\\")
                    else '"' + k + '"'
                )
            )
            + (
                " -> " + (prefix + "_" if prefix != "" else "") + v
                if alias
                else ""
            )
        )
    return s


def check_connection(url="www.google.com", timeout=10.0):  # pragma: no cover
    """Check connection against url.

    Parameters:
        url (str, optional): URL to check connection against
        timeout (float, optional): Connection timeout

    Returns:
        bool: True, if there is a connection, False otherwise
    """
    conn = httplib.HTTPSConnection(url, timeout=timeout)
    try:
        conn.request("HEAD", "/")
        conn.close()
        return True
    except (httplib.HTTPException, socket.error):
        conn.close()
        return False


def encapsulate_mrow(s):
    return "<mrow>" + s + "</mrow>"


def concat(s):
    return '"' + s + '"'


def flatten(l):  # pragma: no cover
    """Flatten a list (or other iterable) recursively"""
    for el in l:
        if isinstance(el, collections.Iterable) and not isinstance(el, str):
            for sub in flatten(el):
                yield sub
        else:
            yield el


class UtilsMat(object):
    """Static class to check matrix-structure of a string and returns its
    LaTeX translation.

    It performs two opertions:

    1) Given a string, it checks if the string could be a rendered as LaTeX
       matrix. A correct matrix structure is:
       L [... (, ...)*], [... (, ...)*] (, [... (, ...)*])* R or
       L (... (, ...)*), (... (, ...)*) (, (... (, ...)*))* R, where L and R
       are all the possible left and right parenthesis defined by the parser;
       '[... (, ...)*]' or '(... (, ...)*)' identifies a row in the matrix
       which can be made by one or more columns, comma separated.
       In order to be considered as a matrix, the string must contain at
       leat two rows and every rows must contain the same number of columns
    2) Given a correctly matrix-like string, it returns the LaTeX translation:
       col (& col)* \\\\ col (& col)* (\\\\ col (& col)*)*
    """

    _left_par = ["(", "(:", "[", "{", "{:", "|:", "||:", "langle", "&langle;"]
    _right_par = [")", ":)", "]", "}", ":}", ":|", ":||", "rangle", "&rangle;"]
    _mathml_par_pattern = re.compile(
        r"<mo>"
        r"(\,|\(|\(:|\[|\{|\{:|\|:|\|\|:|&langle;|"
        r"\)|:\)|\]|\}|:\}|:\||:\|\||&rangle;)"
        r"</mo>",
    )

    @classmethod
    def get_row_par(cls, s):
        """Given a string, it returns the first index i such that the char in
        position i of the string is a left parenthesis, '(' or '[', and the
        open-close parenthesis couple, needed to identify matrix
        rows in the string.

        Args:
            s (str): Input string

        Returns:
            int, list: Index i of the first open parenthesis,
                and the list [left_par, right_par]. -1 and [] if no parenthesis
                is found
        """

        for i, c in enumerate(s):
            if c == "[" or c == "(":
                return i, ["[", "]"] if c == "[" else ["(", ")"]
        return -1, []

    @classmethod
    def check_mat(cls, s):
        """Given a string, runs a matrix-structure check.
        Returns True if the string s has a matrix-structure-like,
        False otherwise. It returns also the row delimiters.

        Args:
            s (str): Input string to be matrix-checked

        Returns:
            bool, list: True, [left_par, right_par] if the string s
                has a matrix-like structure; False, [] otherwise
        """

        rows = 0
        cols = 0
        max_cols = 0
        par_stack = []
        transitions = 0
        i, row_par = cls.get_row_par(s)
        if i != -1 and row_par != []:
            for c in s[i:]:
                # c is a left par
                if c == row_par[0]:
                    if transitions != rows:
                        logging.info("Row without comma")
                        return False, []
                    par_stack.append(c)
                # c is a right par
                elif c == row_par[1]:
                    if len(par_stack) == 0:
                        logging.info("Unmatched pars")
                        return False, []
                    else:
                        par_stack.pop()
                    if len(par_stack) == 0:
                        transitions = transitions + 1
                        if transitions == 1 and max_cols == 0 and cols > 0:
                            max_cols = cols
                        elif max_cols != cols:
                            logging.info("Cols differ")
                            return False, []
                        cols = 0
                elif c == ",":
                    if len(par_stack) == 1 and par_stack[-1] == row_par[0]:
                        cols = cols + 1
                    elif len(par_stack) == 0:
                        # If the comma is not at the and of the string
                        # count another row
                        rows = rows + 1
                        if transitions - rows != 0:
                            logging.info(
                                "No open-close par between two commas"
                            )
                            return False, []
            if len(par_stack) != 0:
                logging.info("Unmatched pars")
                return False, []
            elif transitions < 2:
                logging.info("Not enough rows: at least 2")
                return False, []
            elif rows == 0 or transitions - rows > 1:
                logging.info("Missing comma or empty row")
                return False, []
            return True, row_par
        else:
            return False, []

    @classmethod
    def get_latex_mat(cls, s, row_par=["[", "]"]):
        """Given a known matrix-structured string, translate it into the
        matrix LaTeX format.

        Args:
            s (str): Input string
            max_cols (int): How many columns per rows
            row_par (list, optional): Row delimiters

        Returns:
            str: LaTeX well-formed matrix
        """

        i = 0
        empty_col = True
        stack_par = []
        mat = ""
        if row_par != []:
            while i < len(s):
                c = s[i]
                if c == row_par[0]:
                    stack_par.append(c)
                    if len(stack_par) > 1:
                        mat = mat + c
                elif c == row_par[1]:
                    stack_par.pop()
                    if len(stack_par) > 0:
                        mat = mat + c
                    else:
                        if empty_col:
                            mat = mat + "\\null"
                        empty_col = True
                elif c == ",":
                    if len(stack_par) == 1:
                        mat = mat + (" & " if not empty_col else "\\null & ")
                    elif len(stack_par) == 0:
                        mat = mat + " \\\\ "
                    empty_col = True
                else:
                    # Does not include \\left in the result string
                    if len(stack_par) == 0 and s[i : i + 5] == "\\left":
                        i = i + 4
                    # Does not include \\right in the result string
                    elif (
                        len(stack_par) == 1
                        and s[i : i + 7] == "\\right" + row_par[1]
                    ):
                        i = i + 5
                    else:
                        if not c.isspace():
                            empty_col = False
                        mat = mat + c
                i = i + 1
            return mat
        else:
            return s

    @classmethod
    def get_mathml_mat(cls, s, row_par=["[", "]"]):
        """Given a known matrix-structured string, translate it into the
        matrix LaTeX format.

        Args:
            s (str): Input string
            max_cols (int): How many columns per rows
            row_par (list, optional): Row delimiters

        Returns:
            str: MathML well-formed matrix
        """

        split = re.split(cls._mathml_par_pattern, s,)
        stack_par = []
        mat = ""
        if row_par != []:
            for i, c in enumerate(split):
                # Row delimiting left par
                if c == row_par[0]:
                    stack_par.append(c)
                    if len(stack_par) == 1:
                        # Starts a new row
                        mat = mat + "<mtr><mtd>"
                    elif len(stack_par) > 1:
                        mat = mat + "<mo>" + c + "</mo>"
                # It's an internal par expression
                elif c in cls._left_par:
                    stack_par.append(c)
                    mat = mat + "<mo>" + c + "</mo>"
                # Row delimiting right par
                elif c == row_par[1]:
                    stack_par.pop()
                    if len(stack_par) > 0:
                        mat = mat + "<mo>" + c + "</mo>"
                    else:
                        # Row it's terminated
                        mat = (
                            mat
                            + "</mtd>"
                            # If it's the last par, close also the table row
                            + ("</mtr>" if i == len(split) - 2 else "")
                        )
                elif c in cls._right_par:
                    stack_par.pop()
                    mat = mat + "<mo>" + c + "</mo>"
                elif c == ",":
                    # Column ends
                    if len(stack_par) == 1:
                        mat = mat + "</mtd><mtd>"
                    # Row ends
                    elif len(stack_par) == 0:
                        mat = mat + "</mtr>"
                    else:
                        mat = mat + "<mo>" + c + "</mo>"
                # Initial and ending <mrow></mrow> are not needed
                # if this is a matrix
                elif (c == "<mrow>" or c == "</mrow>") and len(stack_par) == 0:
                    pass
                elif c != "":
                    if len(stack_par) == 1 and stack_par[-1] == row_par[0]:
                        # Strip single column from <mrow></mrow>
                        if (
                            split[i - 1] == row_par[0]
                            and split[i + 1] == row_par[1]
                        ):
                            mat = mat + c[6 : len(c) - 7]
                        # Discard unneeded <mrow> tag
                        # if it's the first element in the row
                        elif split[i - 1] == row_par[0]:
                            mat = mat + c[6:]
                        # Discard unneeded </mrow> tag
                        # if it's the last element in the row
                        elif split[i + 1] == row_par[1]:
                            mat = mat + c[: len(c) - 7]
                        else:
                            mat = mat + c
                    else:
                        mat = mat + c
            return mat
        else:
            return s


if __name__ == "__main__":  # pragma: no cover
    s = "[1,2],[1,2,3]"
    print(UtilsMat.check_mat(s))
