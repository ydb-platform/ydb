

import logging


from lark import Lark, Token, Transformer

from ..utils.log import Log

# standard_library.install_aliases()

logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG)


class Transformer(Transformer):
    log = Log(logger_func=logging.info)

    def get_level(self, l, lvl):
        if lvl > 1:
            return lvl
        for el in l:
            if isinstance(el, list):
                lvl = lvl + 1
                lvl = self.get_level(el, lvl)
        return lvl

    @log
    def list(self, items):
        return items

    @log
    def exp(self, items):
        if isinstance(items[0], Token):
            return items[0].value
        return items[0]

    @log
    def visit(self, l, action="remove"):
        expanded_l = []
        for el in l:
            if isinstance(el, list):
                el = self.visit(el, action=action)
            elif isinstance(el, Token):
                if action == "remove":
                    continue
                elif action == "expand":
                    el = el.value
            expanded_l = expanded_l + [el]
        return expanded_l

    @log
    def mat(self, items):
        max_lvl = self.get_level(items, 0)
        print(("LEVEL", max_lvl))
        if max_lvl > 1:
            return ["["] + items + ["]"]
        else:
            items = self.visit(items, action="remove")[0]
            return (
                "\\begin{bmatrix}"
                + " \\\\ ".join(
                    [
                        " & ".join(el) if isinstance(el, list) else el
                        for el in items
                    ]
                )
                + "\\end{bmatrix}"
            )

    def recursive_join(self, l):
        s = ""
        for el in l:
            if isinstance(el, list):
                el = self.recursive_join(el)
            elif isinstance(el, Token):
                el = el.value
            s = s + el
        return s

    @log
    def stmt(self, items):
        return self.recursive_join(items)


asciimath_parser = Lark(
    r"""
    stmt: _csl
    exp: list
        | mat
        | NUMBER
    mat: "[:" _csl? ":]"
    list: (L _csl? R | DOT_L _csl? R | L _csl? DOT_R) -> list
    L.0: "(" | "[" | "{{"
    R.0: ")" | "]" | "}}"
    DOT_L.1: "[:"
    DOT_R.1: ":]"
    _csl: exp (/,/? exp)* /,/?
    %import common.WS
    %import common.NUMBER
    %ignore WS
""",
    start="stmt",
    parser="lalr",
    debug=True,
)
text = """[:[1,3,[2,3,[1,[2,7]]]]:]"""
parsed_text = asciimath_parser.parse(text)
print(parsed_text.pretty())
print(Transformer().transform(parsed_text))
