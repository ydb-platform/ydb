from .. import fixer_base
from ..fixer_util import Call
from ..fixer_util import Comma
from ..fixer_util import KeywordArg
from ..fixer_util import Name
from ..fixer_util import Node
from ..fixer_util import touch_import
from ..pgen2 import token
from ..pygram import python_symbols as symbols

try:
    from itertools import filterfalse
except ImportError:
    from itertools import ifilterfalse as filterfalse


class FixSorted(fixer_base.BaseFix):
    PATTERN = """
        power< "sorted" trailer< "(" not arglist ")" > any* >
        |
        power< "sorted" trailer< "(" arglist< any "," func_args=any+ > ")" > any* >
        |
        power<any* trailer< any* >* trailer<"." "sort" > trailer<"(" arglist< func_args=any+ > ")"> any* >
        |
        power<any* trailer< any* >* trailer<"." "sort" > trailer<"(" func_arg=any+ ")"> any* >
    """

    def _transform_keyword(self, nodes, result):
        """Transform second and later position argument into keyword argument."""
        if not nodes:
            return

        if not isinstance(nodes, list):
            nodes = [nodes]

        parent = nodes[0].parent

        # transform positional arguments
        positional_args = list(
            filterfalse(lambda arg: arg.type in (token.COMMA, symbols.argument), nodes)
        )
        template = ["cmp", "key", "reverse"]
        new_args = []
        for arg, key in zip(positional_args, template):
            arg_ = arg.clone()
            arg_.prefix = ""

            new_arg = KeywordArg(Name(key), arg_)
            new_arg.prefix = arg.prefix
            new_args.append(new_arg)

            arg.remove()

        for child in list(parent.children):
            if child.type == token.COMMA and (
                child.next_sibling is None or child.next_sibling.type == token.COMMA
            ):
                child.remove()

        # update keyword argument list
        keyword_args = list(filter(lambda arg: arg.type == symbols.argument, nodes))
        keywords = [arg.children[0].value for arg in keyword_args]

        assert parent.type == symbols.arglist
        for arg in new_args:
            if arg.children[0].value not in keywords:
                if len(parent.children) > 0:
                    parent.append_child(Comma())
                parent.append_child(arg)

        # update result mapping
        return parent.children

    def _transform_cmp(self, nodes, result):
        """transform argument `cmp` into `key`"""
        arglist = list(filter(lambda arg: arg.type == symbols.argument, nodes))

        cmp_node = None
        key_node = None
        for arg in arglist:
            if arg.type == symbols.argument:
                if arg.children[0].value == "cmp":
                    cmp_node = arg
                if arg.children[0].value == "key":
                    key_node = arg

        if cmp_node:
            if key_node:
                return  # Do nothing when it have both cmp and key argument

            cmp_node.children[0].value = "key"
            cmp_node.children[2].replace(
                Call(Name("cmp_to_key"), args=[cmp_node.children[2].clone()])
            )
            touch_import("functools", "cmp_to_key", cmp_node)

    def transform(self, node, result):
        if result.get("func_arg"):
            a = Node(symbols.arglist, [r.clone() for r in result["func_arg"]])
            result["func_arg"][0].replace(a)
            result["func_args"] = a.children

        if result.get("func_args"):
            ret = self._transform_keyword(result["func_args"], result)
            self._transform_cmp(ret, result)
