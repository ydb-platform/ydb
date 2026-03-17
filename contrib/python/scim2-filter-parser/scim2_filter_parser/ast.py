"""
This file defines classes for different kinds of nodes of an Abstract
Syntax Tree. In general, you will have a different AST node for
each kind of grammar rule.

This file has a small bit of metaprogramming to simplify specification
and to perform some validation steps.
"""


class AST(object):
    _nodes = {}

    @classmethod
    def __init_subclass__(cls):
        AST._nodes[cls.__name__] = cls

        if not hasattr(cls, "__annotations__"):
            return

        cls._fields = list(cls.__annotations__)

    def __init__(self, *args, **kwargs):
        if len(args) != len(self._fields):
            raise TypeError(f"{str(self)}: Expected {len(self._fields)} arguments")

        for name, val in zip(self._fields, args):
            setattr(self, name, val)

        for name, val in kwargs.items():
            setattr(self, name, val)

    def __repr__(self):
        parts = []

        for name in self._fields:
            val = getattr(self, name)

            if isinstance(val, AST):
                parts.append(f"{name}={type(val).__name__}")

            elif isinstance(val, list):
                parts.append(f"{name}=[ len={len(val)} ]")

            else:
                parts.append(f"{name}={repr(val)}")

        argstr = ", ".join(parts)

        return f"{type(self).__name__}({argstr})"


# ----------------------------------------------------------------------
# Specific AST nodes.
#
# For each of these nodes, you need to add type annotations that
# specify which fields are to be stored.  Just as an example, for a
# binary operator, you might store the operator, the left expression,
# and the right expression like this:
#
#    class Expression(AST):
#          pass
#
#    class BinOp(AST):
#          op : str
#          left : Expression
#          right : Expression
#
# The "types" on the right are merely suggestions and don't have
# any effect on the underlying operation.
#
# ----------------------------------------------------------------------

# Abstract AST nodes.  These are not instantiated directly, but other
# classes inherit from them.


class Filter(AST):
    expr: AST
    negated: bool
    namespace: AST


class LogExpr(AST):
    op: str
    expr1: Filter
    expr2: Filter


class SubAttr(AST):
    value: str


class AttrPath(AST):
    attr_name: str
    sub_attr: (SubAttr, type(None))
    uri: (str, type(None))

    @property
    def case_insensitive(self):
        # userName is always case-insensitive
        # https://datatracker.ietf.org/doc/html/rfc7643#section-4.1.1
        return self.attr_name == "userName"


class CompValue(AST):
    value: str


class AttrExpr(AST):
    value: str
    attr_path: AttrPath
    comp_value: CompValue

    @property
    def case_insensitive(self):
        return self.attr_path.case_insensitive


# The following classes for visiting and rewriting the AST are taken
# from Python's ast module.   It's really easy to make mistakes when
# naming methods in visitor classes so there's a bit of metaprogramming
# to make sure we're not writing duplicate definitions and to make
# sure methods match up with the names of actual AST nodes.


class VisitDict(dict):
    def __setitem__(self, key, value):
        if key in self:
            raise AttributeError(f"Duplicate definition for {key}")
        super().__setitem__(key, value)


class NodeVisitMeta(type):
    @classmethod
    def __prepare__(cls, name, bases):
        return VisitDict()


class NodeVisitor(metaclass=NodeVisitMeta):
    """
    Class for visiting nodes of the parse tree.  This is modeled after
    a similar class in the standard library ast.NodeVisitor.  For each
    node, the visit(node) method calls a method visit_NodeName(node)
    which should be implemented in subclasses.  The generic_visit() method
    is called for all nodes where there is no matching visit_NodeName() method.

    Here is a example of a visitor that examines binary operators::

        class VisitOps(NodeVisitor):
            visit_BinOp(self,node):
                print('Binary operator', node.op)
                self.visit(node.left)
                self.visit(node.right)
            visit_UnaryOp(self,node):
                print('Unary operator', node.op)
                self.visit(node.expr)

        tree = parse(txt)
        VisitOps().visit(tree)
    """

    def visit(self, node):
        """
        Execute a method of the form visit_NodeName(node) where
        NodeName is the name of the class of a particular node.
        """
        if isinstance(node, list):
            for item in node:
                self.visit(item)
        elif isinstance(node, AST):
            method = "visit_" + node.__class__.__name__
            visitor = getattr(self, method, self.generic_visit)
            visitor(node)

    def generic_visit(self, node):
        """
        Method executed if no applicable `visit_` method can be found.
        This examines the node to see if it has `_fields`, is a list,
        or can be further traversed.
        """
        for field in node._fields:
            value = getattr(node, field, None)
            self.visit(value)

    @classmethod
    def __init_subclass__(cls):
        """
        Sanity check. Make sure that visitor classes use the right names.
        """
        for key in vars(cls):
            if key.startswith("visit_"):
                assert key[6:] in AST._nodes, f"{key} doesn't match any AST node"  # noqa: SLF001


def flatten(top):
    """
    Flatten the entire parse tree into a list for the purposes of
    debugging and testing.  This returns a list of tuples of the
    form (depth, node) where depth is an integer representing the
    parse tree depth and node is the associated AST node.
    """

    class Flattener(NodeVisitor):
        def __init__(self):
            self.depth = 0
            self.nodes = []

        def generic_visit(self, node):
            self.nodes.append((self.depth, node))
            self.depth += 1
            NodeVisitor.generic_visit(self, node)
            self.depth -= 1

    d = Flattener()
    d.visit(top)

    return d.nodes
