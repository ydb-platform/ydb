import python_minifier.ast_compat as ast
from python_minifier.ast_annotation import get_parent, set_parent

from python_minifier.rename.binding import Binding
from python_minifier.rename.util import insert
from python_minifier.transforms.suite_transformer import NodeVisitor
from python_minifier.util import is_constant_node


def replace(old_node, new_node):
    parent = get_parent(old_node)
    set_parent(new_node, parent)
    new_node.namespace = old_node.namespace

    for field, old_value in ast.iter_fields(parent):
        if old_value is old_node:
            setattr(parent, field, new_node)
            return

        if isinstance(old_value, list):
            for i, value in enumerate(old_value):
                if value is old_node:
                    old_value[i] = new_node
                    return


class HoistedBinding(Binding):
    def __init__(self, value_node, *args, **kwargs):
        super(HoistedBinding, self).__init__(*args, **kwargs)
        self._value_node = value_node
        self._local_namespace = None

    def __eq__(self, other):
        return type(self.value) is type(other.value) and self.value == other.value

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return hash(repr(self.value))

    def set_local_namespace(self, node):
        self._local_namespace = node

    @property
    def value(self):
        if is_constant_node(self._value_node, (ast.Str, ast.Bytes)):
            return self._value_node.s
        else:
            return self._value_node.value

    def __repr__(self):
        return self.__class__.__name__ + '(value=%r)' % self.value

    def new_mention_count(self):
        # All mentions must be literals, which would be replaced
        # Plus an Assign with the new name
        return len(self.references) + 1

    def old_mention_count(self):
        # For hoisted bindings, the old 'name' is the literal
        # It would be mentioned once, in the Assign
        return 1

    def additional_byte_cost(self):
        return 2  # '=' + '\n'

    def rename(self, new_name):

        for node in self.references:
            replace(node, ast.Name(id=new_name, ctx=ast.Load()))

        self._local_namespace.body = list(
            insert(
                self._local_namespace.body,
                ast.Assign(targets=[ast.Name(id=new_name, ctx=ast.Store())], value=self._value_node),
            )
        )

        self._name = new_name

    def should_rename(self, new_name):
        current_cost = len(self.references) * len(repr(self.value))
        rename_cost = (self.old_mention_count() * len(repr(self.value))) + ((self.new_mention_count()) * len(new_name)) + self.additional_byte_cost()

        return rename_cost <= current_cost


class HoistedValue(object):
    """
    HoistedValue comparator object

    This is for wrapping a value in a set or dict key, and
    ensures different types hash differently, even if they compare equal.

    The problematic values are str/bytes/unicode and int/float.

    """

    def __init__(self, value):
        self._value = value

    def __hash__(self):
        return hash(str(type(self._value)) + str(hash(self._value)))

    def __eq__(self, other):
        return type(self._value) == type(other._value) and self._value == other._value

    def __ne__(self, other):
        return not self == other


class HoistLiterals(NodeVisitor):
    """
    Hoist literal strings to module level variables
    """

    def __call__(self, module, ignore_slots=True):
        self.module = module
        self._ignore_slots = ignore_slots
        self._hoisted = {}
        self.visit(module)
        self.place_bindings()

    def nearest_function_namespace(self, node):
        """
        Return the namespace node for the nearest function scope.

        This could be itself.

        :param node: The node to get the function namespace of
        :type node: ast.Node
        :rtype: ast.Node

        """

        if isinstance(node.namespace, (ast.FunctionDef, ast.Module, ast.AsyncFunctionDef)):
            return node.namespace
        return self.nearest_function_namespace(node.namespace)

    def namespace_path(self, node):
        """
        Return the path of function namespace nodes from the module node down to the input node

        With the source module:
        >>> def a():
        ...   def b():
        ...     c

        >>> namespace_path(c)
        [a, b, c]

        :param node:
        :type node: ast.Node
        :rtype: list[ast.AST]

        """

        path = []

        while True:
            namespace = self.nearest_function_namespace(node)
            path.insert(0, namespace)

            if isinstance(namespace, ast.Module):
                break

            node = namespace

        return path

    def common_path(self, n1_path, n2_path):

        path = []
        for n1_step, n2_step in zip(n1_path, n2_path):
            if n1_step is not n2_step:
                return path
            path.append(n1_step)
        return path

    def place_bindings(self):
        for binding in self._hoisted.values():

            namespace_path = []

            for node in binding.references:
                if not namespace_path:
                    namespace_path = self.namespace_path(node)
                else:
                    namespace_path = self.common_path(namespace_path, self.namespace_path(node))

            namespace_path[-1].bindings.append(binding)
            binding.set_local_namespace(namespace_path[-1])

    def get_binding(self, value, node):
        hoisted_value = HoistedValue(value)
        if hoisted_value in self._hoisted:
            return self._hoisted[hoisted_value]

        binding = HoistedBinding(node)
        self._hoisted[hoisted_value] = binding
        return binding

    def visit_Str(self, node):

        if isinstance(get_parent(node), ast.Expr):
            # This is literal statement
            # The RemoveLiteralStatements transformer must have left it here, so ignore it.
            return

        self.get_binding(node.s, node).add_reference(node)

    def visit_Bytes(self, node):
        self.visit_Str(node)

    def visit_JoinedStr(self, node):
        for v in node.values:
            if is_constant_node(v, ast.Str):
                # Can't hoist this!
                continue
            self.visit(v)

    def visit_TemplateStr(self, node):
        for v in node.values:
            if is_constant_node(v, ast.Str):
                # Can't hoist string literals that are part of the template
                continue
            self.visit(v)

    def visit_NameConstant(self, node):
        self.get_binding(node.value, node).add_reference(node)

    def visit_match_case(self, node):
        # Can't hoist literals in a pattern

        if node.guard is not None:
            self.visit(node.guard)

        for n in node.body:
            self.visit(n)

    def visit_Assign(self, node):
        if not self._ignore_slots:
            return self.generic_visit(node)

        if not isinstance(node.namespace, ast.ClassDef):
            return self.generic_visit(node)

        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == '__slots__':
                # This is a __slots__ assignment, don't hoist the literals
                return None

        return self.generic_visit(node)


def rename_literals(module):
    HoistLiterals()(module)
