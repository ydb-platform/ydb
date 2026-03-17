import python_minifier.ast_compat as ast

from python_minifier.rename.util import arg_rename_in_place, insert


class Binding(object):
    """
    Represents the binding of a name

    :param name: A name for this binding
    :type name: str or None
    :param bool allow_rename: If this binding may be renamed

    """

    def __init__(self, name=None, allow_rename=True):
        self._references = []

        self._allow_rename = allow_rename

        self._name = name
        self._reserved = None

    def __repr__(self):
        return self.__class__.__name__ + '()'

    @property
    def name(self):
        """
        The name for this binding

        This may be changed using the rename() method.
        If this binding doesn't currently have a name, this returns None.

        :rtype: str or None

        """

        return self._name

    @property
    def allow_rename(self):
        """
        Is it allowed to rename this binding

        :rtype: bool

        """

        return self._allow_rename

    def disallow_rename(self):
        """
        Prevent this binding from being renamed
        """

        self._allow_rename = False

    @property
    def reserved(self):
        """
        A reserved name for this binding

        This may be a name which this binding reserves in it's reservation scope,
        regardless of if it is renamed.

        :rtype: str or None

        """

        return self._reserved

    @property
    def references(self):
        """
        The ast Nodes that reference this binding

        :rtype: list[ast.AST]

        """

        return self._references

    @property
    def name_references(self):
        """
        The number of times the name is used
        """
        return len(self._references)

    def additional_byte_cost(self):
        """
        How many additional bytes would be used, if this was renamed
        """

        arg_rename = False
        additional_bytes = 0

        for node in self._references:
            if isinstance(node, ast.Name):
                if isinstance(node.ctx, (ast.Load, ast.Store, ast.Del)):
                    pass
                else:
                    # Python 2 Param context
                    if not arg_rename_in_place(node):
                        arg_rename = True
            elif isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                pass
            elif isinstance(node, ast.ExceptHandler):
                pass
            elif isinstance(node, (ast.Global, ast.Nonlocal)):
                pass
            elif isinstance(node, ast.alias):
                if node.asname is None:
                    additional_bytes += 4  # ' as '
            elif isinstance(node, ast.arguments):
                if node.vararg == self._name:
                    pass
                if node.kwarg == self._name:
                    pass
            elif isinstance(node, ast.arg):
                if not arg_rename_in_place(node):
                    arg_rename = True

            elif isinstance(node, ast.MatchAs):
                if node.name is None:
                    additional_bytes += 4  # ' as '
            elif isinstance(node, ast.MatchStar):
                pass
            elif isinstance(node, ast.MatchMapping):
                pass
            elif isinstance(node, ast.TypeVar):
                pass
            elif isinstance(node, ast.TypeVarTuple):
                pass
            elif isinstance(node, ast.ParamSpec):
                pass

            else:
                raise AssertionError('Unknown reference node')

        return additional_bytes + (2 if arg_rename else 0)

    def old_mention_count(self):
        """
        The number of times the old name would be mentioned in the source code, if this binding was renamed
        """

        arg_rename = False
        mentions = 0

        for node in self._references:
            if isinstance(node, ast.Name):
                if isinstance(node.ctx, (ast.Load, ast.Store, ast.Del)):
                    pass
                else:
                    # Python 2 Param context
                    if not arg_rename_in_place(node):
                        mentions += 1
                        arg_rename = True

            elif isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                pass
            elif isinstance(node, ast.ExceptHandler):
                pass
            elif isinstance(node, (ast.Global, ast.Nonlocal)):
                pass
            elif isinstance(node, ast.alias):
                if node.asname is None:
                    # import foo -> import foo as bar
                    mentions += 1
            elif isinstance(node, ast.arguments):
                pass
            elif isinstance(node, ast.arg):
                if not arg_rename_in_place(node):
                    mentions += 1
                    arg_rename = True

            elif isinstance(node, ast.MatchAs):
                pass
            elif isinstance(node, ast.MatchStar):
                pass
            elif isinstance(node, ast.MatchMapping):
                pass
            elif isinstance(node, ast.TypeVar):
                pass
            elif isinstance(node, ast.TypeVarTuple):
                pass
            elif isinstance(node, ast.ParamSpec):
                pass

            else:
                raise AssertionError('Unknown reference node')

        return mentions + (1 if arg_rename else 0)

    def new_mention_count(self):
        """
        The number of times a new name would be mentioned in the source code
        """

        arg_rename = False
        mentions = 0

        for node in self._references:
            if isinstance(node, ast.Name):
                if isinstance(node.ctx, (ast.Load, ast.Store, ast.Del)):
                    mentions += 1
                else:
                    # Python 2 Param context
                    arg_rename = True
            elif isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                mentions += 1
            elif isinstance(node, ast.ExceptHandler):
                mentions += 1
            elif isinstance(node, (ast.Global, ast.Nonlocal)):
                mentions += len([n for n in node.names if n == self._name])
            elif isinstance(node, ast.alias):
                mentions += 1
            elif isinstance(node, ast.arguments):
                if node.vararg == self._name:
                    mentions += 1
                if node.kwarg == self._name:
                    mentions += 1
            elif isinstance(node, ast.arg):
                arg_rename = True

            elif isinstance(node, ast.MatchAs):
                mentions += 1
            elif isinstance(node, ast.MatchStar):
                mentions += 1
            elif isinstance(node, ast.MatchMapping):
                mentions += 1
            elif isinstance(node, ast.TypeVar):
                mentions += 1
            elif isinstance(node, ast.TypeVarTuple):
                mentions += 1
            elif isinstance(node, ast.ParamSpec):
                mentions += 1

            else:
                raise AssertionError('Unknown reference node')

        return mentions + (1 if arg_rename else 0)

    def add_reference(self, node, allow_rename=True, reserved=None):
        """
        Add a new reference to this binding

        :param node: The node that references this binding
        :type node: :class:`ast.AST`
        :param bool allow_rename: If this binding may be renamed
        :param str reserved: A name used by the node, even if the binding is renamed.
        :param int rename_cost: Additional cost of renaming the reference, in bytes

        """

        self.references.append(node)

        if allow_rename is False:
            self.disallow_rename()

        if reserved is not None:
            self._reserved = reserved

    def should_rename(self, new_name):
        """
        Is it space efficient to rename this binding

        :param str new_name: The candidate name
        :rtype: bool

        """

        raise NotImplementedError

    def rename(self, new_name):
        """
        Rename this binding and all nodes that reference it

        :param str new_name: The new name to use

        """

        raise NotImplementedError


class NameBinding(Binding):
    """
    Represents the binding of a defined name

    A NameBinding will be attached to the local namespace that defines it.

    :param str name: The original bound name
    :param bool allow_rename: If this binding may be renamed
    :param int rename_cost: The cost of renaming this binding in bytes

    """

    def __init__(self, name, *args, **kwargs):
        super(NameBinding, self).__init__(name, *args, **kwargs)

        if name.startswith('__') and name.endswith('__'):
            # System defined name
            self.disallow_rename()

    def __repr__(self):
        return self.__class__.__name__ + '(name=%r, allow_rename=%r) <references=%r>' % (self._name, self._allow_rename, len(self._references))

    def should_rename(self, new_name):
        """
        Is it space efficient to rename this binding

        :param str new_name: The candidate name
        :rtype: bool

        """

        current_cost = len(self.references) * len(self._name)

        old_mentions = self.old_mention_count()
        new_mentions = self.new_mention_count()
        additional_bytes = self.additional_byte_cost()
        rename_cost = (old_mentions * len(self._name)) + (new_mentions * len(new_name)) + additional_bytes

        return rename_cost <= current_cost

    def disallow_rename(self):
        """
        Prevent this binding from being renamed
        """

        super(NameBinding, self).disallow_rename()
        self._reserved = self._name

    def rename(self, new_name):
        """
        Rename this binding and all nodes that reference it

        :param str new_name: The new name to use

        """

        func_namespace_binding = None

        for node in self.references:

            if isinstance(node, ast.Name):

                if isinstance(node.ctx, (ast.Load, ast.Store, ast.Del)):
                    node.id = new_name
                else:
                    # Python 2 Param context

                    if arg_rename_in_place(node):
                        node.id = new_name

                    else:
                        if func_namespace_binding is None:
                            func_namespace_binding = node.namespace
                        else:
                            assert func_namespace_binding is node.namespace

            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                node.name = new_name
            elif isinstance(node, ast.ClassDef):
                node.name = new_name
            elif isinstance(node, ast.alias):
                if new_name == node.name:
                    node.asname = None
                else:
                    node.asname = new_name
            elif isinstance(node, ast.arg):

                if arg_rename_in_place(node):
                    node.arg = new_name

                else:
                    if func_namespace_binding is None:
                        func_namespace_binding = node.namespace
                    else:
                        assert func_namespace_binding is node.namespace

            elif isinstance(node, ast.ExceptHandler):
                node.name = new_name
            elif isinstance(node, (ast.Global, ast.Nonlocal)):
                node.names = [new_name if n == self._name else n for n in node.names]
            elif isinstance(node, ast.arguments):

                rename_vararg = (node.vararg == self._name) and not getattr(node, 'vararg_renamed', False)
                rename_kwarg = (node.kwarg == self._name) and not getattr(node, 'kwarg_renamed', False)

                if rename_vararg:
                    node.vararg = new_name
                    node.vararg_renamed = True
                if rename_kwarg:
                    node.kwarg = new_name
                    node.kwarg_renamed = True

            elif isinstance(node, ast.MatchAs):
                node.name = new_name
            elif isinstance(node, ast.MatchStar):
                node.name = new_name
            elif isinstance(node, ast.MatchMapping):
                node.rest = new_name
            elif isinstance(node, ast.TypeVar):
                node.name = new_name
            elif isinstance(node, ast.TypeVarTuple):
                node.name = new_name
            elif isinstance(node, ast.ParamSpec):
                node.name = new_name

        if func_namespace_binding is not None:
            func_namespace_binding.body = list(
                insert(
                    func_namespace_binding.body,
                    ast.Assign(
                        targets=[ast.Name(id=new_name, ctx=ast.Store())],
                        value=ast.Name(id=self._name, ctx=ast.Load()),
                    ),
                )
            )

        self._name = new_name


class BuiltinBinding(NameBinding):
    """
    Represents the usage of a builtin

    :param str name: The name of the builtin
    :param namespace: The module the builtin is used in
    :type namespace: :class:`ast.Module`

    """

    def __init__(self, name, namespace, *args, **kwargs):
        super(BuiltinBinding, self).__init__(name, *args, **kwargs)
        self.namespace = namespace

        # These builtins actually act like keywords, so should not be changed
        if name == 'super':
            # If we replace 'super' with another name the compiler will neglect to create the
            # __class__ implicit closure reference, breaking the zero argument super() call.
            self.disallow_rename()
        elif name == 'object':
            # Classes must inherit from object to become a new-style class in python2
            self.disallow_rename()

    def new_mention_count(self):
        # All mentions must be Names, which would be replaced
        # Plus an Assign with the new name
        return len(self.references) + 1

    def old_mention_count(self):
        # The old name would be mentioned in the Assign
        return 1

    def additional_byte_cost(self):
        return 2  # '=' + '\n'

    def rename(self, new_name):
        builtin = self._name
        super(BuiltinBinding, self).rename(new_name)
        self.namespace.body = list(
            insert(
                self.namespace.body,
                ast.Assign(
                    targets=[ast.Name(id=new_name, ctx=ast.Store())], value=ast.Name(id=builtin, ctx=ast.Load())
                ),
            )
        )

    def is_redefined(self):
        """
        Do one of the references to this builtin name redefine it?

        Could some references actually not be references to the builtin?

        This can happen with code like:

        class MyClass:
            IndexError = IndexError

        """

        for node in self.references:
            if not isinstance(node, ast.Name):
                return True

            if not isinstance(node.ctx, ast.Load):
                return True

        return False
