import ast


def ast_create_profile_node(modname, profiler_name='profile', attr='add_imported_function_or_module'):
    """Create an abstract syntax tree node that adds an object to the profiler to be profiled.

    An abstract syntax tree node is created which calls the attr method from profile and
    passes modname to it.
    At runtime, this adds the object to the profiler so it can be profiled.
    This node must be added after the first instance of modname in the AST and before it is used.
    The node will look like:
        >>> # xdoctest: +SKIP
        >>> import foo.bar
        >>> profile.add_imported_function_or_module(foo.bar)

    Args:
        script_file (str):
            path to script being profiled.

        prof_mod (List[str]):
            list of imports to profile in script.
            passing the path to script will profile the whole script.
            the objects can be specified using its dotted path or full path (if applicable).

    Returns:
        (_ast.Expr): expr
            AST node that adds modname to profiler.
    """
    func = ast.Attribute(value=ast.Name(id=profiler_name, ctx=ast.Load()), attr=attr, ctx=ast.Load())
    names = modname.split('.')
    value = ast.Name(id=names[0], ctx=ast.Load())
    for name in names[1:]:
        value = ast.Attribute(attr=name, ctx=ast.Load(), value=value)
    expr = ast.Expr(value=ast.Call(func=func, args=[value], keywords=[]))
    return expr


class AstProfileTransformer(ast.NodeTransformer):
    """Transform an abstract syntax tree adding profiling to all of its objects.

    Adds profiler decorators on all functions & methods that are not already decorated with
    the profiler.
    If profile_imports is True, a profiler method call to profile is added to all imports
    immediately after the import.
    """

    def __init__(self, profile_imports=False, profiled_imports=None, profiler_name='profile'):
        """Initializes the AST transformer with the profiler name.

        Args:
            profile_imports (bool):
                If True, profile all imports.

            profiled_imports (List[str]):
                list of dotted paths of imports to skip that have already been added to profiler.

            profiler_name (str):
                the profiler name used as decorator and for the method call to add to the object
                to the profiler.
        """
        self._profile_imports = bool(profile_imports)
        self._profiled_imports = profiled_imports if profiled_imports is not None else []
        self._profiler_name = profiler_name

    def visit_FunctionDef(self, node):
        """Decorate functions/methods with profiler.

        Checks if the function/method already has a profile_name decorator, if not, it will append
        profile_name to the end of the node's decorator list.
        The decorator is added to the end of the list to avoid conflicts with other decorators
        e.g. @staticmethod.

        Args:
            (_ast.FunctionDef): node
                function/method in the AST

        Returns:
            (_ast.FunctionDef): node
                function/method with profiling decorator
        """
        decor_ids = set()
        for decor in node.decorator_list:
            try:
                decor_ids.add(decor.id)
            except AttributeError:
                ...
        if self._profiler_name not in decor_ids:
            node.decorator_list.append(ast.Name(id=self._profiler_name, ctx=ast.Load()))
        return self.generic_visit(node)

    def _visit_import(self, node):
        """Add a node that profiles an import

        If profile_imports is True and the import is not in profiled_imports,
        a node which calls the profiler method, which adds the object to the profiler,
        is added immediately after the import.

        Args:
            node (Union[_ast.Import,_ast.ImportFrom]):
                import in the AST

        Returns:
            (Union[Union[_ast.Import,_ast.ImportFrom],List[Union[_ast.Import,_ast.ImportFrom,_ast.Expr]]]): node
                if profile_imports is False:
                    returns the import node
                if profile_imports is True:
                    returns list containing the import node and the profiling node
        """
        if not self._profile_imports:
            return self.generic_visit(node)
        visited = [self.generic_visit(node)]
        for names in node.names:
            node_name = names.name if names.asname is None else names.asname
            if node_name in self._profiled_imports:
                continue
            self._profiled_imports.append(node_name)
            expr = ast_create_profile_node(node_name)
            visited.append(expr)
        return visited

    def visit_Import(self, node):
        """Add a node that profiles an object imported using the "import foo" sytanx

        Args:
            node (_ast.Import):
                import in the AST

        Returns:
            (Union[_ast.Import,List[Union[_ast.Import,_ast.Expr]]]): node
                if profile_imports is False:
                    returns the import node
                if profile_imports is True:
                    returns list containing the import node and the profiling node
        """
        return self._visit_import(node)

    def visit_ImportFrom(self, node):
        """Add a node that profiles an object imported using the "from foo import bar" syntax

        Args:
            node (_ast.ImportFrom):
                import in the AST

        Returns:
            (Union[_ast.ImportFrom,List[Union[_ast.ImportFrom,_ast.Expr]]]): node
                if profile_imports is False:
                    returns the import node
                if profile_imports is True:
                    returns list containing the import node and the profiling node
        """
        return self._visit_import(node)
