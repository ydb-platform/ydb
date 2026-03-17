"""
This package transforms python source code strings or ast.Module Nodes into
a 'minified' representation of the same source code.

"""

import re

import python_minifier.ast_compat as ast
from python_minifier.ast_annotation import add_parent

from python_minifier.ast_compare import CompareError, compare_ast
from python_minifier.module_printer import ModulePrinter
from python_minifier.rename import (
    add_namespace,
    allow_rename_globals,
    allow_rename_locals,
    bind_names,
    rename,
    rename_literals,
    resolve_names
)
from python_minifier.transforms.combine_imports import CombineImports
from python_minifier.transforms.constant_folding import FoldConstants
from python_minifier.transforms.remove_annotations import RemoveAnnotations
from python_minifier.transforms.remove_annotations_options import RemoveAnnotationsOptions
from python_minifier.transforms.remove_asserts import RemoveAsserts
from python_minifier.transforms.remove_debug import RemoveDebug
from python_minifier.transforms.remove_exception_brackets import remove_no_arg_exception_call
from python_minifier.transforms.remove_explicit_return_none import RemoveExplicitReturnNone
from python_minifier.transforms.remove_literal_statements import RemoveLiteralStatements
from python_minifier.transforms.remove_object_base import RemoveObject
from python_minifier.transforms.remove_pass import RemovePass
from python_minifier.transforms.remove_posargs import remove_posargs


class UnstableMinification(RuntimeError):
    """
    Raised when a minified module differs from the original module in an unexpected way.

    This is raised when the minifier generates source code that doesn't parse back into the
    original module (after known transformations).
    This should never occur and is a bug.

    """

    def __init__(self, exception, source, minified):
        self.exception = exception
        self.source = source
        self.minified = minified

    def __str__(self):
        return 'Minification was unstable! Please create an issue at https://github.com/dflook/python-minifier/issues'


def minify(
    source,
    filename=None,
    remove_annotations=RemoveAnnotationsOptions(),
    remove_pass=True,
    remove_literal_statements=False,
    combine_imports=True,
    hoist_literals=True,
    rename_locals=True,
    preserve_locals=None,
    rename_globals=False,
    preserve_globals=None,
    remove_object_base=True,
    convert_posargs_to_args=True,
    preserve_shebang=True,
    remove_asserts=False,
    remove_debug=False,
    remove_explicit_return_none=True,
    remove_builtin_exception_brackets=True,
    constant_folding=True,
    prefer_single_line=False,
):
    """
    Minify a python module

    The module is transformed according the the arguments.
    If all transformation arguments are False, no transformations are made to the AST, the returned string will
    parse into exactly the same module.

    Using the default arguments only transformations that are always or almost always safe are enabled.

    :param str source: The python module source code
    :param str filename: The original source filename if known

    :param remove_annotations: Configures the removal of type annotations. True removes all annotations, False removes none.
        RemoveAnnotationsOptions can be used to configure the removal of specific annotations.
    :type remove_annotations: bool or RemoveAnnotationsOptions
    :param bool remove_pass: If Pass statements should be removed where possible
    :param bool remove_literal_statements: If statements consisting of a single literal should be removed, including docstrings
    :param bool combine_imports: Combine adjacent import statements where possible
    :param bool hoist_literals: If str and byte literals may be hoisted to the module level where possible.
    :param bool rename_locals: If local names may be shortened
    :param preserve_locals: Locals names to leave unchanged when rename_locals is True
    :type preserve_locals: list[str]
    :param bool rename_globals: If global names may be shortened
    :param preserve_globals: Global names to leave unchanged when rename_globals is True
    :type preserve_globals: list[str]
    :param bool remove_object_base: If object as a base class may be removed
    :param bool convert_posargs_to_args: If positional-only arguments will be converted to normal arguments
    :param bool preserve_shebang: Keep any shebang interpreter directive from the source in the minified output
    :param bool remove_asserts: If assert statements should be removed
    :param bool remove_debug: If conditional statements that test '__debug__ is True' should be removed
    :param bool remove_explicit_return_none: If explicit return None statements should be replaced with a bare return
    :param bool remove_builtin_exception_brackets: If brackets should be removed when raising exceptions with no arguments
    :param bool constant_folding: If literal expressions should be evaluated
    :param bool prefer_single_line: If semi-colons should be preferred over newlines where there is no difference in output size

    :rtype: str

    """

    filename = filename or 'python_minifier.minify source'

    # This will raise if the source file can't be parsed
    module = ast.parse(source, filename)

    add_parent(module)
    add_namespace(module)

    if remove_literal_statements:
        module = RemoveLiteralStatements()(module)

    if combine_imports:
        module = CombineImports()(module)

    if isinstance(remove_annotations, bool):
        remove_annotations_options = RemoveAnnotationsOptions(
            remove_variable_annotations=remove_annotations,
            remove_return_annotations=remove_annotations,
            remove_argument_annotations=remove_annotations,
            remove_class_attribute_annotations=remove_annotations,
        )
    elif isinstance(remove_annotations, RemoveAnnotationsOptions):
        remove_annotations_options = remove_annotations
    else:
        raise TypeError('remove_annotations must be a bool or RemoveAnnotationsOptions')

    if remove_annotations_options:
        module = RemoveAnnotations(remove_annotations_options)(module)

    if remove_pass:
        module = RemovePass()(module)

    if remove_object_base:
        module = RemoveObject()(module)

    if remove_asserts:
        module = RemoveAsserts()(module)

    if remove_debug:
        module = RemoveDebug()(module)

    if remove_explicit_return_none:
        module = RemoveExplicitReturnNone()(module)

    if constant_folding:
        module = FoldConstants()(module)

    bind_names(module)
    resolve_names(module)

    if remove_builtin_exception_brackets and not module.tainted:
        remove_no_arg_exception_call(module)

    if module.tainted:
        rename_globals = False
        rename_locals = False

    if preserve_locals is None:
        preserve_locals = []
    elif isinstance(preserve_locals, str):
        preserve_locals = [preserve_locals]
    if preserve_globals is None:
        preserve_globals = []
    elif isinstance(preserve_globals, str):
        preserve_globals = [preserve_globals]

    preserve_locals.extend(module.preserved)
    preserve_globals.extend(module.preserved)

    allow_rename_locals(module, rename_locals, preserve_locals)
    allow_rename_globals(module, rename_globals, preserve_globals)

    if hoist_literals:
        rename_literals(module)

    rename(module, prefix_globals=not rename_globals, preserved_globals=preserve_globals)

    if convert_posargs_to_args:
        module = remove_posargs(module)

    minified = unparse(module, prefer_single_line=prefer_single_line)

    if preserve_shebang is True:
        shebang_line = _find_shebang(source)
        if shebang_line is not None:
            return shebang_line + '\n' + minified

    return minified


def _find_shebang(source):
    """
    Find a shebang line in source
    """

    if isinstance(source, bytes):
        shebang = re.match(br'^#!.*', source)
        if shebang:
            return shebang.group().decode()
    else:
        shebang = re.match(r'^#!.*', source)
        if shebang:
            return shebang.group()

    return None


def unparse(module, prefer_single_line=False):
    """
    Turn a module AST into python code

    This returns an exact representation of the given module,
    such that it can be parsed back into the same AST.

    :param module: The module to turn into python code
    :type: module: :class:`ast.Module`
    :param bool prefer_single_line: If semi-colons should be preferred over newlines where there is no difference in output size
    :rtype: str

    """

    assert isinstance(module, ast.Module)

    printer = ModulePrinter(prefer_single_line=prefer_single_line)
    printer(module)

    try:
        minified_module = ast.parse(printer.code, 'python_minifier.unparse output')
    except SyntaxError as syntax_error:
        raise UnstableMinification(syntax_error, '', printer.code)

    try:
        compare_ast(module, minified_module)
    except CompareError as compare_error:
        raise UnstableMinification(compare_error, '', printer.code)

    return printer.code


def awslambda(source, filename=None, entrypoint=None):
    """
    Minify a python module for use as an AWS Lambda function

    This returns a string suitable for embedding in a cloudformation template.
    When minifying, all transformations are enabled.

    :param str source: The python module source code
    :param str filename: The original source filename if known
    :param entrypoint: The lambda entrypoint function
    :type entrypoint: str or NoneType
    :rtype: str

    """

    rename_globals = True
    if entrypoint is None:
        rename_globals = False

    return minify(
        source, filename, remove_literal_statements=True, rename_globals=rename_globals, preserve_globals=[entrypoint],
    )
