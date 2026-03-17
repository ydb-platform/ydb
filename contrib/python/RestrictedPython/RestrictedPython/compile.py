import ast
import warnings
from collections import namedtuple

from RestrictedPython._compat import IS_CPYTHON
from RestrictedPython.transformer import RestrictingNodeTransformer


CompileResult = namedtuple(
    'CompileResult', 'code, errors, warnings, used_names')
syntax_error_template = (
    'Line {lineno}: {type}: {msg} at statement: {statement!r}')

NOT_CPYTHON_WARNING = (
    'RestrictedPython is only supported on CPython: use on other Python '
    'implementations may create security issues.'
)


def _compile_restricted_mode(
        source,
        filename='<string>',
        mode="exec",
        flags=0,
        dont_inherit=False,
        policy=RestrictingNodeTransformer):

    if not IS_CPYTHON:
        warnings.warn_explicit(
            NOT_CPYTHON_WARNING, RuntimeWarning, 'RestrictedPython', 0)

    byte_code = None
    collected_errors = []
    collected_warnings = []
    used_names = {}
    if policy is None:
        # Unrestricted Source Checks
        byte_code = compile(source, filename, mode=mode, flags=flags,
                            dont_inherit=dont_inherit)
    elif issubclass(policy, RestrictingNodeTransformer):
        c_ast = None
        allowed_source_types = [str, ast.Module]
        if not issubclass(type(source), tuple(allowed_source_types)):
            raise TypeError('Not allowed source type: '
                            '"{0.__class__.__name__}".'.format(source))
        c_ast = None
        # workaround for pypy issue https://bitbucket.org/pypy/pypy/issues/2552
        if isinstance(source, ast.Module):
            c_ast = source
        else:
            try:
                c_ast = ast.parse(source, filename, mode)
            except (TypeError, ValueError) as e:
                collected_errors.append(str(e))
            except SyntaxError as v:
                collected_errors.append(syntax_error_template.format(
                    lineno=v.lineno,
                    type=v.__class__.__name__,
                    msg=v.msg,
                    statement=v.text.strip() if v.text else None
                ))
        if c_ast:
            policy_instance = policy(
                collected_errors, collected_warnings, used_names)
            policy_instance.visit(c_ast)
            if not collected_errors:
                byte_code = compile(c_ast, filename, mode=mode  # ,
                                    # flags=flags,
                                    # dont_inherit=dont_inherit
                                    )
    else:
        raise TypeError('Unallowed policy provided for RestrictedPython')
    return CompileResult(
        byte_code,
        tuple(collected_errors),
        collected_warnings,
        used_names)


def compile_restricted_exec(
        source,
        filename='<string>',
        flags=0,
        dont_inherit=False,
        policy=RestrictingNodeTransformer):
    """Compile restricted for the mode `exec`."""
    return _compile_restricted_mode(
        source,
        filename=filename,
        mode='exec',
        flags=flags,
        dont_inherit=dont_inherit,
        policy=policy)


def compile_restricted_eval(
        source,
        filename='<string>',
        flags=0,
        dont_inherit=False,
        policy=RestrictingNodeTransformer):
    """Compile restricted for the mode `eval`."""
    return _compile_restricted_mode(
        source,
        filename=filename,
        mode='eval',
        flags=flags,
        dont_inherit=dont_inherit,
        policy=policy)


def compile_restricted_single(
        source,
        filename='<string>',
        flags=0,
        dont_inherit=False,
        policy=RestrictingNodeTransformer):
    """Compile restricted for the mode `single`."""
    return _compile_restricted_mode(
        source,
        filename=filename,
        mode='single',
        flags=flags,
        dont_inherit=dont_inherit,
        policy=policy)


def compile_restricted_function(
        p,  # parameters
        body,
        name,
        filename='<string>',
        globalize=None,  # List of globals (e.g. ['here', 'context', ...])
        flags=0,
        dont_inherit=False,
        policy=RestrictingNodeTransformer):
    """Compile a restricted code object for a function.

    Documentation see:
    http://restrictedpython.readthedocs.io/en/latest/usage/index.html#RestrictedPython.compile_restricted_function
    """
    # Parse the parameters and body, then combine them.
    try:
        body_ast = ast.parse(body, '<func code>', 'exec')
    except SyntaxError as v:
        error = syntax_error_template.format(
            lineno=v.lineno,
            type=v.__class__.__name__,
            msg=v.msg,
            statement=v.text.strip() if v.text else None)
        return CompileResult(
            code=None, errors=(error,), warnings=(), used_names=())

    # The compiled code is actually executed inside a function
    # (that is called when the code is called) so reading and assigning to a
    # global variable like this`printed += 'foo'` would throw an
    # UnboundLocalError.
    # We don't want the user to need to understand this.
    if globalize:
        body_ast.body.insert(0, ast.Global(globalize))
    wrapper_ast = ast.parse('def masked_function_name(%s): pass' % p,
                            '<func wrapper>', 'exec')
    # In case the name you chose for your generated function is not a
    # valid python identifier we set it after the fact
    function_ast = wrapper_ast.body[0]
    assert isinstance(function_ast, ast.FunctionDef)
    function_ast.name = name

    wrapper_ast.body[0].body = body_ast.body
    wrapper_ast = ast.fix_missing_locations(wrapper_ast)

    result = _compile_restricted_mode(
        wrapper_ast,
        filename=filename,
        mode='exec',
        flags=flags,
        dont_inherit=dont_inherit,
        policy=policy)

    return result


def compile_restricted(
        source,
        filename='<unknown>',
        mode='exec',
        flags=0,
        dont_inherit=False,
        policy=RestrictingNodeTransformer):
    """Replacement for the built-in compile() function.

    policy ... `ast.NodeTransformer` class defining the restrictions.

    """
    if mode in ['exec', 'eval', 'single', 'function']:
        result = _compile_restricted_mode(
            source,
            filename=filename,
            mode=mode,
            flags=flags,
            dont_inherit=dont_inherit,
            policy=policy)
    else:
        raise TypeError('unknown mode %s', mode)
    for warning in result.warnings:
        warnings.warn(
            warning,
            SyntaxWarning
        )
    if result.errors:
        raise SyntaxError(result.errors)
    return result.code
