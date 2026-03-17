__all__: list = [
    'attribute',
    'fix_attrs_converters',
    'universal_decorator',
    'expand'
]

import datetime
import functools
import inspect
import linecache
import uuid
from inspect import isclass, signature, Signature, Parameter, BoundArguments
from textwrap import dedent, indent
from typing import Any, Callable, Dict, List, Optional, Tuple, Type

import attr

from ..util import get_signature
from ..util._typing import is_optional_of

REQUIRED_KEY = 'toloka_field_required'
ORIGIN_KEY = 'toloka_field_origin'
READONLY_KEY = 'toloka_field_readonly'
AUTOCAST_KEY = 'toloka_field_autocast'


def _get_decorator_wrapper(wrapped_decorator):

    @functools.wraps(wrapped_decorator)
    def decorator_wrapper(func):
        """Applies the decorator and wraps the resulting function-like object with the original type."""
        decorated_func = wrapped_decorator(func)

        if inspect.isasyncgenfunction(func):

            @functools.wraps(decorated_func)
            async def wrapped(*args, **kwargs):
                async for item in decorated_func(*args, **kwargs):
                    yield item

        elif inspect.isgeneratorfunction(func):

            @functools.wraps(decorated_func)
            def wrapped(*args, **kwargs):
                yield from decorated_func(*args, **kwargs)

        elif inspect.iscoroutinefunction(func):

            @functools.wraps(decorated_func)
            async def wrapped(*args, **kwargs):
                res = await decorated_func(*args, **kwargs)
                return res

        else:  # plain function
            wrapped = decorated_func

        return wrapped

    return decorator_wrapper


def universal_decorator(*, has_parameters):
    """Metadecorator used for writing universal decorators that does not change function-like object type.

    Wrapped decorator will preserve function-like object type: plain functions, async functions, generators and
    async generators will keep their type after being decorated. Warning: if your decorator changes the
    type of the function-like object (e.g. yields plain function result) do NOT use this (meta)decorator.

    Args:
        has_parameters: does wrapped decorator use parameters
    """

    # top level closure for universal_decorator parameters capturing
    def wrapper(dec):
        if has_parameters:
            # @decorator(...) syntax

            @functools.wraps(dec)
            def get_decorator(*args, **kwargs):
                return _get_decorator_wrapper(wrapped_decorator=dec(*args, **kwargs))

            return get_decorator

        # @decorator syntax
        return _get_decorator_wrapper(wrapped_decorator=dec)

    return wrapper


def _remove_annotations_from_signature(sig: Signature) -> Signature:
    """
    Returns a new Signature object that differs from the
    original one only by annotation
    """
    params = sig.parameters.values()
    new_params = [p.replace(annotation=Parameter.empty) for p in params]
    return Signature(parameters=new_params)


def _make_keyword_only_signature(sig: Signature) -> Signature:
    """
    Returns a new Signature object where all arguments are
    positional only
    """
    new_params = []
    for param in sig.parameters.values():
        if param.kind in (Parameter.KEYWORD_ONLY, Parameter.VAR_KEYWORD):
            new_params.append(param)
        elif param.kind == Parameter.POSITIONAL_OR_KEYWORD:
            new_params.append(param.replace(kind=Parameter.KEYWORD_ONLY))
        else:
            raise ValueError(f'Cannot convert signature {sig} to keyword-only')

    return sig.replace(parameters=new_params)


def _get_annotations_from_signature(sig: Signature) -> dict:
    annotations = {
        p.name: p.annotation
        for p in sig.parameters.values()
        if p.annotation != Parameter.empty
    }

    if sig.return_annotation != Parameter.empty:
        annotations['return'] = sig.return_annotation

    return annotations


def _try_bind_arguments(sig: Signature, args: List, kwargs: Dict) -> Tuple[Optional[BoundArguments], Optional[TypeError]]:
    """Checks if arguments are suitable for provided signature. Return bindings and exception."""
    try:
        return sig.bind(*args, **kwargs), None
    except TypeError as err:
        return None, err


def _get_signature_invocation_string(sig: Signature) -> str:
    """
    Generates a string that could be added to a function
    with provided signature to initiate a completely valid
    call assuming that we have variables in our local scope
    named the same way as signature arguments
    """
    tokens = []

    for param in sig.parameters.values():
        if param.kind == Parameter.VAR_POSITIONAL:
            tokens.append(f'*{param.name}')
        elif param.kind == Parameter.VAR_KEYWORD:
            tokens.append(f'**{param.name}')
        elif param.kind == Parameter.KEYWORD_ONLY:
            tokens.append(f'{param.name}={param.name}')
        else:
            tokens.append(param.name)

    return '(' + ', '.join(tokens) + ')'


def _compile_function(func_name, func_sig, func_body, is_coroutine=False, globs=None):
    file_name = f'{func_name}_{uuid.uuid4().hex}'
    annotations = _get_annotations_from_signature(func_sig)
    sig = _remove_annotations_from_signature(func_sig)

    source = f'{"async " if is_coroutine else ""}def {func_name}{sig}:\n{indent(func_body, " " * 4)}'
    bytecode = compile(source, file_name, 'exec')
    namespace = {}
    eval(bytecode, {} if globs is None else globs, namespace)

    func = namespace[func_name]
    func.__annotations__ = annotations

    linecache.cache[file_name] = (
        len(source),
        None,
        source.splitlines(True),
        file_name
    )

    return func


def create_setter(attr_path: str, attr_type=Parameter.empty, module: Optional[str] = None):
    """Generates a setter method for an attribute"""
    attr_name = attr_path.split('.')[-1]
    func = _compile_function(
        f'codegen_setter_for_{attr_path.replace(".", "_")}',
        Signature(parameters=[
            Parameter(name='self', kind=Parameter.POSITIONAL_OR_KEYWORD),
            Parameter(name=attr_name, kind=Parameter.POSITIONAL_OR_KEYWORD, annotation=attr_type),
        ]),
        (
            f'"""A shortcut setter for {attr_path}"""\n'
            f'self.{attr_path} = {attr_name}'
        )
    )
    if module:
        func.__module__ = module
    return func


def codegen_attr_attributes_setters(cls):
    """
    Adds setters for both required or optional attributes with attr
    constructed types. Resulting signatures are identical to the
    attribute type's constructor's.
    """
    for field in attr.fields(cls):
        type_ = is_optional_of(field.type) or field.type
        if attr.has(type_):
            setter_name = f'set_{field.name}'
            setter = expand(field.name)(create_setter(field.name, type_, cls.__module__))
            setattr(cls, setter_name, setter)
    return cls


def expand_func_by_argument(func: Callable, arg_name: str, arg_type: Optional[Type] = None) -> Callable:
    func_sig: Signature = get_signature(func)
    func_params: List[Parameter] = list(func_sig.parameters.values())

    arg_param: Parameter = func_sig.parameters[arg_name]
    arg_index = next(i for (i, p) in enumerate(func_params) if p is arg_param)
    if arg_type is None:
        arg_type = is_optional_of(arg_param.annotation) or arg_param.annotation
    arg_type_sig: Signature = get_signature(arg_type)

    # TODO: add tests
    if arg_param.kind == Parameter.KEYWORD_ONLY:
        arg_type_sig = _make_keyword_only_signature(arg_type_sig)

    if attr.has(arg_type):
        additional_params: dict[str, Parameter] = dict(arg_type_sig.parameters)
        for field in attr.fields(arg_type):
            if field.metadata.get(READONLY_KEY, False):
                additional_params.pop(field.name)
        arg_type_sig = arg_type_sig.replace(parameters=list(additional_params.values()))

    new_params: List[Parameter] = list(func_params)
    new_params[arg_index:arg_index + 1] = arg_type_sig.parameters.values()

    if inspect.iscoroutinefunction(func):
        function_body = dedent(f'''
            {arg_name} = {arg_type.__name__}{_get_signature_invocation_string(arg_type_sig)}
            return await func{_get_signature_invocation_string(func_sig)}
        ''')
    else:
        function_body = dedent(f'''
            {arg_name} = {arg_type.__name__}{_get_signature_invocation_string(arg_type_sig)}
            return func{_get_signature_invocation_string(func_sig)}
        ''')

    expanded_func = _compile_function(
        f'{func.__name__}_expanded_by_{arg_name}',
        func_sig.replace(parameters=new_params),
        function_body,
        inspect.iscoroutinefunction(func),
        {
            arg_type.__name__: arg_type,
            'func': func,
            'NOTHING': attr.NOTHING,
            'datetime': datetime,
        }
    )
    expanded_func.__doc__ = func.__doc__
    return expanded_func


def _check_arg_type_compatibility(func_sig: Signature, arg_name: str, arg_type: Optional[Type], bound: BoundArguments) -> Tuple[bool, Optional[str]]:
    arg_param: Parameter = func_sig.parameters[arg_name]
    if arg_type is None:
        arg_type = is_optional_of(arg_param.annotation) or arg_param.annotation
    bound.apply_defaults()
    arg_candidate: Any = bound.arguments[arg_name]  # incoming argument
    if isinstance(arg_candidate, arg_type):
        return True, None
    return False, f'Argument "{arg_candidate}" has type "{type(arg_candidate)}" that is not a subclass of "{arg_type}"'


@universal_decorator(has_parameters=True)
def expand(arg_name: str, arg_type: Optional[Type] = None, check_type: bool = True) -> Callable:
    """Allows you to call function also via passing values for creating "arg_name", not only via passing an instance of "arg_name"

    If used on class, expands some argument in __init__

    Args:
        arg_name: Parameter that will be expanded.
        arg_type: Specify the type of this parameter. Defaults to None and calc it themselves.
        check_type: If True, check that one argument has a compatible type for none expanded version. If not, calls an expanded version. Defaults to True.
    """
    def wrapper(func):
        if isclass(func):
            func.__init__ = expand(arg_name, arg_type, check_type)(func.__init__)
            return func

        func_sig: Signature = get_signature(func)
        expanded_func: Callable = expand_func_by_argument(func, arg_name, arg_type)
        expanded_func_sig: Signature = get_signature(expanded_func)

        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            bound, func_problem = _try_bind_arguments(func_sig, args, kwargs)
            if bound is not None:
                if check_type and arg_name not in kwargs:
                    fit, func_problem = _check_arg_type_compatibility(func_sig, arg_name, arg_type, bound)
                    if fit:
                        return func(*args, **kwargs)
                else:
                    return func(*args, **kwargs)

            bound, expand_func_problem = _try_bind_arguments(expanded_func_sig, args, kwargs)
            if bound is None:
                raise TypeError(
                    f'Arguments does not fit standart or expanded version.\nStandart version on problem: {func_problem}\nExpand version on problem: {expand_func_problem}')
            return expanded_func(*args, **kwargs)

        wrapped._func = func
        wrapped._expanded_func = expanded_func
        wrapped._func_sig = func_sig
        wrapped._expanded_func_sig = get_signature(expanded_func)
        wrapped._expanded_by = arg_name
        return wrapped

    return wrapper


def attribute(*args, required: bool = False, origin: Optional[str] = None, readonly: bool = False,
              autocast: bool = False, **kwargs):
    """Proxy for attr.attrib(...). Adds several keywords.

    Args:
        *args: All positional arguments from attr.attrib
        required: If True makes attribute not Optional. All other attributes are optional by default. Defaults to False.
        origin: Sets field name in dict for attribute, when structuring/unstructuring from dict. Defaults to None.
        readonly: Affects only when the class 'expanding' as a parameter in some function. If True, drops this attribute from expanded parameters. Defaults to None.
        autocast: If True then converter.structure will be used to convert input value
        **kwargs: All keyword arguments from attr.attrib
    """
    metadata = {}
    if required:
        metadata[REQUIRED_KEY] = True
    if origin:
        metadata[ORIGIN_KEY] = origin
    if readonly:
        metadata[READONLY_KEY] = True
    if autocast:
        metadata[AUTOCAST_KEY] = True
    return attr.attrib(*args, metadata=metadata, **kwargs)


def fix_attrs_converters(cls):
    """
    Due to [https://github.com/Toloka/toloka-kit/issues/37](https://github.com/Toloka/toloka-kit/issues/37)
    we have to support attrs>=20.3.0.
    This version lacks a feature that uses converters' annotations in class's __init__
    (see [https://github.com/python-attrs/attrs/pull/710](https://github.com/python-attrs/attrs/pull/710))).
    This decorator brings this feature to older attrs versions.
    """

    if attr.__version__ < '21.0.3':
        fields_dict = attr.fields_dict(cls)

        def update_param_from_converter(param):

            # Trying to figure out which attribute this parameter is responsible for.
            # Note that attr stips leading underscores from attribute names, so we
            # check both name and _name.
            attribute = fields_dict.get(param.name) or fields_dict.get('_' + param.name)

            # Only process attributes with converter
            if attribute is not None and attribute.converter:
                # Retrieving converter's first (and only) parameter
                converter_sig = signature(attribute.converter)
                converter_param = next(iter(converter_sig.parameters.values()))
                # And use this parameter's annotation for our own
                param = param.replace(annotation=converter_param.annotation)

            return param

        init_sig = signature(cls.__init__)
        new_params = [update_param_from_converter(param) for param in init_sig.parameters.values()]
        new_annotations = {param.name: param.annotation for param in new_params if param.annotation}
        cls.__init__.__signature__ = init_sig.replace(parameters=new_params)
        cls.__init__.__annotations__ = new_annotations

    return cls
