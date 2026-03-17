__all__: list = ['inherit_docstrings']
from collections import OrderedDict
from inspect import Signature
from io import StringIO
from textwrap import indent
from typing import Any, Dict, List, TypeVar, Type, Collection, Optional

import docstring_parser

from ..util import get_signature


def _get_docstring_params_dict(obj: Any) -> Dict[str, docstring_parser.DocstringParam]:
    params = docstring_parser.google.parse(obj.__doc__).params
    params_dict = OrderedDict((param.arg_name, param) for param in params)
    return params_dict


def _replace_params_in_docstring(docstring: docstring_parser.Docstring,
                                 new_params: Collection[docstring_parser.DocstringParam]) -> None:
    # erase old params from docstring
    new_meta = [meta for meta in docstring.meta
                if not isinstance(meta, docstring_parser.DocstringParam)]
    # replace them by new_params
    new_meta.extend(new_params)
    docstring.meta = new_meta


T = TypeVar('T')


def inherit_docstrings(cls: Type[T]) -> Type[T]:
    """Modifies docstring parameters list of class and nested members updating it with docstring parameters from parent
    classes and parent classes members.

    All args and attributes from all parent classes are added to applied class docstring. If any parameter present in
    more than a one class it is updated by the most recent version (according to mro order). The same applies to all
    nested members: nested member is considered to be a parent of other nested member if and only if corresponding outer
    classes have the same relationship and nested members have the same name.

    Args:
        cls: target class.
    """
    cls_docstring = docstring_parser.google.parse(cls.__doc__)
    nested_docstrings = {}
    inherited_params = OrderedDict()
    nested_inherited_params = {}

    for member_name in dir(cls):
        if member_name.startswith('_'):
            continue
        nested_docstring = getattr(getattr(cls, member_name), '__doc__')
        if nested_docstring:
            nested_docstrings[member_name] = docstring_parser.google.parse(nested_docstring)
            nested_inherited_params[member_name] = OrderedDict()

    traverse_order = list(cls.__mro__[::-1])
    traverse_order.append(cls)

    for parent in traverse_order:
        inherited_params.update(_get_docstring_params_dict(parent))
        for member_name in dir(parent):
            if member_name in nested_inherited_params:
                nested_inherited_params[member_name].update(_get_docstring_params_dict(getattr(parent, member_name)))

    _replace_params_in_docstring(cls_docstring, inherited_params.values())
    cls.__doc__ = _construct_docstring(cls_docstring, get_signature(cls))
    for member_name in nested_docstrings:
        if nested_inherited_params[member_name]:
            _replace_params_in_docstring(nested_docstrings[member_name], nested_inherited_params[member_name].values())
            member = getattr(cls, member_name)
            member_sig = None
            if isinstance(member, type):
                member_sig = get_signature(member)
            getattr(cls, member_name).__doc__ = _construct_docstring(nested_docstrings[member_name], member_sig)

    return cls


def _indent_all_lines_except_first(text: str) -> str:
    first_line_end = text.find('\n')
    if first_line_end == -1:
        return text
    return text[:first_line_end + 1] + indent(text[first_line_end + 1:], ' ' * 4)


def _get_key_value_chunk(title: str, keys: List[str], values: List[str]) -> str:
    io = StringIO()
    io.write(f'{title}:\n')
    for key, value in zip(keys, values):
        io.write(indent(f'{key}: {_indent_all_lines_except_first(value)}\n', ' ' * 4))
    return io.getvalue().rstrip('\n')


def _construct_docstring(parsed_docstring: docstring_parser.Docstring, sig: Optional[Signature] = None) -> str:
    chunks = []
    docstring_prefix = ''

    if parsed_docstring.short_description:
        chunks.append(parsed_docstring.short_description)
    else:
        docstring_prefix = '\n'

    if parsed_docstring.long_description:
        chunks.append(parsed_docstring.long_description)

    arg_params = []
    attr_params = []
    params_dict = {param.arg_name: param for param in parsed_docstring.params}
    if sig:
        for sig_param_name in sig.parameters.keys():
            param = params_dict.pop(sig_param_name, None)

            if not param:
                continue

            if param.args[0] == 'attribute':
                attr_params.append(param)
            elif param.args[0] == 'param':
                arg_params.append(param)

    for param in parsed_docstring.params:
        if param.arg_name not in params_dict:
            continue
        if param.args[0] == 'attribute':
            attr_params.append(param)
        elif param.args[0] == 'param':
            arg_params.append(param)

    if arg_params:
        chunk = _get_key_value_chunk(title='Args',
                                     keys=[param.arg_name for param in arg_params],
                                     values=[param.description for param in arg_params])
        chunks.append(chunk)

    if attr_params:
        chunk = _get_key_value_chunk(title='Attributes',
                                     keys=[param.arg_name for param in attr_params],
                                     values=[param.description for param in attr_params])
        chunks.append(chunk)

    returns = parsed_docstring.returns
    if returns:
        chunk = _get_key_value_chunk(title='Yields' if returns.is_generator else 'Returns',
                                     keys=[returns.type_name],
                                     values=[returns.description])
        chunks.append(chunk)

    raises = parsed_docstring.raises
    if raises:
        chunk = _get_key_value_chunk(title='Raises',
                                     keys=[exception.type_name for exception in raises],
                                     values=[exception.description for exception in raises])
        chunks.append(chunk)

    for meta in parsed_docstring.meta:
        if type(meta) not in [docstring_parser.DocstringParam,
                              docstring_parser.DocstringReturns,
                              docstring_parser.DocstringRaises]:
            io = StringIO()
            io.write(f'{meta.args[0].title()}:\n')
            io.write(indent(meta.description, ' ' * 4))
            chunks.append(io.getvalue())

    docstring = docstring_prefix + '\n\n'.join(chunks)
    return docstring
