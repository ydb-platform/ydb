"""
Useful functions used to implement constraints and predicates.
"""
from typing import Any, Dict, Iterable, List, Sequence

from click import Argument, Context, Option, Parameter


def param_value_is_set(param: Parameter, value: Any) -> bool:
    """Define what it means for a parameter of a specific kind to be "set".

    All cases are obvious besides that of boolean options:
    - (common rule) if the value is ``None``, the parameter is unset;
    - a parameter that takes multiple values is set if at least one argument is provided;
    - a boolean **flag** is set only if True;
    - a boolean option is set if not None, even if it's False.
    """
    if value is None:
        return False
    # checking for param.is_flag is redundant but necessary to work around
    # Click 8.0.1 issue: https://github.com/pallets/click/issues/1925
    elif isinstance(param, Option) and param.is_flag and param.is_bool_flag:
        return bool(value)
    elif param.nargs != 1 or param.multiple:
        return len(value) > 0
    return True


def get_param_name(param: Parameter) -> str:
    """Return the name of a parameter casted to ``str``.
    Use this function to avoid typing errors in places where you expect a parameter
    having a name.
    """
    if param.name is None:
        raise TypeError(
            '`param.name` is required to be a string in this context.\n'
            'Hint: `param.name` is None only when `parameter.expose_value` is False, '
            'so you are probably using this option incorrectly.'
        )
    return param.name


def get_params_whose_value_is_set(
    params: Iterable[Parameter], values: Dict[str, Any]
) -> List[Parameter]:
    """Filter ``params``, returning only the parameters that have a value.
    Boolean flags are considered "set" if their value is ``True``."""
    return [p for p in params
            if param_value_is_set(p, values[get_param_name(p)])]


def get_required_params(params: Iterable[Parameter]) -> List[Parameter]:
    return [p for p in params if p.required]


def get_param_label(param: Parameter) -> str:
    if param.param_type_name == 'argument':
        return param.human_readable_name
    return sorted(param.opts, key=len)[-1]


def join_param_labels(params: Iterable[Parameter], sep: str = ', ') -> str:
    return sep.join(get_param_label(p) for p in params)


def join_with_and(strings: Sequence[str], sep: str = ', ') -> str:
    if not strings:
        return ''
    if len(strings) == 1:
        return strings[0]
    return sep.join(strings[:-1]) + ' and ' + strings[-1]


def format_param(param: Parameter) -> str:
    if isinstance(param, Argument):
        return param.human_readable_name

    opts = param.opts
    if len(opts) == 1:
        return opts[0]

    # Use the first long opt as the main/canonical one, put all others
    # opts between parenthesis as aliases, long opts first, then short ones
    long_opts = [opt for opt in opts if opt.startswith("--")]
    short_opts = [opt for opt in opts if not opt.startswith("--")]
    main_opt = long_opts[0]
    aliases = ", ".join(long_opts[1:] + short_opts)
    return f'{main_opt} ({aliases})'


def format_param_list(param_list: Iterable[Parameter], indent: int = 2) -> str:
    lines = map(format_param, param_list)
    indentation = ' ' * indent
    return ''.join(indentation + line + '\n'
                   for line in lines)


def param_label_by_name(ctx: Any, name: str) -> str:
    return get_param_label(ctx.command.get_param_by_name(name))


def get_param_labels(ctx: Any, param_names: Iterable[str]) -> List[str]:
    params = ctx.command.get_params_by_name(param_names)
    return [get_param_label(param) for param in params]


def param_value_by_name(ctx: Context, name: str) -> Any:
    try:
        return ctx.params[name]
    except KeyError:
        raise KeyError(f'"{name}" is not the name of a CLI parameter')
