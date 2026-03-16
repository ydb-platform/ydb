"""
Types for parameter decorators are in this stub for convenience of implementation.
"""
from typing import Any, Callable, List, Optional, Sequence, Tuple, Type, TypeVar, Union

import click
from click.shell_completion import CompletionItem

from cloup import OptionGroup

F = TypeVar('F', bound=Callable[..., Any])
P = TypeVar('P', bound=click.Parameter)

SimpleParamTypeLike = Union[click.ParamType, Type[float], Type[int], Type[str]]
ParamTypeLike = Union[SimpleParamTypeLike, Tuple[SimpleParamTypeLike, ...]]
ParamDefault = Union[Any, Callable[[], Any]]
ParamCallback = Callable[[click.Context, P, Any], Any]
ShellCompleteArg = Callable[
    [click.Context, P, str],
    Union[List[CompletionItem], List[str]],
]


def make_arg_metavar(arg: click.Argument, ctx: click.Context) -> str:
    ...


class Argument(click.Argument):
    def __init__(self, *args: Any, help: Optional[str] = None, **attrs: Any):
        ...

    def get_help_record(self, ctx: click.Context) -> Tuple[str, str]:
        ...


class Option(click.Option):
    def __init__(self, *args: Any, group: Optional[OptionGroup] = None, **attrs: Any):
        ...


def argument(
    *param_decls: str,
    cls: Optional[Type[Argument]] = None,
    help: Optional[str] = None,
    type: Optional[ParamTypeLike] = None,
    required: Optional[bool] = None,
    default: Optional[ParamDefault] = None,
    callback: Optional[ParamCallback[click.Argument]] = None,
    nargs: Optional[int] = None,
    metavar: Optional[str] = None,
    expose_value: bool = True,
    envvar: Optional[Union[str, Sequence[str]]] = None,
    shell_complete: Optional[ShellCompleteArg[click.Argument]] = None,
    **kwargs: Any,
) -> Callable[[F], F]: ...


def option(
    *param_decls: str,
    cls: Optional[Type[click.Option]] = None,
    # Commonly used
    metavar: Optional[str] = None,
    type: Optional[ParamTypeLike] = None,
    is_flag: Optional[bool] = None,
    default: Optional[ParamDefault] = None,
    required: Optional[bool] = None,
    help: Optional[str] = None,
    # Processing
    callback: Optional[ParamCallback[click.Option]] = None,
    is_eager: bool = False,
    # Help text tuning
    show_choices: bool = True,
    show_default: bool = False,
    show_envvar: bool = False,
    # Flag options
    flag_value: Optional[Any] = None,
    count: bool = False,
    # Multiple values
    nargs: Optional[int] = None,
    multiple: bool = False,
    # Prompt
    prompt: Union[bool, str] = False,
    confirmation_prompt: Union[bool, str] = False,
    prompt_required: bool = True,
    hide_input: bool = False,
    # Environment
    allow_from_autoenv: bool = True,
    envvar: Optional[Union[str, Sequence[str]]] = None,
    # Hiding
    hidden: bool = False,
    expose_value: bool = True,
    # Others
    group: Optional[OptionGroup] = None,
    shell_complete: Optional[ShellCompleteArg[click.Option]] = None,
    **kwargs: Any
) -> Callable[[F], F]: ...
