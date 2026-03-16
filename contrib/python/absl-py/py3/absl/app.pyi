from collections.abc import Callable
from typing import Any, NoReturn, TypeVar, overload

from absl.flags import _flag

_MainArgs = TypeVar('_MainArgs')
_Exc = TypeVar('_Exc', bound=Exception)

class ExceptionHandler:

  def wants(self, exc: _Exc) -> bool:
    ...

  def handle(self, exc: _Exc):
    ...

EXCEPTION_HANDLERS: list[ExceptionHandler] = ...

class HelpFlag(_flag.BooleanFlag):
  def __init__(self):
    ...

class HelpshortFlag(HelpFlag):
  ...

class HelpfullFlag(_flag.BooleanFlag):
  def __init__(self):
    ...

class HelpXMLFlag(_flag.BooleanFlag):
  def __init__(self):
    ...

def define_help_flags() -> None:
  ...

@overload
def usage(shorthelp: bool | int = ...,
          writeto_stdout: bool | int = ...,
          detailed_error: Any | None = ...,
          exitcode: None = ...) -> None:
  ...

@overload
def usage(shorthelp: bool | int,
          writeto_stdout: bool | int,
          detailed_error: Any | None,
          exitcode: int) -> NoReturn:
  ...

@overload
def usage(shorthelp: bool | int = ...,
          writeto_stdout: bool | int = ...,
          detailed_error: Any | None = ...,
          *,
          exitcode: int) -> NoReturn:
  ...

def install_exception_handler(handler: ExceptionHandler) -> None:
  ...

class Error(Exception):
  ...

class UsageError(Error):
  exitcode: int

def parse_flags_with_usage(args: list[str]) -> list[str]:
  ...

def call_after_init(callback: Callable[[], Any]) -> None:
  ...

# Without the flag_parser argument, `main` should require a List[str].
@overload
def run(
    main: Callable[[list[str]], Any],
    argv: list[str] | None = ...,
) -> NoReturn:
  ...

@overload
def run(
    main: Callable[[_MainArgs], Any],
    argv: list[str] | None = ...,
    *,
    flags_parser: Callable[[list[str]], _MainArgs],
) -> NoReturn:
  ...
