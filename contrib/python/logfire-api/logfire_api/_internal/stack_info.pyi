from _typeshed import Incomplete
from pathlib import Path
from types import CodeType, FrameType
from typing import TypedDict

StackInfo = TypedDict('StackInfo', {'code.filepath': str, 'code.lineno': int, 'code.function': str}, total=False)
STACK_INFO_KEYS: Incomplete
NON_USER_CODE_PREFIXES: tuple[str, ...]

def add_non_user_code_prefix(path: str | Path) -> None:
    """Add a path to the list of prefixes that are considered non-user code.

    This prevents the stack info from including frames from the given path.

    This is for advanced users and shouldn't often be needed.
    By default, the following prefixes are already included:

    - The standard library
    - site-packages (specifically wherever opentelemetry is installed)
    - The logfire package

    This function is useful if you're writing a library that uses logfire and you want to exclude your library's frames.
    Since site-packages is already included, this is already the case by default for users of your library.
    But this is useful when testing your library since it's not installed in site-packages.
    """
def get_filepath_attribute(file: str) -> StackInfo: ...
def get_code_object_info(code: CodeType) -> StackInfo: ...
def get_stack_info_from_frame(frame: FrameType) -> StackInfo: ...
def get_user_stack_info() -> StackInfo:
    """Get the stack info for the first calling frame in user code.

    See is_user_code for details.
    Returns an empty dict if no such frame is found.
    """
def get_user_frame_and_stacklevel() -> tuple[FrameType | None, int]:
    """Get the first calling frame in user code and a corresponding stacklevel that can be passed to `warnings.warn`.

    See is_user_code for details.
    Returns `(None, 0)` if no such frame is found.
    """
def is_user_code(code: CodeType) -> bool:
    """Check if the code object is from user code.

    A code object is not user code if:
    - It is from a file in
        - the standard library
        - site-packages (specifically wherever opentelemetry is installed)
        - the logfire package
        - an unknown location (e.g. a dynamically generated code object) indicated by a filename starting with '<'
    - It is a list/dict/set comprehension.
        These are artificial frames only created before Python 3.12,
        and they are always called directly from the enclosing function so it makes sense to skip them.
        On the other hand, generator expressions and lambdas might be called far away from where they are defined.
    """
def warn_at_user_stacklevel(msg: str, category: type[Warning]): ...
