import os

if os.name == 'nt':
    from .windows import (
        get_console_mode as _get_console_mode,
        getch as _getch,
        reset_console_mode as _reset_console_mode,
        set_console_mode as _set_console_mode,
    )

else:
    from .posix import getch as _getch

    def _reset_console_mode() -> None:
        pass

    def _set_console_mode() -> bool:
        return False

    def _get_console_mode() -> int:
        return 0


getch = _getch
reset_console_mode = _reset_console_mode
set_console_mode = _set_console_mode
get_console_mode = _get_console_mode
