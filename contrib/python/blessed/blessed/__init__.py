"""
A thin, practical wrapper around terminal capabilities in Python.

http://pypi.python.org/pypi/blessed
"""
# std imports
import platform as _platform

# isort: off
if _platform.system() == 'Windows':
    from blessed.win_terminal import Terminal
else:
    from blessed.terminal import Terminal  # type: ignore[assignment]

from blessed.line_editor import LineEditor, LineHistory

__all__ = ('Terminal', 'LineEditor', 'LineHistory')
__version__ = "1.32.0"
