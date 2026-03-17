# TraceRite

**Beautiful, readable error messages for Python, with text and HTML formatting.**

[![PyPI version](https://badge.fury.io/py/tracerite.svg)](https://pypi.org/project/tracerite/)
![Tests](https://raw.githubusercontent.com/sanic-org/tracerite/main/docs/img/tests-badge.svg)
![Coverage](https://raw.githubusercontent.com/sanic-org/tracerite/main/docs/img/coverage-badge.svg)

![TraceRite features](https://raw.githubusercontent.com/sanic-org/tracerite/main/docs/screenshots/features-composite.webp)

## Installation

### Python scripts or REPL

```sh
pip install tracerite
```

```python
import tracerite; tracerite.load()
```

Any error message after that call will be prettified. Handles any syntax errors and uncaught exceptions, even captures `logging.exception` (optionally).

### IPython or Jupyter Notebook

```ipython
%pip install tracerite
%load_ext tracerite
```

This enables tracebacks in text or HTML format depending on where you are running. Add to `~/.ipython/profile_default/startup/tracerite.ipy` to make it load automatically for all your ipython and notebook sessions. Alternatively, put the two lines at the top of your notebook.

### FastAPI

Add the extension loader at the top of your app module:

```python
from tracerite import patch_fastapi; patch_fastapi()
```

This monkeypatches Starlette error handling and FastAPI routing to work with HTML tracebacks. Note: this runs regardless of whether you are in production mode or debug mode, so you might want to call that function only conditionally in the latter.

### Sanic

Comes with TraceRite built in whenever running in debug mode.

## Clarity in complex situations

![Exception chain comparison](https://raw.githubusercontent.com/sanic-org/tracerite/main/docs/screenshots/chain-comparison.webp)
*TraceRite shows even complex exception chains in chronological order, as opposed to the convoluted order of Python's own tracebacks where the entry point `func()` is near bottom and the flow jumps back and forth.*

## Features

- **Chronological order** - Single timeline with the program entry point is at top, and the finally uncaught exception bottom.
- **Minimalistic output** - Smart pruning to show only relevant pieces of information, excluding library internals where not relevant and avoiding any repetition.
- **ExceptionGroups** - Full tracebacks of the subexceptions from exceptions that occurred in parallel execution.
- **Variable inspection** - See the values of your variables in a pretty printed HTML format, Terminal or JSON-compatible machine-readable dict.
- **JSON output** - Intermediady dict format is JSON-compatible, useful for machine processing and used by our HTML and TTY modules.
- **HTML output** - Works in Jupyter, Colab, and web frameworks such as FastAPI and Sanic as the debug mode error handler.
- **TTY output** - Colorful, formatted tracebacks for terminal applications and Python REPL.
- **Custom Styling** - Theme with your colors by defining CSS variables or tty.COLORS.
- **Automatic dark mode** - Saves your eyes.

![ExceptionGroup comparison](https://raw.githubusercontent.com/sanic-org/tracerite/main/docs/screenshots/group-comparison.webp)
*Python 3.11+ introduced `ExceptionGroup` for parallel execution errors (e.g., `asyncio.TaskGroup`). TraceRite displays these clearly.*

## Usage

### `html_traceback(exc)`

Renders an exception as interactive HTML that can be included on a page. Pass an exception object, or call with no arguments inside an `except` block to use the current exception.

### `extract_chain(exc)`

Extracts exception information as a list of dictionaries—useful for logging, custom formatting, or machine processing.

### `prettyvalue(value)`

Formats any value with smart truncation, array shape display, and SI-scaled numerics. Useful beyond exceptions for debugging tools or custom logging.

### `extract_variables(locals, source)`

Extracts and formats variables mentioned in a line of source code.

### `load()` / `unload()`

Load or remove TraceRite as the default exception handler for terminal applications. Handles both `sys.excepthook` and `threading.excepthook`.

### `tty_traceback(exc)`

Renders an exception as colorful terminal output with ANSI escape codes. Pass an exception object, or call with no arguments inside an `except` block.

See the [API documentation](https://github.com/sanic-org/tracerite/blob/main/docs/API.md) for details, or [Development guide](https://github.com/sanic-org/tracerite/blob/main/docs/Development.md) for contributors.

## License

Public Domain or equivalent.
