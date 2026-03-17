import functools
import inspect
import sys
import linecache
from datetime import datetime

enable_print = False


def print_lines(func):
    if not enable_print:
        return func

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Get function name and determine if it's a method
        is_method = inspect.ismethod(func) or (
            len(args) > 0 and hasattr(args[0].__class__, func.__name__)
        )
        class_name = args[0].__class__.__name__ if is_method else None  # type: ignore

        # Get the source code of the function
        try:
            source_lines, start_line = inspect.getsourcelines(func)
        except OSError:
            # Handle cases where source might not be available
            print(f"Warning: Could not get source for {func.__name__}")
            return func(*args, **kwargs)

        def trace(frame, event, arg):
            if event == "line":
                # Get the current line number and code
                line_no = frame.f_lineno
                line = linecache.getline(frame.f_code.co_filename, line_no).strip()

                # Don't print decorator lines or empty lines
                if line and not line.startswith("@"):
                    # Get local variables
                    local_vars = frame.f_locals.copy()
                    if is_method:
                        # Remove 'self' from local variables for clarity
                        local_vars.pop("self", None)

                    # Format timestamp
                    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]

                    # Create context string (class.method or function)
                    context = (
                        f"{class_name}.{func.__name__}" if class_name else func.__name__
                    )

                    # Print execution information
                    print(f"[{timestamp}] {context} line {line_no}: {line}")

                    # Print local variables if they exist and have changed
                    if local_vars:
                        vars_str = ", ".join(
                            f"{k}={repr(v)}" for k, v in local_vars.items()
                        )
                        print(f"    Variables: {vars_str}")
            return trace

        # Set the trace function
        sys.settrace(trace)

        # Call the original function
        result = func(*args, **kwargs)

        # Disable tracing
        sys.settrace(None)

        return result

    return wrapper
