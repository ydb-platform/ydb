import pathlib
from . import _lowlevel

__all__ = [
    "Environment",
    "TemplateError",
    "safe",
    "escape",
    "render_str",
    "eval_expr",
    "pass_state",
]


def handle_panic(orig):
    def decorator(f):
        from functools import wraps

        @wraps(orig)
        def protected_call(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except BaseException as e:
                if e.__class__.__name__ == "PanicException":
                    info = _lowlevel.get_panic_info()
                    message, loc = info or ("unknown panic", None)
                    raise TemplateError(
                        "panic during rendering: {} ({})".format(
                            message, loc or "unknown location"
                        )
                    )
                raise

        return protected_call

    return decorator


class Environment(_lowlevel.Environment):
    """Represents a MiniJinja environment"""

    def __new__(cls, *args, **kwargs):
        # `_lowlevel.Environment` does not accept any arguments
        return super().__new__(cls)

    def __init__(
        self,
        loader=None,
        templates=None,
        filters=None,
        tests=None,
        globals=None,
        debug=True,
        fuel=None,
        undefined_behavior=None,
        auto_escape_callback=None,
        path_join_callback=None,
        keep_trailing_newline=False,
        trim_blocks=False,
        lstrip_blocks=False,
        finalizer=None,
        reload_before_render=False,
        block_start_string="{%",
        block_end_string="%}",
        variable_start_string="{{",
        variable_end_string="}}",
        comment_start_string="{#",
        comment_end_string="#}",
        line_statement_prefix=None,
        line_comment_prefix=None,
        pycompat=True,
    ):
        super().__init__()
        if loader is not None:
            if templates:
                raise TypeError("Cannot set loader and templates at the same time")
            self.loader = loader
        elif templates is not None:
            self.loader = dict(templates).get
        if fuel is not None:
            self.fuel = fuel
        if filters:
            for name, callback in filters.items():
                self.add_filter(name, callback)
        if tests:
            for name, callback in tests.items():
                self.add_test(name, callback)
        if globals is not None:
            for name, value in globals.items():
                self.add_global(name, value)
        self.debug = debug
        if auto_escape_callback is not None:
            self.auto_escape_callback = auto_escape_callback
        if path_join_callback is not None:
            self.path_join_callback = path_join_callback
        if keep_trailing_newline:
            self.keep_trailing_newline = True
        if trim_blocks:
            self.trim_blocks = True
        if lstrip_blocks:
            self.lstrip_blocks = True
        if finalizer is not None:
            self.finalizer = finalizer
        if undefined_behavior is not None:
            self.undefined_behavior = undefined_behavior
        self.reload_before_render = reload_before_render

        # XXX: because this is not an atomic reconfigure if you set one of
        # the values to a conflicting set, it will immediately error out :(
        self.block_start_string = block_start_string
        self.block_end_string = block_end_string
        self.variable_start_string = variable_start_string
        self.variable_end_string = variable_end_string
        self.comment_start_string = comment_start_string
        self.comment_end_string = comment_end_string
        self.line_statement_prefix = line_statement_prefix
        self.line_comment_prefix = line_comment_prefix
        self.pycompat = pycompat

    @handle_panic(_lowlevel.Environment.render_str)
    def render_str(self, *args, **kwargs):
        return super().render_str(*args, **kwargs)

    @handle_panic(_lowlevel.Environment.eval_expr)
    def eval_expr(self, *args, **kwargs):
        return super().eval_expr(*args, **kwargs)


DEFAULT_ENVIRONMENT = Environment()


def render_str(*args, **context):
    """Shortcut to render a string with the default environment."""
    return DEFAULT_ENVIRONMENT.render_str(*args, **context)


def eval_expr(*args, **context):
    """Evaluate an expression with the default environment."""
    return DEFAULT_ENVIRONMENT.eval_expr(*args, **context)


try:
    from markupsafe import escape, Markup
except ImportError:
    from html import escape as _escape

    class Markup(str):
        def __html__(self):
            return self

    def escape(value):
        callback = getattr(value, "__html__", None)
        if callback is not None:
            return callback()
        return Markup(_escape(str(value)))


def safe(s):
    """Marks a string as safe."""
    return Markup(s)


def pass_state(f):
    """Pass the engine state to the function as first argument."""
    f.__minijinja_pass_state__ = True
    return f


def load_from_path(paths):
    """Load a template from one or more paths."""
    if isinstance(paths, (str, pathlib.Path)):
        paths = [paths]

    def loader(name):
        if "\\" in name:
            return None

        pieces = name.strip("/").split("/")
        if ".." in pieces:
            return None

        for path in paths:
            p = pathlib.Path(path).joinpath(*pieces)
            if p.is_file():
                return p.read_text()

    return loader


class TemplateError(RuntimeError):
    """Represents a runtime error in the template engine."""

    def __init__(self, message):
        super().__init__(message)
        self._info = None

    @property
    def message(self):
        """The short message of the error."""
        return self.args[0]

    @property
    def kind(self):
        """The kind of the error."""
        if self._info is None:
            return "Unknown"
        else:
            return self._info.kind

    @property
    def name(self):
        """The name of the template."""
        if self._info is not None:
            return self._info.name

    @property
    def detail(self):
        """The detail error message of the error."""
        if self._info is not None:
            return self._info.detail

    @property
    def line(self):
        """The line of the error."""
        if self._info is not None:
            return self._info.line

    @property
    def range(self):
        """The range of the error."""
        if self._info is not None:
            return self._info.range

    @property
    def template_source(self):
        """The template source of the error."""
        if self._info is not None:
            return self._info.template_source

    def __str__(self):
        if self._info is not None:
            return self._info.full_description
        return self.message


del handle_panic
