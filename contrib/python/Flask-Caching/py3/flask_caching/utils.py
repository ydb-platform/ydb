import inspect
import string
from typing import Callable
from typing import List
from typing import Optional

TEMPLATE_FRAGMENT_KEY_TEMPLATE = "_template_fragment_cache_%s%s"
# Used to remove control characters and whitespace from cache keys.
valid_chars = set(string.ascii_letters + string.digits + "_.")
del_chars = "".join(c for c in map(chr, range(256)) if c not in valid_chars)
null_control = ({k: None for k in del_chars},)


def wants_args(f: Callable) -> bool:
    """Check if the function wants any arguments"""
    arg_spec = inspect.getfullargspec(f)
    return bool(arg_spec.args or arg_spec.varargs or arg_spec.varkw)


def get_function_parameters(f: Callable) -> List:
    """Get function parameters
    :param f
    :return: Parameter list of function
    """
    return list(inspect.signature(f).parameters.values())


def get_arg_names(f: Callable) -> List[str]:
    """Return arguments of function
    :param f:
    :return: String list of arguments
    """
    return [
        parameter.name
        for parameter in get_function_parameters(f)
        if parameter.kind == parameter.POSITIONAL_OR_KEYWORD
    ]


def get_arg_default(f: Callable, position: int):
    arg = get_function_parameters(f)[position]
    arg_def = arg.default
    return arg_def if arg_def != inspect.Parameter.empty else None


def get_id(obj):
    return getattr(obj, "__caching_id__", repr)(obj)


def function_namespace(f, args=None):
    """Attempts to returns unique namespace for function"""
    m_args = get_arg_names(f)

    instance_token = None

    instance_self = getattr(f, "__self__", None)

    if instance_self and not inspect.isclass(instance_self):
        instance_token = get_id(f.__self__)
    elif m_args and m_args[0] == "self" and args:
        instance_token = get_id(args[0])

    module = f.__module__

    if m_args and m_args[0] == "cls" and not inspect.isclass(args[0]):
        raise ValueError(
            "When using `delete_memoized` on a "
            "`@classmethod` you must provide the "
            "class as the first argument"
        )

    if hasattr(f, "__qualname__"):
        name = f.__qualname__
    else:
        klass = getattr(f, "__self__", None)

        if klass and not inspect.isclass(klass):
            klass = klass.__class__

        if not klass:
            klass = getattr(f, "im_class", None)

        if not klass:
            if m_args and args:
                if m_args[0] == "self":
                    klass = args[0].__class__
                elif m_args[0] == "cls":
                    klass = args[0]

        if klass:
            name = klass.__name__ + "." + f.__name__
        else:
            name = f.__name__

    ns = ".".join((module, name)).translate(*null_control)

    ins = (
        ".".join((module, name, instance_token)).translate(*null_control)
        if instance_token
        else None
    )

    return ns, ins


def make_template_fragment_key(
    fragment_name: str, vary_on: Optional[List[str]] = None
) -> str:
    """Make a cache key for a specific fragment name."""
    if vary_on:
        fragment_name = "%s_" % fragment_name
    else:
        vary_on = []
    return TEMPLATE_FRAGMENT_KEY_TEMPLATE % (fragment_name, "_".join(vary_on))
