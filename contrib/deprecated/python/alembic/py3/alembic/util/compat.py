import collections
import inspect
import io
import os
import sys

py2k = sys.version_info.major < 3
py3k = sys.version_info.major >= 3
py36 = sys.version_info >= (3, 6)
is_posix = os.name == "posix"


ArgSpec = collections.namedtuple(
    "ArgSpec", ["args", "varargs", "keywords", "defaults"]
)


def inspect_getargspec(func):
    """getargspec based on fully vendored getfullargspec from Python 3.3."""

    if inspect.ismethod(func):
        func = func.__func__
    if not inspect.isfunction(func):
        raise TypeError("{!r} is not a Python function".format(func))

    co = func.__code__
    if not inspect.iscode(co):
        raise TypeError("{!r} is not a code object".format(co))

    nargs = co.co_argcount
    names = co.co_varnames
    nkwargs = co.co_kwonlyargcount if py3k else 0
    args = list(names[:nargs])

    nargs += nkwargs
    varargs = None
    if co.co_flags & inspect.CO_VARARGS:
        varargs = co.co_varnames[nargs]
        nargs = nargs + 1
    varkw = None
    if co.co_flags & inspect.CO_VARKEYWORDS:
        varkw = co.co_varnames[nargs]

    return ArgSpec(args, varargs, varkw, func.__defaults__)


if py3k:
    from io import StringIO
else:
    # accepts strings
    from StringIO import StringIO  # noqa

if py3k:
    import builtins as compat_builtins

    string_types = (str,)
    binary_type = bytes
    text_type = str

    def callable(fn):  # noqa
        return hasattr(fn, "__call__")

    def u(s):
        return s

    def ue(s):
        return s

    range = range  # noqa
else:
    import __builtin__ as compat_builtins

    string_types = (basestring,)  # noqa
    binary_type = str
    text_type = unicode  # noqa
    callable = callable  # noqa

    def u(s):
        return unicode(s, "utf-8")  # noqa

    def ue(s):
        return unicode(s, "unicode_escape")  # noqa

    range = xrange  # noqa

if py3k:
    import collections.abc as collections_abc
else:
    import collections as collections_abc  # noqa

if py3k:

    def _formatannotation(annotation, base_module=None):
        """vendored from python 3.7"""

        if getattr(annotation, "__module__", None) == "typing":
            return repr(annotation).replace("typing.", "")
        if isinstance(annotation, type):
            if annotation.__module__ in ("builtins", base_module):
                return annotation.__qualname__
            return annotation.__module__ + "." + annotation.__qualname__
        return repr(annotation)

    def inspect_formatargspec(
        args,
        varargs=None,
        varkw=None,
        defaults=None,
        kwonlyargs=(),
        kwonlydefaults={},
        annotations={},
        formatarg=str,
        formatvarargs=lambda name: "*" + name,
        formatvarkw=lambda name: "**" + name,
        formatvalue=lambda value: "=" + repr(value),
        formatreturns=lambda text: " -> " + text,
        formatannotation=_formatannotation,
    ):
        """Copy formatargspec from python 3.7 standard library.

        Python 3 has deprecated formatargspec and requested that Signature
        be used instead, however this requires a full reimplementation
        of formatargspec() in terms of creating Parameter objects and such.
        Instead of introducing all the object-creation overhead and having
        to reinvent from scratch, just copy their compatibility routine.

        """

        def formatargandannotation(arg):
            result = formatarg(arg)
            if arg in annotations:
                result += ": " + formatannotation(annotations[arg])
            return result

        specs = []
        if defaults:
            firstdefault = len(args) - len(defaults)
        for i, arg in enumerate(args):
            spec = formatargandannotation(arg)
            if defaults and i >= firstdefault:
                spec = spec + formatvalue(defaults[i - firstdefault])
            specs.append(spec)
        if varargs is not None:
            specs.append(formatvarargs(formatargandannotation(varargs)))
        else:
            if kwonlyargs:
                specs.append("*")
        if kwonlyargs:
            for kwonlyarg in kwonlyargs:
                spec = formatargandannotation(kwonlyarg)
                if kwonlydefaults and kwonlyarg in kwonlydefaults:
                    spec += formatvalue(kwonlydefaults[kwonlyarg])
                specs.append(spec)
        if varkw is not None:
            specs.append(formatvarkw(formatargandannotation(varkw)))
        result = "(" + ", ".join(specs) + ")"
        if "return" in annotations:
            result += formatreturns(formatannotation(annotations["return"]))
        return result


else:
    from inspect import formatargspec as inspect_formatargspec  # noqa


if py3k:
    from configparser import ConfigParser as SafeConfigParser
    import configparser
else:
    from ConfigParser import SafeConfigParser  # noqa
    import ConfigParser as configparser  # noqa

if py2k:
    from mako.util import parse_encoding

if py3k:
    import importlib.machinery

    import importlib.util

    def load_module_py(module_id, path):
        spec = importlib.util.spec_from_file_location(module_id, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def load_module_pyc(module_id, path):
        spec = importlib.util.spec_from_file_location(module_id, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def get_bytecode_suffixes():
        try:
            return importlib.machinery.BYTECODE_SUFFIXES
        except AttributeError:
            return importlib.machinery.DEBUG_BYTECODE_SUFFIXES

    def get_current_bytecode_suffixes():
        if py3k:
            suffixes = importlib.machinery.BYTECODE_SUFFIXES
        else:
            if sys.flags.optimize:
                suffixes = importlib.machinery.OPTIMIZED_BYTECODE_SUFFIXES
            else:
                suffixes = importlib.machinery.BYTECODE_SUFFIXES

        return suffixes

    def has_pep3147():
        return True


else:
    import imp

    def load_module_py(module_id, path):  # noqa
        with open(path, "rb") as fp:
            mod = imp.load_source(module_id, path, fp)
            if py2k:
                source_encoding = parse_encoding(fp)
                if source_encoding:
                    mod._alembic_source_encoding = source_encoding
            del sys.modules[module_id]
            return mod

    def load_module_pyc(module_id, path):  # noqa
        with open(path, "rb") as fp:
            mod = imp.load_compiled(module_id, path, fp)
            # no source encoding here
            del sys.modules[module_id]
            return mod

    def get_current_bytecode_suffixes():
        if sys.flags.optimize:
            return [".pyo"]  # e.g. .pyo
        else:
            return [".pyc"]  # e.g. .pyc

    def has_pep3147():
        return False


try:
    exec_ = getattr(compat_builtins, "exec")
except AttributeError:
    # Python 2
    def exec_(func_text, globals_, lcl):
        exec("exec func_text in globals_, lcl")


################################################
# cross-compatible metaclass implementation
# Copyright (c) 2010-2012 Benjamin Peterson


def with_metaclass(meta, base=object):
    """Create a base class with a metaclass."""
    return meta("%sBase" % meta.__name__, (base,), {})


################################################

if py3k:

    def raise_(
        exception, with_traceback=None, replace_context=None, from_=False
    ):
        r"""implement "raise" with cause support.

        :param exception: exception to raise
        :param with_traceback: will call exception.with_traceback()
        :param replace_context: an as-yet-unsupported feature.  This is
         an exception object which we are "replacing", e.g., it's our
         "cause" but we don't want it printed.    Basically just what
         ``__suppress_context__`` does but we don't want to suppress
         the enclosing context, if any.  So for now we make it the
         cause.
        :param from\_: the cause.  this actually sets the cause and doesn't
         hope to hide it someday.

        """
        if with_traceback is not None:
            exception = exception.with_traceback(with_traceback)

        if from_ is not False:
            exception.__cause__ = from_
        elif replace_context is not None:
            # no good solution here, we would like to have the exception
            # have only the context of replace_context.__context__ so that the
            # intermediary exception does not change, but we can't figure
            # that out.
            exception.__cause__ = replace_context

        try:
            raise exception
        finally:
            # credit to
            # https://cosmicpercolator.com/2016/01/13/exception-leaks-in-python-2-and-3/
            # as the __traceback__ object creates a cycle
            del exception, replace_context, from_, with_traceback


else:
    exec(
        "def raise_(exception, with_traceback=None, replace_context=None, "
        "from_=False):\n"
        "    if with_traceback:\n"
        "        raise type(exception), exception, with_traceback\n"
        "    else:\n"
        "        raise exception\n"
    )


# produce a wrapper that allows encoded text to stream
# into a given buffer, but doesn't close it.
# not sure of a more idiomatic approach to this.
class EncodedIO(io.TextIOWrapper):
    def close(self):
        pass


if py2k:
    # in Py2K, the io.* package is awkward because it does not
    # easily wrap the file type (e.g. sys.stdout) and I can't
    # figure out at all how to wrap StringIO.StringIO
    # and also might be user specified too.  So create a full
    # adapter.

    class ActLikePy3kIO(object):

        """Produce an object capable of wrapping either
        sys.stdout (e.g. file) *or* StringIO.StringIO().

        """

        def _false(self):
            return False

        def _true(self):
            return True

        readable = seekable = _false
        writable = _true
        closed = False

        def __init__(self, file_):
            self.file_ = file_

        def write(self, text):
            return self.file_.write(text)

        def flush(self):
            return self.file_.flush()

    class EncodedIO(EncodedIO):
        def __init__(self, file_, encoding):
            super(EncodedIO, self).__init__(
                ActLikePy3kIO(file_), encoding=encoding
            )
