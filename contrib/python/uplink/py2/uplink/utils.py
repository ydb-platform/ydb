# Standard library imports
import collections
import inspect

try:
    # Python 3.2+
    from inspect import signature
except ImportError:  # pragma: no cover
    # Python 2.7
    from inspect import getcallargs as get_call_args, getargspec as _getargspec

    def signature(_):
        raise ImportError

    def get_arg_spec(f):
        arg_spec = _getargspec(f)
        args = arg_spec.args
        if arg_spec.varargs is not None:
            args.append(arg_spec.varargs)
        if arg_spec.keywords is not None:
            args.append(arg_spec.keywords)
        return Signature(args, {}, None)


else:  # pragma: no cover

    def get_call_args(f, *args, **kwargs):
        sig = signature(f)
        arguments = sig.bind(*args, **kwargs).arguments
        # apply defaults:
        new_arguments = []
        for name, param in sig.parameters.items():
            try:
                new_arguments.append((name, arguments[name]))
            except KeyError:
                if param.default is not param.empty:
                    val = param.default
                elif param.kind is param.VAR_POSITIONAL:
                    val = ()
                elif param.kind is param.VAR_KEYWORD:
                    val = {}
                else:
                    continue
                new_arguments.append((name, val))
        return collections.OrderedDict(new_arguments)

    def get_arg_spec(f):
        sig = signature(f)
        parameters = sig.parameters
        args = []
        annotations = collections.OrderedDict()
        has_return_type = sig.return_annotation is not sig.empty
        return_type = sig.return_annotation if has_return_type else None
        for p in parameters:
            if parameters[p].annotation is not sig.empty:
                annotations[p] = parameters[p].annotation
            args.append(p)
        return Signature(args, annotations, return_type)


try:
    import urllib.parse as _urlparse
except ImportError:
    import urlparse as _urlparse


# Third-party imports
import uritemplate


urlparse = _urlparse

Signature = collections.namedtuple(
    "Signature", "args annotations return_annotation"
)

Request = collections.namedtuple("Request", "method uri info return_type")


def is_subclass(cls, class_info):
    return inspect.isclass(cls) and issubclass(cls, class_info)


def no_op(*_, **__):
    pass


class URIBuilder(object):
    @staticmethod
    def variables(uri):
        try:
            return uritemplate.URITemplate(uri).variable_names
        except TypeError:
            return set()

    def __init__(self, uri):
        self._uri = uritemplate.URITemplate(uri or "")

    def set_variable(self, var_dict=None, **kwargs):
        self._uri = self._uri.partial(var_dict, **kwargs)

    def remaining_variables(self):
        return self._uri.variable_names

    def build(self):
        return self._uri.expand()
