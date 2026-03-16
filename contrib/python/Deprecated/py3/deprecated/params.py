# coding: utf-8
"""
Parameters deprecation
======================

.. _Tantale's Blog: https://tantale.github.io/
.. _Deprecated Parameters: https://tantale.github.io/articles/deprecated_params/

This module introduces a :class:`deprecated_params` decorator to specify that one (or more)
parameter(s) are deprecated: when the user executes a function with a deprecated parameter,
he will see a warning message in the console.

The decorator is customizable, the user can specify the deprecated parameter names
and associate to each of them a message providing the reason of the deprecation.
As with the :func:`~deprecated.classic.deprecated` decorator, the user can specify
a version number (using the *version* parameter) and also define the warning message category
(a subclass of :class:`Warning`) and when to display the messages (using the *action* parameter).

The complete study concerning the implementation of this decorator is available on the `Tantale's blog`_,
on the `Deprecated Parameters`_ page.
"""
import collections
import functools
import warnings

try:
    # noinspection PyPackageRequirements
    import inspect2 as inspect
except ImportError:
    import inspect


class DeprecatedParams(object):
    """
    Decorator used to decorate a function which at least one
    of the parameters is deprecated.
    """

    def __init__(self, param, reason="", category=DeprecationWarning):
        self.messages = {}  # type: dict[str, str]
        self.category = category
        self.populate_messages(param, reason=reason)

    def populate_messages(self, param, reason=""):
        if isinstance(param, dict):
            self.messages.update(param)
        elif isinstance(param, str):
            fmt = "'{param}' parameter is deprecated"
            reason = reason or fmt.format(param=param)
            self.messages[param] = reason
        else:
            raise TypeError(param)

    def check_params(self, signature, *args, **kwargs):
        binding = signature.bind(*args, **kwargs)
        bound = collections.OrderedDict(binding.arguments, **binding.kwargs)
        return [param for param in bound if param in self.messages]

    def warn_messages(self, messages):
        # type: (list[str]) -> None
        for message in messages:
            warnings.warn(message, category=self.category, stacklevel=3)

    def __call__(self, f):
        # type: (callable) -> callable
        signature = inspect.signature(f)

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            invalid_params = self.check_params(signature, *args, **kwargs)
            self.warn_messages([self.messages[param] for param in invalid_params])
            return f(*args, **kwargs)

        return wrapper


#: Decorator used to decorate a function which at least one
#: of the parameters is deprecated.
deprecated_params = DeprecatedParams
