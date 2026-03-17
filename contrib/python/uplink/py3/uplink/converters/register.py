# Standard library imports
import collections
import inspect

# Local imports
from uplink.converters import interfaces


class Register:
    def __init__(self):
        self._register = collections.deque()

    def register_converter_factory(self, proxy):
        factory = proxy() if inspect.isclass(proxy) else proxy
        if not isinstance(factory, interfaces.Factory):
            raise TypeError(
                f"Failed to register '{factory}' as a converter factory: it is not an "
                f"instance of '{interfaces.Factory}'."
            )
        self._register.appendleft(factory)
        return proxy

    def get_converter_factories(self):
        return tuple(self._register)


_registry = Register()

register_default_converter_factory = _registry.register_converter_factory
get_default_converter_factories = _registry.get_converter_factories
