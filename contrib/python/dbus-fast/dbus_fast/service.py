# cython: freethreading_compatible = True

from __future__ import annotations

import asyncio
import copy
import inspect
from collections.abc import Callable
from functools import wraps
from typing import TYPE_CHECKING, Any, Concatenate, ParamSpec, Protocol, TypeVar, cast

from . import introspection as intr
from ._private.util import (
    parse_annotation,
    replace_fds_with_idx,
    replace_idx_with_fds,
    signature_contains_type,
)
from .constants import PropertyAccess
from .errors import SignalDisabledError
from .message import Message
from .send_reply import SendReply
from .signature import (
    SignatureBodyMismatchError,
    SignatureTree,
    Variant,
    get_signature_tree,
)

if TYPE_CHECKING:
    from .message_bus import BaseMessageBus

str_ = str

HandlerType = Callable[[Message, SendReply], None]


_P = ParamSpec("_P")
_TInterface = TypeVar("_TInterface", bound="ServiceInterface")


class _MethodCallbackProtocol(Protocol):
    def __call__(self, interface: ServiceInterface, *args: Any) -> Any: ...


_background_tasks: set[asyncio.Task[Any]] = set()


class _Method:
    def __init__(
        self, fn: _MethodCallbackProtocol, name: str, disabled: bool = False
    ) -> None:
        in_signature = ""
        out_signature = ""

        inspection = inspect.signature(fn)
        module = inspect.getmodule(fn)

        in_args: list[intr.Arg] = []
        for i, param in enumerate(inspection.parameters.values()):
            if i == 0:
                # first is self
                continue
            annotation = parse_annotation(param.annotation, module)
            if not annotation:
                raise ValueError(
                    "method parameters must specify the dbus type string as an annotation"
                )
            in_args.append(intr.Arg(annotation, intr.ArgDirection.IN, param.name))
            in_signature += annotation

        out_args: list[intr.Arg] = []
        out_signature = parse_annotation(inspection.return_annotation, module)
        if out_signature:
            for type_ in get_signature_tree(out_signature).types:
                out_args.append(intr.Arg(type_, intr.ArgDirection.OUT))

        self.name = name
        self.fn = fn
        self.disabled = disabled
        self.introspection = intr.Method(name, in_args, out_args)
        self.in_signature = in_signature
        self.out_signature = out_signature
        self.in_signature_tree = get_signature_tree(in_signature)
        self.out_signature_tree = get_signature_tree(out_signature)


def dbus_method(
    name: str | None = None, disabled: bool = False
) -> Callable[[Callable[_P, Any]], Callable[_P, None]]:
    """A decorator to mark a class method of a :class:`ServiceInterface` to be a DBus service method.

    The parameters and return value must each be annotated with a signature
    string of a single complete DBus type.

    This class method will be called when a client calls the method on the DBus
    interface. The parameters given to the function come from the calling
    client and will conform to the dbus-fast type system. The parameters
    returned will be returned to the calling client and must conform to the
    dbus-fast type system. If multiple parameters are returned, they must be
    contained within a :class:`tuple`.

    The decorated method may raise a :class:`DBusError <dbus_fast.DBusError>`
    to return an error to the client.

    :param name: The member name that DBus clients will use to call this method. Defaults to the name of the class method.
    :type name: str
    :param disabled: If set to true, the method will not be visible to clients.
    :type disabled: bool

    :example:

    ::

        @dbus_method()
        def echo(self, val: 's') -> 's':
            return val

        @dbus_method()
        def echo_two(self, val1: 's', val2: 'u') -> 'su':
            return val1, val2

    If you use Python annotations for type hints, you can use :class:`typing.Annotated`
    to specify the Python type and the D-Bus signature at the same time like this::

        from dbus_fast.annotations import DBusSignature, DBusStr, DBusUInt32

        @dbus_method()
        def echo(self, val: DBusStr) -> DBusStr:
            return val

        @dbus_method()
        def echo_two(
            self, val1: DBusStr, val2: DBusUInt32
        ) -> Annotated[tuple[str, int], DBusSignature("su")]:
            return val1, val2

    .. versionchanged:: v4.0.0
        :class:`typing.Annotated` can now be used to provide type hints and the
        D-Bus signature at the same time. Older versions require D-Bus signature
        strings to be used.

    .. versionadded:: v2.46.0
        In older versions, this was named ``@method``. The old name still exists.
    """
    if name is not None and type(name) is not str:
        raise TypeError("name must be a string")
    if type(disabled) is not bool:
        raise TypeError("disabled must be a bool")

    def decorator(fn: Callable[_P, Any]) -> Callable[_P, None]:
        @wraps(fn)
        def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> None:
            fn(*args, **kwargs)

        fn_name = name if name else fn.__name__
        wrapped.__dict__["__DBUS_METHOD"] = _Method(
            cast(_MethodCallbackProtocol, fn), fn_name, disabled=disabled
        )

        return wrapped

    return decorator


method = dbus_method  # backward compatibility alias


class _Signal:
    def __init__(
        self, fn: Callable[..., Any], name: str, disabled: bool = False
    ) -> None:
        inspection = inspect.signature(fn)
        module = inspect.getmodule(fn)

        args: list[intr.Arg] = []
        signature = ""
        signature_tree = None

        return_annotation = parse_annotation(inspection.return_annotation, module)

        if return_annotation:
            signature = return_annotation
            signature_tree = get_signature_tree(signature)
            for type_ in signature_tree.types:
                args.append(intr.Arg(type_, intr.ArgDirection.OUT))
        else:
            signature = ""
            signature_tree = get_signature_tree("")

        self.signature = signature
        self.signature_tree = signature_tree
        self.name = name
        self.disabled = disabled
        self.introspection = intr.Signal(self.name, args)


def dbus_signal(
    name: str | None = None, disabled: bool = False
) -> Callable[
    [Callable[Concatenate[_TInterface, _P], Any]],
    Callable[Concatenate[_TInterface, _P], Any],
]:
    """A decorator to mark a class method of a :class:`ServiceInterface` to be a DBus signal.

    The signal is broadcast on the bus when the decorated class method is
    called by the user.

    If the signal has an out argument, the class method must have a return type
    annotation with a signature string of a single complete DBus type and the
    return value of the class method must conform to the dbus-fast type system.
    If the signal has multiple out arguments, they must be returned within a
    ``tuple``.

    :param name: The member name that will be used for this signal. Defaults to
        the name of the class method.
    :type name: str
    :param disabled: If set to true, the signal will not be visible to clients.
    :type disabled: bool

    :example:

    ::

        @dbus_signal()
        def string_signal(self, val) -> 's':
            return val

        @dbus_signal()
        def two_strings_signal(self, val1, val2) -> 'ss':
            return val1, val2

    If you use Python annotations for type hints, you can use :class:`typing.Annotated`
    to specify the Python type and the D-Bus signature at the same time like this::

        from dbus_fast.annotations import DBusSignature, DBusStr

        @dbus_signal()
        def string_signal(self, val: str) -> DBusStr:
            return val

        @dbus_signal()
        def two_strings_signal(
            self, val1: str, val2: str
        ) -> Annotated[tuple[str, str], DBusSignature("ss")]:
            return val1, val2

    .. versionchanged:: v4.0.0
        :class:`typing.Annotated` can now be used to provide type hints and the
        D-Bus signature at the same time. Older versions require D-Bus signature
        strings to be used.

    .. versionadded:: v2.46.0
        In older versions, this was named ``@signal``. The old name still exists.
    """
    if name is not None and type(name) is not str:
        raise TypeError("name must be a string")
    if type(disabled) is not bool:
        raise TypeError("disabled must be a bool")

    def decorator(
        fn: Callable[Concatenate[_TInterface, _P], Any],
    ) -> Callable[Concatenate[_TInterface, _P], Any]:
        fn_name = name if name else fn.__name__
        signal = _Signal(fn, fn_name, disabled)

        @wraps(fn)
        def wrapped(self: _TInterface, *args: _P.args, **kwargs: _P.kwargs) -> Any:
            if signal.disabled:
                raise SignalDisabledError("Tried to call a disabled signal")
            result = fn(self, *args, **kwargs)
            ServiceInterface._handle_signal(self, signal, result)
            return result

        wrapped.__dict__["__DBUS_SIGNAL"] = signal

        return wrapped

    return decorator


signal = dbus_signal  # backward compatibility alias


class _Property(property):
    def set_options(self, options: dict[str, Any]) -> None:
        self.options = getattr(self, "options", {})
        for k, v in options.items():
            self.options[k] = v

        if "name" in options and options["name"] is not None:
            self.name = options["name"]
        else:
            self.name = self.prop_getter.__name__

        if "access" in options:
            self.access = PropertyAccess(options["access"])
        else:
            self.access = PropertyAccess.READWRITE

        self.disabled = options.get("disabled", False)
        self.introspection = intr.Property(self.name, self.signature, self.access)

        self.__dict__["__DBUS_PROPERTY"] = True

    def __init__(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        self.prop_getter = fn
        self.prop_setter: Callable[..., Any] | None = None

        inspection = inspect.signature(fn)
        if len(inspection.parameters) != 1:
            raise ValueError('the property must only have the "self" input parameter')

        module = inspect.getmodule(fn)

        return_annotation = parse_annotation(inspection.return_annotation, module)

        if not return_annotation:
            raise ValueError(
                "the property must specify the dbus type string as a return annotation string"
            )

        self.signature = return_annotation
        tree = get_signature_tree(return_annotation)

        if len(tree.types) != 1:
            raise ValueError("the property signature must be a single complete type")

        self.type = tree.types[0]

        if "options" in kwargs:
            options = kwargs["options"]
            self.set_options(options)
            del kwargs["options"]

        super().__init__(fn, *args, **kwargs)

    def setter(self, fn: Callable[..., Any], **kwargs: Any) -> _Property:
        # XXX The setter decorator seems to be recreating the class in the list
        # of class members and clobbering the options so we need to reset them.
        # Why does it do that?
        result = cast(_Property, super().setter(fn, **kwargs))
        result.prop_setter = fn
        result.set_options(self.options)
        return result


def dbus_property(
    access: PropertyAccess = PropertyAccess.READWRITE,
    name: str | None = None,
    disabled: bool = False,
) -> Callable[[Callable[..., Any]], _Property]:
    """A decorator to mark a class method of a :class:`ServiceInterface` to be a DBus property.

    The class method must be a Python getter method with a return annotation
    that is a signature string of a single complete DBus type. When a client
    gets the property through the ``org.freedesktop.DBus.Properties``
    interface, the getter will be called and the resulting value will be
    returned to the client.

    If the property is writable, it must have a setter method that takes a
    single parameter that is annotated with the same signature. When a client
    sets the property through the ``org.freedesktop.DBus.Properties``
    interface, the setter will be called with the value from the calling
    client.

    The parameters of the getter and the setter must conform to the dbus-fast
    type system. The getter or the setter may raise a :class:`DBusError
    <dbus_fast.DBusError>` to return an error to the client.

    :param name: The name that DBus clients will use to interact with this
        property on the bus.
    :type name: str
    :param disabled: If set to true, the property will not be visible to
        clients.
    :type disabled: bool

    :example:

    ::

        @dbus_property()
        def string_prop(self) -> 's':
            return self._string_prop

        @string_prop.setter
        def string_prop(self, val: 's'):
            self._string_prop = val

    If you use Python annotations for type hints, you can use :class:`typing.Annotated`
    to specify the Python type and the D-Bus signature at the same time like this::

        from dbus_fast.annotations import DBusStr

        @dbus_property()
        def string_prop(self) -> DBusStr:
            return self._string_prop

        @string_prop.setter
        def string_prop(self, val: DBusStr):
            self._string_prop = val

    .. versionchanged:: v4.0.0
        :class:`typing.Annotated` can now be used to provide type hints and the
        D-Bus signature at the same time. Older versions require D-Bus signature
        strings to be used.
    """
    if type(access) is not PropertyAccess:
        raise TypeError("access must be a PropertyAccess class")
    if name is not None and type(name) is not str:
        raise TypeError("name must be a string")
    if type(disabled) is not bool:
        raise TypeError("disabled must be a bool")

    def decorator(fn: Callable[..., Any]) -> _Property:
        options: dict[str, Any] = {"name": name, "access": access, "disabled": disabled}
        return _Property(fn, options=options)

    return decorator


def _real_fn_result_to_body(
    result: Any | None,
    signature_tree: SignatureTree,
    replace_fds: bool,
) -> tuple[list[Any], list[int]]:
    out_len = len(signature_tree.types)
    if result is None:
        final_result = []
    elif out_len == 1:
        final_result = [result]
    else:
        result_type = type(result)
        is_tuple = result_type is tuple
        if not is_tuple and result_type is not list:
            raise SignatureBodyMismatchError(
                "Expected method or signal handler to return a list or tuple of arguments"
            )
        # Convert tuple to list for D-Bus Message compatibility (Cython requires list type)
        final_result = list(result) if is_tuple else result

    if out_len != len(final_result):
        raise SignatureBodyMismatchError(
            f"Signature and function return mismatch, expected "
            f"{len(signature_tree.types)} arguments but got {len(result)}"  # type: ignore[arg-type]
        )

    if not replace_fds:
        return final_result, []
    return replace_fds_with_idx(signature_tree, final_result)


class ServiceInterface:
    """An abstract class that can be extended by the user to define DBus services.

    Instances of :class:`ServiceInterface` can be exported on a path of the bus
    with the :class:`export <dbus_fast.message_bus.BaseMessageBus.export>`
    method of a :class:`MessageBus <dbus_fast.message_bus.BaseMessageBus>`.

    Use the :func:`@dbus_method <dbus_fast.service.dbus_method>`, :func:`@dbus_property
    <dbus_fast.service.dbus_property>`, and :func:`@dbus_signal
    <dbus_fast.service.dbus_signal>` decorators to mark class methods as DBus
    methods, properties, and signals respectively.

    :ivar name: The name of this interface as it appears to clients. Must be a
        valid interface name.
    :vartype name: str
    """

    def __init__(self, name: str) -> None:
        # TODO cannot be overridden by a dbus member
        self.name = name
        self.__methods: list[_Method] = []
        self.__properties: list[_Property] = []
        self.__signals: list[_Signal] = []
        self.__buses: set[BaseMessageBus] = set()
        self.__handlers: dict[BaseMessageBus, dict[_Method, HandlerType]] = {}
        # Map of methods by bus of name -> method, handler
        self.__handlers_by_name_signature: dict[
            BaseMessageBus, dict[str, tuple[_Method, HandlerType]]
        ] = {}
        for _, member in inspect.getmembers(type(self)):
            member_dict = getattr(member, "__dict__", {})
            if type(member) is _Property:
                # XXX The getter and the setter may show up as different
                # members if they have different names. But if they have the
                # same name, they will be the same member. So we try to merge
                # them together here. I wish we could make this cleaner.
                found = False
                for prop in self.__properties:
                    if prop.prop_getter is member.prop_getter:
                        found = True
                        if member.prop_setter is not None:
                            prop.prop_setter = member.prop_setter

                if not found:
                    self.__properties.append(member)
            elif "__DBUS_METHOD" in member_dict:
                method = member_dict["__DBUS_METHOD"]
                assert type(method) is _Method
                self.__methods.append(method)
            elif "__DBUS_SIGNAL" in member_dict:
                signal = member_dict["__DBUS_SIGNAL"]
                assert type(signal) is _Signal
                self.__signals.append(signal)

        # validate that writable properties have a setter
        for prop in self.__properties:
            if prop.access.writable() and prop.prop_setter is None:
                raise ValueError(
                    f'property "{prop.name}" is writable but does not have a setter'
                )

    def emit_properties_changed(
        self, changed_properties: dict[str, Any], invalidated_properties: list[str] = []
    ) -> None:
        """Emit the ``org.freedesktop.DBus.Properties.PropertiesChanged`` signal.

        This signal is intended to be used to alert clients when a property of
        the interface has changed.

        :param changed_properties: The keys must be the names of properties exposed by this bus. The values must be valid for the signature of those properties.
        :type changed_properties: dict(str, Any)
        :param invalidated_properties: A list of names of properties that are now invalid (presumably for clients who cache the value).
        :type invalidated_properties: list(str)
        """
        # TODO cannot be overridden by a dbus member
        variant_dict = {}

        for prop in ServiceInterface._get_properties(self):
            if prop.name in changed_properties:
                variant_dict[prop.name] = Variant(
                    prop.signature, changed_properties[prop.name]
                )

        body: list[Any] = [self.name, variant_dict, invalidated_properties]
        for bus in ServiceInterface._get_buses(self):
            bus._interface_signal_notify(
                self,
                "org.freedesktop.DBus.Properties",
                "PropertiesChanged",
                "sa{sv}as",
                body,
            )

    def introspect(self) -> intr.Interface:
        """Get introspection information for this interface.

        This might be useful for creating clients for the interface or examining the introspection output of an interface.

        :returns: The introspection data for the interface.
        :rtype: :class:`dbus_fast.introspection.Interface`
        """
        # TODO cannot be overridden by a dbus member
        return intr.Interface(
            self.name,
            methods=[
                method.introspection
                for method in ServiceInterface._get_methods(self)
                if not method.disabled
            ],
            signals=[
                signal.introspection
                for signal in ServiceInterface._get_signals(self)
                if not signal.disabled
            ],
            properties=[
                prop.introspection
                for prop in ServiceInterface._get_properties(self)
                if not prop.disabled
            ],
        )

    @staticmethod
    def _get_properties(interface: ServiceInterface) -> list[_Property]:
        return interface.__properties

    @staticmethod
    def _get_methods(interface: ServiceInterface) -> list[_Method]:
        return interface.__methods

    @staticmethod
    def _get_signals(interface: ServiceInterface) -> list[_Signal]:
        return interface.__signals

    @staticmethod
    def _get_buses(interface: ServiceInterface) -> set[BaseMessageBus]:
        return interface.__buses

    @staticmethod
    def _get_handler(
        interface: ServiceInterface, method: _Method, bus: BaseMessageBus
    ) -> HandlerType:
        return interface.__handlers[bus][method]

    @staticmethod
    def _get_enabled_handler_by_name_signature(
        interface: ServiceInterface,
        bus: BaseMessageBus,
        name: str_,
        signature: str_,
    ) -> HandlerType | None:
        handlers = interface.__handlers_by_name_signature[bus]
        if (method_handler := handlers.get(name)) is None:
            return None
        method = method_handler[0]
        if method.disabled:
            return None
        return method_handler[1] if method.in_signature == signature else None

    @staticmethod
    def _add_bus(
        interface: ServiceInterface,
        bus: BaseMessageBus,
        maker: Callable[[ServiceInterface, _Method], HandlerType],
    ) -> None:
        if bus in interface.__buses:
            raise ValueError(
                "Same interface instance cannot be added to the same bus twice"
            )
        interface.__buses.add(bus)
        interface.__handlers[bus] = {
            method: maker(interface, method) for method in interface.__methods
        }
        interface.__handlers_by_name_signature[bus] = {
            method.name: (method, handler)
            for method, handler in interface.__handlers[bus].items()
        }

    @staticmethod
    def _remove_bus(interface: ServiceInterface, bus: BaseMessageBus) -> None:
        interface.__buses.remove(bus)
        del interface.__handlers[bus]

    @staticmethod
    def _msg_body_to_args(msg: Message) -> list[Any]:
        return ServiceInterface._c_msg_body_to_args(msg)

    @staticmethod
    def _c_msg_body_to_args(msg: Message) -> list[Any]:
        # https://github.com/cython/cython/issues/3327
        if not signature_contains_type(msg.signature_tree, msg.body, "h"):
            return msg.body

        # XXX: This deep copy could be expensive if messages are very
        # large. We could optimize this by only copying what we change
        # here.
        return replace_idx_with_fds(
            msg.signature_tree, copy.deepcopy(msg.body), msg.unix_fds
        )

    @staticmethod
    def _fn_result_to_body(
        result: Any | None,
        signature_tree: SignatureTree,
        replace_fds: bool = True,
    ) -> tuple[list[Any], list[int]]:
        return _real_fn_result_to_body(result, signature_tree, replace_fds)

    @staticmethod
    def _c_fn_result_to_body(
        result: Any | None,
        signature_tree: SignatureTree,
        replace_fds: bool,
    ) -> tuple[list[Any], list[int]]:
        """The high level interfaces may return single values which may be
        wrapped in a list to be a message body. Also they may return fds
        directly for type 'h' which need to be put into an external list."""
        # https://github.com/cython/cython/issues/3327
        return _real_fn_result_to_body(result, signature_tree, replace_fds)

    @staticmethod
    def _handle_signal(
        interface: ServiceInterface, signal: _Signal, result: Any | None
    ) -> None:
        body, fds = ServiceInterface._fn_result_to_body(result, signal.signature_tree)
        for bus in ServiceInterface._get_buses(interface):
            bus._interface_signal_notify(
                interface, interface.name, signal.name, signal.signature, body, fds
            )

    @staticmethod
    def _get_property_value(
        interface: ServiceInterface,
        prop: _Property,
        callback: Callable[[ServiceInterface, _Property, Any, Exception | None], None],
    ) -> None:
        # XXX MUST CHECK TYPE RETURNED BY GETTER
        try:
            if inspect.iscoroutinefunction(prop.prop_getter):
                task: asyncio.Task[Any] = asyncio.ensure_future(
                    prop.prop_getter(interface)
                )

                def get_property_callback(task_: asyncio.Task[Any]) -> None:
                    _background_tasks.remove(task_)
                    try:
                        result = task_.result()
                    except Exception as e:
                        callback(interface, prop, None, e)
                        return

                    callback(interface, prop, result, None)

                task.add_done_callback(get_property_callback)
                _background_tasks.add(task)
                return

            callback(
                interface, prop, getattr(interface, prop.prop_getter.__name__), None
            )
        except Exception as e:
            callback(interface, prop, None, e)

    @staticmethod
    def _set_property_value(
        interface: ServiceInterface,
        prop: _Property,
        value: Any,
        callback: Callable[[ServiceInterface, _Property, Exception | None], None],
    ) -> None:
        # XXX MUST CHECK TYPE TO SET
        try:
            if inspect.iscoroutinefunction(prop.prop_setter):
                task: asyncio.Task[Any] = asyncio.ensure_future(
                    prop.prop_setter(interface, value)
                )

                def set_property_callback(task_: asyncio.Task[Any]) -> None:
                    _background_tasks.remove(task_)
                    try:
                        task_.result()
                    except Exception as e:
                        callback(interface, prop, e)
                        return

                    callback(interface, prop, None)

                task.add_done_callback(set_property_callback)
                _background_tasks.add(task)
                return

            setattr(interface, prop.prop_setter.__name__, value)
            callback(interface, prop, None)
        except Exception as e:
            callback(interface, prop, e)

    @staticmethod
    def _get_all_property_values(
        interface: ServiceInterface,
        callback: Callable[[ServiceInterface, Any, Any, Exception | None], None],
        user_data: Any | None = None,
    ) -> None:
        result: dict[str, Variant | None] = {}
        result_error = None

        for prop in ServiceInterface._get_properties(interface):
            if prop.disabled or not prop.access.readable():
                continue
            result[prop.name] = None

        if not result:
            callback(interface, result, user_data, None)
            return

        def get_property_callback(
            interface: ServiceInterface,
            prop: _Property,
            value: Any,
            e: Exception | None,
        ) -> None:
            nonlocal result_error
            if e is not None:
                result_error = e
                del result[prop.name]
            else:
                try:
                    result[prop.name] = Variant(prop.signature, value)
                except SignatureBodyMismatchError as exc:
                    result_error = exc
                    del result[prop.name]

            if any(v is None for v in result.values()):
                return

            callback(interface, result, user_data, result_error)

        for prop in ServiceInterface._get_properties(interface):
            if prop.disabled or not prop.access.readable():
                continue
            ServiceInterface._get_property_value(interface, prop, get_property_callback)
