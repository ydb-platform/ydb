# -*- coding: utf-8 -*-

"""
    Event system for python
"""

import typing as T

from collections import defaultdict


class HandlerNotFound(Exception):
    """Raised if a handler wasn't found"""

    def __init__(self, event: str, handler: T.Callable) -> None:
        super().__init__()
        self.event = event
        self.handler = handler

    def __str__(self) -> str:
        return "Handler {} wasn't found for event {}".format(self.handler, self.event)


class EventNotFound(Exception):
    """Raised if an event wasn't found"""

    def __init__(self, event: str) -> None:
        super().__init__()
        self.event = event

    def __str__(self) -> str:
        return "Event {} wasn't found".format(self.event)


class Observable(object):
    """Event system for python"""

    def __init__(self) -> None:
        self._events = defaultdict(list)  # type: T.DefaultDict[str, T.List[T.Callable]]

    def get_all_handlers(self) -> T.Dict[str, T.List[T.Callable]]:
        """Returns a dict with event names as keys and lists of
        registered handlers as values."""

        events = {}
        for event, handlers in self._events.items():
            events[event] = list(handlers)
        return events

    def get_handlers(self, event: str) -> T.List[T.Callable]:
        """Returns a list of handlers registered for the given event."""

        return list(self._events.get(event, []))

    def is_registered(self, event: str, handler: T.Callable) -> bool:
        """Returns whether the given handler is registered for the
        given event."""

        return handler in self._events.get(event, [])

    def on(
        self, event: str, *handlers: T.Callable
    ) -> T.Callable:  # pylint: disable=invalid-name
        """Registers one or more handlers to a specified event.
        This method may as well be used as a decorator for the handler."""

        def _on_wrapper(*handlers: T.Callable) -> T.Callable:
            """wrapper for on decorator"""
            self._events[event].extend(handlers)
            return handlers[0]

        if handlers:
            return _on_wrapper(*handlers)
        return _on_wrapper

    def off(
        self, event: str = None, *handlers: T.Callable
    ) -> None:  # pylint: disable=keyword-arg-before-vararg
        """Unregisters a whole event (if no handlers are given) or one
        or more handlers from an event.
        Raises EventNotFound when the given event isn't registered.
        Raises HandlerNotFound when a given handler isn't registered."""

        if not event:
            self._events.clear()
            return

        if event not in self._events:
            raise EventNotFound(event)

        if not handlers:
            self._events.pop(event)
            return

        for callback in handlers:
            if callback not in self._events[event]:
                raise HandlerNotFound(event, callback)
            while callback in self._events[event]:
                self._events[event].remove(callback)
        return

    def once(self, event: str, *handlers: T.Callable) -> T.Callable:
        """Registers one or more handlers to a specified event, but
        removes them when the event is first triggered.
        This method may as well be used as a decorator for the handler."""

        def _once_wrapper(*handlers: T.Callable) -> T.Callable:
            """Wrapper for 'once' decorator"""

            def _wrapper(*args: T.Any, **kw: T.Any) -> None:
                """Wrapper that unregisters itself before executing
                the handlers"""

                self.off(event, _wrapper)
                for handler in handlers:
                    handler(*args, **kw)

            return _wrapper

        if handlers:
            return self.on(event, _once_wrapper(*handlers))
        return lambda x: self.on(event, _once_wrapper(x))

    def trigger(self, event: str, *args: T.Any, **kw: T.Any) -> bool:
        """Triggers all handlers which are subscribed to an event.
        Returns True when there were callbacks to execute, False otherwise."""

        callbacks = list(self._events.get(event, []))
        if not callbacks:
            return False

        for callback in callbacks:
            callback(*args, **kw)
        return True
