# Local imports
from uplink.clients.io import interfaces
from uplink.clients.io import state

__all__ = ["RequestExecutionBuilder"]


class RequestExecutionBuilder(object):
    def __init__(self):
        self._client = None
        self._template = None
        self._io = None
        self._callbacks = []
        self._errbacks = []

    def with_client(self, client):
        self._client = client
        return self

    def with_template(self, template):
        self._template = template
        return self

    def with_io(self, io):
        self._io = io
        return self

    def with_callbacks(self, *callbacks):
        self._callbacks.extend(callbacks)
        return self

    def with_errbacks(self, *errbacks):
        self._errbacks.extend(errbacks)
        return self

    def build(self):
        client, io = self._client, self._io
        for callback in self._callbacks:
            io = CallbackDecorator(io, client, callback)
        for errback in self._errbacks:
            io = ErrbackDecorator(io, errback)
        return DefaultRequestExecution(client, io, self._template)


class DefaultRequestExecution(interfaces.RequestExecution):
    def __init__(self, client, io, template):
        self._client = client
        self._template = template
        self._io = io
        self._state = None

    def before_request(self, request):
        action = self._template.before_request(request)
        next_state = action(self._state)
        self._state = next_state
        return self.execute()

    def after_response(self, request, response):
        action = self._template.after_response(request, response)
        next_state = action(self._state)
        self._state = next_state
        return self.execute()

    def after_exception(self, request, exc_type, exc_val, exc_tb):
        action = self._template.after_exception(
            request, exc_type, exc_val, exc_tb
        )
        next_state = action(self._state)
        self._state = next_state
        return self.execute()

    def send(self, request, callback):
        return self._io.invoke(self._client.send, (request,), {}, callback)

    def sleep(self, duration, callback):
        return self._io.sleep(duration, callback)

    def finish(self, response):
        return self._io.finish(response)

    def fail(self, exc_type, exc_val, exc_tb):
        return self._io.fail(exc_type, exc_val, exc_tb)

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, new_state):
        self._state = new_state

    def execute(self):
        return self.state.execute(self)

    def start(self, request):
        self._state = state.BeforeRequest(request)  # Start state
        return self._io.execute(self)


class FinishingCallback(interfaces.InvokeCallback):
    def __init__(self, io):
        self._io = io

    def on_success(self, result):
        return self._io.finish(result)

    def on_failure(self, exc_type, exc_val, exc_tb):
        return self._io.fail(exc_type, exc_val, exc_tb)


class IOStrategyDecorator(interfaces.IOStrategy):
    def __init__(self, io):
        self._io = io

    def invoke(self, func, args, kwargs, callback):
        return self._io.invoke(func, args, kwargs, callback)

    def sleep(self, duration, callback):
        return self._io.sleep(duration, callback)

    def execute(self, executable):
        return self._io.execute(executable)

    def finish(self, response):
        return self._io.finish(response)

    def fail(self, exc_type, exc_val, exc_tb):  # pragma: no cover
        return self._io.fail(exc_type, exc_val, exc_tb)


class FinishingDecorator(IOStrategyDecorator):
    def _invoke(self, func, *args, **kwargs):
        return self._io.invoke(func, args, kwargs, FinishingCallback(self._io))


class CallbackDecorator(FinishingDecorator):
    def __init__(self, io, client, callback):
        super(CallbackDecorator, self).__init__(io)
        self._client = client
        self._callback = callback

    def finish(self, response):
        return self._invoke(
            self._client.apply_callback, self._callback, response
        )


class ErrbackDecorator(FinishingDecorator):
    def __init__(self, io, errback):
        super(ErrbackDecorator, self).__init__(io)
        self._errback = errback

    def fail(self, exc_type, exc_val, exc_tb):
        return self._invoke(self._errback, exc_type, exc_val, exc_tb)
