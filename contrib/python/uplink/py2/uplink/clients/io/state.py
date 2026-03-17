# Local imports
from uplink.clients.io import interfaces


class _BaseState(interfaces.RequestState):
    def __init__(self, request):
        self._request = request

    def prepare(self, request):
        return BeforeRequest(request)

    def send(self, request):
        return SendRequest(request)

    def fail(self, exc_type, exc_val, exc_tb):
        # Terminal
        return Fail(self._request, exc_type, exc_val, exc_tb)

    def finish(self, response):
        # Terminal
        return Finish(self._request, response)

    def sleep(self, duration):
        return Sleep(self._request, duration)

    def execute(self, execution):  # pragma: no cover
        raise NotImplementedError

    @property
    def request(self):
        return self._request


class BeforeRequest(_BaseState):
    def execute(self, execution):
        return execution.before_request(self._request)

    def __eq__(self, other):
        return (
            isinstance(other, BeforeRequest) and self.request == other.request
        )


class Sleep(interfaces.RequestState):
    class _Callback(interfaces.SleepCallback):
        def __init__(self, execution, request):
            self._context = execution
            self._request = request

        def on_success(self):
            self._context.state = BeforeRequest(self._request)
            return self._context.execute()

        def on_failure(self, exc_type, exc_val, exc_tb):
            self._context.state = AfterException(
                self._request, exc_type, exc_val, exc_tb
            )
            return self._context.execute()

    def __init__(self, request, duration):
        self._request = request
        self._duration = duration

    def execute(self, execution):
        return execution.sleep(
            self._duration, self._Callback(execution, self._request)
        )

    @property
    def request(self):
        return self._request

    @property
    def duration(self):
        return self._duration

    def __eq__(self, other):
        return (
            isinstance(other, Sleep)
            and self.request == other.request
            and self.duration == other.duration
        )


class SendRequest(interfaces.RequestState):
    def __init__(self, request):
        self._request = request

    class SendCallback(interfaces.InvokeCallback):
        def __init__(self, execution, request):
            self._context = execution
            self._request = request

        def on_success(self, response):
            self._context.state = AfterResponse(self._request, response)
            return self._context.execute()

        def on_failure(self, exc_type, exc_val, exc_tb):
            self._context.state = AfterException(
                self._request, exc_type, exc_val, exc_tb
            )
            return self._context.execute()

    def execute(self, execution):
        return execution.send(
            self._request, self.SendCallback(execution, self._request)
        )

    @property
    def request(self):
        return self._request

    def __eq__(self, other):
        return isinstance(other, SendRequest) and self.request == other.request


class AfterResponse(_BaseState):
    def __init__(self, request, response):
        super(AfterResponse, self).__init__(request)
        self._response = response

    def execute(self, execution):
        return execution.after_response(self._request, self._response)

    @property
    def response(self):
        return self._response

    def __eq__(self, other):
        return (
            isinstance(other, AfterResponse)
            and self.request == other.request
            and self.response == other.response
        )


class AfterException(_BaseState):
    def __init__(self, request, exc_type, exc_val, exc_tb):
        super(AfterException, self).__init__(request)
        self._exc_type = exc_type
        self._exc_val = exc_val
        self._exc_tb = exc_tb

    @property
    def exc_type(self):
        return self._exc_type

    @property
    def exc_val(self):
        return self._exc_val

    @property
    def exc_tb(self):
        return self._exc_tb

    def execute(self, execution):
        return execution.after_exception(
            self._request, self._exc_type, self._exc_val, self._exc_tb
        )

    def __eq__(self, other):
        return (
            isinstance(other, AfterException)
            and self.request == other.request
            and self.exc_type == other.exc_type
            and self.exc_val == other.exc_val
            and self.exc_tb == other.exc_tb
        )


class TerminalState(interfaces.RequestState):
    def __init__(self, request):
        self._request = request

    @property
    def request(self):
        return self._request

    def execute(self, execution):  # pragma: no cover
        raise NotImplementedError


class Fail(TerminalState):
    def __init__(self, request, exc_type, exc_val, exc_tb):
        super(Fail, self).__init__(request)
        self._exc_type = exc_type
        self._exc_val = exc_val
        self._exc_tb = exc_tb

    def execute(self, execution):
        return execution.fail(self._exc_type, self._exc_val, self._exc_tb)

    @property
    def exc_type(self):
        return self._exc_type

    @property
    def exc_val(self):
        return self._exc_val

    @property
    def exc_tb(self):
        return self._exc_tb

    def __eq__(self, other):
        return (
            isinstance(other, Fail)
            and self.request == other.request
            and self.exc_type == other.exc_type
            and self.exc_val == other.exc_val
            and self.exc_tb == other.exc_tb
        )


class Finish(TerminalState):
    def __init__(self, request, response):
        super(Finish, self).__init__(request)
        self._response = response

    def execute(self, execution):
        return execution.finish(self._response)

    @property
    def response(self):
        return self._response

    def __eq__(self, other):
        return (
            isinstance(other, Finish)
            and self.request == other.request
            and self.response == other.response
        )
