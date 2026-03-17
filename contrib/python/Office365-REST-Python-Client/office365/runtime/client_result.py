import copy
from typing import TYPE_CHECKING, Callable, Generic, Optional, TypeVar

from typing_extensions import Self

from office365.runtime.client_request_exception import ClientRequestException
from office365.runtime.http.request_options import RequestOptions

if TYPE_CHECKING:
    from office365.runtime.client_runtime_context import ClientRuntimeContext  # noqa
    from office365.runtime.client_value import ClientValue  # noqa

T = TypeVar("T")


class ClientResult(Generic[T]):
    """Client result"""

    def __init__(self, context, default_value=None):
        # type: (ClientRuntimeContext, Optional[T]) -> None
        """Client result"""
        self._context = context
        self._value = copy.deepcopy(default_value)  # type: T

    def before_execute(self, action):
        # type: (Callable[[RequestOptions], None]) -> Self
        """Attach an event handler which is triggered before query is submitted to server"""
        self._context.before_query_execute(action)
        return self

    def after_execute(self, action, execute_first=False, include_response=False):
        # type: (Callable[[Self], None], bool, bool) -> Self
        """Attach an event handler which is triggered after query is submitted to server"""
        self._context.after_query_execute(action, execute_first, include_response)
        return self

    def set_property(self, key, value, persist_changes=False):
        # type: (str, T, bool) -> Self
        from office365.runtime.client_value import ClientValue  # noqa

        if isinstance(self._value, ClientValue):
            self._value.set_property(key, value, persist_changes)
        elif isinstance(self._value, dict):
            self._value[key] = value
        else:
            self._value = value
        return self

    @property
    def value(self):
        """Returns the value"""
        return self._value

    def execute_query(self):
        """Submit request(s) to the server"""
        self._context.execute_query()
        return self

    def execute_query_retry(
        self,
        max_retry=5,
        timeout_secs=5,
        success_callback=None,
        failure_callback=None,
        exceptions=(ClientRequestException,),
    ):
        """
        Executes the current set of data retrieval queries and method invocations and retries it if needed.

        :param int max_retry: Number of times to retry the request
        :param int timeout_secs: Seconds to wait before retrying the request.
        :param (office365.runtime.client_object.ClientObject)-> None success_callback: A callback to call
            if the request executes successfully.
        :param (int, requests.exceptions.RequestException)-> None failure_callback: A callback to call if the request
            fails to execute
        :param exceptions: tuple of exceptions that we retry
        """
        self._context.execute_query_retry(
            max_retry=max_retry,
            timeout_secs=timeout_secs,
            success_callback=success_callback,
            failure_callback=failure_callback,
            exceptions=exceptions,
        )
        return self
