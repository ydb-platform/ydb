import abc
from time import sleep
from typing import TYPE_CHECKING, AnyStr, Callable, List

import requests
from requests import Response
from typing_extensions import Self

from office365.runtime.client_request import ClientRequest
from office365.runtime.client_request_exception import ClientRequestException
from office365.runtime.client_result import ClientResult
from office365.runtime.http.http_method import HttpMethod
from office365.runtime.http.request_options import RequestOptions
from office365.runtime.queries.client_query import ClientQuery
from office365.runtime.queries.read_entity import ReadEntityQuery

if TYPE_CHECKING:
    from office365.runtime.client_object import T


class ClientRuntimeContext(object):
    def __init__(self):
        self._queries = []
        self._current_query = None

    @property
    def current_query(self):
        # type: () -> ClientQuery
        return self._current_query

    @property
    def has_pending_request(self):
        return len(self._queries) > 0

    def build_request(self, query):
        # type: (ClientQuery) -> RequestOptions
        """Builds a request"""
        self._current_query = query
        request = self.pending_request().build_request(query)
        self.pending_request().beforeExecute.notify(request)
        return request

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
        :param (office365.runtime.client_object.ClientObject)-> None success_callback:
        :param (int, requests.exceptions.RequestException)-> None failure_callback:
        :param exceptions: tuple of exceptions that we retry
        """

        for retry in range(1, max_retry + 1):
            try:
                self.execute_query()
                if callable(success_callback):
                    success_callback(self.current_query.return_type)
                break
            except exceptions as e:
                self.add_query(self.current_query)
                if callable(failure_callback):
                    failure_callback(retry, e)
                sleep(timeout_secs)

    @abc.abstractmethod
    def pending_request(self):
        # type: () -> ClientRequest
        pass

    @abc.abstractmethod
    def service_root_url(self):
        # type: () -> str
        pass

    def load(self, client_object, properties_to_retrieve=None):
        # type: (T, List[str]) -> Self
        """Prepare retrieval query"""
        qry = ReadEntityQuery(client_object, properties_to_retrieve)
        self.add_query(qry)
        return self

    def before_query_execute(self, action, once=True):
        # type: (Callable[[RequestOptions], None], bool) -> Self
        """
        Attach an event handler which is triggered before query is submitted to server

        :type action: (office365.runtime.http.request_options.RequestOptions, *args, **kwargs) -> None
        :param bool once: Flag which determines whether action is executed once or multiple times
        """
        if len(self._queries) == 0:
            return self
        query = self._queries[-1]

        def _prepare_request(request):
            # type: (RequestOptions) -> None
            if self.current_query.id == query.id:
                if once:
                    self.pending_request().beforeExecute -= _prepare_request
                action(request)

        self.pending_request().beforeExecute += _prepare_request
        return self

    def before_execute(self, action, once=True):
        # type: (Callable[[RequestOptions], None], bool) -> Self
        """
        Attach an event handler which is triggered before request is submitted to server
        :param (office365.runtime.http.request_options.RequestOptions, any) -> None action:
        :param bool once: Flag which determines whether action is executed once or multiple times
        """

        def _process_request(request):
            # type: (RequestOptions) -> None
            if once:
                self.pending_request().beforeExecute -= _process_request
            action(request)

        self.pending_request().beforeExecute += _process_request
        return self

    def after_query_execute(self, action, execute_first=False, include_response=False):
        # type: (Callable[[T|Response], None], bool, bool) -> Self
        """Attach an event handler which is triggered after query is submitted to server"""
        if len(self._queries) == 0:
            return self
        query = self._queries[-1]

        def _process_response(resp):
            # type: (Response) -> None
            resp.raise_for_status()
            if self.current_query.id == query.id:
                self.pending_request().afterExecute -= _process_response
                action(resp if include_response else query.return_type)

        self.pending_request().afterExecute += _process_response

        if execute_first and len(self._queries) > 1:
            self._queries.insert(0, self._queries.pop())

        return self

    def after_execute(self, action, once=True):
        # type: (Callable[[Response], None], bool) -> Self
        """Attach an event handler which is triggered after request is submitted to server"""

        def _process_response(response):
            # type: (Response) -> None
            if once:
                self.pending_request().afterExecute -= _process_response
            action(response)

        self.pending_request().afterExecute += _process_response
        return self

    def execute_request_direct(self, path):
        # type: (str) -> Response
        full_url = "".join([self.service_root_url(), "/", path])
        request = RequestOptions(full_url)
        return self.pending_request().execute_request_direct(request)

    def execute_query(self):
        """Submit request(s) to the server"""
        while self.has_pending_request:
            qry = self._get_next_query()
            self.pending_request().execute_query(qry)
        return self

    def add_query(self, query):
        # type: (ClientQuery) ->Self
        self._queries.append(query)
        return self

    def clear(self):
        self._current_query = None
        self._queries = []
        return self

    def get_metadata(self):
        # type: () -> ClientResult[AnyStr]
        """Loads API metadata"""
        return_type = ClientResult(self)

        def _construct_request(request):
            # type: (RequestOptions) -> None
            request.url += "/$metadata"
            request.method = HttpMethod.Get

        def _process_response(response):
            # type: (requests.Response) -> None
            response.raise_for_status()
            return_type.set_property("__value", response.content)

        qry = ClientQuery(self)
        self.add_query(qry).before_execute(_construct_request).after_execute(
            _process_response
        )
        return return_type

    def _get_next_query(self, count=1):
        # type: (int) -> ClientQuery
        if count == 1:
            qry = self._queries.pop(0)
        else:
            from office365.runtime.queries.batch import BatchQuery

            qry = BatchQuery(self)
            while self.has_pending_request and count > 0:
                qry.add(self._queries.pop(0))
                count = count - 1
        self._current_query = qry
        return qry
