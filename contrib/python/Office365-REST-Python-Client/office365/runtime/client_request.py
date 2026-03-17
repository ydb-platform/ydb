from abc import abstractmethod

import requests
from requests import HTTPError

from office365.runtime.client_request_exception import ClientRequestException
from office365.runtime.http.http_method import HttpMethod
from office365.runtime.http.request_options import RequestOptions
from office365.runtime.queries.client_query import ClientQuery
from office365.runtime.types.event_handler import EventHandler


class ClientRequest(object):
    def __init__(self):
        """
        Abstract request client
        """
        self.beforeExecute = EventHandler()
        self.afterExecute = EventHandler()

    @abstractmethod
    def build_request(self, query):
        # type: (ClientQuery) -> RequestOptions
        """Builds a request"""
        pass

    @abstractmethod
    def process_response(self, response, query):
        # type: (requests.Response, ClientQuery) -> None
        pass

    def execute_query(self, query):
        # type: (ClientQuery) -> None
        """Submits a pending request to the server"""
        try:
            request = self.build_request(query)
            response = self.execute_request_direct(request)
            self.process_response(response, query)
            self.afterExecute.notify(response)
        except HTTPError as e:
            raise ClientRequestException(*e.args, response=e.response)

    def execute_request_direct(self, request):
        # type: (RequestOptions) -> requests.Response
        """Execute the client request"""
        self.beforeExecute.notify(request)
        if request.method == HttpMethod.Post:
            if request.is_bytes or request.is_file:
                response = requests.post(
                    url=request.url,
                    headers=request.headers,
                    data=request.data,
                    auth=request.auth,
                    verify=request.verify,
                    proxies=request.proxies,
                )
            else:
                response = requests.post(
                    url=request.url,
                    headers=request.headers,
                    json=request.data,
                    auth=request.auth,
                    verify=request.verify,
                    proxies=request.proxies,
                )
        elif request.method == HttpMethod.Patch:
            response = requests.patch(
                url=request.url,
                headers=request.headers,
                json=request.data,
                auth=request.auth,
                verify=request.verify,
                proxies=request.proxies,
            )
        elif request.method == HttpMethod.Delete:
            response = requests.delete(
                url=request.url,
                headers=request.headers,
                auth=request.auth,
                verify=request.verify,
                proxies=request.proxies,
            )
        elif request.method == HttpMethod.Put:
            response = requests.put(
                url=request.url,
                data=request.data,
                headers=request.headers,
                auth=request.auth,
                verify=request.verify,
                proxies=request.proxies,
            )
        else:
            response = requests.get(
                url=request.url,
                headers=request.headers,
                auth=request.auth,
                verify=request.verify,
                stream=request.stream,
                proxies=request.proxies,
            )
        response.raise_for_status()
        return response
