"""Requester"""

from typing import Optional

import requests

import nomad.api.exceptions


class Requester:  # pylint: disable=too-many-instance-attributes,too-few-public-methods
    """
    Base object for endpoints
    """

    ENDPOINT = ""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        address: Optional[str] = None,
        uri: Optional[str] = "http://127.0.0.1",
        port: int = 4646,
        user_agent: Optional[str] = None,
        namespace: Optional[str] = None,
        token: Optional[str] = None,
        timeout: int = 5,
        version: str = "v1",
        verify: bool = False,
        cert: tuple = (),
        region: Optional[str] = None,
        session: requests.Session = None,
    ):
        self.uri = uri
        self.port = port
        self.user_agent = user_agent
        self.namespace = namespace
        self.token = token
        self.timeout = timeout
        self.version = version
        self.verify = verify
        self.cert = cert
        self.address = address
        self.session = session or requests.Session()
        self.region = region

    def _endpoint_builder(self, *args):
        if args:
            args_str = "/".join(args)
            return f"{self.version}/" + args_str

        return "/"

    def _required_namespace(self, endpoint):
        required_namespace = [
            "job",
            "jobs",
            "allocation",
            "allocations",
            "deployment",
            "deployments",
            "acl",
            "client",
            "node",
            "variable",
            "variables",
        ]
        # split 0 -> Api Version
        # split 1 -> Working Endpoint
        endpoint_split = endpoint.split("/")
        try:
            endpoint_name = 1
            required = endpoint_split[endpoint_name] in required_namespace
        except IndexError:
            required = False

        return required

    def _url_builder(self, endpoint):
        url = self.address

        if self.address is None:
            url = f"{self.uri}:{self.port}"
        url = f"{url}/{endpoint}"

        return url

    def _query_string_builder(self, endpoint, params=None):
        query_string = {}

        if not isinstance(params, dict):
            params = {}

        # Remove parameters that are None
        params = {key: val for key, val in params.items() if val is not None}

        if ("namespace" not in params) and (self.namespace and self._required_namespace(endpoint)):
            query_string["namespace"] = self.namespace

        if "region" not in params and self.region:
            query_string["region"] = self.region

        return query_string

    def request(self, *args, **kwargs):
        """
        Send HTTP Request (wrapper around requests)
        """
        endpoint = self._endpoint_builder(self.ENDPOINT, *args)
        response = self._request(
            endpoint=endpoint,
            method=kwargs.get("method"),
            params=kwargs.get("params", None),
            data=kwargs.get("data", None),
            json=kwargs.get("json", None),
            headers=kwargs.get("headers", None),
            allow_redirects=kwargs.get("allow_redirects", False),
            timeout=kwargs.get("timeout", self.timeout),
            stream=kwargs.get("stream", False),
        )

        return response

    def _request(  # pylint: disable=too-many-arguments, too-many-branches
        self,
        method,
        endpoint,
        params=None,
        data=None,
        json=None,
        headers=None,
        allow_redirects=None,
        timeout=None,
        stream=False,
    ):
        url = self._url_builder(endpoint)
        query_string = self._query_string_builder(endpoint=endpoint, params=params)

        if params:
            params.update(query_string)
        else:
            params = query_string

        if self.token:
            if headers is not None:
                headers["X-Nomad-Token"] = self.token
            else:
                headers = {"X-Nomad-Token": self.token}

        if self.user_agent:
            headers["User-Agent"] = self.user_agent

        response = None

        try:
            method = method.lower()
            if method == "get":
                response = self.session.get(
                    allow_redirects=allow_redirects,
                    cert=self.cert,
                    headers=headers,
                    params=params,
                    stream=stream,
                    timeout=timeout,
                    url=url,
                    verify=self.verify,
                )

            elif method == "post":
                response = self.session.post(
                    allow_redirects=allow_redirects,
                    cert=self.cert,
                    data=data,
                    headers=headers,
                    json=json,
                    params=params,
                    timeout=timeout,
                    url=url,
                    verify=self.verify,
                )
            elif method == "put":
                response = self.session.put(
                    cert=self.cert,
                    data=data,
                    headers=headers,
                    json=json,
                    params=params,
                    timeout=timeout,
                    url=url,
                    verify=self.verify,
                )
            elif method == "delete":
                response = self.session.delete(
                    cert=self.cert,
                    headers=headers,
                    params=params,
                    timeout=timeout,
                    url=url,
                    verify=self.verify,
                )

            if response.ok:
                return response
            if response.status_code == 400:
                raise nomad.api.exceptions.BadRequestNomadException(response)
            if response.status_code == 403:
                raise nomad.api.exceptions.URLNotAuthorizedNomadException(response)
            if response.status_code == 404:
                raise nomad.api.exceptions.URLNotFoundNomadException(response)
            if response.status_code == 409:
                raise nomad.api.exceptions.VariableConflict(response)

            raise nomad.api.exceptions.BaseNomadException(response)

        except requests.exceptions.ConnectionError as error:
            if all([stream, timeout]):
                raise nomad.api.exceptions.TimeoutNomadException(error)

            raise nomad.api.exceptions.BaseNomadException(error)

        except requests.RequestException as error:
            raise nomad.api.exceptions.BaseNomadException(error)
