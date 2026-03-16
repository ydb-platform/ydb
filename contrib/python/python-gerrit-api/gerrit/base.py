#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
import netrc
import requests
from requests.adapters import HTTPAdapter
from gerrit.utils.requester import Requester
from gerrit.utils.common import decode_response, strip_trailing_slash
from gerrit.config.config import GerritConfig
from gerrit.projects.projects import GerritProjects
from gerrit.accounts.accounts import GerritAccounts
from gerrit.groups.groups import GerritGroups
from gerrit.plugins.plugins import GerritPlugins
from gerrit.changes.changes import GerritChanges


class GerritClient:
    """
    Python wrapper for the Gerrit V3.x REST API.

    """

    default_headers = {"Content-Type": "application/json; charset=UTF-8"}

    def __init__(
        self,
        base_url,
        username=None,
        password=None,
        use_netrc=False,
        ssl_verify=True,
        cert=None,
        cookies=None,
        cookie_jar=None,
        timeout=60,
        max_retries=None,
        session=None,
        auth_suffix="/a",
    ):
        self._base_url = strip_trailing_slash(base_url)

        # make request session if one isn't provided
        if session is None:
            session = requests.Session()

        if use_netrc:
            password = self.get_password_from_netrc_file()

        if username and password:
            session.auth = (username, password)

        if ssl_verify:
            session.verify = ssl_verify

        if cert is not None:
            session.cert = cert

        if cookies is not None:
            session.cookies.update(cookies)

        if cookie_jar is not None:
            session.cookies = cookie_jar

        if max_retries is not None:
            retry_adapter = HTTPAdapter(max_retries=max_retries)
            session.mount("http://", retry_adapter)
            session.mount("https://", retry_adapter)

        self.session = session

        self.requester = Requester(
            base_url=base_url,
            session=self.session,
            timeout=timeout,
        )
        if self.session.auth is not None:
            self.auth_suffix = auth_suffix
        else:
            self.auth_suffix = ""

    def get_password_from_netrc_file(self):
        """
        Providing the password form .netrc file for getting Host name.
        :return: The related password from .netrc file as a string.
        """

        netrc_client = netrc.netrc()
        auth_tokens = netrc_client.authenticators(self._base_url)
        if not auth_tokens:
            raise ValueError(
                f"The '{self._base_url}' host name is not found in netrc file."
            )
        return auth_tokens[2]

    def get_endpoint_url(self, endpoint):
        """
        Return the complete url including host and port for a given endpoint.
        :param endpoint: service endpoint as str
        :return: complete url (including host and port) as str
        """
        return f"{self._base_url}{self.auth_suffix}{endpoint}"

    @property
    def config(self):
        """
        Config related REST APIs

        :return:
        """
        return GerritConfig(gerrit=self)

    @property
    def projects(self):
        """
        Project related REST APIs
        :return:
        """
        return GerritProjects(gerrit=self)

    @property
    def changes(self):
        """
        Change related REST APIs

        :return:
        """
        return GerritChanges(gerrit=self)

    @property
    def accounts(self):
        """
        Account related REST APIs

        :return:
        """
        return GerritAccounts(gerrit=self)

    @property
    def groups(self):
        """
        Group related REST APIs

        :return:
        """
        return GerritGroups(gerrit=self)

    @property
    def plugins(self):
        """
        Plugin related REST APIs

        :return:
        """
        return GerritPlugins(gerrit=self)

    @property
    def version(self):
        """
        get the version of the Gerrit server.

        :return:
        """
        return self.config.get_version()

    @property
    def server(self):
        """
        get the information about the Gerrit server configuration.

        :return:
        """
        return self.config.get_server_info()

    def get(self, endpoint, **kwargs):
        """
        Send HTTP GET to the endpoint.

        :param endpoint: The endpoint to send to.
        :return:
        """
        response = self.requester.get(self.get_endpoint_url(endpoint), **kwargs)
        result = decode_response(response)
        return result

    def post(self, endpoint, **kwargs):
        """
        Send HTTP POST to the endpoint.

        :param endpoint: The endpoint to send to.
        :return:
        """
        response = self.requester.post(self.get_endpoint_url(endpoint), **kwargs)
        result = decode_response(response)
        return result

    def put(self, endpoint, **kwargs):
        """
        Send HTTP PUT to the endpoint.

        :param endpoint: The endpoint to send to.
        :return:
        """
        response = self.requester.put(self.get_endpoint_url(endpoint), **kwargs)
        result = decode_response(response)
        return result

    def delete(self, endpoint):
        """
        Send HTTP DELETE to the endpoint.

        :param endpoint: The endpoint to send to.
        :return:
        """
        self.requester.delete(self.get_endpoint_url(endpoint))
