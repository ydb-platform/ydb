#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
from typing import Optional
from base64 import b64decode
import requests
from requests.adapters import HTTPAdapter
from gerrit.utils.requester import Requester
from gerrit.utils.common import decode_response, strip_trailing_slash


class GitilesClient:
    def __init__(
        self,
        base_url,
        username=None,
        password=None,
        ssl_verify=True,
        cert=None,
        timeout=60,
        max_retries=None,
    ):
        self._base_url = strip_trailing_slash(base_url)

        # make request session
        _session = requests.Session()
        if username and password:
            _session.auth = (username, password)

        if ssl_verify:
            _session.verify = ssl_verify

        if cert is not None:
            _session.cert = cert

        if max_retries is not None:
            retry_adapter = HTTPAdapter(max_retries=max_retries)
            _session.mount("http://", retry_adapter)
            _session.mount("https://", retry_adapter)

        self.session = _session

        self.requester = Requester(
            base_url=base_url,
            session=self.session,
            timeout=timeout,
        )

    def get_endpoint_url(self, endpoint):
        """
        Return the complete url including host and port for a given endpoint.
        :param endpoint: service endpoint as str
        :return: complete url (including host and port) as str
        """
        return f"{self._base_url}{endpoint}"

    def commit(self, repo: str, commit: str):
        """Retrieves a commit."""
        endpoint = f"/{repo}/+/{commit}"
        params = {"format": "JSON"}

        response = self.requester.get(self.get_endpoint_url(endpoint), params=params)
        result = decode_response(response)

        return result

    def commits(self, repo: str, ref: str, start: Optional[str] = None):
        """query commit history"""
        endpoint = f"/{repo}/+log/{ref}"
        params = {"format": "JSON"}
        if start is not None:
            params.update({"s": start})

        response = self.requester.get(self.get_endpoint_url(endpoint), params=params)
        result = decode_response(response)

        return result

    def download_file(
        self, repo: str, ref: str, path: str, format: str = "TEXT", decode: bool = False
    ):
        """Downloads raw file content from a Gitiles repository."""
        endpoint = f"/{repo}/+/{ref}/{path}"
        params = {"format": format}

        response = self.requester.get(self.get_endpoint_url(endpoint), params=params)
        result = decode_response(response)

        if decode:
            return b64decode(result).decode("utf-8")

        return result
