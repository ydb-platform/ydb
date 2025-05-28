#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import requests
import threading
import zlib
import json
import time


from abc import ABCMeta, abstractmethod

from six.moves.urllib import parse


class HostsInformationProvider:
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_rack(hostname):
        pass

    @abstractmethod
    def get_datacenter(hostname):
        pass

    @abstractmethod
    def get_body(hostname):
        pass


class NopHostsInformationProvider(HostsInformationProvider):
    def __init__(self):
        pass

    def get_rack(self, hostname):
        return hostname

    def get_datacenter(self, hostname):
        # Keep DC name short, because of
        # BAD_REQUEST (nameservice validator: node 1 has data center in Wall-E location longer than 4 symbols)
        return "FAKE"

    def get_body(self, hostname):
        return zlib.crc32(hostname.encode())


class WalleHostsInformationProvider(HostsInformationProvider):
    def __init__(self, provider_url=None, cloud_mode=False):
        if provider_url is None:
            raise RuntimeError("Got empty hosts_provider url")
        self._base_url = provider_url
        self._cache = {}
        self._timeout_seconds = 5
        self._retry_count = 10
        self._cloud_mode = cloud_mode
        self._lock = threading.Lock()

    def _ask_location(self, hostname):
        with self._lock:
            if hostname in self._cache:
                return self._cache[hostname]

        with requests.Session() as session:
            retries = 10
            url = self._base_url + hostname + "?" + parse.urlencode({"fields": "location,name,inv,short_queue_name"})
            headers = dict()
            if "YC_TOKEN" in os.environ:
                headers["Authorization"] = "Bearer " + os.environ.get("YC_TOKEN").strip()

            while retries > 0:
                retries -= 1

                try:
                    response = session.get(url, timeout=self._timeout_seconds, headers=headers)
                    response.raise_for_status()
                    loaded = json.loads(response.content)
                    with self._lock:
                        self._cache[hostname] = loaded
                        return self._cache[hostname]

                except IOError:
                    if retries == 0:
                        raise RuntimeError(
                            "Failed to retrieve information about host %s in %d retries"
                            % (
                                hostname,
                                self._retry_count,
                            )
                        )

                time.sleep(3)
            raise RuntimeError()

    def get_rack(self, hostname):
        short_queue_name = self._ask_location(hostname)["location"]["short_queue_name"]
        rack = self._ask_location(hostname)["location"]["rack"]
        if self._cloud_mode:
            return "{}:{}".format(short_queue_name.lower(), rack.lower())
        return "{}#{}".format(short_queue_name.upper(), rack)

    def get_datacenter(self, hostname):
        short_dc_name = self._ask_location(hostname)["location"]["short_datacenter_name"]
        return short_dc_name if self._cloud_mode else short_dc_name.upper()

    def get_body(self, hostname):
        return self._ask_location(hostname)["inv"]
