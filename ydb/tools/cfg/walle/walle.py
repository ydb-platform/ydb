#!/usr/bin/env python
# -*- coding: utf-8 -*-
import zlib
from abc import ABCMeta, abstractmethod


class HostsFetcherInterface:
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


class NopHostsFetcher(HostsFetcherInterface):
    def __init__(self):
        pass

    def get_rack(self, hostname):
        return hostname

    def get_datacenter(self, hostname):
        return hostname

    def get_body(self, hostname):
        return zlib.crc32(hostname.encode())
