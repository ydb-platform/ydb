#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Exceptions for pyppeteer package."""

import asyncio


class PyppeteerError(Exception):  # noqa: D204
    """Base exception for pyppeteer."""
    pass


class BrowserError(PyppeteerError):  # noqa: D204
    """Exception raised from browser."""
    pass


class ElementHandleError(PyppeteerError):  # noqa: D204
    """ElementHandle related exception."""
    pass


class NetworkError(PyppeteerError):  # noqa: D204
    """Network/Protocol related exception."""
    pass


class PageError(PyppeteerError):  # noqa: D204
    """Page/Frame related exception."""
    pass


class TimeoutError(asyncio.TimeoutError):  # noqa: D204
    """Timeout Error class."""
    pass
