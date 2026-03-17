#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Commands for Pyppeteer."""

import logging

from pyppeteer.chromium_downloader import check_chromium, download_chromium


def install() -> None:
    """Download chromium if not install."""
    if not check_chromium():
        download_chromium()
    else:
        logging.getLogger(__name__).warning('chromium is already installed.')
