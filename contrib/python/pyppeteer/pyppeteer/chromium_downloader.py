#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Chromium download module."""

import logging
import os
import stat
import sys
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import certifi
import urllib3
from pyppeteer import __chromium_revision__, __pyppeteer_home__
from tqdm import tqdm

logger = logging.getLogger(__name__)
# add our own stream handler - we want some output here
handler = logging.StreamHandler()
handler.setFormatter(fmt=logging.Formatter(fmt="[{levelname}] {msg}", style="{"))
handler.setLevel(logging.INFO)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

DOWNLOADS_FOLDER = Path(__pyppeteer_home__) / 'local-chromium'
DEFAULT_DOWNLOAD_HOST = 'https://storage.googleapis.com'
DOWNLOAD_HOST = os.environ.get('PYPPETEER_DOWNLOAD_HOST', DEFAULT_DOWNLOAD_HOST)
BASE_URL = f'{DOWNLOAD_HOST}/chromium-browser-snapshots'

REVISION = os.environ.get('PYPPETEER_CHROMIUM_REVISION', __chromium_revision__)

NO_PROGRESS_BAR = os.environ.get('PYPPETEER_NO_PROGRESS_BAR', '')
if NO_PROGRESS_BAR.lower() in ('1', 'true'):
    NO_PROGRESS_BAR = True  # type: ignore

windowsArchive = 'chrome-win'

downloadURLs = {
    'linux': f'{BASE_URL}/Linux_x64/{REVISION}/chrome-linux.zip',
    'mac': f'{BASE_URL}/Mac/{REVISION}/chrome-mac.zip',
    'win32': f'{BASE_URL}/Win/{REVISION}/{windowsArchive}.zip',
    'win64': f'{BASE_URL}/Win_x64/{REVISION}/{windowsArchive}.zip',
}

chromiumExecutable = {
    'linux': DOWNLOADS_FOLDER / REVISION / 'chrome-linux' / 'chrome',
    'mac': (DOWNLOADS_FOLDER / REVISION / 'chrome-mac' / 'Chromium.app' / 'Contents' / 'MacOS' / 'Chromium'),
    'win32': DOWNLOADS_FOLDER / REVISION / windowsArchive / 'chrome.exe',
    'win64': DOWNLOADS_FOLDER / REVISION / windowsArchive / 'chrome.exe',
}


def current_platform() -> str:
    """Get current platform name by short string."""
    if sys.platform.startswith('linux'):
        return 'linux'
    elif sys.platform.startswith('darwin'):
        return 'mac'
    elif sys.platform.startswith('win') or sys.platform.startswith('msys') or sys.platform.startswith('cyg'):
        if sys.maxsize > 2 ** 31 - 1:
            return 'win64'
        return 'win32'
    raise OSError('Unsupported platform: ' + sys.platform)


def get_url() -> str:
    """Get chromium download url."""
    return downloadURLs[current_platform()]


def download_zip(url: str) -> BytesIO:
    """Download data from url."""
    logger.info('Starting Chromium download.')

    with urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where()) as http:
        # Get data from url.
        # set preload_content=False means using stream later.
        r = http.request('GET', url, preload_content=False)
        if r.status >= 400:
            raise OSError(f'Chromium downloadable not found at {url}: ' f'Received {r.data.decode()}.\n')

        # 10 * 1024
        _data = BytesIO()
        if NO_PROGRESS_BAR:
            for chunk in r.stream(10240):
                _data.write(chunk)
        else:
            try:
                total_length = int(r.headers['content-length'])
            except (KeyError, ValueError, AttributeError):
                total_length = 0
            process_bar = tqdm(total=total_length, unit_scale=True, unit='b')
            for chunk in r.stream(10240):
                _data.write(chunk)
                process_bar.update(len(chunk))
            process_bar.close()

    return _data


def extract_zip(data: BytesIO, path: Path) -> None:
    """Extract zipped data to path."""
    # On mac zipfile module cannot extract correctly, so use unzip instead.
    logger.info('Beginning extraction')
    if current_platform() == 'mac':
        import subprocess
        import shutil

        zip_path = path / 'chrome.zip'
        if not path.exists():
            path.mkdir(parents=True)
        with zip_path.open('wb') as f:
            f.write(data.getvalue())
        if not shutil.which('unzip'):
            raise OSError('Failed to automatically extract chromium.' f'Please unzip {zip_path} manually.')
        proc = subprocess.run(
            ['unzip', str(zip_path)], cwd=str(path), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        )
        if proc.returncode != 0:
            logger.error(proc.stdout.decode())
            raise OSError(f'Failed to unzip {zip_path}.')
        if chromium_executable().exists() and zip_path.exists():
            zip_path.unlink()
    else:
        with ZipFile(data) as zf:
            zf.extractall(str(path))
    exec_path = chromium_executable()
    if not exec_path.exists():
        raise IOError('Failed to extract chromium.')
    exec_path.chmod(exec_path.stat().st_mode | stat.S_IXOTH | stat.S_IXGRP | stat.S_IXUSR)
    logger.info(f'Chromium extracted to: {path}')


def download_chromium() -> None:
    """Download and extract chromium."""
    extract_zip(download_zip(get_url()), DOWNLOADS_FOLDER / REVISION)


def chromium_executable() -> Path:
    """Get path of the chromium executable."""
    return chromiumExecutable[current_platform()]


def check_chromium() -> bool:
    """Check if chromium is placed at correct path."""
    return chromium_executable().exists()
