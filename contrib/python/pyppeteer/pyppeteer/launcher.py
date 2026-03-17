#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Chromium process launcher module."""

import asyncio
import atexit
from copy import copy
import json
from urllib.request import urlopen
from urllib.error import URLError
from http.client import HTTPException
import logging
import os
import os.path
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from typing import Any, Dict, List, TYPE_CHECKING

from pyppeteer import __pyppeteer_home__
from pyppeteer.browser import Browser
from pyppeteer.connection import Connection
from pyppeteer.chromium_downloader import current_platform
from pyppeteer.errors import BrowserError
from pyppeteer.helper import addEventListener, debugError, removeEventListeners
from pyppeteer.target import Target
from pyppeteer.util import check_chromium, chromium_executable
from pyppeteer.util import download_chromium, merge_dict, get_free_port

if TYPE_CHECKING:
    from typing import Optional  # noqa: F401

logger = logging.getLogger(__name__)

pyppeteer_home = Path(__pyppeteer_home__)
CHROME_PROFILE_PATH = pyppeteer_home / '.dev_profile'

DEFAULT_ARGS = [
    '--disable-background-networking',
    '--disable-background-timer-throttling',
    '--disable-breakpad',
    '--disable-browser-side-navigation',
    '--disable-client-side-phishing-detection',
    '--disable-default-apps',
    '--disable-dev-shm-usage',
    '--disable-extensions',
    '--disable-features=site-per-process',
    '--disable-hang-monitor',
    '--disable-popup-blocking',
    '--disable-prompt-on-repost',
    '--disable-sync',
    '--disable-translate',
    '--metrics-recording-only',
    '--no-first-run',
    '--safebrowsing-disable-auto-update',
    '--enable-automation',
    '--password-store=basic',
    '--use-mock-keychain',
]


class Launcher(object):
    """Chrome process launcher class."""

    def __init__(self, options: Dict[str, Any] = None,  # noqa: C901
                 **kwargs: Any) -> None:
        """Make new launcher."""
        options = merge_dict(options, kwargs)

        self.port = get_free_port()
        self.url = f'http://127.0.0.1:{self.port}'
        self._loop = options.get('loop', asyncio.get_event_loop())
        self.chromeClosed = True

        ignoreDefaultArgs = options.get('ignoreDefaultArgs', False)
        args: List[str] = options.get('args', list())
        self.dumpio = options.get('dumpio', False)
        executablePath = options.get('executablePath')
        self.env = options.get('env')
        self.handleSIGINT = options.get('handleSIGINT', True)
        self.handleSIGTERM = options.get('handleSIGTERM', True)
        self.handleSIGHUP = options.get('handleSIGHUP', True)
        self.ignoreHTTPSErrors = options.get('ignoreHTTPSErrors', False)
        self.defaultViewport = options.get('defaultViewport', {'width': 800, 'height': 600})  # noqa: E501
        self.slowMo = options.get('slowMo', 0)
        self.timeout = options.get('timeout', 30000)
        self.autoClose = options.get('autoClose', True)

        logLevel = options.get('logLevel')
        if logLevel:
            logging.getLogger('pyppeteer').setLevel(logLevel)

        self.chromeArguments: List[str] = list()
        if not ignoreDefaultArgs:
            self.chromeArguments.extend(defaultArgs(options))
        elif isinstance(ignoreDefaultArgs, list):
            self.chromeArguments.extend(filter(lambda arg: arg not in ignoreDefaultArgs, defaultArgs(options), ))
        else:
            self.chromeArguments.extend(args)

        self.temporaryUserDataDir: Optional[str] = None

        if not any(arg for arg in self.chromeArguments if arg.startswith('--remote-debugging-')):
            self.chromeArguments.append(f'--remote-debugging-port={self.port}')

        if not any(arg for arg in self.chromeArguments if arg.startswith('--user-data-dir')):
            if not CHROME_PROFILE_PATH.exists():
                CHROME_PROFILE_PATH.mkdir(parents=True)
            self.temporaryUserDataDir = tempfile.mkdtemp(dir=str(CHROME_PROFILE_PATH))  # noqa: E501
            self.chromeArguments.append(f'--user-data-dir={self.temporaryUserDataDir}')  # noqa: E501

        self.chromeExecutable = executablePath
        if not self.chromeExecutable:
            if not check_chromium():
                download_chromium()
            self.chromeExecutable = str(chromium_executable())

        self.cmd = [self.chromeExecutable] + self.chromeArguments

    def _cleanup_tmp_user_data_dir(self) -> None:
        for retry in range(100):
            if self.temporaryUserDataDir and os.path.exists(self.temporaryUserDataDir):
                shutil.rmtree(self.temporaryUserDataDir, ignore_errors=True)
                if os.path.exists(self.temporaryUserDataDir):
                    time.sleep(0.01)
            else:
                break
        else:
            raise IOError('Unable to remove Temporary User Data')

    async def launch(self) -> Browser:  # noqa: C901
        """Start chrome process and return `Browser` object."""
        self.chromeClosed = False
        self.connection: Optional[Connection] = None

        options = dict()
        options['env'] = self.env
        if not self.dumpio:
            # discard stdout, it's never read in any case.
            options['stdout'] = subprocess.DEVNULL
            options['stderr'] = subprocess.STDOUT

        self.proc = subprocess.Popen(  # type: ignore
            self.cmd, **options, )

        def _close_process(*args: Any, **kwargs: Any) -> None:
            if not self.chromeClosed:
                self._loop.run_until_complete(self.killChrome())

        # don't forget to close browser process
        if self.autoClose:
            atexit.register(_close_process)
        if self.handleSIGINT:
            signal.signal(signal.SIGINT, _close_process)
        if self.handleSIGTERM:
            signal.signal(signal.SIGTERM, _close_process)
        if not sys.platform.startswith('win'):
            # SIGHUP is not defined on windows
            if self.handleSIGHUP:
                signal.signal(signal.SIGHUP, _close_process)

        connectionDelay = self.slowMo
        self.browserWSEndpoint = get_ws_endpoint(self.url)
        logger.info(f'Browser listening on: {self.browserWSEndpoint}')
        self.connection = Connection(self.browserWSEndpoint, self._loop, connectionDelay, )
        browser = await Browser.create(self.connection, [], self.ignoreHTTPSErrors, self.defaultViewport, self.proc,
                                       self.killChrome)
        await self.ensureInitialPage(browser)
        return browser

    async def ensureInitialPage(self, browser: Browser) -> None:
        """Wait for initial page target to be created."""
        for target in browser.targets():
            if target.type == 'page':
                return

        initialPagePromise = self._loop.create_future()

        def initialPageCallback() -> None:
            initialPagePromise.set_result(True)

        def check_target(target: Target) -> None:
            if target.type == 'page':
                initialPageCallback()

        listeners = [addEventListener(browser, 'targetcreated', check_target)]
        await initialPagePromise
        removeEventListeners(listeners)

    def waitForChromeToClose(self) -> None:
        """Terminate chrome."""
        if self.proc.poll() is None and not self.chromeClosed:
            self.chromeClosed = True
            try:
                self.proc.terminate()
                self.proc.wait()
            except Exception:
                # browser process may be already closed
                pass

    async def killChrome(self) -> None:
        """Terminate chromium process."""
        logger.info('terminate chrome process...')
        if self.connection and self.connection._connected:
            try:
                await self.connection.send('Browser.close')
                await self.connection.dispose()
            except Exception as e:
                # ignore errors on browser termination process
                debugError(logger, e)
        if self.temporaryUserDataDir and os.path.exists(self.temporaryUserDataDir):  # noqa: E501
            # Force kill chrome only when using temporary userDataDir
            self.waitForChromeToClose()
            self._cleanup_tmp_user_data_dir()


def get_ws_endpoint(url) -> str:
    url = url + '/json/version'
    timeout = time.time() + 30
    while (True):
        if time.time() > timeout:
            raise BrowserError('Browser closed unexpectedly:\n')
        try:
            with urlopen(url) as f:
                data = json.loads(f.read().decode())
            break
        except (URLError, HTTPException):
            pass
        time.sleep(0.1)

    return data['webSocketDebuggerUrl']


async def launch(options: dict = None, **kwargs: Any) -> Browser:
    """Start chrome process and return :class:`~pyppeteer.browser.Browser`.
    This function is a shortcut to :meth:`Launcher(options, **kwargs).launch`.
    Available options are:
    * ``ignoreHTTPSErrors`` (bool): Whether to ignore HTTPS errors. Defaults to
      ``False``.
    * ``headless`` (bool): Whether to run browser in headless mode. Defaults to
      ``True`` unless ``appMode`` or ``devtools`` options is ``True``.
    * ``executablePath`` (str): Path to a Chromium or Chrome executable to run
      instead of default bundled Chromium.
    * ``slowMo`` (int|float): Slow down pyppeteer operations by the specified
      amount of milliseconds.
    * ``defaultViewport`` (dict): Set a consistent viewport for each page.
      Defaults to an 800x600 viewport. ``None`` disables default viewport.
      * ``width`` (int): page width in pixels.
      * ``height`` (int): page height in pixels.
      * ``deviceScaleFactor`` (int|float): Specify device scale factor (can be
        thought as dpr). Defaults to ``1``.
      * ``isMobile`` (bool): Whether the ``meta viewport`` tag is taken into
        account. Defaults to ``False``.
      * ``hasTouch`` (bool): Specify if viewport supports touch events.
        Defaults to ``False``.
      * ``isLandscape`` (bool): Specify if viewport is in landscape mode.
        Defaults to ``False``.
    * ``args`` (List[str]): Additional arguments (flags) to pass to the browser
      process.
    * ``ignoreDefaultArgs`` (bool or List[str]): If ``True``, do not use
      :func:`~pyppeteer.defaultArgs`. If list is given, then filter out given
      default arguments. Dangerous option; use with care. Defaults to
      ``False``.
    * ``handleSIGINT`` (bool): Close the browser process on Ctrl+C. Defaults to
      ``True``.
    * ``handleSIGTERM`` (bool): Close the browser process on SIGTERM. Defaults
      to ``True``.
    * ``handleSIGHUP`` (bool): Close the browser process on SIGHUP. Defaults to
      ``True``.
    * ``dumpio`` (bool): Whether to pipe the browser process stdout and stderr
      into ``process.stdout`` and ``process.stderr``. Defaults to ``False``.
    * ``userDataDir`` (str): Path to a user data directory.
    * ``env`` (dict): Specify environment variables that will be visible to the
      browser. Defaults to same as python process.
    * ``devtools`` (bool): Whether to auto-open a DevTools panel for each tab.
      If this option is ``True``, the ``headless`` option will be set
      ``False``.
    * ``logLevel`` (int|str): Log level to print logs. Defaults to same as the
      root logger.
    * ``autoClose`` (bool): Automatically close browser process when script
      completed. Defaults to ``True``.
    * ``loop`` (asyncio.AbstractEventLoop): Event loop (**experimental**).
    * ``appMode`` (bool): Deprecated.
    This function combines 3 steps:
    1. Infer a set of flags to launch chromium with using
       :func:`~pyppeteer.defaultArgs`.
    2. Launch browser and start managing its process according to the
       ``executablePath``, ``handleSIGINT``, ``dumpio``, and other options.
    3. Create an instance of :class:`~pyppeteer.browser.Browser` class and
       initialize it with ``defaultViewport``, ``slowMo``, and
       ``ignoreHTTPSErrors``.
    ``ignoreDefaultArgs`` option can be used to customize behavior on the (1)
    step. For example, to filter out ``--mute-audio`` from default arguments:
    .. code::
        browser = await launch(ignoreDefaultArgs=['--mute-audio'])
    .. note::
        Pyppeteer can also be used to control the Chrome browser, but it works
        best with the version of Chromium it is bundled with. There is no
        guarantee it will work with any other version. Use ``executablePath``
        option with extreme caution.
    """
    return await Launcher(options, **kwargs).launch()


async def connect(options: dict = None, **kwargs: Any) -> Browser:
    """Connect to the existing chrome.
    ``browserWSEndpoint`` or ``browserURL`` option is necessary to connect to
    the chrome. The format of ``browserWSEndpoint`` is
    ``ws://${host}:${port}/devtools/browser/<id>`` and format of ``browserURL``
    is ``http://127.0.0.1:9222```.
    The value of ``browserWSEndpoint`` can get by :attr:`~pyppeteer.browser.Browser.wsEndpoint`.
    Available options are:
    * ``browserWSEndpoint`` (str): A browser websocket endpoint to connect to.
    * ``browserURL`` (str): A browser URL to connect to.
    * ``ignoreHTTPSErrors`` (bool): Whether to ignore HTTPS errors. Defaults to
      ``False``.
    * ``defaultViewport`` (dict): Set a consistent viewport for each page.
      Defaults to an 800x600 viewport. ``None`` disables default viewport.
      * ``width`` (int): page width in pixels.
      * ``height`` (int): page height in pixels.
      * ``deviceScaleFactor`` (int|float): Specify device scale factor (can be
        thought as dpr). Defaults to ``1``.
      * ``isMobile`` (bool): Whether the ``meta viewport`` tag is taken into
        account. Defaults to ``False``.
      * ``hasTouch`` (bool): Specify if viewport supports touch events.
        Defaults to ``False``.
      * ``isLandscape`` (bool): Specify if viewport is in landscape mode.
        Defaults to ``False``.
    * ``slowMo`` (int|float): Slow down pyppeteer's by the specified amount of
      milliseconds.
    * ``logLevel`` (int|str): Log level to print logs. Defaults to same as the
      root logger.
    * ``loop`` (asyncio.AbstractEventLoop): Event loop (**experimental**).
    """
    options = merge_dict(options, kwargs)
    logLevel = options.get('logLevel')
    if logLevel:
        logging.getLogger('pyppeteer').setLevel(logLevel)

    browserWSEndpoint = options.get('browserWSEndpoint')
    if not browserWSEndpoint:
        browserURL = options.get('browserURL')
        if not browserURL:
            raise BrowserError('Need `browserWSEndpoint` or `browserURL` option.')
        browserWSEndpoint = get_ws_endpoint(browserURL)
    connectionDelay = options.get('slowMo', 0)
    connection = Connection(browserWSEndpoint, options.get('loop', asyncio.get_event_loop()), connectionDelay)
    browserContextIds = (await connection.send('Target.getBrowserContexts')).get('browserContextIds', [])
    ignoreHTTPSErrors = bool(options.get('ignoreHTTPSErrors', False))
    defaultViewport = options.get('defaultViewport', {'width': 800, 'height': 600})
    return await Browser.create(connection, browserContextIds, ignoreHTTPSErrors, defaultViewport, None,
                                lambda: connection.send('Browser.close'))


def executablePath() -> str:
    """Get executable path of default chromium."""
    return str(chromium_executable())


def defaultArgs(options: Dict = None, **kwargs: Any) -> List[str]:  # noqa: C901,E501
    """Get the default flags the chromium will be launched with.
    ``options`` or keyword arguments are set of configurable options to set on
    the browser. Can have the following fields:
    * ``headless`` (bool): Whether to run browser in headless mode. Defaults to
      ``True`` unless the ``devtools`` option is ``True``.
    * ``args`` (List[str]): Additional arguments to pass to the browser
      instance. The list of chromium flags can be found
      `here <http://peter.sh/experiments/chromium-command-line-switches/>`__.
    * ``userDataDir`` (str): Path to a User Data Directory.
    * ``devtools`` (bool): Whether to auto-open DevTools panel for each tab. If
      this option is ``True``, the ``headless`` option will be set ``False``.
    """
    options = merge_dict(options, kwargs)
    devtools = options.get('devtools', False)
    headless = options.get('headless', not devtools)
    args = options.get('args', list())
    userDataDir = options.get('userDataDir')
    chromeArguments = copy(DEFAULT_ARGS)

    if userDataDir:
        chromeArguments.append(f'--user-data-dir={userDataDir}')
    if devtools:
        chromeArguments.append('--auto-open-devtools-for-tabs')
    if headless:
        chromeArguments.extend(('--headless', '--hide-scrollbars', '--mute-audio',))
        if current_platform().startswith('win'):
            chromeArguments.append('--disable-gpu')

    if all(map(lambda arg: arg.startswith('-'), args)):  # type: ignore
        chromeArguments.append('about:blank')
    chromeArguments.extend(args)

    return chromeArguments
