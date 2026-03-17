import logging
import multiprocessing
import os
import platform
import signal
import socket
import time

import pytest


# force 'fork' on macOS
if platform.system() == "Darwin":
    multiprocessing = multiprocessing.get_context("fork")


class LiveServer:  # pragma: no cover
    """The helper class used to manage a live server. Handles creation and
    stopping application in a separate process.

    :param app: The application to run.
    :param host: The host where to listen (default localhost).
    :param port: The port to run application.
    :param wait: The timeout after which test case is aborted if
                 application is not started.
    """

    def __init__(self, app, host, port, wait, clean_stop=False):
        self.app = app
        self.port = port
        self.host = host
        self.wait = wait
        self.clean_stop = clean_stop
        self._process = None

    def start(self):
        """Start application in a separate process."""

        def worker(app, host, port):
            app.run(host=host, port=port, use_reloader=False, threaded=True)

        self._process = multiprocessing.Process(
            target=worker, args=(self.app, self.host, self.port)
        )
        self._process.daemon = True
        self._process.start()

        keep_trying = True
        start_time = time.time()
        while keep_trying:
            elapsed_time = time.time() - start_time
            if elapsed_time > self.wait:
                pytest.fail(
                    "Failed to start the server after {!s} "
                    "seconds.".format(self.wait)
                )
            if self._is_ready():
                keep_trying = False

    def _is_ready(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((self.host, self.port))
        except socket.error:
            ret = False
        else:
            ret = True
        finally:
            sock.close()
        return ret

    def url(self, url=""):
        """Returns the complete url based on server options."""
        return "http://{host!s}:{port!s}{url!s}".format(
            host=self.host, port=self.port, url=url
        )

    def stop(self):
        """Stop application process."""
        if self._process:
            if self.clean_stop and self._stop_cleanly():
                return
            if self._process.is_alive():
                # If it's still alive, kill it
                self._process.terminate()

    def _stop_cleanly(self, timeout=5):
        """Attempts to stop the server cleanly by sending a SIGINT
        signal and waiting for ``timeout`` seconds.

        :return: True if the server was cleanly stopped, False otherwise.
        """
        try:
            os.kill(self._process.pid, signal.SIGINT)
            self._process.join(timeout)
            return True
        except Exception as ex:
            logging.error("Failed to join the live server process: %r", ex)
            return False

    def __repr__(self):
        return "<LiveServer listening at %s>" % self.url()
