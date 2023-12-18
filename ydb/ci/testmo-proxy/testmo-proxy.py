#!/usr/bin/env python3
import argparse
import http.server
import ssl
import time
import urllib.parse
from typing import Tuple

import requests
from requests.utils import CaseInsensitiveDict


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    error_message_format = '{"status":"error", "code":"%(code)d", "message": "%(message)s"}\n'

    # noinspection PyMissingConstructor
    def __init__(self, target_url: str, timeout: Tuple[int, int], max_reqest_time: int):
        self._target_url = target_url
        self._timeout = timeout
        self._max_request_time = max_reqest_time

    def __call__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def log_date_time_string(self):
        """Return the current time formatted for logging."""
        now = time.time()
        msecs = int((now - int(now)) * 1000) + 0.0
        return f"{time.strftime('%Y-%m-%d %H:%M:%S')},{msecs:03.0f}"

    def _proxy_request(self, method):
        url = urllib.parse.urljoin(self._target_url, self.path)

        headers = CaseInsensitiveDict(self.headers.items())
        del headers["host"]

        body = ""

        if "content-length" in headers:
            body = self.rfile.read(int(headers["content-length"]))

        self.log_message(f"< method={method} url={url} length={len(body)}")

        start_time = time.monotonic()

        response = None

        while time.monotonic() - start_time < self._max_request_time:
            try:
                response = requests.request(method, url, data=body, headers=headers, timeout=self._timeout)
                break
            except requests.exceptions.RequestException as e:
                self.log_message("! catch %s, retry", e)
                time.sleep(0.25)
                continue

        if response is None:
            self.log_message("! Unable to get testmo response, send 204 response for skipping this batch")
            self.send_response_only(204)
            self.send_header("X-Fake-Status", "true")
            self.end_headers()
            return

        response_body = response.content
        self.log_message(f"= status={response.status_code}, length={len(response_body)}")

        self.send_response_only(response.status_code, response.reason)

        for k, v in response.headers.items():
            if k.lower() in ("connection", "transfer-encoding", "content-length"):
                continue

            self.send_header(k, v)

        self.send_header("Content-Length", str(len(response_body)))
        self.end_headers()
        self.wfile.write(response_body)

    def do_GET(self):
        self._proxy_request("GET")

    def do_POST(self):
        self._proxy_request("POST")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", dest="listen", default="127.0.0.1:8888")
    parser.add_argument("--cert-file", dest="certfile", required=True)
    parser.add_argument("--cert-key", dest="keyfile", required=True)
    parser.add_argument("--target-timeout", dest="target_timeout", default="3,10")
    parser.add_argument("--max-request-time", dest="max_request_time", type=int, default=55)
    parser.add_argument("target_url")
    args = parser.parse_args()

    addr_parts = args.listen.split(":")
    addr = (addr_parts[0], int(addr_parts[1]))
    timeout = tuple(map(int, args.target_timeout.split(",")))

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile=args.certfile, keyfile=args.keyfile)

    # noinspection PyTypeChecker
    srv = http.server.HTTPServer(addr, Handler(args.target_url, timeout, args.max_request_time))
    srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
