#!/usr/bin/env python3
"""
Fault-injection proxy in front of real mimobalanser.

Real balancer:
  GET http://s3.mds.yandex.net/hostname  -> text/plain backend host[:port]

For the first N seconds after start (default 60):
  - "even" clients  -> fetch real balancer and return its body as-is
  - "odd"  clients  -> hang forever (no response)

After that window every client gets a normal upstream response.

Even/odd is decided from the peer IP last octet by default.

Point BlobDepot BalancerHost at this process instead of s3.mds.yandex.net.
During the fault window odd YDB nodes stay on bootstrap direct :443 while
even nodes switch to the real backend — mixed :443 / :4480 traffic.
"""

from __future__ import annotations

import argparse
import ipaddress
import logging
import socket
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional, Tuple


log = logging.getLogger("mimobalanser_fault")

DEFAULT_UPSTREAM = "http://s3.mds.yandex.net/hostname"


def last_octet(host: str) -> Optional[int]:
    host = host.split("%", 1)[0]
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return None
    return int(ip.packed[-1])


def is_even_client(peer: str, mode: str) -> bool:
    if mode == "always-ok":
        return True
    if mode == "always-hang":
        return False

    octet = last_octet(peer)
    if octet is None:
        octet = sum(ord(c) for c in peer) & 0xFF
        log.warning("peer %r is not an IP, using hash octet=%s", peer, octet)

    even = (octet % 2) == 0
    if mode == "odd-ok":
        even = not even
    return even


def fetch_upstream(url: str, timeout: float) -> Tuple[int, bytes, str]:
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            ctype = resp.headers.get("Content-Type", "text/plain; charset=utf-8")
            return resp.status, body, ctype
    except urllib.error.HTTPError as e:
        body = e.read() if e.fp else b""
        ctype = e.headers.get("Content-Type", "text/plain; charset=utf-8") if e.headers else "text/plain"
        return e.code, body, ctype


class Handler(BaseHTTPRequestHandler):
    upstream: str = DEFAULT_UPSTREAM
    mode: str = "even-ok"
    hang_sleep: float = 365 * 24 * 3600
    upstream_timeout: float = 5.0
    start_monotonic: float = 0.0
    fault_seconds: float = 60.0
    _normal_announced: bool = False

    def log_message(self, fmt: str, *args) -> None:
        log.info("%s - " + fmt, self.address_string(), *args)

    def fault_window_active(self) -> bool:
        return (time.monotonic() - self.start_monotonic) < self.fault_seconds

    def maybe_announce_normal_mode(self) -> None:
        if self._normal_announced or self.fault_window_active():
            return
        # Class-level latch so we log once across all handler threads.
        type(self)._normal_announced = True
        log.info(
            "fault window ended (%.0fs) — serving normal upstream to ALL clients: %s",
            self.fault_seconds,
            self.upstream,
        )

    def reply_upstream(self, peer: str, reason: str) -> None:
        try:
            status, body, ctype = fetch_upstream(self.upstream, self.upstream_timeout)
        except Exception as e:
            log.error("peer=%s %s -> upstream %s failed: %s", peer, reason, self.upstream, e)
            msg = ("upstream error: %s\n" % e).encode("utf-8")
            self.send_response(502)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(msg)))
            self.end_headers()
            self.wfile.write(msg)
            return

        log.info(
            "peer=%s %s -> upstream %s status=%s body=%r",
            peer,
            reason,
            self.upstream,
            status,
            body[:200],
        )
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        # NHttp may send a request-target without "/", so BaseHTTPRequestHandler
        # can end up with path == "HTTP/1.1" (2-word request line). Treat any GET
        # as a balancer probe and ignore the path.
        raw_path = self.path
        peer = self.client_address[0]
        elapsed = time.monotonic() - self.start_monotonic
        self.maybe_announce_normal_mode()
        log.info("peer=%s raw_path=%r elapsed=%.1fs", peer, raw_path, elapsed)

        if not self.fault_window_active():
            self.reply_upstream(peer, "AFTER_FAULT(t=%.1fs)" % elapsed)
            return

        even = is_even_client(peer, self.mode)
        if not even:
            log.warning(
                "peer=%s ODD in fault window (t=%.1fs/%.0fs) -> hang",
                peer,
                elapsed,
                self.fault_seconds,
            )
            try:
                time.sleep(self.hang_sleep)
            except Exception:
                pass
            return

        self.reply_upstream(peer, "EVEN in fault window (t=%.1fs)" % elapsed)


def make_server(listen: str, port: int, handler: type) -> ThreadingHTTPServer:
    """Bind IPv4 or IPv6; for '::' also accept IPv4-mapped clients when possible."""
    ipv6 = listen == "::" or ":" in listen  # bare hostname stays IPv4

    class _Server(ThreadingHTTPServer):
        address_family = socket.AF_INET6 if ipv6 else socket.AF_INET
        allow_reuse_address = True

        def server_bind(self) -> None:
            if ipv6:
                # Dual-stack when kernel allows it (Linux usually does).
                try:
                    self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
                except OSError as e:
                    log.warning("IPV6_V6ONLY=0 failed (%s); IPv6-only listen", e)
            super().server_bind()

    return _Server((listen, port), handler)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--listen", default="0.0.0.0", help="bind address (default: 0.0.0.0; use :: for IPv6)")
    p.add_argument("--port", type=int, default=8080, help="listen port (default: 8080)")
    p.add_argument(
        "--upstream",
        default=DEFAULT_UPSTREAM,
        help=f"real mimobalanser URL (default: {DEFAULT_UPSTREAM})",
    )
    p.add_argument(
        "--upstream-timeout",
        type=float,
        default=5.0,
        help="timeout for upstream GET seconds (default: 5)",
    )
    p.add_argument(
        "--mode",
        choices=("even-ok", "odd-ok", "always-ok", "always-hang"),
        default="even-ok",
        help="even-ok: even peer octet proxies upstream, odd hangs (default)",
    )
    p.add_argument(
        "--fault-seconds",
        type=float,
        default=60.0,
        help="how long even/odd fault logic stays active after start (default: 60)",
    )
    p.add_argument(
        "--hang-seconds",
        type=float,
        default=365 * 24 * 3600,
        help="how long odd clients hang during the fault window (default: ~1 year)",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    Handler.upstream = args.upstream
    Handler.mode = args.mode
    Handler.hang_sleep = args.hang_seconds
    Handler.upstream_timeout = args.upstream_timeout
    Handler.start_monotonic = time.monotonic()
    Handler.fault_seconds = args.fault_seconds
    Handler._normal_announced = False

    server = make_server(args.listen, args.port, Handler)

    log.info(
        "listening on http://%s:%s/  upstream=%r mode=%s fault_seconds=%.0f",
        args.listen,
        args.port,
        args.upstream,
        args.mode,
        args.fault_seconds,
    )
    log.info(
        "fault logic active for %.0fs; will switch to normal upstream for everyone after that",
        args.fault_seconds,
    )

    def _announce_when_fault_ends() -> None:
        time.sleep(max(0.0, args.fault_seconds))
        if not Handler._normal_announced:
            Handler._normal_announced = True
            log.info(
                "fault window ended (%.0fs) — serving normal upstream to ALL clients: %s",
                args.fault_seconds,
                args.upstream,
            )

    threading.Thread(target=_announce_when_fault_ends, name="fault-window", daemon=True).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("stopped")


if __name__ == "__main__":
    main()
