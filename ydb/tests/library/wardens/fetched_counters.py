# -*- coding: utf-8 -*-
"""One /counters/json snapshot per node and slot for liveness checks."""
import sys
import time


def _log(line):
    sys.stderr.write(line + "\n")
    sys.stderr.flush()


# sensor() refetches after the monitor TTL. This pass only reads.
_SNAPSHOT_TTL_SECONDS = 10 ** 9


class _UnreadMonitor(object):

    def sensor(self, counters, sensor, _default=None, **kwargs):
        return _default

    def get_by_name(self, sensor):
        return []

    def fetch(self, deadline=60):
        return self

    def has_actual_data(self):
        return False


class FetchedCounters(object):
    def __init__(self):
        self._items = {}

    def put(self, endpoint, monitor, readable):
        self._items[(endpoint.host, endpoint.mon_port)] = (monitor, readable)

    def readable(self, endpoint):
        item = self._items.get((endpoint.host, endpoint.mon_port))
        return bool(item and item[1])

    def monitor(self, endpoint):
        item = self._items.get((endpoint.host, endpoint.mon_port))
        if item is None or not item[1]:
            return _UnreadMonitor()
        return item[0]


def _group(cluster, attr):
    group = getattr(cluster, attr, None) or {}
    return list(group.values())


def _slot_label(endpoint):
    mon_port = endpoint.mon_port
    ic_port = getattr(endpoint, "ic_port", None)
    slot_no = None
    if isinstance(mon_port, int) and mon_port >= 31002 and (mon_port - 31002) % 10 == 0:
        slot_no = (mon_port - 31002) // 10 + 1
    if slot_no is not None and ic_port is not None:
        return "%s slot %s (kikimr-multi@%s, mon %s)" % (
            endpoint.host,
            slot_no,
            ic_port,
            mon_port,
        )
    return "%s mon %s" % (endpoint.host, mon_port)


def _fetch_one(fetched, endpoint):
    started = time.time()
    host = endpoint.host
    port = endpoint.mon_port
    _log("counters start %s:%s" % (host, port))
    readable = False
    try:
        monitor = endpoint.monitor.fetch(deadline=_SNAPSHOT_TTL_SECONDS)
        readable = bool(monitor._by_sensor_name)
        fetched.put(endpoint, monitor, readable)
    except Exception as exc:
        # fetch() does not catch a connection reset while reading the body.
        fetched.put(endpoint, None, False)
        detail = " ".join(str(exc).split())
        if len(detail) > 200:
            detail = detail[:200]
        _log("counters error %s:%s %s: %s" % (host, port, type(exc).__name__, detail))
    _log(
        "counters done %s:%s %s %.1fs" % (
            host,
            port,
            "ok" if readable else "fail",
            time.time() - started,
        )
    )
    return readable


def fetch_liveness_counters(cluster):
    """Download /counters/json once. Returns (counters, unreachable slot labels)."""
    fetched = FetchedCounters()
    for endpoint in _group(cluster, "nodes"):
        _fetch_one(fetched, endpoint)
    unreachable_slots = []
    for endpoint in _group(cluster, "slots"):
        if not _fetch_one(fetched, endpoint):
            unreachable_slots.append(_slot_label(endpoint))
    return fetched, unreachable_slots
