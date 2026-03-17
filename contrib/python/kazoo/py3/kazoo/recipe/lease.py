"""Zookeeper lease implementations

:Maintainer: Lars Albertsson <lars.albertsson@gmail.com>
:Maintainer: Jyrki Pulliainen <jyrki@spotify.com>
:Status: Beta

"""
import datetime
import json
import socket

from kazoo.exceptions import CancelledError


class NonBlockingLease(object):
    """Exclusive lease that does not block.

    An exclusive lease ensures that only one client at a time owns the lease.
    The client may renew the lease without losing it by obtaining a new lease
    with the same path and same identity.  The lease object evaluates to True
    if the lease was obtained.

    A common use case is a situation where a task should only run on a single
    host.  In this case, the clients that did not obtain the lease should exit
    without performing the protected task.

    The lease stores time stamps using client clocks, and will therefore only
    work if client clocks are roughly synchronised.  It uses UTC, and works
    across time zones and daylight savings.

    Example usage: with a :class:`~kazoo.client.KazooClient` instance::

        zk = KazooClient()
        zk.start()
        # Hold lease over an hour in order to keep job on same machine,
        # with failover if it dies.
        lease = zk.NonBlockingLease(
            "/db_leases/hourly_cleanup", datetime.timedelta(minutes = 70),
            identifier = "DB hourly cleanup on " + socket.gethostname())
        if lease:
            do_hourly_database_cleanup()
    """

    # Bump when storage format changes
    _version = 1
    _date_format = "%Y-%m-%dT%H:%M:%S"
    _byte_encoding = "utf-8"

    def __init__(
        self,
        client,
        path,
        duration,
        identifier=None,
        utcnow=datetime.datetime.utcnow,
    ):
        """Create a non-blocking lease.

        :param client: A :class:`~kazoo.client.KazooClient` instance.
        :param path: The lease path to use.
        :param duration: Duration during which the lease is reserved.  A
                         :class:`~datetime.timedelta` instance.
        :param identifier: Unique name to use for this lease holder. Reuse in
                           order to renew the lease. Defaults to
                           :meth:`socket.gethostname()`.
        :param utcnow: Clock function, by default returning
                       :meth:`datetime.datetime.utcnow()`. Used for testing.

        """
        ident = identifier or socket.gethostname()
        self.obtained = False
        self._attempt_obtaining(client, path, duration, ident, utcnow)

    def _attempt_obtaining(self, client, path, duration, ident, utcnow):
        client.ensure_path(path)
        holder_path = path + "/lease_holder"
        lock = client.Lock(path, ident)
        try:
            with lock:
                now = utcnow()
                if client.exists(holder_path):
                    raw, _ = client.get(holder_path)
                    data = self._decode(raw)
                    if data["version"] != self._version:
                        # We need an upgrade, let someone else take the lease
                        return
                    current_end = datetime.datetime.strptime(
                        data["end"], self._date_format
                    )
                    if data["holder"] != ident and now < current_end:
                        # Another client is still holding the lease
                        return
                    client.delete(holder_path)
                end_lease = (now + duration).strftime(self._date_format)
                new_data = {
                    "version": self._version,
                    "holder": ident,
                    "end": end_lease,
                }
                client.create(holder_path, self._encode(new_data))
                self.obtained = True

        except CancelledError:
            pass

    def _encode(self, data_dict):
        return json.dumps(data_dict).encode(self._byte_encoding)

    def _decode(self, raw):
        return json.loads(raw.decode(self._byte_encoding))

    # Python 2.x
    def __nonzero__(self):
        return self.obtained

    # Python 3.x
    def __bool__(self):
        return self.obtained


class MultiNonBlockingLease(object):
    """Exclusive lease for multiple clients.

    This type of lease is useful when a limited set of hosts should run a
    particular task. It will attempt to obtain leases trying a sequence of
    ZooKeeper lease paths.

    :param client: A :class:`~kazoo.client.KazooClient` instance.
    :param count: Number of host leases allowed.
    :param path: ZooKeeper path under which lease files are stored.
    :param duration: Duration during which the lease is reserved.  A
                     :class:`~datetime.timedelta` instance.
    :param identifier: Unique name to use for this lease holder. Reuse in order
                       to renew the lease.
           Defaults do :meth:`socket.gethostname()`.
    :param utcnow: Clock function, by default returning
                   :meth:`datetime.datetime.utcnow()`.  Used for testing.

    """

    def __init__(
        self,
        client,
        count,
        path,
        duration,
        identifier=None,
        utcnow=datetime.datetime.utcnow,
    ):
        self.obtained = False
        for num in range(count):
            ls = NonBlockingLease(
                client,
                "%s/%d" % (path, num),
                duration,
                identifier=identifier,
                utcnow=utcnow,
            )
            if ls:
                self.obtained = True
                break

    # Python 2.x
    def __nonzero__(self):
        return self.obtained

    # Python 3.x
    def __bool__(self):
        return self.obtained
