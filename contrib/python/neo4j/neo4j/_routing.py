# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from collections.abc import MutableSet
from contextlib import suppress
from logging import getLogger
from time import monotonic

from ._addressing import Address


log = getLogger("neo4j.pool")


class OrderedSet(MutableSet):
    def __init__(self, elements=()):
        # dicts keep insertion order starting with Python 3.7
        self._elements = dict.fromkeys(elements)
        self._current = None

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(("
            f"{', '.join(map(repr, self._elements))}"
            f"))"
        )

    def __str__(self):
        return f"{{{', '.join(map(repr, self._elements))}}}"

    def __contains__(self, element):
        return element in self._elements

    def __iter__(self):
        return iter(self._elements)

    def __len__(self):
        return len(self._elements)

    def __getitem__(self, index):
        return list(self._elements.keys())[index]

    def add(self, element):
        self._elements[element] = None

    def clear(self):
        self._elements.clear()

    def discard(self, element):
        with suppress(KeyError):
            del self._elements[element]

    def remove(self, element):
        try:
            del self._elements[element]
        except KeyError:
            raise ValueError(element) from None

    def update(self, elements=()):
        self._elements.update(dict.fromkeys(elements))

    def replace(self, elements=()):
        e = self._elements
        e.clear()
        e.update(dict.fromkeys(elements))


class RoutingTable:
    @classmethod
    def parse_routing_info(cls, *, database, servers, ttl):
        """
        Construct a new RoutingTable instance from the given routing info.

        The routing table info is returned from the procedure or BOLT message.
        """
        routers = []
        readers = []
        writers = []
        try:
            for server in servers:
                role = server["role"]
                addresses = [
                    Address.parse(address, default_port=7687)
                    for address in server["addresses"]
                ]
                if role == "ROUTE":
                    routers.extend(addresses)
                elif role == "READ":
                    readers.extend(addresses)
                elif role == "WRITE":
                    writers.extend(addresses)
        except (KeyError, TypeError) as exc:
            raise ValueError("Cannot parse routing info") from exc
        else:
            return cls(
                database=database,
                routers=routers,
                readers=readers,
                writers=writers,
                ttl=ttl,
            )

    def __init__(self, *, database, routers=(), readers=(), writers=(), ttl=0):
        self.initial_routers = OrderedSet(routers)
        self.routers = OrderedSet(routers)
        self.readers = OrderedSet(readers)
        self.writers = OrderedSet(writers)
        self.initialized_without_writers = not self.writers
        self.last_updated_time = monotonic()
        self.ttl = ttl
        self.database = database

    def __repr__(self):
        return (
            "RoutingTable("
            f"database={self.database!r}, "
            f"routers={tuple(self.routers)!r}, "
            f"readers={tuple(self.readers)!r}, "
            f"writers={tuple(self.writers)!r}, "
            f"last_updated_time={self.last_updated_time!r}, "
            f"ttl={self.ttl!r}"
            ")"
        )

    def __contains__(self, address):
        return (
            address in self.routers
            or address in self.readers
            or address in self.writers
        )

    def is_fresh(self, readonly=False):
        """Indicate whether routing information is still usable."""
        assert isinstance(readonly, bool)
        expired = self.last_updated_time + self.ttl <= monotonic()
        if readonly:
            has_server_for_mode = bool(self.readers)
        else:
            has_server_for_mode = bool(self.writers)
        res = not expired and self.routers and has_server_for_mode
        log.debug(
            "[#0000]  _: <ROUTING> checking table freshness "
            "(readonly=%r): table expired=%r, "
            "has_server_for_mode=%r, table routers=%r => %r",
            readonly,
            expired,
            has_server_for_mode,
            self.routers,
            res,
        )
        return res

    def should_be_purged_from_memory(self):
        """
        Check if routing table needs to be purged from memory.

        This is the case if the routing table is stale and has not been used
        for a long time.

        :returns: Returns true if it is old and not used for a while.
        :rtype: bool
        """
        from ._conf import RoutingConfig

        perf_time = monotonic()
        valid_until = (
            self.last_updated_time
            + self.ttl
            + RoutingConfig.routing_table_purge_delay
        )
        should_be_purged = valid_until <= perf_time
        log.debug(
            "[#0000]  _: <ROUTING> purge check: "
            "last_updated_time=%r, ttl=%r, perf_time=%r => %r",
            self.last_updated_time,
            self.ttl,
            perf_time,
            should_be_purged,
        )
        return should_be_purged

    def update(self, new_routing_table):
        """Update the routing table with new routing information."""
        self.routers.replace(new_routing_table.routers)
        self.readers.replace(new_routing_table.readers)
        self.writers.replace(new_routing_table.writers)
        self.initialized_without_writers = not self.writers
        self.last_updated_time = monotonic()
        self.ttl = new_routing_table.ttl
        log.debug("[#0000]  _: <ROUTING> updated table=%r", self)

    def servers(self):
        return set(self.routers) | set(self.writers) | set(self.readers)

    def __eq__(self, other):
        if not isinstance(other, RoutingTable):
            return NotImplemented
        return (
            self.database == other.database
            and self.routers == other.routers
            and self.readers == other.readers
            and self.writers == other.writers
            and self.ttl == other.ttl
        )
