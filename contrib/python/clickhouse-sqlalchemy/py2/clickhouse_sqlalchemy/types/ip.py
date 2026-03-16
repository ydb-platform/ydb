from ipaddress import IPv4Network, IPv6Network

from sqlalchemy import or_, and_, types, func
from sqlalchemy.sql.type_api import UserDefinedType

from ..util.compat import text_type


class BaseIPComparator(UserDefinedType.Comparator):
    network_class = None

    def _split_other(self, other):
        """
        Split values between addresses and networks
        This allows to generate complex filters with both addresses
        and networks in the same IN
        ie in_('10.0.0.0/24', '192.168.0.1')
        """
        addresses = []
        networks = []
        for sub in other:
            sub = self.network_class(sub)
            if sub.prefixlen == sub.max_prefixlen:
                # this is an address
                addresses.append(sub.network_address)
            else:
                networks.append(sub)
        return addresses, networks

    def in_(self, other):
        if isinstance(other, (list, tuple)):
            addresses, networks = self._split_other(other)
            addresses_clause = super(BaseIPComparator, self).in_(
                addresses
            ) if addresses else None
            networks_clause = or_(*[
                and_(
                    self >= net[0],
                    self <= net[-1]
                )
                for net in networks
            ]) if networks else None
            if addresses_clause is not None and networks_clause is not None:
                return or_(addresses_clause, networks_clause)
            elif addresses_clause is not None and networks_clause is None:
                return addresses_clause
            elif networks_clause is not None and addresses_clause is None:
                return networks_clause
            else:
                # other is an empty array
                return super(BaseIPComparator, self).in_(other)

        if not isinstance(other, self.network_class):
            other = self.network_class(other)

        return and_(
            self >= other[0],
            self <= other[-1]
        )

    def notin_(self, other):
        if isinstance(other, (list, tuple)):
            addresses, networks = self._split_other(other)
            addresses_clause = super(BaseIPComparator, self).notin_(
                addresses
            ) if addresses else None
            networks_clause = and_(*[
                or_(
                    self < net[0],
                    self > net[-1]
                )
                for net in networks
            ]) if networks else None
            if addresses_clause is not None and networks_clause is not None:
                return and_(addresses_clause, networks_clause)
            elif addresses_clause is not None and networks_clause is None:
                return addresses_clause
            elif networks_clause is not None and addresses_clause is None:
                return networks_clause
            else:
                # other is an empty array
                return super(BaseIPComparator, self).notin_(other)

        if not isinstance(other, self.network_class):
            other = self.network_class(other)

        return or_(
            self < other[0],
            self > other[-1]
        )


class IPv4(types.UserDefinedType):
    __visit_name__ = "ipv4"

    def bind_processor(self, dialect):
        def process(value):
            return text_type(value)

        return process

    def literal_processor(self, dialect):
        bp = self.bind_processor(dialect)

        def process(value):
            return "'%s'" % bp(value)

        return process

    def bind_expression(self, bindvalue):
        return func.toIPv4(bindvalue)

    class comparator_factory(BaseIPComparator):
        network_class = IPv4Network


class IPv6(types.UserDefinedType):
    __visit_name__ = "ipv6"

    def bind_processor(self, dialect):
        def process(value):
            return text_type(value)

        return process

    def literal_processor(self, dialect):
        bp = self.bind_processor(dialect)

        def process(value):
            return "'%s'" % bp(value)

        return process

    def bind_expression(self, bindvalue):
        return func.toIPv6(bindvalue)

    class comparator_factory(BaseIPComparator):
        network_class = IPv6Network
