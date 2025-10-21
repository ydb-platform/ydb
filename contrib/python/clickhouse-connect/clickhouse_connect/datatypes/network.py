import socket
from ipaddress import ip_address, IPv4Address, IPv6Address
from typing import Union, MutableSequence, Sequence, Any

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver.common import write_array, int_size, first_value
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.query import QueryContext
from clickhouse_connect.driver.types import ByteSource
from clickhouse_connect.driver.ctypes import data_conv

IPV4_V6_MASK = b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff'
V6_NULL = bytes(b'\x00' * 16)


# pylint: disable=protected-access
class IPv4(ClickHouseType):
    _array_type = 'L' if int_size == 2 else 'I'
    valid_formats = 'string', 'native', 'int'
    python_type = IPv4Address
    byte_size = 4

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext, _read_state: Any):
        if self.read_format(ctx) == 'int':
            return source.read_array(self._array_type, num_rows)
        if self.read_format(ctx) == 'string':
            column = source.read_array(self._array_type, num_rows)
            return [socket.inet_ntoa(x.to_bytes(4, 'big')) for x in column]
        return data_conv.read_ipv4_col(source, num_rows)

    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: bytearray, ctx: InsertContext):
        first = first_value(column, self.nullable)
        if isinstance(first, str):
            fixed = 24, 16, 8, 0
            # pylint: disable=consider-using-generator
            column = [(sum([int(b) << fixed[ix] for ix, b in enumerate(x.split('.'))])) if x else 0 for x in column]
        else:
            if self.nullable:
                column = [x._ip if x else 0 for x in column]
            else:
                column = [x._ip for x in column]
        write_array(self._array_type, column, dest, ctx.column_name)

    def _active_null(self, ctx: QueryContext):
        fmt = self.read_format(ctx)
        if ctx.use_none:
            return None
        if fmt == 'string':
            return '0.0.0.0'
        if fmt == 'int':
            return 0
        return None


# pylint: disable=protected-access
class IPv6(ClickHouseType):
    valid_formats = 'string', 'native'
    python_type = IPv6Address
    byte_size = 16

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext, _read_state: Any):
        if self.read_format(ctx) == 'string':
            return self._read_binary_str(source, num_rows)
        return self._read_binary_ip(source, num_rows)

    @staticmethod
    def _read_binary_ip(source: ByteSource, num_rows: int) -> list[IPv6Address]:
        """Read IPv6 addresses in native format, always returning IPv6Address objects."""
        fast_ip_v6 = IPv6Address.__new__
        with_scope_id = '_scope_id' in IPv6Address.__slots__
        new_col = []
        app = new_col.append
        ifb = int.from_bytes
        for _ in range(num_rows):
            int_value = ifb(source.read_bytes(16), 'big')
            ipv6 = fast_ip_v6(IPv6Address)
            ipv6._ip = int_value
            if with_scope_id:
                ipv6._scope_id = None
            app(ipv6)
        return new_col

    @staticmethod
    def _read_binary_str(source: ByteSource, num_rows: int) -> list[str]:
        """Read IPv6 addresses in string format, always returning IPv6Address strings."""
        new_col = []
        app = new_col.append
        tov6 = socket.inet_ntop
        af6 = socket.AF_INET6
        for _ in range(num_rows):
            x = source.read_bytes(16)
            # Always use IPv6 string representation, even for IPv4-mapped addresses
            app(tov6(af6, x))
        return new_col

    def _write_column_binary(
        self,
        column: Union[Sequence, MutableSequence],
        dest: bytearray,
        ctx: InsertContext,
    ):
        """Write IPv6 addresses, promoting IPv4 addresses to IPv4-mapped IPv6 addresses."""
        for value in column:
            if value is None:
                dest += V6_NULL
                continue

            try:
                addr = ip_address(value)
            except ValueError as e:
                raise ValueError(
                    f"Failed to parse '{value}' as a valid IP address for column '{ctx.column_name}'"
                ) from e

            # Now handle parsed object
            if isinstance(addr, IPv6Address):
                dest += addr.packed
            elif isinstance(addr, IPv4Address):
                # We have an IPv4, but the column is IPv6 so convert to IPv4-mapped.
                dest += IPV4_V6_MASK + addr.packed

    def _active_null(self, ctx):
        if ctx.use_none:
            return None
        return '::' if self.read_format(ctx) == 'string' else V6_NULL
