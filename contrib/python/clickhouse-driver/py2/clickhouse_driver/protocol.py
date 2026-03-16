
class ClientPacketTypes(object):
    """
    Packet types that client transmits
    """
    # Name, version, revision, default DB
    HELLO = 0

    # Query id, query settings, stage up to which the query must be executed,
    # whether the compression must be used, query text
    # (without data for INSERTs).
    QUERY = 1

    # A block of data (compressed or not).
    DATA = 2

    # Cancel the query execution.
    CANCEL = 3

    # Check that connection to the server is alive.
    PING = 4

    # Check status of tables on the server.
    TABLES_STATUS_REQUEST = 5

    _types_str = [
        'Hello', 'Query', 'Data', 'Cancel', 'Ping', 'TablesStatusRequest'
    ]

    @classmethod
    def to_str(cls, packet):
        return 'Unknown packet' if packet > 5 else cls._types_str[packet]


class ServerPacketTypes(object):
    """
    Packet types that server transmits.
    """
    # Name, version, revision.
    HELLO = 0

    # A block of data (compressed or not).
    DATA = 1

    # The exception during query execution.
    EXCEPTION = 2

    # Query execution progress: rows read, bytes read.
    PROGRESS = 3

    # Ping response
    PONG = 4

    # All packets were transmitted
    END_OF_STREAM = 5

    # Packet with profiling info.
    PROFILE_INFO = 6

    # A block with totals (compressed or not).
    TOTALS = 7

    # A block with minimums and maximums (compressed or not).
    EXTREMES = 8

    # A response to TablesStatus request.
    TABLES_STATUS_RESPONSE = 9

    # System logs of the query execution
    LOG = 10

    # Columns' description for default values calculation
    TABLE_COLUMNS = 11

    _types_str = [
        'Hello', 'Data', 'Exception', 'Progress', 'Pong', 'EndOfStream',
        'ProfileInfo', 'Totals', 'Extremes', 'TablesStatusResponse', 'Log',
        'TableColumns'
    ]

    @classmethod
    def to_str(cls, packet):
        return 'Unknown packet' if packet > 11 else cls._types_str[packet]

    @classmethod
    def strings_in_message(cls, packet):
        if packet == cls.TABLE_COLUMNS:
            return 2
        return 0


class Compression(object):
    DISABLED = 0
    ENABLED = 1


class CompressionMethod(object):
    LZ4 = 1
    LZ4HC = 2
    ZSTD = 3


class CompressionMethodByte(object):
    LZ4 = 0x82
    ZSTD = 0x90
