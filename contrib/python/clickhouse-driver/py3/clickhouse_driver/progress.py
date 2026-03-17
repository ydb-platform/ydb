from . import defines
from .varint import read_varint


class Progress(object):
    def __init__(self):
        self.rows = 0
        self.bytes = 0
        self.total_rows = 0  # total_rows_to_read
        self.total_bytes = 0  # total_bytes_to_read
        self.written_rows = 0
        self.written_bytes = 0
        self.elapsed_ns = 0

        super(Progress, self).__init__()

    def read(self, server_info, fin):
        self.rows = read_varint(fin)
        self.bytes = read_varint(fin)

        revision = server_info.used_revision
        if revision >= defines.DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS:
            self.total_rows = read_varint(fin)

        if revision >= defines. \
                DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS:
            self.total_bytes = read_varint(fin)

        if revision >= defines.DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO:
            self.written_rows = read_varint(fin)
            self.written_bytes = read_varint(fin)

        if revision >= defines. \
                DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS:
            self.elapsed_ns = read_varint(fin)

    def increment(self, another_progress):
        self.rows += another_progress.rows
        self.bytes += another_progress.bytes
        self.total_rows += another_progress.total_rows
        self.total_bytes += another_progress.total_bytes
        self.written_rows += another_progress.written_rows
        self.written_bytes += another_progress.written_bytes
        self.elapsed_ns += another_progress.elapsed_ns
