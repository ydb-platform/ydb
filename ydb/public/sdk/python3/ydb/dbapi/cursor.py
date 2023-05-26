from __future__ import absolute_import, unicode_literals

import collections
import datetime
import itertools
import logging

import ydb
from .errors import DatabaseError


LOGGER = logging.getLogger(__name__)


STR_QUOTE_MAP = (
    ("\\", "\\\\"),
    ("'", r"\'"),
    ("\0", r"\x00"),
    # To re-check: \b \f \r \n \t
)


def render_str(value):
    for r_from, r_to in STR_QUOTE_MAP:
        value = value.replace(r_from, r_to)
    return "'" + value + "'"


def render_date(value):
    return "Date({})".format(render_str(value.isoformat()))


def render_datetime(value):
    # TODO: is there a better solution for this?
    return "DateTime::MakeDatetime(DateTime::ParseIso8601({}))".format(render_str(value.isoformat()))


def render(value):
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return render_str(value)
    if isinstance(value, datetime.datetime):
        return render_datetime(value)
    if isinstance(value, datetime.date):
        return render_date(value)
    return repr(value)


def render_sql(sql, parameters):
    if not parameters:
        return sql

    assert sql.count("?") == len(parameters), "num of placeholders != num of params"

    quoted_params = [render(param) for param in parameters]
    quoted_params += [""]
    sql_pieces = sql.split("?")
    assert len(sql_pieces) == len(quoted_params)
    return "".join(piece for pair in zip(sql_pieces, quoted_params) for piece in pair if piece)


def named_result_for(column_names):
    # TODO fix: this doesn't allow columns names starting with underscore, e.g. `select 1 as _a`.
    return collections.namedtuple("NamedResult", column_names)


def _get_column_type(type_obj):
    return str(type_obj)


def get_column_type(type_obj):
    return _get_column_type(ydb.convert.type_to_native(type_obj))


class Cursor(object):
    def __init__(self, connection):
        self.connection = connection
        self.description = []
        self.arraysize = 1
        self.logger = LOGGER
        self.rows = None
        self._rows_prefetched = None

    def execute(self, sql, parameters=None):
        fsql = render_sql(sql, parameters)
        self.logger.debug("execute sql: %s", fsql)
        try:
            chunks = self.connection.driver.table_client.scan_query(fsql)
        except ydb.Error as e:
            raise DatabaseError(e.message, e.issues, e.status)

        self.description = []

        rows = self._rows_iterable(chunks)
        # Prefetch the description:
        try:
            first_row = next(rows)
        except StopIteration:
            pass
        else:
            rows = itertools.chain((first_row,), rows)
        if self.rows is not None:
            rows = itertools.chain(self.rows, rows)

        self.rows = rows

    def _rows_iterable(self, chunks_iterable):
        description = None
        try:
            for chunk in chunks_iterable:
                if description is None and len(chunk.result_set.rows) > 0:
                    description = [
                        (
                            col.name,
                            get_column_type(col.type),
                            None,
                            None,
                            None,
                            None,
                            None,
                        )
                        for col in chunk.result_set.columns
                    ]
                    self.description = description
                for row in chunk.result_set.rows:
                    # returns tuple to be compatible with SqlAlchemy and because
                    #  of this PEP to return a sequence: https://www.python.org/dev/peps/pep-0249/#fetchmany
                    yield row[::]
        except ydb.Error as e:
            raise DatabaseError(e.message, e.issues, e.status)

    def _ensure_prefetched(self):
        if self.rows is not None and self._rows_prefetched is None:
            self._rows_prefetched = list(self.rows)
            self.rows = iter(self._rows_prefetched)
        return self._rows_prefetched

    def executemany(self, sql, seq_of_parameters):
        for parameters in seq_of_parameters:
            self.execute(sql, parameters)

    def executescript(self, script):
        return self.execute(script)

    def fetchone(self):
        if self.rows is None:
            return None
        try:
            return next(self.rows)
        except StopIteration:
            return None

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize

        return list(itertools.islice(self.rows, size))

    def fetchall(self):
        return list(self.rows)

    def nextset(self):
        self.fetchall()

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, column=None):
        pass

    def close(self):
        self.rows = None
        self._rows_prefetched = None

    @property
    def rowcount(self):
        return len(self._ensure_prefetched())
