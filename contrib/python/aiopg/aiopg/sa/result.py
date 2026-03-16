import weakref
from collections.abc import Mapping, Sequence

from sqlalchemy.sql import expression, sqltypes

from . import exc
from .utils import SQLALCHEMY_VERSION

if SQLALCHEMY_VERSION >= ["1", "4"]:
    from sqlalchemy.util import string_or_unprintable
else:
    from sqlalchemy.sql.expression import (
        _string_or_unprintable as string_or_unprintable,
    )


class RowProxy(Mapping):
    __slots__ = ("_result_proxy", "_row", "_processors", "_keymap")

    def __init__(self, result_proxy, row, processors, keymap):
        """RowProxy objects are constructed by ResultProxy objects."""
        self._result_proxy = result_proxy
        self._row = row
        self._processors = processors
        self._keymap = keymap

    def __iter__(self):
        return iter(self._result_proxy.keys)

    def __len__(self):
        return len(self._row)

    def __getitem__(self, key):
        try:
            processor, obj, index = self._keymap[key]
        except KeyError:
            processor, obj, index = self._result_proxy._key_fallback(key)
        # Do we need slicing at all? RowProxy now is Mapping not Sequence
        # except TypeError:
        #     if isinstance(key, slice):
        #         l = []
        #         for processor, value in zip(self._processors[key],
        #                                     self._row[key]):
        #             if processor is None:
        #                 l.append(value)
        #             else:
        #                 l.append(processor(value))
        #         return tuple(l)
        #     else:
        #         raise
        if index is None:
            raise exc.InvalidRequestError(
                f"Ambiguous column name {key!r} in result set! "
                f"try 'use_labels' option on select statement."
            )
        if processor is not None:
            return processor(self._row[index])
        else:
            return self._row[index]

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as e:
            raise AttributeError(e.args[0])

    def __contains__(self, key):
        return self._result_proxy._has_key(self._row, key)

    __hash__ = None

    def __eq__(self, other):
        if isinstance(other, RowProxy):
            return self.as_tuple() == other.as_tuple()
        elif isinstance(other, Sequence):
            return self.as_tuple() == other
        else:
            return NotImplemented

    def __ne__(self, other):
        return not self == other

    def as_tuple(self):
        return tuple(self[k] for k in self)

    def __repr__(self):
        return repr(self.as_tuple())


class ResultMetaData:
    """Handle cursor.description, applying additional info from an execution
    context."""

    def __init__(self, result_proxy, cursor_description):
        self._processors = processors = []

        map_type, map_column_name = self.result_map(result_proxy._result_map)

        # We do not strictly need to store the processor in the key mapping,
        # though it is faster in the Python version (probably because of the
        # saved attribute lookup self._processors)
        self._keymap = keymap = {}
        self.keys = []
        dialect = result_proxy.dialect

        # `dbapi_type_map` property removed in SQLAlchemy 1.2+.
        # Usage of `getattr` only needed for backward compatibility with
        # older versions of SQLAlchemy.
        typemap = getattr(dialect, "dbapi_type_map", {})

        assert (
            dialect.case_sensitive
        ), "Doesn't support case insensitive database connection"

        # high precedence key values.
        primary_keymap = {}

        assert (
            not dialect.description_encoding
        ), "psycopg in py3k should not use this"

        for i, rec in enumerate(cursor_description):
            colname = rec[0]
            coltype = rec[1]

            # PostgreSQL doesn't require this.
            # if dialect.requires_name_normalize:
            #     colname = dialect.normalize_name(colname)

            name, obj, type_ = (
                map_column_name.get(colname, colname),
                None,
                map_type.get(colname, typemap.get(coltype, sqltypes.NULLTYPE)),
            )

            processor = type_._cached_result_processor(dialect, coltype)

            processors.append(processor)
            rec = (processor, obj, i)

            # indexes as keys. This is only needed for the Python version of
            # RowProxy (the C version uses a faster path for integer indexes).
            primary_keymap[i] = rec

            # populate primary keymap, looking for conflicts.
            if primary_keymap.setdefault(name, rec) != rec:
                # place a record that doesn't have the "index" - this
                # is interpreted later as an AmbiguousColumnError,
                # but only when actually accessed.   Columns
                # colliding by name is not a problem if those names
                # aren't used; integer access is always
                # unambiguous.
                primary_keymap[name] = rec = (None, obj, None)

            self.keys.append(name)
            if obj:
                for o in obj:
                    keymap[o] = rec
                    # technically we should be doing this but we
                    # are saving on callcounts by not doing so.
                    # if keymap.setdefault(o, rec) is not rec:
                    #    keymap[o] = (None, obj, None)

        # overwrite keymap values with those of the
        # high precedence keymap.
        keymap.update(primary_keymap)

    def result_map(self, data_map):
        data_map = data_map or {}
        map_type = {}
        map_column_name = {}
        for elem in data_map:
            name = elem[0]
            priority_name = getattr(elem[2][0], "key", None) or name
            map_type[name] = elem[3]  # type column
            map_column_name[name] = priority_name

        return map_type, map_column_name

    def _key_fallback(self, key, raiseerr=True):
        map = self._keymap
        result = None
        if isinstance(key, str):
            result = map.get(key)
        # fallback for targeting a ColumnElement to a textual expression
        # this is a rare use case which only occurs when matching text()
        # or colummn('name') constructs to ColumnElements, or after a
        # pickle/unpickle roundtrip
        elif isinstance(key, expression.ColumnElement):
            if key._label and key._label in map:
                result = map[key._label]
            elif hasattr(key, "key") and key.key in map:
                # match is only on name.
                result = map[key.key]
            # search extra hard to make sure this
            # isn't a column/label name overlap.
            # this check isn't currently available if the row
            # was unpickled.
            if result is not None and result[1] is not None:
                for obj in result[1]:
                    if key._compare_name_for_result(obj):
                        break
                else:
                    result = None
        if result is None:
            if raiseerr:
                raise exc.NoSuchColumnError(
                    f"Could not locate column in row for column "
                    f"{string_or_unprintable(key)!r}"
                )
            else:
                return None
        else:
            map[key] = result
        return result

    def _has_key(self, row, key):
        if key in self._keymap:
            return True
        else:
            return self._key_fallback(key, False) is not None


class ResultProxy:
    """Wraps a DB-API cursor object to provide easier access to row columns.

    Individual columns may be accessed by their integer position,
    case-insensitive column name, or by sqlalchemy schema.Column
    object. e.g.:

      row = fetchone()

      col1 = row[0]    # access via integer position

      col2 = row['col2']   # access via name

      col3 = row[mytable.c.mycol] # access via Column object.

    ResultProxy also handles post-processing of result column
    data using sqlalchemy TypeEngine objects, which are referenced from
    the originating SQL statement that produced this result set.
    """

    def __init__(self, connection, cursor, dialect, result_map=None):
        self._dialect = dialect
        self._result_map = result_map
        self._cursor = cursor
        self._connection = connection
        self._rowcount = cursor.rowcount
        self._metadata = None
        self._weak = None
        self._init_metadata()

    @property
    def dialect(self):
        """SQLAlchemy dialect."""
        return self._dialect

    @property
    def cursor(self):
        return self._cursor

    def keys(self):
        """Return the current set of string keys for rows."""
        if self._metadata:
            return tuple(self._metadata.keys)
        else:
            return ()

    @property
    def rowcount(self):
        """Return the 'rowcount' for this result.

        The 'rowcount' reports the number of rows *matched*
        by the WHERE criterion of an UPDATE or DELETE statement.

        .. note::

           Notes regarding .rowcount:


           * This attribute returns the number of rows *matched*,
             which is not necessarily the same as the number of rows
             that were actually *modified* - an UPDATE statement, for example,
             may have no net change on a given row if the SET values
             given are the same as those present in the row already.
             Such a row would be matched but not modified.

           * .rowcount is *only* useful in conjunction
             with an UPDATE or DELETE statement.  Contrary to what the Python
             DBAPI says, it does *not* return the
             number of rows available from the results of a SELECT statement
             as DBAPIs cannot support this functionality when rows are
             unbuffered.

           * Statements that use RETURNING may not return a correct
             rowcount.
        """
        return self._rowcount

    def _init_metadata(self):
        cursor_description = self.cursor.description
        if cursor_description is not None:
            self._metadata = ResultMetaData(self, cursor_description)
            self._weak = weakref.ref(self, lambda _: self.close())
        else:
            self.close()

    @property
    def returns_rows(self):
        """True if this ResultProxy returns rows.

        I.e. if it is legal to call the methods .fetchone(),
        .fetchmany() and .fetchall()`.
        """
        return self._metadata is not None

    @property
    def closed(self):
        if self._cursor is None:
            return True

        return bool(self._cursor.closed)

    def close(self):
        """Close this ResultProxy.

        Closes the underlying DBAPI cursor corresponding to the execution.

        Note that any data cached within this ResultProxy is still available.
        For some types of results, this may include buffered rows.

        If this ResultProxy was generated from an implicit execution,
        the underlying Connection will also be closed (returns the
        underlying DBAPI connection to the connection pool.)

        This method is called automatically when:

        * all result rows are exhausted using the fetchXXX() methods.
        * cursor.description is None.
        """

        if self._cursor is None:
            return

        if not self._cursor.closed:
            self._cursor.close()

        self._cursor = None
        self._weak = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        ret = await self.fetchone()
        if ret is not None:
            return ret
        raise StopAsyncIteration

    def _non_result(self):
        if self._metadata is None:
            raise exc.ResourceClosedError(
                "This result object does not return rows. "
                "It has been closed automatically."
            )
        else:
            raise exc.ResourceClosedError("This result object is closed.")

    def _process_rows(self, rows):
        process_row = RowProxy
        metadata = self._metadata
        keymap = metadata._keymap
        processors = metadata._processors
        return [process_row(metadata, row, processors, keymap) for row in rows]

    async def fetchall(self):
        """Fetch all rows, just like DB-API cursor.fetchall()."""
        try:
            rows = await self.cursor.fetchall()
        except AttributeError:
            self._non_result()
        else:
            res = self._process_rows(rows)
            self.close()
            return res

    async def fetchone(self):
        """Fetch one row, just like DB-API cursor.fetchone().

        If a row is present, the cursor remains open after this is called.
        Else the cursor is automatically closed and None is returned.
        """
        try:
            row = await self.cursor.fetchone()
        except AttributeError:
            self._non_result()
        else:
            if row is not None:
                return self._process_rows([row])[0]
            else:
                self.close()
                return None

    async def fetchmany(self, size=None):
        """Fetch many rows, just like DB-API
        cursor.fetchmany(size=cursor.arraysize).

        If rows are present, the cursor remains open after this is called.
        Else the cursor is automatically closed and an empty list is returned.
        """
        try:
            if size is None:
                rows = await self.cursor.fetchmany()
            else:
                rows = await self.cursor.fetchmany(size)
        except AttributeError:
            self._non_result()
        else:
            res = self._process_rows(rows)
            if len(res) == 0:
                self.close()
            return res

    async def first(self):
        """Fetch the first row and then close the result set unconditionally.

        Returns None if no row is present.
        """
        if self._metadata is None:
            self._non_result()
        try:
            return await self.fetchone()
        finally:
            self.close()

    async def scalar(self):
        """Fetch the first column of the first row, and close the result set.

        Returns None if no row is present.
        """
        row = await self.first()
        if row is not None:
            return row[0]
        else:
            return None
