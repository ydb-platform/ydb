"""
Implements an SQL database writer and reader for storing CAN messages.

.. note:: The database schema is given in the documentation of the loggers.
"""

import logging
import sqlite3
import threading
import time
from collections.abc import Generator, Iterator
from typing import Any

from typing_extensions import TypeAlias

from can.listener import BufferedReader
from can.message import Message

from ..typechecking import StringPathLike
from .generic import MessageReader, MessageWriter

log = logging.getLogger("can.io.sqlite")

_MessageTuple: TypeAlias = "tuple[float, int, bool, bool, bool, int, memoryview[int]]"


class SqliteReader(MessageReader):
    """
    Reads recorded CAN messages from a simple SQL database.

    This class can be iterated over or used to fetch all messages in the
    database with :meth:`~SqliteReader.read_all`.

    Calling :func:`len` on this object might not run in constant time.

    :attr str table_name: the name of the database table used for storing the messages

    .. note:: The database schema is given in the documentation of the loggers.
    """

    def __init__(
        self,
        file: StringPathLike,
        table_name: str = "messages",
        **kwargs: Any,
    ) -> None:
        """
        :param file: a `str`  path like object that points
                     to the database file to use
        :param str table_name: the name of the table to look for the messages

        .. warning:: In contrary to all other readers/writers the Sqlite handlers
                     do not accept file-like objects as the `file` parameter.
                     It also runs in ``append=True`` mode all the time.
        """
        self._conn = sqlite3.connect(file)
        self._cursor = self._conn.cursor()
        self.table_name = table_name

    def __iter__(self) -> Generator[Message, None, None]:
        for frame_data in self._cursor.execute(f"SELECT * FROM {self.table_name}"):
            yield SqliteReader._assemble_message(frame_data)

    @staticmethod
    def _assemble_message(frame_data: _MessageTuple) -> Message:
        timestamp, can_id, is_extended, is_remote, is_error, dlc, data = frame_data
        return Message(
            timestamp=timestamp,
            is_remote_frame=bool(is_remote),
            is_extended_id=bool(is_extended),
            is_error_frame=bool(is_error),
            arbitration_id=can_id,
            dlc=dlc,
            data=data,
        )

    def __len__(self) -> int:
        # this might not run in constant time
        result = self._cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
        return int(result.fetchone()[0])

    def read_all(self) -> Iterator[Message]:
        """Fetches all messages in the database.

        :rtype: Generator[can.Message]
        """
        result = self._cursor.execute(f"SELECT * FROM {self.table_name}").fetchall()
        return (SqliteReader._assemble_message(frame) for frame in result)

    def stop(self) -> None:
        """Closes the connection to the database."""
        self._conn.close()


class SqliteWriter(MessageWriter, BufferedReader):
    """Logs received CAN data to a simple SQL database.

    The sqlite database may already exist, otherwise it will
    be created when the first message arrives.

    Messages are internally buffered and written to the SQL file in a background
    thread. Ensures that all messages that are added before calling :meth:`~can.SqliteWriter.stop()`
    are actually written to the database after that call returns. Thus, calling
    :meth:`~can.SqliteWriter.stop()` may take a while.

    :attr str table_name: the name of the database table used for storing the messages
    :attr int num_frames: the number of frames actually written to the database, this
                          excludes messages that are still buffered
    :attr float last_write: the last time a message war actually written to the database,
                            as given by ``time.time()``

    .. note::

        When the listener's :meth:`~SqliteWriter.stop` method is called the
        thread writing to the database will continue to receive and internally
        buffer messages if they continue to arrive before the
        :attr:`~SqliteWriter.GET_MESSAGE_TIMEOUT`.

        If the :attr:`~SqliteWriter.GET_MESSAGE_TIMEOUT` expires before a message
        is received, the internal buffer is written out to the database file.

        However if the bus is still saturated with messages, the Listener
        will continue receiving until the :attr:`~can.SqliteWriter.MAX_TIME_BETWEEN_WRITES`
        timeout is reached or more than
        :attr:`~can.SqliteWriter.MAX_BUFFER_SIZE_BEFORE_WRITES` messages are buffered.

    .. note:: The database schema is given in the documentation of the loggers.

    """

    GET_MESSAGE_TIMEOUT = 0.25
    """Number of seconds to wait for messages from internal queue"""

    MAX_TIME_BETWEEN_WRITES = 5.0
    """Maximum number of seconds to wait between writes to the database"""

    MAX_BUFFER_SIZE_BEFORE_WRITES = 500
    """Maximum number of messages to buffer before writing to the database"""

    def __init__(
        self,
        file: StringPathLike,
        table_name: str = "messages",
        **kwargs: Any,
    ) -> None:
        """
        :param file: a `str` or path like object that points
                     to the database file to use
        :param str table_name: the name of the table to store messages in

        .. warning:: In contrary to all other readers/writers the Sqlite handlers
                     do not accept file-like objects as the `file` parameter.
        """
        if kwargs.get("append", False):
            raise ValueError(
                f"The append argument should not be used in "
                f"conjunction with the {self.__class__.__name__}."
            )
        BufferedReader.__init__(self)
        self.table_name = table_name
        self._db_filename = file
        self._stop_running_event = threading.Event()
        self._writer_thread = threading.Thread(target=self._db_writer_thread)
        self._writer_thread.start()
        self.num_frames = 0
        self.last_write = time.time()
        self._insert_template = (
            f"INSERT INTO {self.table_name} VALUES (?, ?, ?, ?, ?, ?, ?)"
        )

    @staticmethod
    def _create_db(file: StringPathLike, table_name: str) -> sqlite3.Connection:
        """Creates a new databae or opens a connection to an existing one.

        .. note::
            You can't share sqlite3 connections between threads (by default)
            hence we setup the db here. It has the upside of running async.
        """
        log.debug("Creating sqlite database")
        conn = sqlite3.connect(file)

        # create table structure
        conn.cursor().execute(
            f"""CREATE TABLE IF NOT EXISTS {table_name}
            (
              ts REAL,
              arbitration_id INTEGER,
              extended INTEGER,
              remote INTEGER,
              error INTEGER,
              dlc INTEGER,
              data BLOB
            )"""
        )
        conn.commit()

        return conn

    def _db_writer_thread(self) -> None:
        conn = SqliteWriter._create_db(self._db_filename, self.table_name)

        try:
            while True:
                messages: list[_MessageTuple] = []  # reset buffer

                msg = self.get_message(self.GET_MESSAGE_TIMEOUT)
                while msg is not None:
                    # log.debug("SqliteWriter: buffering message")

                    messages.append(
                        (
                            msg.timestamp,
                            msg.arbitration_id,
                            msg.is_extended_id,
                            msg.is_remote_frame,
                            msg.is_error_frame,
                            msg.dlc,
                            memoryview(msg.data),
                        )
                    )

                    if (
                        time.time() - self.last_write > self.MAX_TIME_BETWEEN_WRITES
                        or len(messages) > self.MAX_BUFFER_SIZE_BEFORE_WRITES
                    ):
                        break

                    # just go on
                    msg = self.get_message(self.GET_MESSAGE_TIMEOUT)

                count = len(messages)
                if count > 0:
                    with conn:
                        # log.debug("Writing %d frames to db", count)
                        conn.executemany(self._insert_template, messages)
                        conn.commit()  # make the changes visible to the entire database
                    self.num_frames += count
                    self.last_write = time.time()

                # check if we are still supposed to run and go back up if yes
                if self._stop_running_event.is_set():
                    break

        finally:
            conn.close()
            log.info("Stopped sqlite writer after writing %d messages", self.num_frames)

    def stop(self) -> None:
        """Stops the reader an writes all remaining messages to the database. Thus, this
        might take a while and block.
        """
        BufferedReader.stop(self)
        self._stop_running_event.set()
        self._writer_thread.join()
