#cython: unraisable_tracebacks=True
from pathlib import Path
from typing import Iterator, Tuple, Union

from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.stdint cimport uint8_t, uint32_t, uint64_t
from cpython cimport dict

from .sd_journal cimport *
from .sd_id128 cimport sd_id128_t

import os
import logging
import warnings
from datetime import datetime, timezone
from uuid import UUID
from contextlib import contextmanager
from errno import errorcode
from enum import IntEnum, IntFlag


log = logging.getLogger(__name__)


WAIT_MAX_TIME = 4294967295


class JournalEvent(IntEnum):
    NOP = SD_JOURNAL_NOP
    APPEND = SD_JOURNAL_APPEND
    INVALIDATE =  SD_JOURNAL_INVALIDATE


cdef enum MATCHER_OPERATION:
    MATCHER_OPERATION_CONJUNCTION,
    MATCHER_OPERATION_DISJUNCTION,


class MatchOperation(IntEnum):
    AND = MATCHER_OPERATION_CONJUNCTION
    OR = MATCHER_OPERATION_DISJUNCTION


cdef extern from "<poll.h>":
    cdef const int POLLIN
    cdef const int POLLOUT


class Poll(IntEnum):
    IN = POLLIN
    OUT = POLLOUT


cdef class Rule:
    cdef object _expression
    cdef object _child
    cdef object _root
    cdef object _operand

    def __init__(self, str key, str value):
        cdef str exp = "=".join((key.upper(), value))

        if '\0' in exp:
            raise ValueError("Expression must not contains \\0 character")

        self._expression = exp.encode()
        self._child = None
        self._root = self
        self._operand = MatchOperation.AND

    @property
    def expression(self) -> bytes:
        return self._expression

    @property
    def child(self) -> Rule:
        return self._child

    @child.setter
    def child(self, Rule child) -> None:
        self._child = child

    @property
    def root(self) -> Rule:
        return self._root

    @root.setter
    def root(self, Rule root) -> None:
        self._root = root

    @property
    def operand(self) -> MatchOperation:
        return self._operand

    @operand.setter
    def operand(self, uint8_t op) -> None:
        self._operand = MatchOperation(op)

    def __and__(self, Rule other) -> Rule:
        self.operand = MatchOperation.AND
        self.child = other
        other.root = self.root

        return other

    def __or__(self, Rule other) -> Rule:
        self.operand = MatchOperation.OR
        self.child = other
        other.root = self.root

        return other

    def __repr__(self) -> str:
        ret = []
        for opcode, exp in self:
            ret.append("%r" % exp.decode())
            ret.append(opcode.name)

        return 'Rule(%r)' % ' '.join(ret[:-1])

    def __iter__(self) -> Iterator[Tuple[MatchOperation, bytes]]:
        rule = self.root

        while rule is not None:
            yield rule.operand, rule.expression
            rule = rule.child


def check_error_code(int code) -> int:
    if code >= 0:
        return code

    code = -code

    if code in errorcode:
        error = errorcode[code]
        raise SystemError(os.strerror(code), error)



class JournalOpenMode(IntFlag):
    LOCAL_ONLY = SD_JOURNAL_LOCAL_ONLY
    RUNTIME_ONLY = SD_JOURNAL_RUNTIME_ONLY
    SYSTEM = SD_JOURNAL_SYSTEM
    CURRENT_USER = SD_JOURNAL_CURRENT_USER

    @classmethod
    def _missing_(cls, value) -> 'JournalOpenMode':
        if value == SD_JOURNAL_SYSTEM_ONLY:
            warnings.warn(
                "The JournalOpenMode.SYSTEM_ONLY is deprecated and the alias of "
                "JournalOpenMode.SYSTEM in the systemd library.",
                DeprecationWarning,
                stacklevel=2
            )
            return cls.SYSTEM
        raise ValueError(f"{value} is not a valid open mode")


cdef enum READER_STATE:
    READER_CLOSED,
    READER_OPENED,
    READER_LOCKED,
    READER_NULL,


cdef str _check_dir_path(object path):
    path = str(path)

    if not os.path.exists(path):
        raise OSError('Directory not found')
    elif os.path.islink(path):
        c = 0
        while not os.path.islink(path):
            path = os.path.abspath(os.readlink(path))
            c += 1
            if c > 255:
                raise OSError("Link recursive reslolution error")

        return _check_dir_path(path)
    elif not os.path.isdir(path):
        raise OSError("It's not a directory")
    else:
        path = os.path.abspath(path)

    return path


cdef str _check_file_path(object path):
    path = str(path)

    if not os.path.exists(path):
        raise OSError('File not found')
    elif os.path.islink(path):
        c = 0
        while not os.path.islink(path):
            path = os.path.abspath(os.readlink(path))
            c += 1
            if c > 255:
                raise OSError("Link recursive reslolution error")

        return _check_file_path(path)
    elif not os.path.isfile(path):
        raise OSError("It's not a regular file")
    else:
        path = os.path.abspath(path)

    return path


cdef class JournalEntry:
    cdef sd_id128_t __boot_id
    cdef object __boot_uuid
    cdef char* cursor
    cdef uint64_t monotonic_usec
    cdef uint64_t realtime_usec
    cdef dict __data
    cdef object _data
    cdef object __date

    max_message_size = 2**20

    def __cinit__(self, JournalReader reader):
        cdef const void *data
        cdef size_t length = 0

        self.__data = {}
        check_error_code(sd_journal_get_realtime_usec(reader.context, &self.realtime_usec))
        check_error_code(sd_journal_get_monotonic_usec(reader.context, &self.monotonic_usec, &self.__boot_id))
        check_error_code(sd_journal_get_cursor(reader.context, &self.cursor))

        sd_journal_restart_data(reader.context)

        while True:

            length = 0

            result = sd_journal_enumerate_data(reader.context, <const void **>&data, &length)

            if result == 0 or length == 0:
                break

            if length > self.max_message_size:
                log.warning("got message with enormous length %d", length)
                break

            value = bytes((<char*> data)[:length]).decode(errors='replace')
            if '=' not in value:
                log.warning("got unexpected %r from sd_journal_enumerate_data", value)
                break
            key, value = value.split("=", 1)

            if key in self.__data:
                if not isinstance(self.__data[key], list):
                    self.__data[key] = [self.__data[key]]
                self.__data[key].append(value)
            else:
                self.__data[key] = value

        self._data = self.__data
        self.__boot_uuid = UUID(bytes=self.__boot_id.bytes[:16])
        self.__date = datetime.fromtimestamp(self.get_realtime_sec(), timezone.utc)

    @property
    def cursor(self) -> bytes:
        return self.cursor

    cpdef double get_realtime_sec(self):
        cdef double result = self.realtime_usec / 1000000.
        return result

    cpdef double get_monotonic_sec(self):
        cdef double result = self.monotonic_usec / 1000000.
        return self.monotonic_usec / 1000000.

    cpdef uint64_t get_realtime_usec(self):
        return self.realtime_usec

    cpdef uint64_t get_monotonic_usec(self):
        return self.monotonic_usec

    @property
    def boot_id(self) -> UUID:
        return self.__boot_uuid

    @property
    def date(self) -> datetime:
        return self.__date

    @property
    def data(self) -> dict[str, str]:
        return self._data

    def __dealloc__(self):
        PyMem_Free(self.cursor)

    def __repr__(self) -> str:
        return "<JournalEntry: %r>" % self.date

    def __getitem__(self, str key) -> str:
        return self._data[key]


cdef class JournalReader:
    cdef sd_journal* context
    cdef char state
    cdef object flags

    def __init__(self):
        self.state = READER_NULL
        self.flags = None

    def open(self, flags=JournalOpenMode.CURRENT_USER) -> int:
        self.flags = JournalOpenMode(int(flags))

        with self._lock(opening=True):
            return check_error_code(sd_journal_open(&self.context, self.flags.value))

    def open_directory(self, path) -> None:
        path = _check_dir_path(path)

        with self._lock(opening=True):
            cstr = path.encode()
            check_error_code(sd_journal_open_directory(&self.context, cstr, 0))

    def open_files(self, *file_names: Union[str, Path]) -> None:
        file_names = tuple(map(_check_file_path, file_names))

        cdef size_t n = len(file_names)
        cdef const char **paths = <const char **>PyMem_Malloc((n + 1) * sizeof(char*))

        for i, s in enumerate(file_names):
            cstr = s.encode()
            paths[i] = cstr
        paths[n] = <char *>0

        try:
            with self._lock(opening=True):
                check_error_code(sd_journal_open_files(&self.context, paths, 0))
        finally:
            PyMem_Free(paths)

    @property
    def data_threshold(self) -> int:
        cdef size_t result
        cdef int rcode

        with nogil:
            rcode = sd_journal_get_data_threshold(self.context, &result)

        check_error_code(rcode)
        return result

    @data_threshold.setter
    def data_threshold(self, size) -> None:
        cdef size_t sz = size
        cdef int result

        with nogil:
            result = sd_journal_set_data_threshold(self.context, sz)

        check_error_code(result)

    @property
    def closed(self) -> bool:
        return self.state == READER_CLOSED

    @property
    def locked(self) -> bool:
        return self.state == READER_LOCKED

    @property
    def idle(self) -> bool:
        return self.state == READER_OPENED

    @contextmanager
    def _lock(self, opening=False) -> None:
        if self.closed:
            raise RuntimeError("Can't lock closed reader")
        elif self.locked:
            raise RuntimeError("Reader locked")
        elif opening and self.state != READER_NULL:
            raise RuntimeError("Can't reopen opened reader")

        self.state = READER_LOCKED

        try:
            yield
        finally:
            self.state = READER_OPENED

    def seek_head(self) -> bool:
        cdef int result

        with self._lock():
            result = sd_journal_seek_head(self.context)

        check_error_code(result)

        return True

    def seek_tail(self) -> bool:
        cdef int result

        with self._lock():
            result = sd_journal_seek_tail(self.context)

        check_error_code(result)
        # have to call previous to get the last entry, otherwise it will have an unknown cursor
        self.previous()
        return True

    def seek_monotonic_usec(self, boot_id: UUID, uint64_t usec) -> bool:
        cdef sd_id128_t cboot_id
        cdef int result

        cboot_id.bytes = boot_id.bytes

        with self._lock():
            result = sd_journal_seek_monotonic_usec(self.context, cboot_id, usec)

        check_error_code(result)
        return True

    def seek_realtime_usec(self, uint64_t usec) -> bool:
        cdef uint64_t cusec = usec
        cdef int result

        with self._lock():
            result = sd_journal_seek_realtime_usec(self.context, cusec)

        check_error_code(result)
        return True

    def seek_cursor(self, bytes cursor) -> bool:
        cdef char* ccursor = cursor
        cdef int result

        with nogil:
            result = sd_journal_seek_cursor(self.context, ccursor)

        check_error_code(result)
        return True

    cpdef wait(self, uint32_t timeout = WAIT_MAX_TIME):
        cdef uint64_t timeout_usec = timeout * 1000000
        cdef int result

        with self._lock():
            with nogil:
                result = sd_journal_wait(self.context, timeout_usec)

        return JournalEvent(check_error_code(result))

    def __iter__(self) -> Iterator[JournalEntry]:
        return iter(self.next, None)

    def next(self, uint64_t skip=0) -> JournalEntry:
        cdef int result

        with self._lock():
            if skip:
                result = sd_journal_next_skip(self.context, skip)
            else:
                result = sd_journal_next(self.context)

            if check_error_code(result) > 0:
                return JournalEntry(self)

    def skip_next(self, uint64_t skip) -> int:
        cdef int result

        with self._lock():
            result = sd_journal_next_skip(self.context, skip)

        return check_error_code(result)

    def previous(self, uint64_t skip=0) -> JournalEntry:
        cdef int result

        with self._lock():
            if skip:
                result = sd_journal_previous_skip(self.context, skip)
            else:
                result = sd_journal_previous(self.context)

            if check_error_code(result) > 0:
                return JournalEntry(self)

    def skip_previous(self, uint64_t skip) -> int:
        cdef int result

        with self._lock():
            result = sd_journal_previous_skip(self.context, skip)

        return check_error_code(result)

    def add_filter(self, Rule rule) -> None:
        cdef int result
        cdef char* exp

        with self._lock():
            for operand, exp in rule:
                result = sd_journal_add_match(self.context, exp, 0)
                check_error_code(result)

                if operand == MatchOperation.OR:
                    result = sd_journal_add_disjunction(self.context)
                    check_error_code(result)

                elif operand == MatchOperation.AND:
                    result = sd_journal_add_conjunction(self.context)
                    check_error_code(result)

                else:
                    raise ValueError('Invalid operation')

    def clear_filter(self) -> None:
        sd_journal_flush_matches(self.context)

    def __repr__(self) -> str:
        return "<Reader[%s]: %s>" % (
            self.flags, 'closed' if self.closed else 'opened'
        )

    def __dealloc__(self):
        sd_journal_close(self.context)

    @property
    def fd(self) -> int:
        return check_error_code(sd_journal_get_fd(self.context))

    @property
    def events(self) -> Poll:
        return Poll(check_error_code(sd_journal_get_events(self.context)))

    @property
    def timeout(self) -> int:
        cdef uint64_t timeout
        check_error_code(sd_journal_get_timeout(self.context, &timeout))
        return timeout

    def process_events(self) -> JournalEvent:
        return JournalEvent(check_error_code(sd_journal_process(self.context)))

    def get_catalog(self) -> Path:
        cdef int result
        cdef char* catalog
        cdef bytes bcatalog

        result = sd_journal_get_catalog(self.context, &catalog)

        length = check_error_code(result)
        bcatalog = catalog[:length]
        PyMem_Free(catalog)

        return Path(bcatalog.decode())

    def get_catalog_for_message_id(self, message_id: UUID) -> Path:
        cdef int result
        cdef char* catalog
        cdef bytes bcatalog
        cdef sd_id128_t id128

        id128.bytes = message_id.bytes

        with nogil:
            result = sd_journal_get_catalog_for_message_id(id128, &catalog)

        length = check_error_code(result)
        bcatalog = catalog[:length]
        PyMem_Free(catalog)

        return Path(bcatalog.decode())
