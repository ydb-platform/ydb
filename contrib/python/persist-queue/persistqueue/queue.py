"""A thread-safe disk based persistent queue in Python."""
import logging
import os
import tempfile
import threading
from time import time as _time
import persistqueue.serializers.pickle
from persistqueue.exceptions import Empty, Full
from typing import Any, Optional, Tuple, BinaryIO

log = logging.getLogger(__name__)


def _truncate(fn: str, length: int) -> None:
    """Truncate the file to a specified length."""
    with open(fn, 'ab+') as f:
        f.truncate(length)


def atomic_rename(src: str, dst: str) -> None:
    """Atomically rename a file from src to dst."""
    os.replace(src, dst)


class Queue:
    """Thread-safe, persistent queue."""

    def __init__(
        self,
        path: str,
        maxsize: int = 0,
        chunksize: int = 100,
        tempdir: Optional[str] = None,
        serializer: Any = persistqueue.serializers.pickle,
        autosave: bool = False
    ) -> None:
        """Create a persistent queue object on a given path.

        The argument path indicates a directory where enqueued data should be
        persisted. If the directory doesn't exist, one will be created.
        If maxsize is <= 0, the queue size is infinite. The optional argument
        chunksize indicates how many entries should exist in each chunk file on
        disk.

        The tempdir parameter indicates where temporary files should be stored.
        The tempdir has to be located on the same disk as the enqueued data in
        order to obtain atomic operations.

        The serializer parameter controls how enqueued data is serialized. It
        must have methods dump(value, fp) and load(fp). The dump method must
        serialize value and write it to fp, and may be called for multiple
        values with the same fp. The load method must deserialize and return
        one value from fp, and may be called multiple times with the same fp
        to read multiple values.

        The autosave parameter controls when data removed from the queue is
        persisted. By default, (disabled), the change is only persisted when
        task_done() is called. If autosave is enabled, data is persisted
        immediately when get() is called. Adding data to the queue with put()
        will always persist immediately regardless of this setting.
        """
        log.debug('Initializing File based Queue with path {}'.format(path))
        self.path = path
        self.chunksize = chunksize
        self.tempdir = tempdir
        self.maxsize = maxsize
        self.serializer = serializer
        self.autosave = autosave
        self._init(maxsize)
        if self.tempdir:
            if os.stat(self.path).st_dev != os.stat(self.tempdir).st_dev:
                raise ValueError(
                    "tempdir has to be located on same path filesystem")
        else:
            fd, tempdir = tempfile.mkstemp()
            if os.stat(self.path).st_dev != os.stat(tempdir).st_dev:
                self.tempdir = self.path
                log.warning("Default tempdir '%(dft_dir)s' is not on the "
                            "same filesystem with queue path '%(queue_path)s'"
                            ",defaulting to '%(new_path)s'." % {
                                "dft_dir": tempdir,
                                "queue_path": self.path,
                                "new_path": self.tempdir})
            os.close(fd)
            os.remove(tempdir)
        self.info = self._loadinfo()
        # truncate head in case it contains garbage
        hnum, hcnt, hoffset = self.info['head']
        headfn = self._qfile(hnum)
        if os.path.exists(headfn):
            if hoffset < os.path.getsize(headfn):
                _truncate(headfn, hoffset)
        # let the head file open
        self.headf = self._openchunk(hnum, 'ab+')
        tnum, _, toffset = self.info['tail']
        self.tailf = self._openchunk(tnum)
        self.tailf.seek(toffset)
        # update unfinished tasks with the current number of enqueued tasks
        self.unfinished_tasks = self.info['size']
        self.update_info = True

    def _init(self, maxsize: int) -> None:
        self.mutex = threading.Lock()
        self.not_empty = threading.Condition(self.mutex)
        self.not_full = threading.Condition(self.mutex)
        self.all_tasks_done = threading.Condition(
            self.mutex)
        if not os.path.exists(self.path):
            os.makedirs(self.path)

    def join(self) -> None:
        with self.all_tasks_done:
            while self.unfinished_tasks:
                self.all_tasks_done.wait()

    def qsize(self) -> int:
        with self.mutex:
            return self._qsize()

    def _qsize(self) -> int:
        return self.info['size']

    def empty(self) -> bool:
        return self.qsize() == 0

    def full(self) -> bool:
        return self.maxsize > 0 and self.qsize() == self.maxsize

    def put(self, item: Any, block: bool = True,
            timeout: Optional[float] = None) -> None:
        self.not_full.acquire()
        try:
            if self.maxsize > 0:
                if not block:
                    if self._qsize() == self.maxsize:
                        raise Full
                elif timeout is None:
                    while self._qsize() == self.maxsize:
                        self.not_full.wait()
                elif timeout < 0:
                    raise ValueError("'timeout' must be a non-negative number")
                else:
                    endtime = _time() + timeout
                    while self._qsize() == self.maxsize:
                        remaining = endtime - _time()
                        if remaining <= 0.0:
                            raise Full
                        self.not_full.wait(remaining)
            self._put(item)
            self.unfinished_tasks += 1
            self.not_empty.notify()
        finally:
            self.not_full.release()

    def _put(self, item: Any) -> None:
        self.serializer.dump(item, self.headf)
        self.headf.flush()
        hnum, hpos, _ = self.info['head']
        hpos += 1
        if hpos == self.info['chunksize']:
            hpos = 0
            hnum += 1
            os.fsync(self.headf.fileno())
            self.headf.close()
            self.headf = self._openchunk(hnum, 'ab+')
        self.info['size'] += 1
        self.info['head'] = [hnum, hpos, self.headf.tell()]
        self._saveinfo()

    def put_nowait(self, item: Any) -> None:
        self.put(item, False)

    def get(self, block: bool = True, timeout: Optional[float] = None) -> Any:
        self.not_empty.acquire()
        try:
            if not block:
                if not self._qsize():
                    raise Empty
            elif timeout is None:
                while not self._qsize():
                    self.not_empty.wait()
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                endtime = _time() + timeout
                while not self._qsize():
                    remaining = endtime - _time()
                    if remaining <= 0.0:
                        raise Empty
                    self.not_empty.wait(remaining)
            item = self._get()
            self.not_full.notify()
            return item
        finally:
            self.not_empty.release()

    def get_nowait(self) -> Any:
        return self.get(False)

    def _get(self) -> Any:
        tnum, tcnt, toffset = self.info['tail']
        hnum, hcnt, _ = self.info['head']
        if [tnum, tcnt] >= [hnum, hcnt]:
            return None
        data = self.serializer.load(self.tailf)
        toffset = self.tailf.tell()
        tcnt += 1
        if tcnt == self.info['chunksize'] and tnum <= hnum:
            tcnt = toffset = 0
            tnum += 1
            self.tailf.close()
            self.tailf = self._openchunk(tnum)
        self.info['size'] -= 1
        self.info['tail'] = [tnum, tcnt, toffset]
        if self.autosave:
            self._saveinfo()
            self.update_info = False
        else:
            self.update_info = True
        return data

    def task_done(self) -> None:
        with self.all_tasks_done:
            unfinished = self.unfinished_tasks - 1
            if unfinished <= 0:
                if unfinished < 0:
                    raise ValueError("task_done() called too many times.")
                self.all_tasks_done.notify_all()
            self.unfinished_tasks = unfinished
            self._task_done()

    def _task_done(self) -> None:
        if self.autosave:
            return
        if self.update_info:
            self._saveinfo()
            self.update_info = False

    def _openchunk(self, number: int, mode: str = 'rb') -> BinaryIO:
        return open(self._qfile(number), mode)

    def _loadinfo(self) -> dict:
        infopath = self._infopath()
        if os.path.exists(infopath):
            with open(infopath, 'rb') as f:
                info = self.serializer.load(f)
        else:
            info = {
                'chunksize': self.chunksize,
                'size': 0,
                'tail': [0, 0, 0],
                'head': [0, 0, 0],
            }
        return info

    def _gettempfile(self) -> Tuple[int, str]:
        if self.tempdir:
            return tempfile.mkstemp(dir=self.tempdir)
        else:
            return tempfile.mkstemp()

    def _saveinfo(self) -> None:
        tmpfd, tmpfn = self._gettempfile()
        with os.fdopen(tmpfd, "wb") as tmpfo:
            self.serializer.dump(self.info, tmpfo)
        atomic_rename(tmpfn, self._infopath())
        self._clear_tail_file()

    def _clear_tail_file(self) -> None:
        """Remove the tail files whose items were already get."""
        tnum, _, _ = self.info['tail']
        while tnum >= 1:
            tnum -= 1
            path = self._qfile(tnum)
            if os.path.exists(path):
                os.remove(path)
            else:
                break

    def _qfile(self, number: int) -> str:
        return os.path.join(self.path, 'q%05d' % number)

    def _infopath(self) -> str:
        return os.path.join(self.path, 'info')

    def __del__(self) -> None:
        """Handles the removal of queue."""
        for to_close in self.headf, self.tailf:
            if to_close and not to_close.closed:
                to_close.close()
