"""Integrate the shelve module with flask."""
import os
import shelve
import fcntl
import time

import flask
from flask import _request_ctx_stack


LOCK_POLL_SECS = 0.02


def init_app(app):
    """Initialize the flask app.

    Before calling this function, the `SHELVE_FILENAME` config
    value must be set, e.g.::

        app.config['SHELVE_FILENAME'] = 'my_db_file'

    This function will associate an object with the current flask
    app, which is accessible using the ``get_shelve`` function.

    """
    if 'SHELVE_FILENAME' not in app.config:
        raise RuntimeError("SHELVE_FILENAME is required in the "
                           "app configuration.")
    app.config.setdefault('SHELVE_PROTOCOL', None)
    app.config.setdefault('SHELVE_WRITEBACK', False)
    app.config.setdefault('SHELVE_LOCKFILE',
                          app.config['SHELVE_FILENAME'] + '.lock')
    app.extensions['shelve'] = _Shelve(app)


def get_shelve(mode='c'):
    """Get an instance of shelve.

    This function will return a ``shelve.Shelf`` instance.
    It does this by finding the shelve object associated with
    the current flask app (using ``flask.current_app``).

    """
    return flask.current_app.extensions['shelve'].open_db(mode=mode)


class _Shelve(object):
    def __init__(self, app):
        self.app = app
        self.app.teardown_request(self.close_db)
        self._lock = _FileLock(app.config['SHELVE_LOCKFILE'])
        # "touch" the db file so that view functions can
        # open the db with mode='r' and not have to worry
        # about the db not existing.
        self._open_db('c').close()

    def open_db(self, mode='r'):
        if self._is_write_mode(mode):
            fileno = self._lock.acquire_write_lock()
            writer = self._open_db(mode)
            writer.fileno = fileno
            _request_ctx_stack.top.shelve_writer = writer
            return writer
        else:
            fileno = self._lock.acquire_read_lock()
            reader = self._open_db(mode)
            reader.fileno = fileno
            _request_ctx_stack.top.shelve_reader = reader
            return reader

    def _is_write_mode(self, mode):
        return mode in ('c', 'w', 'n')

    def _open_db(self, flag):
        cfg = self.app.config
        return shelve.open(
            filename=cfg['SHELVE_FILENAME'],
            flag=flag,
            protocol=cfg['SHELVE_PROTOCOL'],
            writeback=cfg['SHELVE_WRITEBACK']
        )

    def close_db(self, ignore_arg):
        top = _request_ctx_stack.top
        if hasattr(top, 'shelve_writer'):
            writer = top.shelve_writer
            writer.close()
            self._lock.release_write_lock(writer.fileno)
        elif hasattr(top, 'shelve_reader'):
            reader = top.shelve_reader
            reader.close()
            self._lock.release_read_lock(reader.fileno)


class _FileLock(object):
    def __init__(self, lockfile):
        self._filename = lockfile
        self._waiting_for_write_lock = False
        self._waiting_for_read_lock = False
        # Touch the file so we can acquire read locks.
        open(self._filename, 'w').close()

    def acquire_read_lock(self):
        # Keep in mind that we're operating in a multithreaded environment.
        # If someone is waiting on a write lock, we can essentially keep them
        # waiting indefinitely if we keep on handing on read locks (there
        # can be multiple read locks issued at any time).  So instead, if
        # someone is waiting on a write lock, we poll until they've
        # acquired the lock and then block on aquiring a lock.
        while self._waiting_for_write_lock:
            time.sleep(LOCK_POLL_SECS)
        fileno = os.open(self._filename, os.O_RDWR)
        self._waiting_for_read_lock = True
        fcntl.flock(fileno, fcntl.LOCK_SH)
        self._waiting_for_read_lock = False
        return fileno

    def acquire_write_lock(self):
        fileno = os.open(self._filename, os.O_RDWR)
        self._waiting_for_write_lock = True
        fcntl.flock(fileno, fcntl.LOCK_EX)
        self._waiting_for_write_lock = False
        return fileno

    def release_read_lock(self, fileno):
        fcntl.flock(fileno, fcntl.LOCK_UN)
        os.close(fileno)

    def release_write_lock(self, fileno):
        fcntl.flock(fileno, fcntl.LOCK_UN)
        os.close(fileno)
