# This module is taken from https://gist.github.com/ionrock/3015700

# A file lock implementation that tries to avoid platform specific
# issues. It is inspired by a whole bunch of different implementations
# listed below.

#  - https://bitbucket.org/jaraco/yg.lockfile/src/6c448dcbf6e5/yg/lockfile/__init__.py
#  - http://svn.zope.org/zc.lockfile/trunk/src/zc/lockfile/__init__.py?rev=121133&view=markup
#  - http://stackoverflow.com/questions/489861/locking-a-file-in-python
#  - http://www.evanfosmark.com/2009/01/cross-platform-file-locking-support-in-python/
#  - http://packages.python.org/lockfile/lockfile.html

# There are some tests below and a blog posting conceptually the
# problems I wanted to try and solve. The tests reflect these ideas.

#  - http://ionrock.wordpress.com/2012/06/28/file-locking-in-python/

# I'm not advocating using this package. But if you do happen to try it
# out and have suggestions please let me know.

import os
import platform
import tempfile
import time

from post_office.settings import get_lock_file_name


class FileLocked(Exception):
    pass


class FileLock:
    def __init__(self, lock_filename, timeout=None, force=False):
        self.lock_filename = '%s.lock' % lock_filename
        self.timeout = timeout
        self.force = force
        self._pid = str(os.getpid())
        # Store pid in a file in the same directory as desired lockname
        self.pid_filename = os.path.join(os.path.dirname(self.lock_filename), self._pid) + '.lock'

    def get_lock_pid(self):
        try:
            return int(open(self.lock_filename).read())
        except OSError:
            # If we can't read symbolic link, there are two possibilities:
            # 1. The symbolic link is dead (point to non existing file)
            # 2. Symbolic link is not there
            # In either case, we can safely release the lock
            self.release()
        except ValueError:
            # most likely an empty or otherwise invalid lock file
            self.release()

    def valid_lock(self):
        """
        See if the lock exists and is left over from an old process.
        """

        lock_pid = self.get_lock_pid()

        # If we're unable to get lock_pid
        if lock_pid is None:
            return False

        # this is our process
        if self._pid == lock_pid:
            return True

        # it is/was another process
        # see if it is running
        try:
            os.kill(lock_pid, 0)
        except OSError:
            self.release()
            return False

        # it is running
        return True

    def is_locked(self, force=False):
        # We aren't locked
        if not self.valid_lock():
            return False

        # We are locked, but we want to force it without waiting
        if not self.timeout:
            if self.force:
                self.release()
                return False
            else:
                # We're not waiting or forcing the lock
                raise FileLocked()

        # Locked, but want to wait for an unlock
        interval = 0.1
        intervals = int(self.timeout / interval)

        while intervals:
            if self.valid_lock():
                intervals -= 1
                time.sleep(interval)
                # print('stopping %s' % intervals)
            else:
                return True

        # check one last time
        if self.valid_lock():
            if self.force:
                self.release()
            else:
                # still locked :(
                raise FileLocked()

    def acquire(self):
        """Create a pid filename and create a symlink (the actual lock file)
        across platforms that points to it. Symlink is used because it's an
        atomic operation across platforms.
        """

        pid_file = os.open(self.pid_filename, os.O_CREAT | os.O_EXCL | os.O_RDWR)
        os.write(pid_file, str(os.getpid()).encode('utf-8'))
        os.close(pid_file)

        if hasattr(os, 'symlink') and platform.system() != 'Windows':
            os.symlink(self.pid_filename, self.lock_filename)
        else:
            # Windows platforms doesn't support symlinks, at least not through the os API
            self.lock_filename = self.pid_filename

    def release(self):
        """Try to delete the lock files. Doesn't matter if we fail"""
        if self.lock_filename != self.pid_filename:
            try:
                os.unlink(self.lock_filename)
            except OSError:
                pass

        try:
            os.remove(self.pid_filename)
        except OSError:
            pass

    def __enter__(self):
        if not self.is_locked():
            self.acquire()
        return self

    def __exit__(self, type, value, traceback):
        self.release()


default_lockfile = os.path.join(tempfile.gettempdir(), get_lock_file_name())
