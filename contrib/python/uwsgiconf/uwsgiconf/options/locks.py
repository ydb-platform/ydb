from pathlib import Path
from typing import Union

from ..base import OptionsGroup


class Locks(OptionsGroup):
    """Locks.

    * http://uwsgi.readthedocs.io/en/latest/Locks.html

    """

    def set_basic_params(self, *, count: int = None, thunder_lock: bool = None, lock_engine: str = None):
        """
        :param count: Create the specified number of shared locks.

        :param thunder_lock: Serialize accept() usage (if possible)
            Could improve performance on Linux with robust pthread mutexes.

            http://uwsgi.readthedocs.io/en/latest/articles/SerializingAccept.html

        :param lock_engine: Set the lock engine.

            Example:
                - ipcsem

        """
        self._set('thunder-lock', thunder_lock, cast=bool)
        self._set('lock-engine', lock_engine)
        self._set('locks', count)

        return self._section

    def set_ipcsem_params(self, *, ftok: str = None, persistent: bool = None):
        """Sets ipcsem lock engine params.

        :param ftok: Set the ipcsem key via ftok() for avoiding duplicates.

        :param persistent: Do not remove ipcsem's on shutdown.

        """
        self._set('ftok', ftok)
        self._set('persistent-ipcsem', persistent, cast=bool)

        return self._section

    def lock_file(self, fpath: Union[str, Path], *, after_setup: bool = False, wait: bool = False):
        """Locks the specified file.

        :param fpath: File path.

        :param after_setup:
            True  - after logging/daemon setup
            False - before starting

        :param wait:
            True  - wait if locked
            False - exit if locked

        """
        command = 'flock-wait' if wait else 'flock'

        if after_setup:
            command = f'{command}2'

        self._set(command, str(fpath))

        return self._section
