# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from mo_future import STDOUT, STDERR

_Log = None


def get_logger():
    global _Log
    if _Log:
        return _Log
    try:
        from mo_logs import Log as _Log

        return _Log
    except Exception as e:
        _Log = PoorLogger()
        _Log.warning("`pip install mo-logs` for better logging.", cause=e)
        return _Log


class PoorLogger:
    @classmethod
    def info(cls, note, **kwargs):
        STDOUT.write(note.encode("utf8") + b"\n")

    @classmethod
    def warning(cls, note, **kwargs):
        STDOUT.write(b"WARNING: " + note.encode("utf8") + b"\n")

    @classmethod
    def error(cls, note, **kwargs):
        STDERR.write(note.encode("utf8"))
        if str("cause") in kwargs:
            raise kwargs[str("cause")]
        else:
            raise Exception(note)
