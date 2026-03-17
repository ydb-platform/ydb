# Copyright 2011 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.
import atexit
import fcntl
import json
import os
import sys
import threading
import time

import six

_lock = threading.Lock()

_enabled = False
_log_file = None

_cur_events = [] # events that have yet to be buffered

_tls = threading.local() # tls used to detect forking/etc
_atexit_regsitered_for_pid = None

_control_allowed = True

class TraceException(Exception):
  pass

def _note(msg, *args):
  pass
#  print "%i: %s" % (os.getpid(), msg)


def _safe_json_dumps(struct):
  try:
    return json.dumps(struct)
  except UnicodeDecodeError as e:
    return '<Bad encoding: %s>' % e


def _locked(fn):
  def locked_fn(*args,**kwargs):
    _lock.acquire()
    try:
      ret = fn(*args,**kwargs)
    finally:
      _lock.release()
    return ret
  return locked_fn

def _disallow_tracing_control():
  global _control_allowed
  _control_allowed = False

def trace_enable(log_file=None):
  _trace_enable(log_file)

@_locked
def _trace_enable(log_file=None):
  global _enabled
  if _enabled:
    raise TraceException("Already enabled")
  if not _control_allowed:
    raise TraceException("Tracing control not allowed in child processes.")
  _enabled = True
  global _log_file
  if log_file is None:
    if sys.argv[0] == '':
      n = 'trace_event'
    else:
      n = sys.argv[0]
    log_file = open("{0!s}.json".format(n), "ab", False)
    _note("trace_event: tracelog name is {0!s}.json".format(n))
  elif isinstance(log_file, six.string_types):
    _note("trace_event: tracelog name is {0!s}".format(log_file))
    log_file = open("{0!s}".format(log_file), "ab", False)
  elif not hasattr(log_file, 'fileno'):
    raise TraceException("Log file must be None, a string, or a file-like object with a fileno()")

  _log_file = log_file
  fcntl.lockf(_log_file.fileno(), fcntl.LOCK_EX)
  _log_file.seek(0, os.SEEK_END)

  lastpos = _log_file.tell()
  creator = lastpos == 0
  if creator:
    _note("trace_event: Opened new tracelog, lastpos=%i", lastpos)
    _log_file.write(six.b('['))

    tid = threading.current_thread().ident
    if not tid:
      tid = os.getpid()
    x = {"ph": "M", "cat": "process_argv",
         "pid": os.getpid(), "tid": threading.current_thread().ident,
         "ts": time.time(),
         "name": "process_argv", "args": {"argv": sys.argv}}
    _log_file.write(six.b("{0!s}\n".format(_safe_json_dumps(x))))
  else:
    _note("trace_event: Opened existing tracelog")
  _log_file.flush()
  fcntl.lockf(_log_file.fileno(), fcntl.LOCK_UN)

@_locked
def trace_flush():
  if _enabled:
    _flush()

@_locked
def trace_disable():
  global _enabled
  if not _control_allowed:
    raise TraceException("Tracing control not allowed in child processes.")
  if not _enabled:
    return
  _enabled = False
  _flush(close=True)

def _flush(close=False):
  global _log_file
  fcntl.lockf(_log_file.fileno(), fcntl.LOCK_EX)
  _log_file.seek(0, os.SEEK_END)
  if len(_cur_events):
    _log_file.write(six.b(",\n"))
    _log_file.write(six.b(",\n".join([_safe_json_dumps(e) for e in _cur_events])))
    del _cur_events[:]

  if close:
    # We might not be the only process writing to this logfile. So,
    # we will simply close the file rather than writign the trailing ] that
    # it technically requires. The trace viewer understands that this may happen
    # and will insert a trailing ] during loading.
    pass
  _log_file.flush()
  fcntl.lockf(_log_file.fileno(), fcntl.LOCK_UN)

  if close:
    _note("trace_event: Closed")
    _log_file.close()
    _log_file = None
  else:
    _note("trace_event: Flushed")

@_locked
def trace_is_enabled():
  return _enabled

@_locked
def add_trace_event(ph, ts, cat, name, args=None, kwargs=None):
  global _enabled
  if not _enabled:
    return
  if not hasattr(_tls, 'pid') or _tls.pid != os.getpid():
    _tls.pid = os.getpid()
    global _atexit_regsitered_for_pid
    if _tls.pid != _atexit_regsitered_for_pid:
      _atexit_regsitered_for_pid = _tls.pid
      atexit.register(_trace_disable_atexit)
      _tls.pid = os.getpid()
      del _cur_events[:] # we forked, clear the event buffer!
    tid = threading.current_thread().ident
    if not tid:
      tid = os.getpid()
    _tls.tid = tid

  if ts:
    ts = 1000000 * ts

  ev = {"ph": ph, "ts": ts, "cat": cat, "name": name,
        "pid": _tls.pid, "tid": _tls.tid,
        }
  if args:
    ev["args"] = args
  if kwargs:
    ev.update(kwargs)
  _cur_events.append(ev);

def trace_begin(name, args=None, category="python"):
  add_trace_event("B", time.time(), category, name, args)

def trace_end(name, args=None, category="python"):
  add_trace_event("E", time.time(), category, name, args)

def _trace_disable_atexit():
  trace_disable()

def trace_async_begin(name, id, args=None, category="python"):
  add_trace_event("b", time.time(), category, name, args, {"id": id})

def trace_async_instant(name, id, args=None, category="python"):
  add_trace_event("n", time.time(), category, name, args, {"id": id})

def trace_async_end(name, id, args=None, category="python"):
  add_trace_event("e", time.time(), category, name, args, {"id": id})
