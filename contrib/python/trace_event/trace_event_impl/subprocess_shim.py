import os
import shlex
import subprocess
import threading
import traceback

from . import log

_RealPopen = subprocess.Popen

class _Options(object):
  _trace_stack = False
  _trace_env = False

def trace_stack_enable():
  _Options._trace_stack = True

def trace_stack_disable():
  _Options._trace_stack = False

def trace_env_enable():
  _Options._trace_env = True

def trace_env_disable():
  _Options._trace_env = False


class PopenShim(subprocess.Popen):

  def __init__(self, cmd, *args, **kwargs):
    self._finished = None
    if log.trace_is_enabled():
      cmdline = shlex.split(cmd) if kwargs.get('shell') else cmd
      self._name = cmdline[0].split('/')[-1]
      self._finished = False
      self._lock = threading.Lock()
      extra = {'cmdline': cmdline}
      if _Options._trace_env:
        extra['env'] = sorted(["{}={}".format(k, v) for k, v in os.environ.items()])
      if _Options._trace_stack:
        extra['stack'] = ''.join(traceback.format_stack()[:-1])
      log.trace_begin(self._name, extra)
    _RealPopen.__init__(self, cmd, *args, **kwargs)

  def wait(self, **kwargs):
    res = _RealPopen.wait(self, **kwargs)
    if self._finished is False:
      with self._lock:
        if self._finished is False:
          # available in subprocess32
          if kwargs.get('timeout'):
            raise Exception("timeout arg for wait() not supported")

          self._finished = True
          log.trace_end(self._name)
          log.trace_flush()
    return res

  def poll(self):
    res = _RealPopen.poll(self)
    if res is None:
      return None
    if self._finished is False:
      with self._lock:
        if self._finished is False:
          self._finished = True
          log.trace_end(self._name)
          log.trace_flush()
    return res


if subprocess.Popen != PopenShim:
  subprocess.Popen = PopenShim
