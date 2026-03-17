# Copyright 2011 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.
import multiprocessing
import sys

try:
  from multiprocessing.context import BaseContext
except:
  pass

from .multiprocessing_shim import ProcessShim
from . import log

if sys.platform != 'win32':
  _RealProcess = multiprocessing.context.ForkProcess
else:
  _RealProcess = multiprocessing.context.SpawnProcess

__all__ = []
_real_bootstrap = _RealProcess._bootstrap

class ProcessSubclass(_RealProcess):
  def __init__(self, shim, *args, **kwards):
    _RealProcess.__init__(self, *args, **kwards)
    self._shim = shim

  def run(self,*args,**kwargs):
    log._disallow_tracing_control()
    try:
      r = _RealProcess.run(self, *args, **kwargs)
    finally:
      if log.trace_is_enabled():
        log.trace_flush() # todo, reduce need for this...
    return r

class ProcessShim3(ProcessShim):
  def kill(self):
    self._proc.kill()

  @property
  def _popen(self):
    return self._proc._popen

  @staticmethod
  def _Popen(process_obj):
    from multiprocessing.popen_fork import Popen
    return Popen(process_obj)

  # DEVTOOLS-4281
  _bootstrap = _real_bootstrap

class ShimForkContext(BaseContext):
  _name = 'fork'
  Process = ProcessShim3

# Monkeypatch in our process replacement.
if sys.platform != 'win32' and (multiprocessing.context.ForkProcess != ProcessShim3 or multiprocessing.Process != ProcessShim3):
  if multiprocessing.context.ForkProcess != ProcessShim3:
    multiprocessing.context.ForkProcess = ProcessShim3
    multiprocessing.context.ForkContext = ShimForkContext
    multiprocessing.context.ForkContext = ShimForkContext
    fork_context = multiprocessing.context.ForkContext()
    multiprocessing.context._default_context._default_context = fork_context
  if multiprocessing.Process != ProcessShim3:
    multiprocessing.Process = ProcessShim3
elif sys.platform == 'win32' and multiprocessing.context.SpawnProcess != ProcessShim3:
  multiprocessing.context.SpawnProcess = ProcessShim3



