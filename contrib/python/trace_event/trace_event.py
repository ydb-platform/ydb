# Copyright 2011 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.
r"""Instrumentation-based profiling for Python.

trace_event allows you to hand-instrument your code with areas of interest.
When enabled, trace_event logs the start and stop times of these events to a
logfile. These resulting logfiles can be viewed with either Chrome's about:tracing
UI or with the standalone trace_event_viewer available at
  http://www.github.com/natduca/trace_event_viewer/

To use trace event, simply call trace_event_enable and start instrumenting your code:
   from trace_event import *

   if "--trace" in sys.argv:
     trace_enable("myfile.trace")

   @traced
   def foo():
     ...

   class MyFoo(object):
     @traced
     def bar(self):
       ...

trace_event records trace events to an in-memory buffer. If your application is
long running and you want to see the results of a trace before it exits, you can call
trace_flush to write any in-memory events to disk.

To help intregrating trace_event into existing codebases that dont want to add
trace_event as a dependancy, trace_event is split into an import shim
(trace_event.py) and an implementaiton (trace_event_impl/*). You can copy the
shim, trace_event.py, directly into your including codebase. If the
trace_event_impl is not found, the shim will simply noop.

trace_event is safe with regard to Python threads. Simply trace as you normally would and each
thread's timing will show up in the trace file.

Multiple processes can safely output into a single trace_event logfile. If you
fork after enabling tracing, the child process will continue outputting to the
logfile. Use of the multiprocessing module will work as well. In both cases,
however, note that disabling tracing in the parent process will not stop tracing
in the child processes.
"""

try:
  import trace_event_impl
except ImportError:
  trace_event_impl = None

def trace_can_enable():
  """
  Returns True if a trace_event_impl was found. If false,
  trace_enable will fail. Regular tracing methods, including
  trace_begin and trace_end, will simply be no-ops.
  """
  return trace_event_impl is not None

if trace_event_impl:
  import time

  from trace_event_impl import (
    TraceException,
    trace_is_enabled,
    trace_enable,
    trace_disable,
    trace_flush,
    trace_begin,
    trace_end,
    trace,
    traced,
    trace_async_begin,
    trace_async_instant,
    trace_async_end,
  )

else:
  import contextlib

  def trace_enable():
    raise TraceException("Cannot enable trace_event. No trace_event_impl module found.")

  def trace_disable():
    pass

  def trace_is_enabled():
    return False

  def trace_flush():
    pass

  def trace_begin(self, name, **kwargs):
    pass

  def trace_end(self, name):
    pass

  @contextlib.contextmanager
  def trace(name, **kwargs):
    yield

  def traced(fn):
    return fn


trace_enable.__doc__ = """Enables tracing.

  Once enabled, the enabled bit propagates to forked processes and
  multiprocessing subprocesses. Regular child processes, e.g. those created via
  os.system/popen, or subprocess.Popen instances, will not get traced. You can,
  however, enable tracing on those subprocess manually.

  Trace files are multiprocess safe, so you can have multiple processes
  outputting to the same tracelog at once.

  log_file can be one of three things:

    None: a logfile is opened based on sys[argv], namely
          "./" + sys.argv[0] + ".json"

    string: a logfile of the given name is opened.

    file-like object: the fileno() is is used. The underlying file descriptor
                      must support fcntl.lockf() operations.
  """

trace_disable.__doc__ =   """Disables tracing, if enabled.

  Will not disable tracing on any existing child proceses that were forked
  from this process. You must disable them yourself.
  """

trace_flush.__doc__ = """Flushes any currently-recorded trace data to disk.

  trace_event records traces into an in-memory buffer for efficiency. Flushing
  is only done at process exit or when this method is called.
  """

trace_is_enabled.__doc__ = """Returns whether tracing is enabled.
  """

trace_begin.__doc__ = """Records the beginning of an event of the given name.

  The building block for performance tracing. A typical example is:
     from trace_event import *
     def something_heavy():
        trace_begin("something_heavy")

        trace_begin("read")
        try:
          lines = open().readlines()
        finally:
          trace_end("read")

        trace_begin("parse")
        try:
          parse(lines)
        finally:
          trace_end("parse")

        trace_end("something_heavy")

  Note that a trace_end call must be issued for every trace_begin call. When
  tracing around blocks that might throw exceptions, you should use the trace function,
  or a try-finally pattern to ensure that the trace_end method is called.

  See the documentation for the @traced decorator for a simpler way to instrument
  functions and methods.
  """

trace_end.__doc__ = """Records the end of an event of the given name.

  See the documentation for trace_begin for more information.

  Make sure to issue a trace_end for every trace_begin issued. Failure to pair
  these calls will lead to bizarrely tall looking traces in the
  trace_event_viewer UI.
  """

trace.__doc__ = """Traces a block of code using a with statement.

  Example usage:
    from trace_event import *
    def something_heavy(lines):
      with trace("parse_lines", lines=lines):
        parse(lines)

  If tracing an entire function call, prefer the @traced decorator.
  """

traced.__doc__ = """
  Traces the provided function, using the function name for the actual generated event.

  Prefer this decorator over the explicit trace_begin and trace_end functions
  whenever you are tracing the start and stop of a function. It automatically
  issues trace_begin/end events, even when the wrapped function throws.

  You can also pass the function's argument names to traced, and the argument
  values will be added to the trace. Example usage:
    from trace_event import *
    @traced("url")
    def send_request(url):
      urllib2.urlopen(url).read()
  """
