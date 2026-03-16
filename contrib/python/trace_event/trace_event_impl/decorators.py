# Copyright 2011 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.
import contextlib
import functools
import inspect
import time

import six

from . import log


@contextlib.contextmanager
def trace(name, **kwargs):
  category = "python"
  start = time.time()
  args_to_log = {key: repr(value) for key, value in six.iteritems(kwargs)}
  log.add_trace_event("B", start, category, name, args_to_log)
  try:
    yield
  finally:
    end = time.time()
    log.add_trace_event("E", end, category, name)

def traced(*args):
  def get_wrapper(func):
    if inspect.isgeneratorfunction(func):
      raise Exception("Can not trace generators.")

    category = "python"

    arg_spec = inspect.getargspec(func) if six.PY2 else inspect.getfullargspec(func)
    is_method = arg_spec.args and arg_spec.args[0] == "self"

    def arg_spec_tuple(name):
      arg_index = arg_spec.args.index(name)
      defaults_length = len(arg_spec.defaults) if arg_spec.defaults else 0
      default_index = arg_index + defaults_length - len(arg_spec.args)
      if default_index >= 0:
        default = arg_spec.defaults[default_index]
      else:
        default = None
      return (name, arg_index, default)

    args_to_log = map(arg_spec_tuple, arg_names)

    @functools.wraps(func)
    def traced_function(*args, **kwargs):
      # Everything outside traced_function is done at decoration-time.
      # Everything inside traced_function is done at run-time and must be fast.
      if not log._enabled:  # This check must be at run-time.
        return func(*args, **kwargs)

      def get_arg_value(name, index, default):
        if name in kwargs:
          return kwargs[name]
        elif index < len(args):
          return args[index]
        else:
          return default

      if is_method:
        name = "{0!s}.{1!s}".format(args[0].__class__.__name__, func.__name__)
      else:
        name = "{0!s}.{1!s}".format(func.__module__, func.__name__)

      # Be sure to repr before calling func, because the argument values may change.
      arg_values = {
          name: repr(get_arg_value(name, index, default))
          for name, index, default in args_to_log}

      start = time.time()
      log.add_trace_event("B", start, category, name, arg_values)
      try:
        return func(*args, **kwargs)
      finally:
        end = time.time()
        log.add_trace_event("E", end, category, name)
    return traced_function

  no_decorator_arguments = len(args) == 1 and callable(args[0])
  if no_decorator_arguments:
    arg_names = ()
    return get_wrapper(args[0])
  else:
    arg_names = args
    return get_wrapper
