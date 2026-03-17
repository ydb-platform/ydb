"""Utilities for handling command line arguments.

StoreInModule:
  Enables emulating a gflags-esque API (flag affects global value), but one
  doesn't necessarily need to use flags to set values.

  Example usage:
    DEFAULT_VALUE = 0
    ARG_PARSER = argv.ModuleParser()
    ARG_PARSER.add_argument(
        '--override-value', action=argv.StoreInModule,
        default=DEFAULT_VALUE, target='%s.DEFAULT_VALUE' % __name__)

  Then in an entry point (main() function), use that parser as a parent:
    parser = argparse.ArgumentParser(parents=[other_module.ARG_PARSER])
    parser.parse_args()
"""

import argparse
import sys


def ModuleParser():
  return argparse.ArgumentParser(add_help=False)


class StoreInModule(argparse.Action):

  def __init__(self, *args, **kwargs):
    self._tgt_mod, self._tgt_attr = kwargs.pop('target').rsplit('.', 1)
    proxy_cls = kwargs.pop('proxy', None)
    if proxy_cls is not None:
      self._proxy = proxy_cls(*args, **kwargs)
    super(StoreInModule, self).__init__(*args, **kwargs)

  def __call__(self, parser, namespace, values, option_string=None):
    if hasattr(self, '_proxy'):
      values = self._proxy(parser, namespace, values)
    setattr(self._resolve_module(), self._tgt_attr, values)
    # self.val = values

  def _resolve_module(self):
    if '.' in self._tgt_mod:
      base, mod = self._tgt_mod.rsplit('.', 1)
      __import__(base, fromlist=[mod])
      return sys.modules[self._tgt_mod]
    else:
      return __import__(self._tgt_mod)


class _StoreValueInModule(StoreInModule):
  """Stores a value in a module level variable when set."""

  def __init__(self, const, *args, **kwargs):
    kwargs.update(nargs=0, const=const)
    super(_StoreValueInModule, self).__init__(*args, **kwargs)

  def __call__(self, parser, namespace, values, option_string=None):
    del values  # Unused.
    super(_StoreValueInModule, self).__call__(
        parser, namespace, self.const, option_string=option_string)


class StoreTrueInModule(_StoreValueInModule):
  """Stores True in a module level variable when set."""

  def __init__(self, *args, **kwargs):
    super(StoreTrueInModule, self).__init__(True, *args, **kwargs)


class StoreFalseInModule(_StoreValueInModule):
  """Stores False in module level variable when set."""

  def __init__(self, *args, **kwargs):
    super(StoreFalseInModule, self).__init__(False, *args, **kwargs)


class StoreRepsInModule(StoreInModule):
  """Store a count of number of times the flag was repeated in a module."""

  def __init__(self, *args, **kwargs):
    kwargs.update(nargs=0, const=None)
    super(StoreRepsInModule, self).__init__(*args, **kwargs)

  def __call__(self, parser, namespace, values, option_string=None):
    del values  # Unused.
    old_count = getattr(self._resolve_module(), self._tgt_attr)
    if old_count is None:
      super(StoreRepsInModule, self).__call__(
              parser, namespace, 0, option_string=option_string)
    else:
      super(StoreRepsInModule, self).__call__(
              parser, namespace, old_count + 1, option_string=option_string)
