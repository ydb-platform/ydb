from __future__ import print_function, unicode_literals

import sys
import functools

def debug(fn):
  def wrapper(*args, **kwargs):
    print(fn.__name__, 'called!')
    print(sorted(args), tuple(sorted(kwargs.items())))
    res = fn(*args, **kwargs)
    print(res)
    return res
  return wrapper


class cache(object):

  def __init__(self, fn):
    self.fn = fn
    self._cache = {}
    functools.update_wrapper(self, fn)

  def __call__(self, *args, **kwargs):
    key = str(args) + str(kwargs)
    if key in self._cache:
      ret = self._cache[key]
    else:
      ret = self._cache[key] = self.fn(*args, **kwargs)

    return ret

  def clear_cache(self):
    self._cache = {}


# from http://stackoverflow.com/questions/3627793/best-output-type-and-encoding-practices-for-repr-functions
def stdout_encode(u, default='UTF8'):
  encoding = sys.stdout.encoding or default
  if sys.version_info > (3, 0):
    return u.encode(encoding).decode(encoding)
  return u.encode(encoding)
