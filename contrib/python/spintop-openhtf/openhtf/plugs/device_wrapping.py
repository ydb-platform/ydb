# Copyright 2017 Google Inc. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""OpenHTF base plugs for thinly wrapping existing device abstractions.

Sometimes you already have a Python interface to a device or instrument; you
just need to put that interface in plug form to get it into your test phase.
Device-wrapping plugs are your friends in such times.
"""

import functools

import openhtf
import six


def short_repr(obj, max_len=40):
  """Returns a short, term-friendly string representation of the object.

  Args:
    obj: An object for which to return a string representation.
    max_len: Maximum length of the returned string. Longer reprs will be turned
        into a brief descriptive string giving the type and length of obj.
  """
  obj_repr = repr(obj)
  if len(obj_repr) <= max_len:
    return obj_repr
  return '<{} of length {}>'.format(type(obj).__name__, len(obj_repr))


class DeviceWrappingPlug(openhtf.plugs.BasePlug):
  """A base plug for wrapping existing device abstractions.

  Attribute access is delegated to the _device attribute, which is normally set
  by passing some device instance to the constructor of this base class.
  Subclasses can use the @conf.inject_positional_args decorator on their
  constructors to get any configuration needed to construct the inner device
  instance.

  Example:
    class BleSnifferPlug(DeviceWrappingPlug):
      ...
      @conf.inject_positional_args
      def __init__(self, ble_sniffer_host, ble_sniffer_port):
        super(BleSnifferPlug, self).__init__(
            ble_sniffer.BleSniffer(ble_sniffer_host, ble_sniffer_port))
        ...

  Because not all third-party device and instrument control libraries can be
  counted on to do sufficient logging, some debug logging is provided here in
  the plug layer to show which attributes were called and with what arguments.

  Args:
    device: The device to wrap; must not be None.

  Raises:
    openhtf.plugs.InvalidPlugError: The _device attribute has the value None
        when attribute access is attempted.
  """

  verbose = True  # overwrite on subclass to disable logging_wrapper.

  def __init__(self, device):
    super(DeviceWrappingPlug, self).__init__()
    self._device = device
    if hasattr(self._device, 'tearDown') and self.uses_base_tear_down():
      self.logger.warning('Wrapped device %s implements a tearDown method, '
                          'but using the no-op BasePlug tearDown method.',
                          type(self._device))

  def __getattr__(self, attr):
    if self._device is None:
      raise openhtf.plugs.InvalidPlugError(
          'DeviceWrappingPlug instances must set the _device attribute.')
    if attr == 'as_base_types':
      return super(DeviceWrappingPlug, self).__getattr__(attr)

    attribute = getattr(self._device, attr)

    if not self.verbose or not callable(attribute):
      return attribute

    # Attribute callable; return a wrapper that logs calls with args and kwargs.
    functools.wraps(attribute, assigned=('__name__', '__doc__'))
    def logging_wrapper(*args, **kwargs):
      """Wraps a callable with a logging statement."""
      args_strings = tuple(short_repr(arg) for arg in args)
      kwargs_strings = tuple(
          ('%s=%s' % (key, short_repr(val))
           for key, val in six.iteritems(kwargs))
      )
      log_line = '%s calling "%s" on device.' % (type(self).__name__, attr)
      if args_strings or kwargs_strings:
        log_line += ' Args: \n  %s' % (', '.join(args_strings + kwargs_strings))
      self.logger.debug(log_line)
      return attribute(*args, **kwargs)

    return logging_wrapper

