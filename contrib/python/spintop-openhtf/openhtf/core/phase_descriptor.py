# Copyright 2014 Google Inc. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Modified by Tack Verification, 2020


"""Phases in OpenHTF.

Phases in OpenHTF are distinct steps in a test.  Each phase is an instance
of PhaseDescriptor class.

"""
import inspect
import pdb
import sys

import enum
import mutablerecords

import openhtf
from openhtf import util
from openhtf.core import test_record
import openhtf.plugs
from openhtf.util import data, functions

import six


class PhaseWrapError(Exception):
  """Error with phase wrapping."""


# Result of a phase.
#
# These values can be returned by a test phase to control what the framework
# does after the phase.
PhaseResult = enum.Enum('PhaseResult', [   # pylint: disable=invalid-name
    # Causes the framework to process the phase measurement outcomes and execute
    # the next phase.
    'CONTINUE',
    # Causes the framework to mark the phase with a fail outcome and execute the
    # next phase.
    'FAIL_AND_CONTINUE',
    # Causes the framework to execute the same phase again, ignoring the
    # measurement outcomes for this instance. If returned more than the phase's
    # repeat_limit option, this will be treated as a STOP.
    'REPEAT',
    # Causes the framework to ignore the measurement outcomes and execute the
    # next phase.  The phase is still logged, unlike with run_if.
    'SKIP',
    # Causes the framework to stop executing, indicating a failure.
    'STOP'
])

PhaseType = enum.Enum('PhaseType', [
  'MAIN',
  'SETUP',
  'TEARDOWN'
])


class PhaseOptions(mutablerecords.Record('PhaseOptions', [], {
    'name': None, 'timeout_s': None, 'run_if': None, 'requires_state': None,
    'repeat_limit': None, 'run_under_pdb': False})):
  """Options used to override default test phase behaviors.

  Attributes:
      name: Override for the name of the phase. Can be formatted in several
          different ways as defined in util.format_string.
      timeout_s: Timeout to use for the phase, in seconds.
      run_if: Callback that decides whether to run the phase or not; if not run,
          the phase will also not be logged. Optionally, this callback may take
          a single parameter: the test.state dictionnary. This allows dynamic
          test selection based on variables in the user defined state.
      requires_state: If True, pass the whole TestState into the first argument,
          otherwise only the TestApi will be passed in.  This is useful if a
          phase needs to wrap another phase for some reason, as
          PhaseDescriptors can only be invoked with a TestState instance.
      repeat_limit:  Maximum number of repeats.  None indicates a phase will
          be repeated infinitely as long as PhaseResult.REPEAT is returned.
      run_under_pdb: If True, run the phase under the Python Debugger (pdb).  When
          setting this option, increase the phase timeout as well because the
          timeout will still apply when under the debugger.

  Example Usages:
      @PhaseOptions(timeout_s=1)
      def PhaseFunc(test):
          pass

      @PhaseOptions(name='Phase({port})')
      def PhaseFunc(test, port, other_info):
          pass
  """

  def format_strings(self, **kwargs):
    """String substitution of name."""
    return mutablerecords.CopyRecord(
        self, name=util.format_string(self.name, kwargs))

  def update(self, **kwargs):
    for key, value in six.iteritems(kwargs):
      if key not in self.__slots__:
        raise AttributeError('Type %s does not have attribute %s' % (
            type(self).__name__, key))
      setattr(self, key, value)

  def __call__(self, phase_func):
    phase = PhaseDescriptor.wrap_or_copy(phase_func)
    for attr in self.__slots__:
      value = getattr(self, attr)
      if value is not None:
        setattr(phase.options, attr, value)
    return phase
  
  def call_run_if(self, user_defined_state):
    if self.run_if:
      arg_info = functions.getargspec(self.run_if)
      if arg_info.args:
        return self.run_if(user_defined_state)
      else:
        return self.run_if()
    else:
      return True
    

TestPhase = PhaseOptions


class PhaseDescriptor(mutablerecords.Record(
    'PhaseDescriptor', ['func'],
    {'options': PhaseOptions, 'plugs': list, 'measurements': list,
     'extra_kwargs': dict, 'code_info': test_record.CodeInfo.uncaptured(),
     'run_options': dict})):
  """Phase function and related information.

  Attributes:
    func: Function to be called (with TestApi as first argument).
    options: PhaseOptions instance.
    plugs: List of PhasePlug instances.
    measurements: List of Measurement objects.
    extra_kwargs: Keyword arguments that will be passed to the function.
    code_info: Info about the source code of func.
    type: PhaseType instance.
  """

  @classmethod
  def wrap_or_copy(cls, func, **options):
    """Return a new PhaseDescriptor from the given function or instance.

    We want to return a new copy so that you can reuse a phase with different
    options, plugs, measurements, etc.

    Args:
      func: A phase function or PhaseDescriptor instance.
      **options: Options to update on the result.

    Raises:
      PhaseWrapError: if func is a openhtf.PhaseGroup.

    Returns:
      A new PhaseDescriptor object.
    """
    if isinstance(func, openhtf.PhaseGroup):
      raise PhaseWrapError('Cannot wrap PhaseGroup <%s> as a phase.' % (
          func.name or 'Unnamed'))
    if isinstance(func, cls):
      # We want to copy so that a phase can be reused with different options
      # or kwargs.  See with_args() below for more details.
      retval = mutablerecords.CopyRecord(func)
    else:
      retval = cls(func)
    retval.options.update(**options)
    return retval
  
  def as_type(self, phase_type):
    self.run_options['type'] = phase_type
    return self
  
  def as_depth(self, depth):
    self.run_options['depth'] = depth
    return self

  def _asdict(self):
    asdict = {
        k: data.convert_to_base_types(getattr(self, k), ignore_keys=('cls',))
        for k in self.optional_attributes
    }
    asdict.update(name=self.name, doc=self.doc)
    return asdict

  @property
  def name(self):
    return self.options.name or self.func.__name__

  @property
  def doc(self):
    return self.func.__doc__

  def with_known_args(self, **kwargs):
    """Send only known keyword-arguments to the phase when called."""
    argspec = inspect.getfullargspec(self.func)
    stored = {}
    for key, arg in six.iteritems(kwargs):
      if argspec.varkw or key in argspec.args or key in argspec.kwonlyargs:
        stored[key] = arg
    if stored:
      return self.with_args(**stored)
    return self

  def with_args(self, **kwargs):
    """Send these keyword-arguments to the phase when called."""
    # Make a copy so we can have multiple of the same phase with different args
    # in the same test.
    new_info = mutablerecords.CopyRecord(self)
    new_info.options = new_info.options.format_strings(**kwargs)
    new_info.extra_kwargs.update(kwargs)
    new_info.measurements = [m.with_args(**kwargs) for m in self.measurements]
    return new_info

  def with_known_plugs(self, **subplugs):
    """Substitute only known plugs for placeholders for this phase."""
    return self._apply_with_plugs(subplugs, error_on_unknown=False)

  def with_plugs(self, **subplugs):
    """Substitute plugs for placeholders for this phase, error on unknowns."""
    return self._apply_with_plugs(subplugs, error_on_unknown=True)

  def _apply_with_plugs(self, subplugs, error_on_unknown):
    """Substitute plugs for placeholders for this phase.

    Args:
      subplugs: dict of plug name to plug class, plug classes to replace.
      error_on_unknown: bool, if True, then error when an unknown plug name is
          provided.

    Raises:
      openhtf.plugs.InvalidPlugError if for one of the plug names one of the
      following is true:
        - error_on_unknown is True and the plug name is not registered.
        - The new plug subclass is not a subclass of the original.
        - The original plug class is not a placeholder or automatic placeholder.

    Returns:
      PhaseDescriptor with updated plugs.
    """
    plugs_by_name = {plug.name: plug for plug in self.plugs}
    new_plugs = dict(plugs_by_name)

    for name, sub_class in six.iteritems(subplugs):
      original_plug = plugs_by_name.get(name)
      accept_substitute = True
      if original_plug is None:
        if not error_on_unknown:
          continue
        accept_substitute = False
      elif isinstance(original_plug.cls, openhtf.plugs.PlugPlaceholder):
        accept_substitute = issubclass(sub_class, original_plug.cls.base_class)
      else:
        # Check __dict__ to see if the attribute is explicitly defined in the
        # class, rather than being defined in a parent class.
        accept_substitute = ('auto_placeholder' in original_plug.cls.__dict__
                             and original_plug.cls.auto_placeholder
                             and issubclass(sub_class, original_plug.cls))

      if not accept_substitute:
        raise openhtf.plugs.InvalidPlugError(
            'Could not find valid placeholder for substitute plug %s '
            'required for phase %s' % (name, self.name))
      new_plugs[name] = mutablerecords.CopyRecord(original_plug, cls=sub_class)

    return mutablerecords.CopyRecord(
        self,
        plugs=list(new_plugs.values()),
        options=self.options.format_strings(**subplugs),
        measurements=[m.with_args(**subplugs) for m in self.measurements])

  def __call__(self, test_state):
    """Invoke this Phase, passing in the appropriate args.

    By default, an openhtf.TestApi is passed as the first positional arg, but if
    the 'requires_state' option is set, then a test_state.TestState is passed
    instead. If no positional args are expected, then neither is passed in. In
    any case, keyword args are passed in based on extra_kwargs, set via
    with_args(), combined with plugs (plugs override extra_kwargs).

    Args:
      test_state: test_state.TestState for the currently executing Test.

    Returns:
      The return value from calling the underlying function.
    """
    kwargs = dict(self.extra_kwargs)
    kwargs.update(test_state.plug_manager.provide_plugs(
        (plug.name, plug.cls) for plug in self.plugs if plug.update_kwargs))

    arg_info = functions.getargspec(self.func)
    keywords = arg_info.keywords
    # Pass in test_api if the phase takes *args, or **kwargs with at least 1
    # positional, or more positional args than we have keyword args.
    if arg_info.varargs or (keywords and len(arg_info.args) >= 1) or (
        len(arg_info.args) > len(kwargs)):
      args = []
      if self.options.requires_state:
        args.append(test_state)
      else:
        args.append(test_state.test_api)

      if self.options.run_under_pdb:
        return pdb.runcall(self.func, *args, **kwargs)
      else:
        return self.func(*args, **kwargs)
    if self.options.run_under_pdb:
      return pdb.runcall(self.func, **kwargs)
    else:
      return self.func(**kwargs)
