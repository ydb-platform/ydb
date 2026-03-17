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


"""The plugs module provides boilerplate for accessing hardware.

Most tests require interaction with external hardware.  This module provides
framework support for such interfaces, allowing for automatic setup and
teardown of the objects.

Test phases can be decorated as using Plug objects, which then get passed
into the test via parameters.  Plugs are all instantiated at the
beginning of a test, and all plugs' tearDown() methods are called at the
end of a test.  It's up to the Plug implementation to do any sort of
is-ready check.

A plug may be made "frontend-aware", allowing it, in conjunction with the
Station API, to update any frontends each time the plug's state changes. See
FrontendAwareBasePlug for more info.

Example implementation of a plug:

  from openhtf import plugs

  class ExamplePlug(plugs.BasePlug):
    '''A Plug that does nothing.'''

    def __init__(self):
      print 'Instantiating %s!' % type(self).__name__

    def DoSomething(self):
      print '%s doing something!' % type(self).__name__

    def tearDown(self):
      # This method is optional.  If implemented, it will be called at the end
      # of the test.
      print 'Tearing down %s!' % type(self).__name__

Example usage of the above plug:

  from openhtf import plugs
  from my_custom_plugs_package import example

  @plugs.plug(example=example.ExamplePlug)
  def TestPhase(test, example):
    print 'Test phase started!'
    example.DoSomething()
    print 'Test phase done!'

Putting all this together, when the test is run (with just that phase), you
would see the output (with other framework logs before and after):

  Instantiating ExamplePlug!
  Test phase started!
  ExamplePlug doing something!
  Test phase done!
  Tearing down ExamplePlug!

Plugs will often need to use configuration values.  The recommended way
of doing this is with the conf.inject_positional_args decorator:

  from openhtf import plugs
  from openhtf.util import conf

  conf.declare('my_config_key', default_value='my_config_value')

  class ExamplePlug(plugs.BasePlug):
    '''A plug that requires some configuration.'''

    @conf.inject_positional_args
    def __init__(self, my_config_key)
      self._my_config = my_config_key

Note that Plug constructors shouldn't take any other arguments; the
framework won't pass any, so you'll get a TypeError.  Any values that are only
known at run time must be either passed into other methods or set via explicit
setter methods.  See openhtf/conf.py for details, but with the above
example, you would also need a configuration .yaml file with something like:

  my_config_key: my_config_value

This will result in the ExamplePlug being constructed with
self._my_config having a value of 'my_config_value'.
"""

import collections
import logging

import mutablerecords

from openhtf import util
import openhtf.core.phase_descriptor
from openhtf.util import classproperty
from openhtf.util import conf
from openhtf.util import data
from openhtf.util import logs
from openhtf.util import threads
import six


_LOG = logging.getLogger(__name__)


conf.declare('plug_teardown_timeout_s', default_value=0, description=
             'Timeout (in seconds) for each plug tearDown function if > 0; '
             'otherwise, will wait an unlimited time.')


PlugDescriptor = collections.namedtuple('PlugDescriptor', ['mro'])  # pylint: disable=invalid-name

# Placeholder for a specific plug to be provided before test execution.
#
# Use the with_plugs() method to provide the plug before test execution. The
# with_plugs() method checks to make sure the substitute plug is a subclass of
# the PlugPlaceholder's base_class.
PlugPlaceholder = collections.namedtuple('PlugPlaceholder', ['base_class'])  # pylint: disable=invalid-name


class PhasePlug(mutablerecords.Record(
    'PhasePlug', ['name', 'cls'], {'update_kwargs': True})):
  """Information about the use of a plug in a phase."""


class PlugOverrideError(Exception):
  """Raised when a plug would be overridden by a kwarg."""


class DuplicatePlugError(Exception):
  """Raised when the same plug is required multiple times on a phase."""


class InvalidPlugError(Exception):
  """Raised when a plug declaration or requested name is invalid."""


class BasePlug(object):
  """All plug types must subclass this type.

  Attributes:
    logger: This attribute will be set by the PlugManager (and as such it
        doesn't appear here), and is the same logger as passed into test
        phases via TestApi.
  """
  # Override this to True in subclasses to support remote Plug access.
  enable_remote = False
  # Allow explicitly disabling remote access to specific attributes.
  disable_remote_attrs = set()
  # Override this to True in subclasses to support using with_plugs with this
  # plug without needing to use placeholder.  This will only affect the classes
  # that explicitly define this; subclasses do not share the declaration.
  auto_placeholder = False
  # Default logger to be used only in __init__ of subclasses.
  # This is overwritten both on the class and the instance so don't store
  # a copy of it anywhere.
  logger = _LOG

  @classproperty
  def placeholder(cls):
    """Returns a PlugPlaceholder for the calling class."""
    return PlugPlaceholder(cls)

  def _asdict(self):
    """Return a dictionary representation of this plug's state.

    This is called repeatedly during phase execution on any plugs that are in
    use by that phase.  The result is reported via the Station API by the
    PlugManager (if the Station API is enabled, which is the default).

    Note that this method is called in a tight loop, it is recommended that you
    decorate it with functions.call_at_most_every() to limit the frequency at
    which updates happen (pass a number of seconds to it to limit samples to
    once per that number of seconds).

    You can also implement an `as_base_types` function that can return a dict
    where the values must be base types at all levels.  This can help prevent
    recursive copying, which is time intensive.
    """
    return {}

  def tearDown(self):
    """This method is called automatically at the end of each Test execution."""
    pass

  @classmethod
  def uses_base_tear_down(cls):
    """Checks whether the tearDown method is the BasePlug implementation."""
    this_tear_down = getattr(cls, 'tearDown')
    base_tear_down = getattr(BasePlug, 'tearDown')
    return this_tear_down.__code__ is base_tear_down.__code__


class FrontendAwareBasePlug(BasePlug, util.SubscribableStateMixin):
  """A plug that notifies of any state updates.

  Plugs inheriting from this class may be used in conjunction with the Station
  API to update any frontends each time the plug's state changes. The plug
  should call notify_update() when and only when the state returned by _asdict()
  changes.

  Since the Station API runs in a separate thread, the _asdict() method of
  frontend-aware plugs should be written with thread safety in mind.
  """
  enable_remote = True


def plug(update_kwargs=True, **plugs_map):
  """Creates a decorator that passes in plugs when invoked.

  This function returns a decorator for a function that will replace positional
  arguments to that function with the plugs specified.  See the module
  docstring for details and examples.

  Note this decorator does not work with class or bound methods, but does work
  with @staticmethod.

  Args:
    update_kwargs: If true, makes the decorated phase take this plug as a kwarg.
    **plugs_map: Dict mapping name to Plug type.

  Returns:
    A PhaseDescriptor that will pass plug instances in as kwargs when invoked.

  Raises:
    InvalidPlugError: If a type is provided that is not a subclass of BasePlug.
  """
  for a_plug in plugs_map.values():
    if not (isinstance(a_plug, PlugPlaceholder)
            or issubclass(a_plug, BasePlug)):
      raise InvalidPlugError(
          'Plug %s is not a subclass of plugs.BasePlug nor a placeholder '
          'for one' % a_plug)

  def result(func):
    """Wrap the given function and return the wrapper.

    Args:
      func: The function to wrap.

    Returns:
      A PhaseDescriptor that, when called will invoke the wrapped function,
        passing plugs as keyword args.

    Raises:
      DuplicatePlugError:  If a plug name is declared twice for the
          same function.
    """
    phase = openhtf.core.phase_descriptor.PhaseDescriptor.wrap_or_copy(func)
    duplicates = (frozenset(p.name for p in phase.plugs) &
                  frozenset(plugs_map))
    if duplicates:
      raise DuplicatePlugError(
          'Plugs %s required multiple times on phase %s' % (duplicates, func))

    phase.plugs.extend([
        PhasePlug(name, a_plug, update_kwargs=update_kwargs)
        for name, a_plug in six.iteritems(plugs_map)])
    return phase
  return result


class _PlugTearDownThread(threads.KillableThread):
  """Killable thread that runs a plug's tearDown function."""

  def __init__(self, a_plug, *args, **kwargs):
    super(_PlugTearDownThread, self).__init__(*args, **kwargs)
    self._plug = a_plug

  def _thread_proc(self):
    try:
      self._plug.tearDown()
    except Exception:  # pylint: disable=broad-except
      # Including the stack trace from ThreadTerminationErrors received when
      # killed.
      _LOG.warning('Exception calling tearDown on %s:',
                   self._plug, exc_info=True)


class PlugManager(object):
  """Class to manage the lifetimes of plugs.

  This class handles instantiation of plugs at test start and calling
  tearDown() on all plugs when the test completes.  It is used by
  the executor, and should not be instantiated outside the framework itself.

  Note this class is not thread-safe.  It should only ever be used by the
  main framework thread anyway.

  Attributes:
    _plug_types: Initial set of plug types, additional plug types may be
        passed into calls to initialize_plugs().
    _plugs_by_type: Dict mapping plug type to plug instance.
    _plugs_by_name: Dict mapping plug name to plug instance.
    _plug_descriptors: Dict mapping plug type to plug descriptor.
    logger: logging.Logger instance that can save logs to the running test
        record.
  """

  def __init__(self, plug_types=None, record_logger=None):
    self._plug_types = plug_types or set()
    for plug in self._plug_types:
      if isinstance(plug, PlugPlaceholder):
        raise InvalidPlugError('Plug %s is a placeholder, replace it using '
                               'with_plugs().' % plug)
    self._plugs_by_type = {}
    self._plugs_by_name = {}
    self._plug_descriptors = {}
    if not record_logger:
      record_logger = _LOG
    self.logger = record_logger.getChild('plug')

  def as_base_types(self):
    return {
        'plug_descriptors': {
            name: dict(descriptor._asdict())  # Convert OrderedDict to dict.
            for name, descriptor in six.iteritems(self._plug_descriptors)
        },
        'plug_states': {
            name: data.convert_to_base_types(plug)
            for name, plug in six.iteritems(self._plugs_by_name)
        },
    }

  def _make_plug_descriptor(self, plug_type):
    """Returns the plug descriptor, containing info about this plug type."""
    return PlugDescriptor(self.get_plug_mro(plug_type))

  def get_plug_mro(self, plug_type):
    """Returns a list of names identifying the plug classes in the plug's MRO.

    For example:
        ['openhtf.plugs.user_input.UserInput']
    Or:
        ['openhtf.plugs.user_input.UserInput',
         'my_module.advanced_user_input.AdvancedUserInput']
    """
    ignored_classes = (BasePlug, FrontendAwareBasePlug)
    return [
        self.get_plug_name(base_class) for base_class in plug_type.mro()
        if (issubclass(base_class, BasePlug) and
            base_class not in ignored_classes)
    ]

  def get_plug_name(self, plug_type):
    """Returns the plug's name, which is the class name and module.

    For example:
        'openhtf.plugs.user_input.UserInput'
    """
    return '%s.%s' % (plug_type.__module__, plug_type.__name__)

  def initialize_plugs(self, plug_types=None):
    """Instantiate required plugs.

    Instantiates plug types and saves the instances in self._plugs_by_type for
    use in provide_plugs().

    Args:
      plug_types: Plug types may be specified here rather than passed
                  into the constructor (this is used primarily for unit testing
                  phases).
    """
    types = plug_types if plug_types is not None else self._plug_types
    for plug_type in types:
      # Create a logger for this plug. All plug loggers go under the 'plug'
      # sub-logger in the logger hierarchy.
      plug_logger = self.logger.getChild(plug_type.__name__)
      if plug_type in self._plugs_by_type:
        continue
      try:
        if not issubclass(plug_type, BasePlug):
          raise InvalidPlugError(
              'Plug type "%s" is not an instance of BasePlug' % plug_type)
        if plug_type.logger != _LOG:
          # They put a logger attribute on the class itself, overriding ours.
          raise InvalidPlugError(
              'Do not override "logger" in your plugs.', plug_type)

        # Override the logger so that __init__'s logging goes into the record.
        plug_type.logger = plug_logger
        try:
          plug_instance = plug_type()
        finally:
          # Now set it back since we'll give the instance a logger in a moment.
          plug_type.logger = _LOG
        # Set the logger attribute directly (rather than in BasePlug) so we
        # don't depend on subclasses' implementation of __init__ to have it
        # set.
        if plug_instance.logger != _LOG:
          raise InvalidPlugError(
              'Do not set "self.logger" in __init__ in your plugs', plug_type)
        else:
          # Now the instance has its own copy of the test logger.
          plug_instance.logger = plug_logger
      except Exception:  # pylint: disable=broad-except
        plug_logger.exception('Exception instantiating plug type %s', plug_type)
        self.tear_down_plugs()
        raise
      self.update_plug(plug_type, plug_instance)

  def get_plug_by_class_path(self, plug_name):
    """Get a plug instance by name (class path).

    This provides a way for extensions to OpenHTF to access plug instances for
    a running test via that test's plug manager.

    Args:
      plug_name: Plug name, e.g. 'openhtf.plugs.user_input.UserInput'.

    Returns:
      The plug manager's instance of the specified plug.
    """
    return self._plugs_by_name.get(plug_name)

  def update_plug(self, plug_type, plug_value):
    """Update internal data stores with the given plug value for plug type.

    Safely tears down the old instance if one was already created, but that's
    generally not the case outside unittests.  Also, we explicitly pass the
    plug_type rather than detecting it from plug_value to allow unittests to
    override plugs with Mock instances.

    Note this should only be used inside unittests, as this mechanism is not
    compatible with RemotePlug support.
    """
    self._plug_types.add(plug_type)
    if plug_type in self._plugs_by_type:
      self._plugs_by_type[plug_type].tearDown()
    plug_name = self.get_plug_name(plug_type)
    self._plugs_by_type[plug_type] = plug_value
    self._plugs_by_name[plug_name] = plug_value
    self._plug_descriptors[plug_name] = self._make_plug_descriptor(plug_type)

  def provide_plugs(self, plug_name_map):
    """Provide the requested plugs [(name, type),] as {name: plug instance}."""
    return {name: self._plugs_by_type[cls] for name, cls in plug_name_map}

  def tear_down_plugs(self):
    """Call tearDown() on all instantiated plugs.

    Note that initialize_plugs must have been called before calling
    this method, and initialize_plugs must be called again after calling
    this method if you want to access the plugs attribute again.

    Any exceptions in tearDown() methods are logged, but do not get raised
    by this method.
    """
    _LOG.debug('Tearing down all plugs.')
    for plug_type, plug_instance in six.iteritems(self._plugs_by_type):
      if plug_instance.uses_base_tear_down():
        name = '<PlugTearDownThread: BasePlug No-Op for %s>' % plug_type
      else:
        name = '<PlugTearDownThread: %s>' % plug_type
      thread = _PlugTearDownThread(plug_instance, name=name)
      thread.start()
      timeout_s = (conf.plug_teardown_timeout_s
                   if conf.plug_teardown_timeout_s
                   else None)
      thread.join(timeout_s)
      if thread.is_alive():
        thread.kill()
        _LOG.warning('Killed tearDown for plug %s after timeout.',
                     plug_instance)
    self._plugs_by_type.clear()
    self._plugs_by_name.clear()

  def wait_for_plug_update(self, plug_name, remote_state, timeout_s):
    """Wait for a change in the state of a frontend-aware plug.

    Args:
      plug_name: Plug name, e.g. 'openhtf.plugs.user_input.UserInput'.
      remote_state: The last observed state.
      timeout_s: Number of seconds to wait for an update.

    Returns:
      An updated state, or None if the timeout runs out.

    Raises:
      InvalidPlugError: The plug can't be waited on either because it's not in
          use or it's not a frontend-aware plug.
    """
    plug = self._plugs_by_name.get(plug_name)

    if plug is None:
      raise InvalidPlugError('Cannot wait on unknown plug "%s".' % plug_name)

    if not isinstance(plug, FrontendAwareBasePlug):
      raise InvalidPlugError('Cannot wait on a plug %s that is not an subclass '
                             'of FrontendAwareBasePlug.' % plug_name)

    state, update_event = plug.asdict_with_event()
    if state != remote_state:
      return state

    if update_event.wait(timeout_s):
      return plug._asdict()

  def get_frontend_aware_plug_names(self):
    """Returns the names of frontend-aware plugs."""
    return [name for name, plug in six.iteritems(self._plugs_by_name)
            if isinstance(plug, FrontendAwareBasePlug)]
