# Copyright 2018 Google Inc. All Rights Reserved.

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

"""Phase Groups in OpenHTF.

Phase Groups are collections of Phases that are used to control phase
shortcutting due to terminal errors to better guarentee when teardown phases
run.

PhaseGroup instances have three primary member fields:
  `setup`: a list of phases, run first.  If these phases are all non-terminal,
      the PhaseGroup is entered.
  `main`: a list of phases, run after the setup phases as long as those are
      non-terminal.  If any of these phases are terminal, then the rest of the
      main phases will be skipped.
  `teardown`: a list of phases, guarenteed to run after the main phases as long
      as the PhaseGroup was entered.  If any are terminal, other teardown phases
      will continue to be run.  One exception is that a second CTRL-C sent to
      the main thread will abort all teardown phases.
There is one optional field:
  `name`: str, an arbitrary description used for logging.

PhaseGroup instances can be nested inside of each other.  A PhaseGroup is
terminal if any of its Phases or further nested PhaseGroups are also terminal.
"""

import collections
import functools

import mutablerecords

from openhtf.core import phase_descriptor
from openhtf.core import test_record


class PhaseGroup(mutablerecords.Record(
    'PhaseGroup', [], {
        'setup': tuple,
        'main': tuple,
        'teardown': tuple,
        'name': None,
    })):
  """Phase group with guaranteed end phase running.

  If the setup phases all continue, then the main phases and teardown phases are
  run. Even if any main phase or teardown phases has a terminal error, all the
  teardown phases are guaranteed to be run.
  """

  def __init__(self, setup=None, main=None, teardown=None, name=None):
    if not setup:
      setup = ()
    elif isinstance(setup, PhaseGroup):
      setup = (setup,)
    if not main:
      main = ()
    elif isinstance(main, PhaseGroup):
      main = (main,)
    if not teardown:
      teardown = ()
    elif isinstance(teardown, PhaseGroup):
      teardown = (teardown,)
      
    setup = tuple(self.fn_as_type(p, phase_descriptor.PhaseType.SETUP) for p in setup)
    main = tuple(self.fn_as_type(p, phase_descriptor.PhaseType.MAIN) for p in main)
    teardown = tuple(self.fn_as_type(p, phase_descriptor.PhaseType.TEARDOWN) for p in teardown)
      
    super(PhaseGroup, self).__init__(
        setup=tuple(setup), main=tuple(main), teardown=tuple(teardown),
        name=name)

  def fn_as_type(self, fn, phase_type):
    try:
      return fn.as_type(phase_type)
    except AttributeError:
      return fn

  @classmethod
  def convert_if_not(cls, phases_or_groups):
    """Convert list of phases or groups into a new PhaseGroup if not already."""
    if isinstance(phases_or_groups, PhaseGroup):
      return mutablerecords.CopyRecord(phases_or_groups)

    flattened = flatten_phases_and_groups(phases_or_groups)
    return cls(main=flattened)

  @classmethod
  def with_context(cls, setup_phases, teardown_phases):
    """Create PhaseGroup creator function with setup and teardown phases.

    Args:
      setup_phases: list of phase_descriptor.PhaseDescriptors/PhaseGroups/
          callables/iterables, phases to run during the setup for the PhaseGroup
          returned from the created function.
      teardown_phases: list of phase_descriptor.PhaseDescriptors/PhaseGroups/
          callables/iterables, phases to run during the teardown for the
          PhaseGroup returned from the created function.

    Returns:
      Function that takes *phases and returns a PhaseGroup with the predefined
      setup and teardown phases, with *phases as the main phases.
    """
    setup = flatten_phases_and_groups(setup_phases)
    teardown = flatten_phases_and_groups(teardown_phases)

    def _context_wrapper(*phases):
      return cls(setup=setup,
                 main=flatten_phases_and_groups(phases),
                 teardown=teardown)
    return _context_wrapper

  @classmethod
  def with_setup(cls, *setup_phases):
    """Create PhaseGroup creator function with predefined setup phases."""
    return cls.with_context(setup_phases, [])

  @classmethod
  def with_teardown(cls, *teardown_phases):
    """Create PhaseGroup creator function with predefined teardown phases."""
    return cls.with_context([], teardown_phases)

  def combine(self, other, name=None):
    """Combine with another PhaseGroup and return the result."""
    return PhaseGroup(
        setup=self.setup + other.setup,
        main=self.main + other.main,
        teardown=self.teardown + other.teardown,
        name=name)

  def wrap(self, main_phases, name=None):
    """Returns PhaseGroup with additional main phases."""
    new_main = list(self.main)
    if isinstance(main_phases, collections.abc.Iterable):
      new_main.extend(main_phases)
    else:
      new_main.append(main_phases)
    return PhaseGroup(
        setup=self.setup,
        main=new_main,
        teardown=self.teardown,
        name=name)

  def transform(self, transform_fn):
    return PhaseGroup(
        setup=[transform_fn(p) for p in self.setup],
        main=[transform_fn(p) for p in self.main],
        teardown=[transform_fn(p) for p in self.teardown],
        name=self.name)

  def with_args(self, **kwargs):
    """Send known keyword-arguments to each contained phase the when called."""
    return self.transform(functools.partial(optionally_with_args, **kwargs))

  def with_plugs(self, **subplugs):
    """Substitute only known plugs for placeholders for each contained phase."""
    return self.transform(functools.partial(optionally_with_plugs, **subplugs))

  def as_type(self, type):
    """Same interface as PhaseDescriptors"""
    return self
  
  def as_depth(self, depth):
    """Same interface as PhaseDescriptors"""
    return self

  def _iterate(self, phases):
    for phase in phases:
      if isinstance(phase, PhaseGroup):
        for p in phase:
          yield p
      else:
        yield phase

  def __iter__(self):
    """Iterate directly over the phases."""
    for phase in self._iterate(self.setup):
      yield phase
    for phase in self._iterate(self.main):
      yield phase
    for phase in self._iterate(self.teardown):
      yield phase

  def flatten(self):
    """Internally flatten out nested iterables."""
    return PhaseGroup(
        setup=flatten_phases_and_groups(self.setup),
        main=flatten_phases_and_groups(self.main),
        teardown=flatten_phases_and_groups(self.teardown),
        name=self.name)

  def build_tree(self):
    return build_phase_group_tree(self)

  def load_code_info(self, with_source=False):
    """Load coded info for all contained phases."""
    return PhaseGroup(
        setup=load_code_info(self.setup, with_source=with_source),
        main=load_code_info(self.main, with_source=with_source),
        teardown=load_code_info(self.teardown, with_source=with_source),
        name=self.name)


def load_code_info(phases_or_groups, with_source=False):
  """Recursively load code info for a PhaseGroup or list of phases or groups."""
  if isinstance(phases_or_groups, PhaseGroup):
    return phases_or_groups.load_code_info(with_source=with_source)
  ret = []
  for phase in phases_or_groups:
    if isinstance(phase, PhaseGroup):
      ret.append(phase.load_code_info(with_source=with_source))
    else:
      ret.append(
          mutablerecords.CopyRecord(
              phase, code_info=test_record.CodeInfo.for_function(phase.func, with_source=with_source)))
  return ret


def flatten_phases_and_groups(phases_or_groups):
  """Recursively flatten nested lists for the list of phases or groups."""
  if isinstance(phases_or_groups, PhaseGroup):
    phases_or_groups = [phases_or_groups]
  ret = []
  
  _flatten_functions(phases_or_groups, 
    on_phase_group = lambda phase: ret.append(phase.flatten()),
    on_iterable = lambda phase: ret.extend(flatten_phases_and_groups(phase)),
    on_else = lambda phase: ret.append(phase_descriptor.PhaseDescriptor.wrap_or_copy(phase))
  )
  
  return ret

def build_phase_group_tree(phases_or_groups, _depth=0):
  if isinstance(phases_or_groups, PhaseGroup):
    phases_or_groups = [phases_or_groups]
    
  tree = []
  
  _flatten_functions(phases_or_groups, 
    on_phase_group = lambda phase: tree.append(build_phase_group_tree(phase.setup + phase.main + phase.teardown, _depth=_depth+1)),
    on_iterable = lambda phase: tree.append(build_phase_group_tree(phase, _depth=_depth+1)),
    on_else = lambda phase: tree.append(phase_descriptor.PhaseDescriptor.wrap_or_copy(phase).as_depth(_depth))
  )
  
  return tree
  

def _flatten_functions(list_to_flatten, on_phase_group, on_iterable, on_else):
  for item in list_to_flatten:
    if isinstance(item, PhaseGroup):
      on_phase_group(item)
    elif isinstance(item, collections.abc.Iterable):
      on_iterable(item)
    else:
      on_else(item)
      
  


def optionally_with_args(phase, **kwargs):
  """Apply only the args that the phase knows.

  If the phase has a **kwargs-style argument, it counts as knowing all args.

  Args:
    phase: phase_descriptor.PhaseDescriptor or PhaseGroup or callable, or
        iterable of those, the phase or phase group (or iterable) to apply
        with_args to.
    **kwargs: arguments to apply to the phase.

  Returns:
    phase_descriptor.PhaseDescriptor or PhaseGroup or iterable with the updated
    args.
  """
  if isinstance(phase, PhaseGroup):
    return phase.with_args(**kwargs)
  if isinstance(phase, collections.abc.Iterable):
    return [optionally_with_args(p, **kwargs) for p in phase]

  if not isinstance(phase, phase_descriptor.PhaseDescriptor):
    phase = phase_descriptor.PhaseDescriptor.wrap_or_copy(phase)
  return phase.with_known_args(**kwargs)


def optionally_with_plugs(phase, **subplugs):
  """Apply only the with_plugs that the phase knows.

  This will determine the subset of plug overrides for only plugs the phase
  actually has.

  Args:
    phase: phase_descriptor.PhaseDescriptor or PhaseGroup or callable, or
        iterable of those, the phase or phase group (or iterable) to apply the
        plug changes to.
    **subplugs: mapping from plug name to derived plug class, the subplugs to
        apply.

  Raises:
    openhtf.plugs.InvalidPlugError: if a specified subplug class is not a valid
        replacement for the specified plug name.

  Returns:
    phase_descriptor.PhaseDescriptor or PhaseGroup or iterable with the updated
    plugs.
  """
  if isinstance(phase, PhaseGroup):
    return phase.with_plugs(**subplugs)
  if isinstance(phase, collections.abc.Iterable):
    return [optionally_with_plugs(p, **subplugs) for p in phase]

  if not isinstance(phase, phase_descriptor.PhaseDescriptor):
    phase = phase_descriptor.PhaseDescriptor.wrap_or_copy(phase)
  return phase.with_known_plugs(**subplugs)
