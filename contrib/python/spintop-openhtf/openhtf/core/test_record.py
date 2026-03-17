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


"""OpenHTF module responsible for managing records of tests."""


import collections
import hashlib
import inspect
import logging
import os

from enum import Enum

import mutablerecords

from openhtf import util
from openhtf.util import data
from openhtf.util import logs

import six

_LOG = logging.getLogger(__name__)


class InvalidMeasurementDimensions(Exception):
  """Raised when a measurement is taken with the wrong number of dimensions."""


OutcomeDetails = collections.namedtuple(
    'OutcomeDetails', 'code description')
Outcome = Enum('Outcome', ['PASS', 'FAIL', 'ERROR', 'TIMEOUT', 'ABORTED'])  # pylint: disable=invalid-name
# LogRecord is in openhtf.util.logs.LogRecord.


class Attachment(collections.namedtuple('Attachment', 'data mimetype')):
  """Encapsulate attachment data and guessed MIME type."""

  @property
  def sha1(self):
    return hashlib.sha1(six.ensure_binary(self.data)).hexdigest()

  def _asdict(self):
    # Don't include the attachment data when converting to dict.
    return {
        'mimetype': self.mimetype,
        'sha1': self.sha1,
    }

class TestRecord(  # pylint: disable=no-init
    mutablerecords.Record(
        'TestRecord', ['dut_id', 'station_id'],
        {'start_time_millis': int,
         'end_time_millis': None,
         'outcome': None,
         'outcome_details': list,
         'code_info': None,
         'metadata': dict,
         'phases': list,
         'log_records': list,
         '_cached_record': dict,
         '_cached_phases': list,
         '_cached_log_records': list,
         '_cached_config_from_metadata': dict,
        })):
  """The record of a single run of a test."""

  def __init__(self, *args, **kwargs):
    super(TestRecord, self).__init__(*args, **kwargs)
    # Cache data that does not change during execution.
    # Cache the metadata config so it does not recursively copied over and over
    # again.
    self._cached_config_from_metadata = self.metadata.get('config')
    self._cached_record = {
        'station_id': data.convert_to_base_types(self.station_id),
        'code_info': data.convert_to_base_types(self.code_info),
    }

  def add_outcome_details(self, code, description=''):
    """Adds a code with optional description to this record's outcome_details.

    Args:
      code: A code name or number.
      description: A string providing more details about the outcome code.
    """
    self.outcome_details.append(OutcomeDetails(code, description))

  def add_phase_record(self, phase_record):
    self.phases.append(phase_record)
    self._cached_phases.append(phase_record.as_base_types())

  def add_log_record(self, log_record):
    self.log_records.append(log_record)
    self._cached_log_records.append(log_record._asdict())

  def as_base_types(self):
    """Convert to a dict representation composed exclusively of base types."""
    metadata = data.convert_to_base_types(self.metadata,
                                          ignore_keys=('config',))
    metadata['config'] = self._cached_config_from_metadata
    ret = {
        'dut_id': data.convert_to_base_types(self.dut_id),
        'start_time_millis': self.start_time_millis,
        'end_time_millis': self.end_time_millis,
        'outcome': data.convert_to_base_types(self.outcome),
        'outcome_details': data.convert_to_base_types(self.outcome_details),
        'metadata': metadata,
        'phases': self._cached_phases,
        'log_records': self._cached_log_records,
    }
    ret.update(self._cached_record)
    return ret
  
  def is_started(self):
    return bool(self.start_time_millis)
  
# PhaseResult enumerations are converted to these outcomes by the PhaseState.
PhaseOutcome = Enum(  # pylint: disable=invalid-name
    'PhaseOutcome', [
        'PASS',  # CONTINUE with allowed measurement outcomes.
        'FAIL',  # CONTINUE with failed measurements or FAIL_AND_CONTINUE.
        'SKIP',  # SKIP or REPEAT when under the phase's repeat limit.
        'ERROR',  # Any terminal result.
    ])


class PhaseRecord(  # pylint: disable=no-init
    mutablerecords.Record(
        'PhaseRecord', ['descriptor_id', 'name', 'codeinfo'],
        {'measurements': None, 'options': None,
         'start_time_millis': int, 'end_time_millis': None,
         'attachments': dict, 'result': None, 'outcome': None})):
  """The record of a single run of a phase.

  Measurement metadata (declarations) and values are stored in separate
  dictionaries, each of which map measurement name to the respective object.  In
  the case of the measurements field, those objects are measurements.Measurement
  instances.  The 'value' attribute of each of those instances is an instance of
  measurments.MeasuredValue, which contains either a single value, or a list of
  values in the case of a dimensioned measurement.

  See measurements.Record.GetValues() for more information.

  The 'result' attribute contains a phase_executor.PhaseExecutionOutcome
  instance, which wraps the openhtf.PhaseResult returned by the phase or an
  error condition that terminated the phase.

  The 'outcome' attribute is a PhaseOutcome, which caches the pass/fail outcome
  of the phase's measurements or indicates that the verification was skipped.
  """

  @classmethod
  def from_descriptor(cls, phase_desc):
    return cls(id(phase_desc), phase_desc.name, phase_desc.code_info)

  def as_base_types(self):
    """Convert to a dict representation composed exclusively of base types."""
    base_types_dict = {
        k: data.convert_to_base_types(getattr(self, k))
        for k in self.optional_attributes
    }
    base_types_dict.update(
        descriptor_id=self.descriptor_id,
        name=self.name,
        codeinfo=data.convert_to_base_types(self.codeinfo),
    )
    return base_types_dict

  def record_start_time(self):
    """Record the phase start time and return it."""
    self.start_time_millis = util.time_millis()
    return self.start_time_millis

  def finalize_phase(self, options):
    self.end_time_millis = util.time_millis()
    self.options = options


def _get_source_safely(obj):
  try:
    return inspect.getsource(obj)
  except Exception:  # pylint: disable=broad-except
    logs.log_once(
        _LOG.warning,
        'Unable to load source code for %s. Only logging this once.', obj)
    return ''


class CodeInfo(mutablerecords.HashableRecord(
    'CodeInfo', ['name', 'docstring', 'sourcecode'])):
  """Information regarding the running tester code."""

  @classmethod
  def for_module_from_stack(cls, levels_up=1, after_file=None, with_source=False):
    # levels_up is how many frames up to go:
    #  0: This function (useless).
    #  1: The function calling this (likely).
    #  2+: The function calling 'you' (likely in the framework).
    stack = inspect.stack(context=0)
    if after_file:
      found_file = False
      for stackframe in stack:
        frame, filename = stackframe[:2]
        if filename == after_file:
          found_file = True
        elif found_file:
          # we skipped the file. All good !
          break
        
    else:
      frame, filename = stack[levels_up][:2]
    
    module = inspect.getmodule(frame)
    if with_source:
      source = _get_source_safely(frame)
    else:
      source = ''
    return cls(os.path.basename(filename), inspect.getdoc(module), source)

  @classmethod
  def for_function(cls, func, with_source=False):
    if with_source:
      source = _get_source_safely(func)
    else:
      source = ''
    return cls(func.__name__, inspect.getdoc(func), source)

  @classmethod
  def uncaptured(cls):
    return cls('', None, '')
