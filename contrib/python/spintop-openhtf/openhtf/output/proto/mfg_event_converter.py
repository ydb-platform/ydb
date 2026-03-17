"""Convert a TestRecord into a mfg_event proto for upload to mfg inspector.

Also includes utilities to handle multi-dim conversion into an attachment
and the reverse.

A decision had to be made on how to handle phases, measurements and attachments
with non-unique names.  Approach taken is to append a _X to the names.
"""

import collections
import itertools
import json
import logging
import numbers
import os
import sys

from openhtf.core import measurements
from openhtf.core import test_record as htf_test_record
from openhtf.output.proto import mfg_event_pb2
from openhtf.output.proto import test_runs_converter
from openhtf.output.proto import test_runs_pb2
from openhtf.util import data as htf_data
from openhtf.util import units
from openhtf.util import validators


from past.builtins import unicode


TEST_RECORD_ATTACHMENT_NAME = 'OpenHTF_record.json'

#  To be lazy loaded by _LazyLoadUnitsByCode when needed.
UNITS_BY_CODE = {}

# Map test run Status (proto) name to measurement Outcome (python) enum's and
# the reverse.  Note: there is data lost in converting an UNSET/PARTIALLY_SET to
# an ERROR so we can't completely reverse the transformation.
MEASUREMENT_OUTCOME_TO_TEST_RUN_STATUS_NAME = {
    measurements.Outcome.PASS: 'PASS',
    measurements.Outcome.FAIL: 'FAIL',
    measurements.Outcome.UNSET: 'ERROR',
    measurements.Outcome.PARTIALLY_SET: 'ERROR',
}
TEST_RUN_STATUS_NAME_TO_MEASUREMENT_OUTCOME = {
    'PASS': measurements.Outcome.PASS,
    'FAIL': measurements.Outcome.FAIL,
    'ERROR': measurements.Outcome.UNSET
}


def _lazy_load_units_by_code():
  """Populate dict of units by code iff UNITS_BY_CODE is empty."""
  if UNITS_BY_CODE:
    # already populated
    return

  for unit in units.UNITS_BY_NAME.values():
    UNITS_BY_CODE[unit.code] = unit


def mfg_event_from_test_record(record):
  """Convert an OpenHTF TestRecord to an MfgEvent proto.

  Most fields are copied over directly and some are pulled out of metadata
  (listed below). Multi-dimensional measurements are stored only in the JSON
  dump of the record.

  Important Note:  This function mutates the test_record so any output callbacks
  called after this callback will operate on the mutated record.

  Metadata fields:
    test_name: The name field from the test's TestOptions.
    config: The OpenHTF config, as a dictionary.
    assembly_events: List of AssemblyEvent protos.
        (see proto/assembly_event.proto).
    operator_name: Name of the test operator.

  Args:
    record: An OpenHTF TestRecord.

  Returns:
    An MfgEvent proto representing the given test record.
  """
  mfg_event = mfg_event_pb2.MfgEvent()

  _populate_basic_data(mfg_event, record)
  _attach_record_as_json(mfg_event, record)
  _attach_argv(mfg_event)
  _attach_config(mfg_event, record)

  # Only include assembly events if the test passed.
  if ('assembly_events' in record.metadata and
      mfg_event.test_status == test_runs_pb2.PASS):
    for assembly_event in record.metadata['assembly_events']:
      mfg_event.assembly_events.add().CopyFrom(assembly_event)
  convert_multidim_measurements(record.phases)
  phase_copier = PhaseCopier(phase_uniquizer(record.phases))
  phase_copier.copy_measurements(mfg_event)
  phase_copier.copy_attachments(mfg_event)

  return mfg_event


def _populate_basic_data(mfg_event, record):
  """Copies data from the OpenHTF TestRecord to the MfgEvent proto."""
  # TODO:
  #   * Missing in proto: set run name from metadata.
  #   * `part_tags` field on proto is unused
  #   * `timings` field on proto is unused.
  #   * Handle arbitrary units as uom_code/uom_suffix.

  # Populate non-repeated fields.
  mfg_event.dut_serial = record.dut_id
  mfg_event.start_time_ms = record.start_time_millis
  mfg_event.end_time_ms = record.end_time_millis
  mfg_event.tester_name = record.station_id
  mfg_event.test_name = record.metadata.get('test_name') or record.station_id
  mfg_event.test_status = test_runs_converter.OUTCOME_MAP[record.outcome]
  mfg_event.operator_name = record.metadata.get('operator_name', '')
  mfg_event.test_version = str(record.metadata.get('test_version', ''))
  mfg_event.test_description = record.metadata.get('test_description', '')

  # Populate part_tags.
  mfg_event.part_tags.extend(record.metadata.get('part_tags', []))

  # Populate phases.
  for phase in record.phases:
    mfg_phase = mfg_event.phases.add()
    mfg_phase.name = phase.name
    mfg_phase.description = phase.codeinfo.sourcecode
    mfg_phase.timing.start_time_millis = phase.start_time_millis
    mfg_phase.timing.end_time_millis = phase.end_time_millis

  # Populate failure codes.
  for details in record.outcome_details:
    failure_code = mfg_event.failure_codes.add()
    failure_code.code = details.code
    failure_code.details = details.description

  # Populate test logs.
  for log_record in record.log_records:
    test_log = mfg_event.test_logs.add()
    test_log.timestamp_millis = log_record.timestamp_millis
    test_log.log_message = log_record.message
    test_log.logger_name = log_record.logger_name
    test_log.levelno = log_record.level
    if log_record.level <= logging.DEBUG:
      test_log.level = test_runs_pb2.TestRunLogMessage.DEBUG
    elif log_record.level <= logging.INFO:
      test_log.level = test_runs_pb2.TestRunLogMessage.INFO
    elif log_record.level <= logging.WARNING:
      test_log.level = test_runs_pb2.TestRunLogMessage.WARNING
    elif log_record.level <= logging.ERROR:
      test_log.level = test_runs_pb2.TestRunLogMessage.ERROR
    elif log_record.level <= logging.CRITICAL:
      test_log.level = test_runs_pb2.TestRunLogMessage.CRITICAL
    test_log.log_source = log_record.source
    test_log.lineno = log_record.lineno


def _attach_record_as_json(mfg_event, record):
  """Attach a copy of the record as JSON so we have an un-mangled copy."""
  attachment = mfg_event.attachment.add()
  attachment.name = TEST_RECORD_ATTACHMENT_NAME
  test_record_dict = htf_data.convert_to_base_types(record)
  attachment.value_binary = _convert_object_to_json(test_record_dict)
  attachment.type = test_runs_pb2.TEXT_UTF8


def _convert_object_to_json(obj):
  # Since there will be parts of this that may have unicode, either as
  # measurement or in the logs, we have to be careful and convert everything
  # to unicode, merge, then encode to UTF-8 to put it into the proto.
  json_encoder = json.JSONEncoder(sort_keys=True, indent=2, ensure_ascii=False)
  pieces = []
  for piece in json_encoder.iterencode(obj):
    if isinstance(piece, bytes):
      pieces.append(unicode(piece, errors='replace'))
    else:
      pieces.append(piece)

  return (u''.join(pieces)).encode('utf8', errors='replace')


def _attach_config(mfg_event, record):
  """Attaches the OpenHTF config file as JSON."""
  if 'config' not in record.metadata:
    return
  attachment = mfg_event.attachment.add()
  attachment.name = 'config'
  attachment.value_binary = _convert_object_to_json(record.metadata['config'])
  attachment.type = test_runs_pb2.TEXT_UTF8


def _attach_argv(mfg_event):
  attachment = mfg_event.attachment.add()
  attachment.name = 'argv'
  argv = [os.path.realpath(sys.argv[0])] + sys.argv[1:]
  attachment.value_binary = _convert_object_to_json(argv)
  attachment.type = test_runs_pb2.TEXT_UTF8


class UniqueNameMaker(object):
  """Makes unique names for phases, attachments, etc with duplicate names."""

  def __init__(self, all_names):
    self._counts = collections.Counter(all_names)
    self._seen = collections.Counter()

  def make_unique(self, name):
    count = self._counts[name]
    assert count >= 1, 'Seeing a new name that was not given to the constructor'
    if count == 1:
      # It's unique, so let's skip extra calculations.
      return name
    # Count the number of times we've seen this and return this one's index.
    self._seen[name] += 1
    main, ext = os.path.splitext(name)

    return '%s_%d%s' % (main, self._seen[name] - 1, ext)


def phase_uniquizer(all_phases):
  """Makes the names of phase measurement and attachments unique.

  This function will make the names of measurements and attachments unique.
  It modifies the input all_phases.

  Args:
    all_phases: the phases to make unique

  Returns:
    the phases now modified.
  """
  measurement_name_maker = UniqueNameMaker(
      itertools.chain.from_iterable(
          phase.measurements.keys() for phase in all_phases
          if phase.measurements))
  attachment_names = list(itertools.chain.from_iterable(
      phase.attachments.keys() for phase in all_phases))
  attachment_names.extend(itertools.chain.from_iterable([
      'multidim_' + name for name, meas in phase.measurements.items()
      if meas.dimensions is not None
  ] for phase in all_phases if phase.measurements))
  attachment_name_maker = UniqueNameMaker(attachment_names)
  for phase in all_phases:
    # Make measurements unique.
    for name, _ in sorted(phase.measurements.items()):
      old_name = name
      name = measurement_name_maker.make_unique(name)

      phase.measurements[old_name].name = name
      phase.measurements[name] = phase.measurements.pop(old_name)
    # Make attachments unique.
    for name, _ in sorted(phase.attachments.items()):
      old_name = name
      name = attachment_name_maker.make_unique(name)
      phase.attachments[old_name].name = name
      phase.attachments[name] = phase.attachments.pop(old_name)
  return all_phases


def multidim_measurement_to_attachment(name, measurement):
  """Convert a multi-dim measurement to an `openhtf.test_record.Attachment`."""

  dimensions = list(measurement.dimensions)
  if measurement.units:
    dimensions.append(
        measurements.Dimension.from_unit_descriptor(measurement.units))

  dims = []
  for d in dimensions:
    if d.suffix is None:
      suffix = u''
    # Ensure that the suffix is unicode. It's typically str/bytes because
    # units.py looks them up against str/bytes.
    elif isinstance(d.suffix, unicode):
      suffix = d.suffix
    else:
      suffix = d.suffix.decode('utf8')
    dims.append({
        'uom_suffix': suffix,
        'uom_code': d.code,
        'name': d.name,
    })
  # Refer to the module docstring for the expected schema.
  dimensioned_measured_value = measurement.measured_value
  value = (sorted(dimensioned_measured_value.value, key=lambda x: x[0])
           if dimensioned_measured_value.is_value_set else None)
  outcome_str = MEASUREMENT_OUTCOME_TO_TEST_RUN_STATUS_NAME[measurement.outcome]
  data = _convert_object_to_json({
      'outcome': outcome_str,
      'name': name,
      'dimensions': dims,
      'value': value,
  })
  attachment = htf_test_record.Attachment(data, test_runs_pb2.MULTIDIM_JSON)

  return attachment


def convert_multidim_measurements(all_phases):
  """Converts each multidim measurements into attachments for all phases.."""
  # Combine actual attachments with attachments we make from multi-dim
  # measurements.
  attachment_names = list(itertools.chain.from_iterable(
      phase.attachments.keys() for phase in all_phases))
  attachment_names.extend(itertools.chain.from_iterable([
      'multidim_' + name for name, meas in phase.measurements.items()
      if meas.dimensions is not None
  ] for phase in all_phases if phase.measurements))
  attachment_name_maker = UniqueNameMaker(attachment_names)

  for phase in all_phases:
    # Process multi-dim measurements into unique attachments.
    for name, measurement in sorted(phase.measurements.items()):
      if measurement.dimensions:
        old_name = name
        name = attachment_name_maker.make_unique('multidim_%s' % name)
        attachment = multidim_measurement_to_attachment(name, measurement)
        phase.attachments[name] = attachment
        phase.measurements.pop(old_name)
  return all_phases


class PhaseCopier(object):
  """Copies measurements and attachments to an MfgEvent."""

  def __init__(self, all_phases):
    self._phases = all_phases

  def copy_measurements(self, mfg_event):
    for phase in self._phases:
      for name, measurement in sorted(phase.measurements.items()):
        # Multi-dim measurements should already have been removed.
        assert measurement.dimensions is None
        self._copy_unidimensional_measurement(
            phase, name, measurement, mfg_event)

  def _copy_unidimensional_measurement(
      self, phase, name, measurement, mfg_event):
    """Copy uni-dimensional measurements to the MfgEvent."""
    mfg_measurement = mfg_event.measurement.add()

    # Copy basic measurement fields.
    mfg_measurement.name = name
    if measurement.docstring:
      mfg_measurement.description = measurement.docstring
    mfg_measurement.parameter_tag.append(phase.name)
    if (measurement.units and
        measurement.units.code in test_runs_converter.UOM_CODE_MAP):
      mfg_measurement.unit_code = (
          test_runs_converter.UOM_CODE_MAP[measurement.units.code])

    # Copy failed measurements as failure_codes. This happens early to include
    # unset measurements.
    if (measurement.outcome != measurements.Outcome.PASS and
        phase.outcome != htf_test_record.PhaseOutcome.SKIP):
      failure_code = mfg_event.failure_codes.add()
      failure_code.code = name
      failure_code.details = '\n'.join(str(v) for v in measurement.validators)

    # Copy measurement value.
    measured_value = measurement.measured_value
    status_str = MEASUREMENT_OUTCOME_TO_TEST_RUN_STATUS_NAME[
        measurement.outcome]
    mfg_measurement.status = test_runs_pb2.Status.Value(status_str)
    if not measured_value.is_value_set:
      return
    value = measured_value.value

    if isinstance(value, numbers.Number):
      mfg_measurement.numeric_value = float(value)
    elif isinstance(value, bytes):
      # text_value expects unicode or ascii-compatible strings, so we must
      # 'decode' it, even if it's actually just garbage bytestring data.
      mfg_measurement.text_value = unicode(value, errors='replace')
    elif isinstance(value, unicode):
      # Don't waste time and potential errors decoding unicode.
      mfg_measurement.text_value = value
    else:
      # Coercing to string.
      mfg_measurement.text_value = str(value)

    # Copy measurement validators.
    for validator in measurement.validators:
      if isinstance(validator, validators.RangeValidatorBase):
        if validator.minimum is not None:
          mfg_measurement.numeric_minimum = float(validator.minimum)
        if validator.maximum is not None:
          mfg_measurement.numeric_maximum = float(validator.maximum)
      elif isinstance(validator, validators.RegexMatcher):
        mfg_measurement.expected_text = validator.regex
      else:
        mfg_measurement.description += '\nValidator: ' + str(validator)

  def copy_attachments(self, mfg_event):
    for phase in self._phases:
      for name, (data, mimetype) in sorted(phase.attachments.items()):
        self._copy_attachment(name, data, mimetype, mfg_event)

  def _copy_attachment(self, name, data, mimetype, mfg_event):
    """Copies an attachment to mfg_event."""
    attachment = mfg_event.attachment.add()
    attachment.name = name
    if isinstance(data, unicode):
      data = data.encode('utf8')
    attachment.value_binary = data
    if mimetype in test_runs_converter.MIMETYPE_MAP:
      attachment.type = test_runs_converter.MIMETYPE_MAP[mimetype]
    elif mimetype == test_runs_pb2.MULTIDIM_JSON:
      attachment.type = mimetype
    else:
      attachment.type = test_runs_pb2.BINARY


def test_record_from_mfg_event(mfg_event):
  """Extract the original test_record saved as an attachment on a mfg_event."""
  for attachment in mfg_event.attachment:
    if attachment.name == TEST_RECORD_ATTACHMENT_NAME:
      return json.loads(attachment.value_binary)

  raise ValueError('Could not find test record JSON in the given MfgEvent.')


def attachment_to_multidim_measurement(attachment, name=None):
  """Convert an OpenHTF test record attachment to a multi-dim measurement.

  This is a best effort attempt to reverse, as some data is lost in converting
  from a multidim to an attachment.

  Args:
    attachment: an `openhtf.test_record.Attachment` from a multi-dim.
    name: an optional name for the measurement.  If not provided will use the
     name included in the attachment.

  Returns:
    An multi-dim `openhtf.Measurement`.
  """
  data = json.loads(attachment.data)

  name = name or data.get('name')
  # attachment_dimn are a list of dicts with keys 'uom_suffix' and 'uom_code'
  attachment_dims = data.get('dimensions', [])
  # attachment_value is a list of lists [[t1, x1, y1, f1], [t2, x2, y2, f2]]
  attachment_values = data.get('value')

  attachment_outcome_str = data.get('outcome')
  if attachment_outcome_str not in TEST_RUN_STATUS_NAME_TO_MEASUREMENT_OUTCOME:
    # Fpr backward compatibility with saved data we'll convert integers to str
    try:
      attachment_outcome_str = test_runs_pb2.Status.Name(
          int(attachment_outcome_str))
    except ValueError:
      attachment_outcome_str = None

  # Convert test status outcome str to measurement outcome
  outcome = TEST_RUN_STATUS_NAME_TO_MEASUREMENT_OUTCOME.get(
      attachment_outcome_str)

  # convert dimensions into htf.Dimensions
  _lazy_load_units_by_code()
  dims = []
  for d in attachment_dims:
    # Try to convert into htf.Dimension including backwards compatibility.
    unit = UNITS_BY_CODE.get(d.get('uom_code'), units.NONE)
    description = d.get('name', '')
    dims.append(measurements.Dimension(description=description, unit=unit))

  # Attempt to determine if units are included.
  if attachment_values and len(dims) == len(attachment_values[0]):
    # units provided
    units_ = dims[-1].unit
    dimensions = dims[:-1]
  else:
    units_ = None
    dimensions = dims

  # created dimensioned_measured_value and populate with values.
  measured_value = measurements.DimensionedMeasuredValue(
      name=name,
      num_dimensions=len(dimensions)
  )
  for row in attachment_values:
    coordinates = tuple(row[:-1])
    val = row[-1]
    measured_value[coordinates] = val

  measurement = measurements.Measurement(
      name=name,
      units=units_,
      dimensions=tuple(dimensions),
      measured_value=measured_value,
      outcome=outcome
  )
  return measurement
