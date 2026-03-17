"""Utils to convert OpenHTF TestRecord to test_runs_pb2 proto.

MULTIDIM_JSON schema:
{
  "$schema": "http://json-schema.org/draft-04/schema#",
  "title": "Multi-dimensional test parameter",
  "type": "object",
  "properties": {
    "outcome": {"enum": ["PASS", "FAIL", "ERROR"]},
    "name": {"type": "string"},
    "dimensions": {
      "type": array,
      "minItems": 1,
      "items": {
        "type": "object",
        "properties": {
          "uom_code": {"type": "string"},
          "uom_suffix": {"type": "string"}
        }
      }
    },
    "values": {
      "type": "array",
      "items": {}
    }
  }
}
"""

import json
import logging
import numbers

from openhtf.core import measurements
from openhtf.core import test_record
from openhtf.output.callbacks import json_factory
from openhtf.util import validators

from openhtf.output.proto import test_runs_pb2

try:
  from past.types import unicode  # pylint: disable=redefined-builtin,g-import-not-at-top
except ImportError:
  pass

import six  # pylint: disable=g-import-not-at-top,g-bad-import-order


# pylint: disable=no-member
MIMETYPE_MAP = {
    'image/jpeg': test_runs_pb2.JPG,
    'image/png': test_runs_pb2.PNG,
    'audio/x-wav': test_runs_pb2.WAV,
    'text/plain': test_runs_pb2.TEXT_UTF8,
    'image/tiff': test_runs_pb2.TIFF,
    'video/mp4': test_runs_pb2.MP4,
}
OUTCOME_MAP = {
    test_record.Outcome.ERROR: test_runs_pb2.ERROR,
    test_record.Outcome.FAIL: test_runs_pb2.FAIL,
    test_record.Outcome.PASS: test_runs_pb2.PASS,
    test_record.Outcome.TIMEOUT: test_runs_pb2.ERROR,
    test_record.Outcome.ABORTED: test_runs_pb2.ERROR,
}

UOM_CODE_MAP = {
    u.GetOptions().Extensions[
        test_runs_pb2.uom_code]: num
    for num, u in six.iteritems(
        test_runs_pb2.Units.UnitCode.DESCRIPTOR.values_by_number)
}
# pylint: enable=no-member

# Control how many flattened parameters we'll output per multidimensional
# measurement.
MAX_PARAMS_PER_MEASUREMENT = 100


# pylint: disable=invalid-name
def _populate_header(record, testrun):
  """Populate header-like info in testrun from record.

  Mostly obvious, some stuff comes from metadata, see docstring of
  _test_run_from_test_record for details.

  Args:
    record: the OpenHTF TestRecord being converted
    testrun: a TestRun proto
  """
  testrun.dut_serial = record.dut_id
  testrun.tester_name = record.station_id
  if 'test_name' in record.metadata:
    testrun.test_info.name = record.metadata['test_name']
  else:
    # Default to copying tester_name into test_info.name.
    testrun.test_info.name = record.station_id
  if 'test_description' in record.metadata:
    testrun.test_info.description = record.metadata['test_description']
  if 'test_version' in record.metadata:
    testrun.test_info.version_string = record.metadata['test_version']
  testrun.test_status = OUTCOME_MAP[record.outcome]
  testrun.start_time_millis = record.start_time_millis
  testrun.end_time_millis = record.end_time_millis
  if 'run_name' in record.metadata:
    testrun.run_name = record.metadata['run_name']
  for details in record.outcome_details:
    testrun_code = testrun.failure_codes.add()
    testrun_code.code = details.code
    testrun_code.details = details.description
  for phase in record.phases:
    testrun_phase = testrun.phases.add()
    testrun_phase.name = phase.name
    testrun_phase.description = phase.codeinfo.sourcecode
    testrun_phase.timing.start_time_millis = phase.start_time_millis
    testrun_phase.timing.end_time_millis = phase.end_time_millis
  if 'config' in record.metadata:
    attachment = testrun.info_parameters.add()
    attachment.name = 'config'
    attachment.value_binary = json.dumps(
        record.metadata['config'], sort_keys=True, indent=4).encode('utf-8')


def _ensure_unique_parameter_name(name, used_parameter_names):
  while name in used_parameter_names:
    name += '_'  # Hack to avoid collisions between phases.
  used_parameter_names.add(name)
  return name


def _attach_json(record, testrun):
  """Attach a copy of the JSON-ified record as an info parameter.

  Save a copy of the JSON-ified record in an attachment so we can access
  un-mangled fields later if we want.  Remove attachments since those get
  copied over and can potentially be quite large.

  Args:
    record: the OpenHTF TestRecord being converted
    testrun: a TestRun proto
  """
  record_json = json_factory.OutputToJSON(
      inline_attachments=False,
      sort_keys=True, indent=2).serialize_test_record(record)
  testrun_param = testrun.info_parameters.add()
  testrun_param.name = 'OpenHTF_record.json'
  testrun_param.value_binary = record_json.encode('utf-8')
  # pylint: disable=no-member
  testrun_param.type = test_runs_pb2.TEXT_UTF8
  # pylint: enable=no-member


def _extract_attachments(phase, testrun, used_parameter_names):
  """Extract attachments, just copy them over."""
  for name, attachment in sorted(six.iteritems(phase.attachments)):
    attachment_data, mimetype = attachment
    name = _ensure_unique_parameter_name(name, used_parameter_names)
    testrun_param = testrun.info_parameters.add()
    testrun_param.name = name
    if isinstance(attachment_data, unicode):
      attachment_data = attachment_data.encode('utf-8')
    testrun_param.value_binary = attachment_data
    if mimetype in MIMETYPE_MAP:
      testrun_param.type = MIMETYPE_MAP[mimetype]
    else:
      # pylint: disable=no-member
      testrun_param.type = test_runs_pb2.BINARY
      # pylint: enable=no-member


def _mangle_measurement(name, measured_value, measurement, mangled_parameters,
                        attachment_name):  # pylint: disable=g-doc-args
  """Flatten parameters for backwards compatibility, watch for collisions.

  We generate these by doing some name mangling, using some sane limits for
  very large multidimensional measurements.
  """
  for coord, val in list(measured_value.value_dict.items(
      ))[:MAX_PARAMS_PER_MEASUREMENT]:
    # Mangle names so they look like 'myparameter_Xsec_Ynm_ZHz'
    mangled_name = '_'.join([name] + [
        '%s%s' % (
            dim_val,
            dim_units.suffix if dim_units.suffix else '') for
        dim_val, dim_units in zip(
            coord, measurement.dimensions)])
    while mangled_name in mangled_parameters:
      logging.warning('Mangled name %s already in use', mangled_name)
      mangled_name += '_'
    mangled_param = test_runs_pb2.TestParameter()
    mangled_param.name = mangled_name
    mangled_param.associated_attachment = attachment_name
    mangled_param.description = (
        'Mangled parameter from measurement %s with dimensions %s' % (
            name, tuple(d.suffix for d in measurement.dimensions)))

    if isinstance(val, numbers.Number):
      mangled_param.numeric_value = float(val)
    else:
      mangled_param.text_value = str(val)
    # Check for validators we know how to translate.
    for validator in measurement.validators:
      mangled_param.description += '\nValidator: ' + str(validator)

    if measurement.units and measurement.units.code in UOM_CODE_MAP:
      mangled_param.unit_code = UOM_CODE_MAP[measurement.units.code]
    mangled_parameters[mangled_name] = mangled_param


def _extract_parameters(record, testrun, used_parameter_names):
  """Extract parameters from phases."""
  # Generate mangled parameters afterwards so we give real measurements priority
  # getting names.
  mangled_parameters = {}
  for phase in record.phases:
    _extract_attachments(phase, testrun, used_parameter_names)
    for name, measurement in sorted(six.iteritems(phase.measurements)):
      tr_name = _ensure_unique_parameter_name(name, used_parameter_names)
      testrun_param = testrun.test_parameters.add()
      testrun_param.name = tr_name
      if measurement.docstring:
        testrun_param.description = measurement.docstring
      if measurement.units and measurement.units.code in UOM_CODE_MAP:
        testrun_param.unit_code = UOM_CODE_MAP[measurement.units.code]

      if measurement.outcome == measurements.Outcome.PASS:
        testrun_param.status = test_runs_pb2.PASS
      elif (not measurement.measured_value
            or not measurement.measured_value.is_value_set):
        testrun_param.status = test_runs_pb2.ERROR
        continue
      else:
        testrun_param.status = test_runs_pb2.FAIL

      value = None
      if measurement.measured_value.is_value_set:
        value = measurement.measured_value.value
      else:
        testrun_param.status = test_runs_pb2.ERROR
      if measurement.dimensions is None:
        # Just a plain ol' value.
        if isinstance(value, numbers.Number):
          testrun_param.numeric_value = float(value)
        else:
          testrun_param.text_value = str(value)
        # Check for validators we know how to translate.
        for validator in measurement.validators:
          if isinstance(validator, validators.RangeValidatorBase):
            if validator.minimum is not None:
              testrun_param.numeric_minimum = float(validator.minimum)
            if validator.maximum is not None:
              testrun_param.numeric_maximum = float(validator.maximum)
          elif isinstance(validator, validators.RegexMatcher):
            testrun_param.expected_text = validator.regex
          else:
            testrun_param.description += '\nValidator: ' + str(validator)
      else:
        attachment = testrun.info_parameters.add()
        attachment.name = 'multidim_%s' % name
        dims = [{
            'uom_suffix': d.suffix,
            'uom_code': d.code}
                for d in measurement.dimensions]
        # Refer to the module docstring for the expected schema.
        attachment.value_binary = json.dumps({
            'outcome': str(testrun_param.status), 'name': name,
            'dimensions': dims,
            'value': value
        }, sort_keys=True).encode('utf-8')
        attachment.type = test_runs_pb2.MULTIDIM_JSON
        _mangle_measurement(
            name, measurement.measured_value, measurement, mangled_parameters,
            attachment.name)
      if testrun_param.status == test_runs_pb2.FAIL:
        testrun_code = testrun.failure_codes.add()
        testrun_code.code = testrun_param.name
        if measurement.dimensions is None:
          if isinstance(testrun_param.numeric_value, float):
            testrun_code.details = str(testrun_param.numeric_value)
          else:
            testrun_code.details = testrun_param.text_value
  return mangled_parameters


def _add_mangled_parameters(testrun, mangled_parameters, used_parameter_names):
  """Add any mangled parameters we generated from multidim measurements."""
  for mangled_name, mangled_param in sorted(six.iteritems(mangled_parameters)):
    if mangled_name != _ensure_unique_parameter_name(mangled_name,
                                                     used_parameter_names):
      logging.warning('Mangled name %s in use by non-mangled parameter',
                      mangled_name)
    testrun_param = testrun.test_parameters.add()
    testrun_param.CopyFrom(mangled_param)


def _add_log_lines(record, testrun):
  """Copy log records over, this is a fairly straightforward mapping."""
  for log in record.log_records:
    testrun_log = testrun.test_logs.add()
    testrun_log.timestamp_millis = log.timestamp_millis
    testrun_log.log_message = log.message
    testrun_log.logger_name = log.logger_name
    testrun_log.levelno = log.level
    # pylint: disable=no-member
    if log.level <= logging.DEBUG:
      testrun_log.level = test_runs_pb2.TestRunLogMessage.DEBUG
    elif log.level <= logging.INFO:
      testrun_log.level = test_runs_pb2.TestRunLogMessage.INFO
    elif log.level <= logging.WARNING:
      testrun_log.level = test_runs_pb2.TestRunLogMessage.WARNING
    elif log.level <= logging.ERROR:
      testrun_log.level = test_runs_pb2.TestRunLogMessage.ERROR
    elif log.level <= logging.CRITICAL:
      testrun_log.level = test_runs_pb2.TestRunLogMessage.CRITICAL
    # pylint: enable=no-member
    testrun_log.log_source = log.source
    testrun_log.lineno = log.lineno


def test_run_from_test_record(record):
  """Create a TestRun proto from an OpenHTF TestRecord.

  Most fields are just copied over, some are pulled out of metadata (listed
  below), and measurements are munged a bit for backwards compatibility.

  Metadata fields:
    'test_description': TestInfo's description field.
    'test_version': TestInfo's version_string field.
    'test_name': TestInfo's name field.
    'run_name': TestRun's run_name field.
    'operator_name': TestRun's operator_name field.

  Args:
    record: the OpenHTF TestRecord being converted

  Returns:
    An instance of the TestRun proto for the given record.
  """
  testrun = test_runs_pb2.TestRun()
  _populate_header(record, testrun)
  _attach_json(record, testrun)

  used_parameter_names = set(['OpenHTF_record.json'])
  mangled_parameters = _extract_parameters(record, testrun,
                                           used_parameter_names)
  _add_mangled_parameters(testrun, mangled_parameters, used_parameter_names)
  _add_log_lines(record, testrun)
  return testrun
