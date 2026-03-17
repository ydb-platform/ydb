"""Module for outputting test record to JSON-formatted files."""

import base64
import json

from openhtf.output import callbacks
from openhtf.util import data
import six


class OutputToJSON(callbacks.OutputToFile):
  """Return an output callback that writes JSON Test Records.
  Example filename_patterns might be:
    '/data/test_records/{dut_id}.{metadata[test_name]}.json', indent=4)) or
    '/data/test_records/%(dut_id)s.%(start_time_millis)s'
  To use this output mechanism:
    test = openhtf.Test(PhaseOne, PhaseTwo)
    test.add_output_callback(openhtf.output.callbacks.OutputToJson(
        '/data/test_records/{dut_id}.{metadata[test_name]}.json'))

  Args:
    filename_pattern: A format string specifying the filename to write to,
      will be formatted with the Test Record as a dictionary.  May also be a
      file-like object to write to directly.
    inline_attachments: Whether attachments should be included inline in the
      output. Set to False if you expect to have large binary attachments. If
      True (the default), then attachments are base64 encoded to allow for
      binary data that's not supported by JSON directly.
  """

  def __init__(self, filename_pattern=None, inline_attachments=True, **kwargs):
    super(OutputToJSON, self).__init__(filename_pattern)
    self.inline_attachments = inline_attachments

    # Conform strictly to the JSON spec by default.
    kwargs.setdefault('allow_nan', False)
    self.allow_nan = kwargs['allow_nan']
    self.json_encoder = json.JSONEncoder(**kwargs)

  def serialize_test_record(self, test_record):
    return self.json_encoder.encode(self.convert_to_dict(test_record))

  def convert_to_dict(self, test_record):
    as_dict = data.convert_to_base_types(test_record,
                                         json_safe=(not self.allow_nan))
    if self.inline_attachments:
      for phase, original_phase in zip(as_dict['phases'], test_record.phases):
        for name, attachment in six.iteritems(phase['attachments']):
          original_data = original_phase.attachments[name].data
          attachment['data'] = base64.standard_b64encode(original_data).decode('utf-8')
    return as_dict
