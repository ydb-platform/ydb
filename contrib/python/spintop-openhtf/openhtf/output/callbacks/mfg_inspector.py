"""Output and/or upload a TestRun or MfgEvent proto for mfg-inspector.com.
"""

import json
import logging
import threading
import time
import zlib

import httplib2
import oauth2client.client

from openhtf.output import callbacks
from openhtf.output.proto import guzzle_pb2
from openhtf.output.proto import test_runs_converter

import six
from six.moves import range


class UploadFailedError(Exception):
  """Raised when an upload to mfg-inspector fails."""


class InvalidTestRunError(Exception):
  """Raised if test run is invalid."""


def _send_mfg_inspector_request(envelope_data, credentials, destination_url):
  """Send upload http request.  Intended to be run in retry loop."""
  logging.info('Uploading result...')
  http = httplib2.Http()

  if credentials.access_token_expired:
    credentials.refresh(http)
  credentials.authorize(http)

  resp, content = http.request(destination_url, 'POST', envelope_data)

  try:
    result = json.loads(content)
  except Exception:
    logging.debug('Upload failed with response %s: %s', resp, content)
    raise UploadFailedError(resp, content)

  if resp.status != 200:
    logging.debug('Upload failed: %s', result)
    raise UploadFailedError(result['error'], result)

  return result


def send_mfg_inspector_data(inspector_proto, credentials, destination_url):
  """Upload MfgEvent to steam_engine."""
  envelope = guzzle_pb2.TestRunEnvelope()
  envelope.payload = zlib.compress(inspector_proto.SerializeToString())
  envelope.payload_type = guzzle_pb2.COMPRESSED_MFG_EVENT
  envelope_data = envelope.SerializeToString()

  for _ in range(5):
    try:
      result = _send_mfg_inspector_request(
          envelope_data, credentials, destination_url)
      return result
    except UploadFailedError:
      time.sleep(1)

  logging.critical(
      'Could not upload to mfg-inspector after 5 attempts. Giving up.')

  return {}


class _MemStorage(oauth2client.client.Storage):
  # pylint: disable=invalid-name
  """Helper Storage class that keeps credentials in memory."""

  def __init__(self):
    self._lock = threading.Lock()
    self._credentials = None

  def acquire_lock(self):
    self._lock.acquire(True)

  def release_lock(self):
    self._lock.release()

  def locked_get(self):
    return self._credentials

  def locked_put(self, credentials):
    self._credentials = credentials


class MfgInspector(object):
  """Interface to convert a TestRun to a mfg-inspector compatible proto.

  Instances of this class are typically used to create callbacks that are
  compatible with the OpenHTF output callbacks.

  Typical usage:
  interface = mfg_inspector.MfgInspector.from_json().set_converter(
    my_custom_converter)
  my_tester.add_output_callbacks(interface.save_to_disk(), interface.upload())

  **Important** the conversion of the TestRecord to protofbuf as specified in
  the _converter callable attribute only occurs once and the resulting protobuf
  is cached in memory on the instance.

  The upload callback will upload to mfg-inspector.com using the given
  username and authentication key (which should be the key data itself, not a
  filename or file).

  In typical productin setups, we *first* save the protobuf to disk then attempt
  to upload the protobuf to mfg-inspector.  In the event of a network outage,
  the result of the test run is available on disk and a separate process can
  retry the upload when network is available.
  """

  TOKEN_URI = 'https://accounts.google.com/o/oauth2/token'
  SCOPE_CODE_URI = 'https://www.googleapis.com/auth/glass.infra.quantum_upload'
  DESTINATION_URL = ('https://clients2.google.com/factoryfactory/'
                     'uploads/quantum_upload/?json')

  # These attributes control format of callback and what actions are undertaken
  # when called.  These should either be set by a subclass or via configure.

  # _converter is a callable that can be set either via set_converter method
  # or by defining a _converter @staticmethod on subclasses.
  _converter = None

  # A default filename pattern can be specified on subclasses for use when
  # saving to disk via save_to_disk.
  _default_filename_pattern = None

  def __init__(self, user=None, keydata=None,
               token_uri=TOKEN_URI, destination_url=DESTINATION_URL):
    self.user = user
    self.keydata = keydata
    self.token_uri = token_uri
    self.destination_url = destination_url

    if user and keydata:
      self.credentials = oauth2client.client.SignedJwtAssertionCredentials(
          service_account_name=self.user,
          private_key=six.ensure_binary(self.keydata),
          scope=self.SCOPE_CODE_URI,
          user_agent='OpenHTF Guzzle Upload Client',
          token_uri=self.token_uri)
      self.credentials.set_store(_MemStorage())
    else:
      self.credentials = None

    self.upload_result = None

    self._cached_proto = None

  @classmethod
  def from_json(cls, json_data):
    """Create an uploader given (parsed) JSON data.

    Note that this is a JSON-formatted key file downloaded from Google when
    the service account key is created, *NOT* a json-encoded
    oauth2client.client.SignedJwtAssertionCredentials object.

    Args:
      json_data: Dict containing the loaded JSON key data.

    Returns:
      a MfgInspectorCallback with credentials.
    """
    return cls(user=json_data['client_email'],
               keydata=json_data['private_key'],
               token_uri=json_data['token_uri'])

  def _convert(self, test_record_obj):
    """Convert and cache a test record to a mfg-inspector proto."""

    if self._cached_proto is None:
      self._cached_proto = self._converter(test_record_obj)

    return self._cached_proto

  def save_to_disk(self, filename_pattern=None):
    """Returns a callback to convert test record to proto and save to disk."""
    if not self._converter:
      raise RuntimeError(
          'Must set _converter on subclass or via set_converter before calling '
          'save_to_disk.')

    pattern = filename_pattern or self._default_filename_pattern
    if not pattern:
      raise RuntimeError(
          'Must specify provide a filename_pattern or set a '
          '_default_filename_pattern on subclass.')

    def save_to_disk_callback(test_record_obj):
      proto = self._convert(test_record_obj)
      output_to_file = callbacks.OutputToFile(pattern)

      with output_to_file.open_output_file(test_record_obj) as outfile:
        outfile.write(proto.SerializeToString())

    return save_to_disk_callback

  def upload(self):
    """Returns a callback to convert a test record to a proto and upload."""
    if not self._converter:
      raise RuntimeError(
          'Must set _converter on subclass or via set_converter before calling '
          'upload.')

    if not self.credentials:
      raise RuntimeError('Must provide credentials to use upload callback.')

    def upload_callback(test_record_obj):
      proto = self._convert(test_record_obj)
      self.upload_result = send_mfg_inspector_data(
          proto, self.credentials, self.destination_url)

    return upload_callback

  def set_converter(self, converter):
    """Set converter callable to convert a OpenHTF tester_record to a proto.

    Args:
      converter: a callable that accepts an OpenHTF TestRecord and returns a
        manufacturing-inspector compatible protobuf.

    Returns:
      self to make this call chainable.
    """
    assert callable(converter), 'Converter must be callable.'

    self._converter = converter

    return self


# LEGACY / DEPRECATED
class UploadToMfgInspector(MfgInspector):
  """Generate a mfg-inspector TestRun proto and upload it.

  LEGACY / DEPRECATED
  This class is provided only for legacy reasons and may be deleted in future.
  Please replace usage by configuring a MfgInspectorCallback directly. For
  example:
  test.add_output_callbacks(
    mfg_inspector.MfgInspectorCallback.from_json(**json_data).set_converter(
      test_runs_converter.test_run_from_test_record).upload()
  )
  """

  @staticmethod
  def _converter(test_record_obj):
    return test_runs_converter.test_run_from_test_record(test_record_obj)

  def __call__(self, test_record_obj):  # pylint: disable=invalid-name
    upload_callback = self.upload()
    upload_callback(test_record_obj)

