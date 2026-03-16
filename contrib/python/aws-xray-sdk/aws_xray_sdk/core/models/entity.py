import logging
import os
import binascii
import time
import string

import json

from ..utils.compat import annotation_value_types
from ..utils.conversion import metadata_to_dict
from .throwable import Throwable
from . import http
from ..exceptions.exceptions import AlreadyEndedException

log = logging.getLogger(__name__)

# Valid characters can be found at http://docs.aws.amazon.com/xray/latest/devguide/xray-api-segmentdocuments.html
_common_invalid_name_characters = '?;*()!$~^<>'
_valid_annotation_key_characters = string.ascii_letters + string.digits + '_'

ORIGIN_TRACE_HEADER_ATTR_KEY = '_origin_trace_header'


class Entity:
    """
    The parent class for segment/subsegment. It holds common properties
    and methods on segment and subsegment.
    """

    def __init__(self, name, entity_id=None):
        if not entity_id:
            self.id = self._generate_random_id()
        else:
            self.id = entity_id

        # required attributes
        self.name = name
        self.name = ''.join([c for c in name if c not in _common_invalid_name_characters])
        self.start_time = time.time()
        self.parent_id = None

        if self.name != name:
            log.warning("Removing Segment/Subsugment Name invalid characters from {}.".format(name))

        # sampling
        self.sampled = True

        # state
        self.in_progress = True

        # meta fields
        self.http = {}
        self.annotations = {}
        self.metadata = {}
        self.aws = {}
        self.cause = {}

        # child subsegments
        # list is thread-safe
        self.subsegments = []

    def close(self, end_time=None):
        """
        Close the trace entity by setting `end_time`
        and flip the in progress flag to False.

        :param float end_time: Epoch in seconds. If not specified
            current time will be used.
        """
        self._check_ended()

        if end_time:
            self.end_time = end_time
        else:
            self.end_time = time.time()
        self.in_progress = False

    def add_subsegment(self, subsegment):
        """
        Add input subsegment as a child subsegment.
        """
        self._check_ended()
        subsegment.parent_id = self.id

        if not self.sampled and subsegment.sampled:
            log.warning("This sampled subsegment is being added to an unsampled parent segment/subsegment and will be orphaned.")

        self.subsegments.append(subsegment)

    def remove_subsegment(self, subsegment):
        """
        Remove input subsegment from child subsegments.
        """
        self.subsegments.remove(subsegment)

    def put_http_meta(self, key, value):
        """
        Add http related metadata.

        :param str key: Currently supported keys are:
            * url
            * method
            * user_agent
            * client_ip
            * status
            * content_length
        :param value: status and content_length are int and for other
            supported keys string should be used.
        """
        self._check_ended()

        if value is None:
            return

        if key == http.STATUS:
            if isinstance(value, str):
                value = int(value)
            self.apply_status_code(value)

        if key in http.request_keys:
            if 'request' not in self.http:
                self.http['request'] = {}
            self.http['request'][key] = value
        elif key in http.response_keys:
            if 'response' not in self.http:
                self.http['response'] = {}
            self.http['response'][key] = value
        else:
            log.warning("ignoring unsupported key %s in http meta.", key)

    def put_annotation(self, key, value):
        """
        Annotate segment or subsegment with a key-value pair.
        Annotations will be indexed for later search query.

        :param str key: annotation key
        :param object value: annotation value. Any type other than
            string/number/bool will be dropped
        """
        self._check_ended()

        if not isinstance(key, str):
            log.warning("ignoring non string type annotation key with type %s.", type(key))
            return

        if not isinstance(value, annotation_value_types):
            log.warning("ignoring unsupported annotation value type %s.", type(value))
            return

        if any(character not in _valid_annotation_key_characters for character in key):
            log.warning("ignoring annnotation with unsupported characters in key: '%s'.", key)
            return

        self.annotations[key] = value

    def put_metadata(self, key, value, namespace='default'):
        """
        Add metadata to segment or subsegment. Metadata is not indexed
        but can be later retrieved by BatchGetTraces API.

        :param str namespace: optional. Default namespace is `default`.
            It must be a string and prefix `AWS.` is reserved.
        :param str key: metadata key under specified namespace
        :param object value: any object that can be serialized into JSON string
        """
        self._check_ended()

        if not isinstance(namespace, str):
            log.warning("ignoring non string type metadata namespace")
            return

        if namespace.startswith('AWS.'):
            log.warning("Prefix 'AWS.' is reserved, drop metadata with namespace %s", namespace)
            return

        if self.metadata.get(namespace, None):
            self.metadata[namespace][key] = value
        else:
            self.metadata[namespace] = {key: value}

    def set_aws(self, aws_meta):
        """
        set aws section of the entity.
        This method is called by global recorder and botocore patcher
        to provide additonal information about AWS runtime.
        It is not recommended to manually set aws section.
        """
        self._check_ended()
        self.aws = aws_meta

    def add_throttle_flag(self):
        self.throttle = True

    def add_fault_flag(self):
        self.fault = True

    def add_error_flag(self):
        self.error = True

    def apply_status_code(self, status_code):
        """
        When a trace entity is generated under the http context,
        the status code will affect this entity's fault/error/throttle flags.
        Flip these flags based on status code.
        """
        self._check_ended()
        if not status_code:
            return

        if status_code >= 500:
            self.add_fault_flag()
        elif status_code == 429:
            self.add_throttle_flag()
            self.add_error_flag()
        elif status_code >= 400:
            self.add_error_flag()

    def add_exception(self, exception, stack, remote=False):
        """
        Add an exception to trace entities.

        :param Exception exception: the caught exception.
        :param list stack: the output from python built-in
            `traceback.extract_stack()`.
        :param bool remote: If False it means it's a client error
            instead of a downstream service.
        """
        self._check_ended()
        self.add_fault_flag()

        if hasattr(exception, '_recorded'):
            setattr(self, 'cause', getattr(exception, '_cause_id'))
            return

        if not isinstance(self.cause, dict):
            log.warning("The current cause object is not a dict but an id: {}. Resetting the cause and recording the "
                        "current exception".format(self.cause))
            self.cause = {}

        if 'exceptions' in self.cause:
            exceptions = self.cause['exceptions']
        else:
            exceptions = []

        exceptions.append(Throwable(exception, stack, remote))

        self.cause['exceptions'] = exceptions
        self.cause['working_directory'] = os.getcwd()

    def save_origin_trace_header(self, trace_header):
        """
        Temporarily store additional data fields in trace header
        to the entity for later propagation. The data will be
        cleaned up upon serialization.
        """
        setattr(self, ORIGIN_TRACE_HEADER_ATTR_KEY, trace_header)

    def get_origin_trace_header(self):
        """
        Retrieve saved trace header data.
        """
        return getattr(self, ORIGIN_TRACE_HEADER_ATTR_KEY, None)

    def serialize(self):
        """
        Serialize to JSON document that can be accepted by the
        X-Ray backend service. It uses json to perform serialization.
        """
        return json.dumps(self.to_dict(), default=str)

    def to_dict(self):
        """
        Convert Entity(Segment/Subsegment) object to dict
        with required properties that have non-empty values.
        """
        entity_dict = {}

        for key, value in vars(self).items():
            if isinstance(value, bool) or value:
                if key == 'subsegments':
                    # child subsegments are stored as List
                    subsegments = []
                    for subsegment in value:
                        subsegments.append(subsegment.to_dict())
                    entity_dict[key] = subsegments
                elif key == 'cause':
                    if isinstance(self.cause, dict):
                        entity_dict[key] = {}
                        entity_dict[key]['working_directory'] = self.cause['working_directory']
                        # exceptions are stored as List
                        throwables = []
                        for throwable in value['exceptions']:
                            throwables.append(throwable.to_dict())
                        entity_dict[key]['exceptions'] = throwables
                    else:
                        entity_dict[key] = self.cause
                elif key == 'metadata':
                    entity_dict[key] = metadata_to_dict(value)
                elif key != 'sampled' and key != ORIGIN_TRACE_HEADER_ATTR_KEY:
                    entity_dict[key] = value

        return entity_dict

    def _check_ended(self):
        if not self.in_progress:
            raise AlreadyEndedException("Already ended segment and subsegment cannot be modified.")

    def _generate_random_id(self):
        """
        Generate a random 16-digit hex str.
        This is used for generating segment/subsegment id.
        """
        return binascii.b2a_hex(os.urandom(8)).decode('utf-8')
