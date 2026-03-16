import threading
import logging
import os

from .exceptions.exceptions import SegmentNotFoundException
from .models.dummy_entities import DummySegment
from aws_xray_sdk import global_sdk_config


log = logging.getLogger(__name__)

MISSING_SEGMENT_MSG = 'cannot find the current segment/subsegment, please make sure you have a segment open'
SUPPORTED_CONTEXT_MISSING = ('RUNTIME_ERROR', 'LOG_ERROR', 'IGNORE_ERROR')
CXT_MISSING_STRATEGY_KEY = 'AWS_XRAY_CONTEXT_MISSING'


class Context:
    """
    The context storage class to store trace entities(segments/subsegments).
    The default implementation uses threadlocal to store these entities.
    It also provides interfaces to manually inject trace entities which will
    replace the current stored entities and to clean up the storage.

    For any data access or data mutation, if there is no active segment present
    it will use user-defined behavior to handle such case. By default it throws
    an runtime error.

    This data structure is thread-safe.
    """
    def __init__(self, context_missing='LOG_ERROR'):

        self._local = threading.local()
        strategy = os.getenv(CXT_MISSING_STRATEGY_KEY, context_missing)
        self._context_missing = strategy

    def put_segment(self, segment):
        """
        Store the segment created by ``xray_recorder`` to the context.
        It overrides the current segment if there is already one.
        """
        setattr(self._local, 'entities', [segment])

    def end_segment(self, end_time=None):
        """
        End the current active segment.

        :param float end_time: epoch in seconds. If not specified the current
            system time will be used.
        """
        entity = self.get_trace_entity()
        if not entity:
            log.warning("No segment to end")
            return
        if self._is_subsegment(entity):
            entity.parent_segment.close(end_time)
        else:
            entity.close(end_time)

    def put_subsegment(self, subsegment):
        """
        Store the subsegment created by ``xray_recorder`` to the context.
        If you put a new subsegment while there is already an open subsegment,
        the new subsegment becomes the child of the existing subsegment.
        """
        entity = self.get_trace_entity()
        if not entity:
            log.warning("Active segment or subsegment not found. Discarded %s." % subsegment.name)
            return

        entity.add_subsegment(subsegment)
        self._local.entities.append(subsegment)

    def end_subsegment(self, end_time=None):
        """
        End the current active segment. Return False if there is no
        subsegment to end.

        :param float end_time: epoch in seconds. If not specified the current
            system time will be used.
        """
        entity = self.get_trace_entity()
        if self._is_subsegment(entity):
            entity.close(end_time)
            self._local.entities.pop()
            return True
        elif isinstance(entity, DummySegment):
            return False
        else:
            log.warning("No subsegment to end.")
            return False

    def get_trace_entity(self):
        """
        Return the current trace entity(segment/subsegment). If there is none,
        it behaves based on pre-defined ``context_missing`` strategy.
        If the SDK is disabled, returns a DummySegment
        """
        if not getattr(self._local, 'entities', None):
            if not global_sdk_config.sdk_enabled():
                return DummySegment()
            return self.handle_context_missing()

        return self._local.entities[-1]

    def set_trace_entity(self, trace_entity):
        """
        Store the input trace_entity to local context. It will overwrite all
        existing ones if there is any.
        """
        setattr(self._local, 'entities', [trace_entity])

    def clear_trace_entities(self):
        """
        clear all trace_entities stored in the local context.
        In case of using threadlocal to store trace entites, it will
        clean up all trace entities created by the current thread.
        """
        self._local.__dict__.clear()

    def handle_context_missing(self):
        """
        Called whenever there is no trace entity to access or mutate.
        """
        if self.context_missing == 'RUNTIME_ERROR':
            raise SegmentNotFoundException(MISSING_SEGMENT_MSG)
        elif self.context_missing == 'LOG_ERROR':
            log.error(MISSING_SEGMENT_MSG)

    def _is_subsegment(self, entity):

        return hasattr(entity, 'type') and entity.type == 'subsegment'

    @property
    def context_missing(self):
        return self._context_missing

    @context_missing.setter
    def context_missing(self, value):
        if value not in SUPPORTED_CONTEXT_MISSING:
            log.warning('specified context_missing not supported, using default.')
            return

        self._context_missing = value
