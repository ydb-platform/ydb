import os
import logging
import threading

from aws_xray_sdk import global_sdk_config
from .models.dummy_entities import DummySegment
from .models.facade_segment import FacadeSegment
from .models.trace_header import TraceHeader
from .context import Context

log = logging.getLogger(__name__)


LAMBDA_TRACE_HEADER_KEY = '_X_AMZN_TRACE_ID'
LAMBDA_TASK_ROOT_KEY = 'LAMBDA_TASK_ROOT'
TOUCH_FILE_DIR = '/tmp/.aws-xray/'
TOUCH_FILE_PATH = '/tmp/.aws-xray/initialized'


def check_in_lambda():
    """
    Return None if SDK is not loaded in AWS Lambda worker.
    Otherwise drop a touch file and return a lambda context.
    """
    if not os.getenv(LAMBDA_TASK_ROOT_KEY):
        return None

    try:
        os.mkdir(TOUCH_FILE_DIR)
    except OSError:
        log.debug('directory %s already exists', TOUCH_FILE_DIR)

    try:
        f = open(TOUCH_FILE_PATH, 'w+')
        f.close()
        # utime force second parameter in python2.7
        os.utime(TOUCH_FILE_PATH, None)
    except (IOError, OSError):
        log.warning("Unable to write to %s. Failed to signal SDK initialization." % TOUCH_FILE_PATH)

    return LambdaContext()


class LambdaContext(Context):
    """
    Lambda service will generate a segment for each function invocation which
    cannot be mutated. The context doesn't keep any manually created segment
    but instead every time ``get_trace_entity()`` gets called it refresh the
    segment based on environment variables set by Lambda worker.
    """
    def __init__(self):

        self._local = threading.local()

    def put_segment(self, segment):
        """
        No-op.
        """
        log.warning('Cannot create segments inside Lambda function. Discarded.')

    def end_segment(self, end_time=None):
        """
        No-op.
        """
        log.warning('Cannot end segment inside Lambda function. Ignored.')

    def put_subsegment(self, subsegment):
        """
        Refresh the segment every time this function is invoked to prevent
        a new subsegment from being attached to a leaked segment/subsegment.
        """
        current_entity = self.get_trace_entity()

        if not self._is_subsegment(current_entity) and (getattr(current_entity, 'initializing', None) or isinstance(current_entity, DummySegment)):
            if global_sdk_config.sdk_enabled() and not os.getenv(LAMBDA_TRACE_HEADER_KEY):
                log.warning("Subsegment %s discarded due to Lambda worker still initializing" % subsegment.name)
            return

        current_entity.add_subsegment(subsegment)
        self._local.entities.append(subsegment)

    def set_trace_entity(self, trace_entity):
        """
        For Lambda context, we additionally store the segment in the thread local.
        """
        if self._is_subsegment(trace_entity):
            segment = trace_entity.parent_segment
        else:
            segment = trace_entity

        setattr(self._local, 'segment', segment)
        setattr(self._local, 'entities', [trace_entity])

    def get_trace_entity(self):
        self._refresh_context()
        if getattr(self._local, 'entities', None):
            return self._local.entities[-1]
        else:
            return self._local.segment

    def _refresh_context(self):
        """
        Get current segment. To prevent resource leaking in Lambda worker,
        every time there is segment present, we compare its trace id to current
        environment variables. If it is different we create a new segment
        and clean up subsegments stored.
        """
        header_str = os.getenv(LAMBDA_TRACE_HEADER_KEY)
        trace_header = TraceHeader.from_header_str(header_str)
        if not global_sdk_config.sdk_enabled():
            trace_header._sampled = False

        segment = getattr(self._local, 'segment', None)

        if segment:
            # Ensure customers don't have leaked subsegments across invocations
            if not trace_header.root or trace_header.root == segment.trace_id:
                return
            else:
                self._initialize_context(trace_header)
        else:
            self._initialize_context(trace_header)

    @property
    def context_missing(self):
        return None

    @context_missing.setter
    def context_missing(self, value):
        pass

    def handle_context_missing(self):
        """
        No-op.
        """
        pass

    def _initialize_context(self, trace_header):
        """
        Create a segment based on environment variables set by
        AWS Lambda and initialize storage for subsegments.
        """
        sampled = None
        if not global_sdk_config.sdk_enabled():
            # Force subsequent subsegments to be disabled and turned into DummySegments.
            sampled = False
        elif trace_header.sampled == 0:
            sampled = False
        elif trace_header.sampled == 1:
            sampled = True

        segment = None
        if not trace_header.root or not trace_header.parent or trace_header.sampled is None:
            segment = DummySegment()
            log.debug("Creating NoOp/Dummy parent segment")
        else:
            segment = FacadeSegment(
                name='facade',
                traceid=trace_header.root,
                entityid=trace_header.parent,
                sampled=sampled,
            )
        segment.save_origin_trace_header(trace_header)
        setattr(self._local, 'segment', segment)
        setattr(self._local, 'entities', [])
