import copy
import traceback

from .entity import Entity
from .traceid import TraceId
from ..utils.atomic_counter import AtomicCounter
from ..exceptions.exceptions import SegmentNameMissingException

ORIGIN_TRACE_HEADER_ATTR_KEY = '_origin_trace_header'


class SegmentContextManager:
    """
    Wrapper for segment and recorder to provide segment context manager.
    """

    def __init__(self, recorder, name=None, **segment_kwargs):
        self.name = name
        self.segment_kwargs = segment_kwargs
        self.recorder = recorder
        self.segment = None

    def __enter__(self):
        self.segment = self.recorder.begin_segment(
            name=self.name, **self.segment_kwargs)
        return self.segment

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.segment is None:
            return

        if exc_type is not None:
            self.segment.add_exception(
                exc_val,
                traceback.extract_tb(
                    exc_tb,
                    limit=self.recorder.max_trace_back,
                )
            )
        self.recorder.end_segment()


class Segment(Entity):
    """
    The compute resources running your application logic send data
    about their work as segments. A segment provides the resource's name,
    details about the request, and details about the work done.
    """
    def __init__(self, name, entityid=None, traceid=None,
                 parent_id=None, sampled=True):
        """
        Create a segment object.

        :param str name: segment name. If not specified a
            SegmentNameMissingException will be thrown.
        :param str entityid: hexdigits segment id.
        :param str traceid: The trace id of the segment.
        :param str parent_id: The parent id of the segment. It comes
            from id of an upstream segment or subsegment.
        :param bool sampled: If False this segment will not be sent
            to the X-Ray daemon.
        """
        if not name:
            raise SegmentNameMissingException("Segment name is required.")

        super().__init__(name)

        if not traceid:
            traceid = TraceId().to_id()
        self.trace_id = traceid
        if entityid:
            self.id = entityid

        self.in_progress = True
        self.sampled = sampled
        self.user = None
        self.ref_counter = AtomicCounter()
        self._subsegments_counter = AtomicCounter()

        if parent_id:
            self.parent_id = parent_id

    def add_subsegment(self, subsegment):
        """
        Add input subsegment as a child subsegment and increment
        reference counter and total subsegments counter.
        """
        super().add_subsegment(subsegment)
        self.increment()

    def increment(self):
        """
        Increment reference counter to track on open subsegments
        and total subsegments counter to track total size of subsegments
        it currently hold.
        """
        self.ref_counter.increment()
        self._subsegments_counter.increment()

    def decrement_ref_counter(self):
        """
        Decrement reference counter by 1 when a subsegment is closed.
        """
        self.ref_counter.decrement()

    def ready_to_send(self):
        """
        Return True if the segment doesn't have any open subsegments
        and itself is not in progress.
        """
        return self.ref_counter.get_current() <= 0 and not self.in_progress

    def get_total_subsegments_size(self):
        """
        Return the number of total subsegments regardless of open or closed.
        """
        return self._subsegments_counter.get_current()

    def decrement_subsegments_size(self):
        """
        Decrement total subsegments by 1. This usually happens when
        a subsegment is streamed out.
        """
        return self._subsegments_counter.decrement()

    def remove_subsegment(self, subsegment):
        """
        Remove the reference of input subsegment.
        """
        super().remove_subsegment(subsegment)
        self.decrement_subsegments_size()

    def set_user(self, user):
        """
        set user of a segment. One segment can only have one user.
        User is indexed and can be later queried.
        """
        super()._check_ended()
        self.user = user

    def set_service(self, service_info):
        """
        Add python runtime and version info.
        This method should be only used by the recorder.
        """
        self.service = service_info

    def set_rule_name(self, rule_name):
        """
        Add the matched centralized sampling rule name
        if a segment is sampled because of that rule.
        This method should be only used by the recorder.
        """
        if not self.aws.get('xray', None):
            self.aws['xray'] = {}
        self.aws['xray']['sampling_rule_name'] = rule_name

    def to_dict(self):   
        """
        Convert Segment object to dict with required properties
        that have non-empty values. 
        """ 
        segment_dict = super().to_dict()
          
        del segment_dict['ref_counter']
        del segment_dict['_subsegments_counter']
        
        return segment_dict
