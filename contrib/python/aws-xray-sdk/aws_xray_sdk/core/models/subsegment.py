import copy
import traceback

import wrapt

from .entity import Entity
from ..exceptions.exceptions import SegmentNotFoundException


# Attribute starts with _self_ to prevent wrapt proxying to underlying function
SUBSEGMENT_RECORDING_ATTRIBUTE = '_self___SUBSEGMENT_RECORDING_ATTRIBUTE__'


def set_as_recording(decorated_func, wrapped):
    # If the wrapped function has the attribute, then it has already been patched
    setattr(decorated_func, SUBSEGMENT_RECORDING_ATTRIBUTE, hasattr(wrapped, SUBSEGMENT_RECORDING_ATTRIBUTE))


def is_already_recording(func):
    # The function might have the attribute, but its value might still be false
    # as it might be the first decorator
    return getattr(func, SUBSEGMENT_RECORDING_ATTRIBUTE, False)


@wrapt.decorator
def subsegment_decorator(wrapped, instance, args, kwargs):
    decorated_func = wrapt.decorator(wrapped)(*args, **kwargs)
    set_as_recording(decorated_func, wrapped)
    return decorated_func


class SubsegmentContextManager:
    """
    Wrapper for segment and recorder to provide segment context manager.
    """

    def __init__(self, recorder, name=None, **subsegment_kwargs):
        self.name = name
        self.subsegment_kwargs = subsegment_kwargs
        self.recorder = recorder
        self.subsegment = None

    @subsegment_decorator
    def __call__(self, wrapped, instance, args, kwargs):
        if is_already_recording(wrapped):
            # The wrapped function is already decorated, the subsegment will be created later,
            # just return the result
            return wrapped(*args, **kwargs)

        func_name = self.name
        if not func_name:
            func_name = wrapped.__name__

        return self.recorder.record_subsegment(
            wrapped, instance, args, kwargs,
            name=func_name,
            namespace='local',
            meta_processor=None,
        )

    def __enter__(self):
        self.subsegment = self.recorder.begin_subsegment(
            name=self.name, **self.subsegment_kwargs)
        return self.subsegment

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.subsegment is None:
            return

        if exc_type is not None:
            self.subsegment.add_exception(
                exc_val,
                traceback.extract_tb(
                    exc_tb,
                    limit=self.recorder.max_trace_back,
                )
            )
        self.recorder.end_subsegment()


class Subsegment(Entity):
    """
    The work done in a single segment can be broke down into subsegments.
    Subsegments provide more granular timing information and details about
    downstream calls that your application made to fulfill the original request.
    A subsegment can contain additional details about a call to an AWS service,
    an external HTTP API, or an SQL database.
    """
    def __init__(self, name, namespace, segment):
        """
        Create a new subsegment.

        :param str name: Subsegment name is required.
        :param str namespace: The namespace of the subsegment. Currently
            support `aws`, `remote` and `local`.
        :param Segment segment: The parent segment
        """
        super().__init__(name)

        if not segment:
            raise SegmentNotFoundException("A parent segment is required for creating subsegments.")

        self.parent_segment = segment
        self.trace_id = segment.trace_id

        self.type = 'subsegment'
        self.namespace = namespace

        self.sql = {}

    def add_subsegment(self, subsegment):
        """
        Add input subsegment as a child subsegment and increment
        reference counter and total subsegments counter of the
        parent segment.
        """
        super().add_subsegment(subsegment)
        self.parent_segment.increment()

    def remove_subsegment(self, subsegment):
        """
        Remove input subsegment from child subsegemnts and
        decrement parent segment total subsegments count.

        :param Subsegment: subsegment to remove.
        """
        super().remove_subsegment(subsegment)
        self.parent_segment.decrement_subsegments_size()

    def close(self, end_time=None):
        """
        Close the trace entity by setting `end_time`
        and flip the in progress flag to False. Also decrement
        parent segment's ref counter by 1.

        :param float end_time: Epoch in seconds. If not specified
            current time will be used.
        """
        super().close(end_time)
        self.parent_segment.decrement_ref_counter()

    def set_sql(self, sql):
        """
        Set sql related metadata. This function is used by patchers
        for database connectors and is not recommended to
        invoke manually.

        :param dict sql: sql related metadata
        """
        self.sql = sql

    def to_dict(self): 
        """
        Convert Subsegment object to dict with required properties
        that have non-empty values. 
        """    
        subsegment_dict = super().to_dict()
        
        del subsegment_dict['parent_segment']

        return subsegment_dict
