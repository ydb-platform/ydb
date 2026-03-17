import os
from .noop_traceid import NoOpTraceId
from .traceid import TraceId
from .segment import Segment
from .subsegment import Subsegment


class DummySegment(Segment):
    """
    A dummy segment is created when ``xray_recorder`` decide to not sample
    the segment based on sampling rules.
    Adding data to a dummy segment becomes a no-op except for
    subsegments. This is to reduce the memory footprint of the SDK.
    A dummy segment will not be sent to the X-Ray daemon. Manually creating
    dummy segments is not recommended.
    """

    def __init__(self, name='dummy'):
        no_op_id = os.getenv('AWS_XRAY_NOOP_ID')
        if no_op_id and no_op_id.lower() == 'false':
            super().__init__(name=name, traceid=TraceId().to_id())
        else:
            super().__init__(name=name, traceid=NoOpTraceId().to_id(), entityid='0000000000000000')
        self.sampled = False

    def set_aws(self, aws_meta):
        """
        No-op
        """
        pass

    def put_http_meta(self, key, value):
        """
        No-op
        """
        pass

    def put_annotation(self, key, value):
        """
        No-op
        """
        pass

    def put_metadata(self, key, value, namespace='default'):
        """
        No-op
        """
        pass

    def set_user(self, user):
        """
        No-op
        """
        pass

    def set_service(self, service_info):
        """
        No-op
        """
        pass

    def apply_status_code(self, status_code):
        """
        No-op
        """
        pass

    def add_exception(self, exception, stack, remote=False):
        """
        No-op
        """
        pass

    def serialize(self):
        """
        No-op
        """
        pass


class DummySubsegment(Subsegment):
    """
    A dummy subsegment will be created when ``xray_recorder`` tries
    to create a subsegment under a not sampled segment. Adding data
    to a dummy subsegment becomes no-op. Dummy subsegment will not
    be sent to the X-Ray daemon.
    """

    def __init__(self, segment, name='dummy'):
        super().__init__(name, 'dummy', segment)
        no_op_id = os.getenv('AWS_XRAY_NOOP_ID')
        if no_op_id and no_op_id.lower() == 'false':
            super(Subsegment, self).__init__(name)
        else:
            super(Subsegment, self).__init__(name, entity_id='0000000000000000')
        self.sampled = False

    def set_aws(self, aws_meta):
        """
        No-op
        """
        pass

    def put_http_meta(self, key, value):
        """
        No-op
        """
        pass

    def put_annotation(self, key, value):
        """
        No-op
        """
        pass

    def put_metadata(self, key, value, namespace='default'):
        """
        No-op
        """
        pass

    def set_sql(self, sql):
        """
        No-op
        """
        pass

    def apply_status_code(self, status_code):
        """
        No-op
        """
        pass

    def add_exception(self, exception, stack, remote=False):
        """
        No-op
        """
        pass

    def serialize(self):
        """
        No-op
        """
        pass
