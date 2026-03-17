import copy
import json
import logging
import os
import platform
import time

from aws_xray_sdk import global_sdk_config
from aws_xray_sdk.version import VERSION
from .models.segment import Segment, SegmentContextManager
from .models.subsegment import Subsegment, SubsegmentContextManager
from .models.default_dynamic_naming import DefaultDynamicNaming
from .models.dummy_entities import DummySegment, DummySubsegment
from .emitters.udp_emitter import UDPEmitter
from .streaming.default_streaming import DefaultStreaming
from .context import Context
from .daemon_config import DaemonConfig
from .plugins.utils import get_plugin_modules
from .lambda_launcher import check_in_lambda
from .exceptions.exceptions import SegmentNameMissingException, SegmentNotFoundException
from .utils import stacktrace

log = logging.getLogger(__name__)

TRACING_NAME_KEY = 'AWS_XRAY_TRACING_NAME'
DAEMON_ADDR_KEY = 'AWS_XRAY_DAEMON_ADDRESS'
CONTEXT_MISSING_KEY = 'AWS_XRAY_CONTEXT_MISSING'

XRAY_META = {
    'xray': {
        'sdk': 'X-Ray for Python',
        'sdk_version': VERSION
    }
}

SERVICE_INFO = {
    'runtime': platform.python_implementation(),
    'runtime_version': platform.python_version()
}


class AWSXRayRecorder:
    """
    A global AWS X-Ray recorder that will begin/end segments/subsegments
    and send them to the X-Ray daemon. This recorder is initialized during
    loading time so you can use::

        from aws_xray_sdk.core import xray_recorder

    in your module to access it
    """
    def __init__(self):

        self._streaming = DefaultStreaming()
        context = check_in_lambda()
        if context:
            # Special handling when running on AWS Lambda.
            from .sampling.local.sampler import LocalSampler
            self._context = context
            self.streaming_threshold = 0
            self._sampler = LocalSampler()
        else:
            from .sampling.sampler import DefaultSampler
            self._context = Context()
            self._sampler = DefaultSampler()

        self._emitter = UDPEmitter()
        self._sampling = True
        self._max_trace_back = 10
        self._plugins = None
        self._service = os.getenv(TRACING_NAME_KEY)
        self._dynamic_naming = None
        self._aws_metadata = copy.deepcopy(XRAY_META)
        self._origin = None
        self._stream_sql = True

        if type(self.sampler).__name__ == 'DefaultSampler':
            self.sampler.load_settings(DaemonConfig(), self.context)

    def configure(self, sampling=None, plugins=None,
                  context_missing=None, sampling_rules=None,
                  daemon_address=None, service=None,
                  context=None, emitter=None, streaming=None,
                  dynamic_naming=None, streaming_threshold=None,
                  max_trace_back=None, sampler=None,
                  stream_sql=True):
        """Configure global X-Ray recorder.

        Configure needs to run before patching thrid party libraries
        to avoid creating dangling subsegment.

        :param bool sampling: If sampling is enabled, every time the recorder
            creates a segment it decides whether to send this segment to
            the X-Ray daemon. This setting is not used if the recorder
            is running in AWS Lambda. The recorder always respect the incoming
            sampling decisions regardless of this setting.
        :param sampling_rules: Pass a set of local custom sampling rules.
            Can be an absolute path of the sampling rule config json file
            or a dictionary that defines those rules. This will also be the
            fallback rules in case of centralized sampling opted-in while
            the cetralized sampling rules are not available.
        :param sampler: The sampler used to make sampling decisions. The SDK
            provides two built-in samplers. One is centralized rules based and
            the other is local rules based. The former is the default.
        :param tuple plugins: plugins that add extra metadata to each segment.
            Currently available plugins are EC2Plugin, ECS plugin and
            ElasticBeanstalkPlugin.
            If you want to disable all previously enabled plugins,
            pass an empty tuple ``()``.
        :param str context_missing: recorder behavior when it tries to mutate
            a segment or add a subsegment but there is no active segment.
            RUNTIME_ERROR means the recorder will raise an exception.
            LOG_ERROR means the recorder will only log the error and
            do nothing.
            IGNORE_ERROR means the recorder will do nothing
        :param str daemon_address: The X-Ray daemon address where the recorder
            sends data to.
        :param str service: default segment name if creating a segment without
            providing a name.
        :param context: You can pass your own implementation of context storage
            for active segment/subsegment by overriding the default
            ``Context`` class.
        :param emitter: The emitter that sends a segment/subsegment to
            the X-Ray daemon. You can override ``UDPEmitter`` class.
        :param dynamic_naming: a string that defines a pattern that host names
            should match. Alternatively you can pass a module which
            overrides ``DefaultDynamicNaming`` module.
        :param streaming: The streaming module to stream out trace documents
            when they grow too large. You can override ``DefaultStreaming``
            class to have your own implementation of the streaming process.
        :param streaming_threshold: If breaks within a single segment it will
            start streaming out children subsegments. By default it is the
            maximum number of subsegments within a segment.
        :param int max_trace_back: The maxinum number of stack traces recorded
            by auto-capture. Lower this if a single document becomes too large.
        :param bool stream_sql: Whether SQL query texts should be streamed.

        Environment variables AWS_XRAY_DAEMON_ADDRESS, AWS_XRAY_CONTEXT_MISSING
        and AWS_XRAY_TRACING_NAME respectively overrides arguments
        daemon_address, context_missing and service.
        """

        if sampling is not None:
            self.sampling = sampling
        if sampler:
            self.sampler = sampler
        if service:
            self.service = os.getenv(TRACING_NAME_KEY, service)
        if sampling_rules:
            self._load_sampling_rules(sampling_rules)
        if emitter:
            self.emitter = emitter
        if daemon_address:
            self.emitter.set_daemon_address(os.getenv(DAEMON_ADDR_KEY, daemon_address))
        if context:
            self.context = context
        if context_missing:
            self.context.context_missing = os.getenv(CONTEXT_MISSING_KEY, context_missing)
        if dynamic_naming:
            self.dynamic_naming = dynamic_naming
        if streaming:
            self.streaming = streaming
        if streaming_threshold is not None:
            self.streaming_threshold = streaming_threshold
        if type(max_trace_back) == int and max_trace_back >= 0:
            self.max_trace_back = max_trace_back
        if stream_sql is not None:
            self.stream_sql = stream_sql

        if plugins:
            plugin_modules = get_plugin_modules(plugins)
            for plugin in plugin_modules:
                plugin.initialize()
                if plugin.runtime_context:
                    self._aws_metadata[plugin.SERVICE_NAME] = plugin.runtime_context
                    self._origin = plugin.ORIGIN
        # handling explicitly using empty list to clean up plugins.
        elif plugins is not None:
            self._aws_metadata = copy.deepcopy(XRAY_META)
            self._origin = None

        if type(self.sampler).__name__ == 'DefaultSampler':
            self.sampler.load_settings(DaemonConfig(daemon_address),
                                       self.context, self._origin)

    def in_segment(self, name=None, **segment_kwargs):
        """
        Return a segment context manager.

        :param str name: the name of the segment
        :param dict segment_kwargs: remaining arguments passed directly to `begin_segment`
        """
        return SegmentContextManager(self, name=name, **segment_kwargs)

    def in_subsegment(self, name=None, **subsegment_kwargs):
        """
        Return a subsegment context manager.

        :param str name: the name of the subsegment
        :param dict subsegment_kwargs: remaining arguments passed directly to `begin_subsegment`
        """
        return SubsegmentContextManager(self, name=name, **subsegment_kwargs)

    def begin_segment(self, name=None, traceid=None,
                      parent_id=None, sampling=None):
        """
        Begin a segment on the current thread and return it. The recorder
        only keeps one segment at a time. Create the second one without
        closing existing one will overwrite it.

        :param str name: the name of the segment
        :param str traceid: trace id of the segment
        :param int sampling: 0 means not sampled, 1 means sampled
        """
        # Disable the recorder; return a generated dummy segment.
        if not global_sdk_config.sdk_enabled():
            return DummySegment(global_sdk_config.DISABLED_ENTITY_NAME)

        seg_name = name or self.service
        if not seg_name:
            raise SegmentNameMissingException("Segment name is required.")

        # Sampling decision is None if not sampled.
        # In a sampled case it could be either a string or 1
        # depending on if centralized or local sampling rule takes effect.
        decision = True

        # we respect the input sampling decision
        # regardless of recorder configuration.
        if sampling == 0:
            decision = False
        elif sampling:
            decision = sampling
        elif self.sampling:
            decision = self._sampler.should_trace({'service': seg_name})

        if not decision:
            segment = DummySegment(seg_name)
        else:
            segment = Segment(name=seg_name, traceid=traceid,
                              parent_id=parent_id)
            self._populate_runtime_context(segment, decision)

        self.context.put_segment(segment)
        return segment

    def end_segment(self, end_time=None):
        """
        End the current segment and send it to X-Ray daemon
        if it is ready to send. Ready means segment and
        all its subsegments are closed.

        :param float end_time: segment completion in unix epoch in seconds.
        """
        # When the SDK is disabled we return
        if not global_sdk_config.sdk_enabled():
            return

        self.context.end_segment(end_time)
        segment = self.current_segment()
        if segment and segment.ready_to_send():
            self._send_segment()

    def current_segment(self):
        """
        Return the currently active segment. In a multithreading environment,
        this will make sure the segment returned is the one created by the
        same thread.
        """

        entity = self.get_trace_entity()
        if self._is_subsegment(entity):
            return entity.parent_segment
        else:
            return entity

    def _begin_subsegment_helper(self, name, namespace='local', beginWithoutSampling=False):
        '''
        Helper method to begin_subsegment and begin_subsegment_without_sampling
        '''
        # Generating the parent dummy segment is necessary.
        # We don't need to store anything in context. Assumption here
        # is that we only work with recorder-level APIs.
        if not global_sdk_config.sdk_enabled():
            return DummySubsegment(DummySegment(global_sdk_config.DISABLED_ENTITY_NAME))

        segment = self.current_segment()
        if not segment:
            log.warning("No segment found, cannot begin subsegment %s." % name)
            return None

        current_entity = self.get_trace_entity()
        if not current_entity.sampled or beginWithoutSampling:
            subsegment = DummySubsegment(segment, name)
        else:
            subsegment = Subsegment(name, namespace, segment)

        self.context.put_subsegment(subsegment)
        return subsegment



    def begin_subsegment(self, name, namespace='local'):
        """
        Begin a new subsegment.
        If there is open subsegment, the newly created subsegment will be the
        child of latest opened subsegment.
        If not, it will be the child of the current open segment.

        :param str name: the name of the subsegment.
        :param str namespace: currently can only be 'local', 'remote', 'aws'.
        """
        return self._begin_subsegment_helper(name, namespace)


    def begin_subsegment_without_sampling(self, name):
        """
        Begin a new unsampled subsegment.
        If there is open subsegment, the newly created subsegment will be the
        child of latest opened subsegment.
        If not, it will be the child of the current open segment.

        :param str name: the name of the subsegment.
        """
        return self._begin_subsegment_helper(name, beginWithoutSampling=True)

    def current_subsegment(self):
        """
        Return the latest opened subsegment. In a multithreading environment,
        this will make sure the subsegment returned is one created
        by the same thread.
        """
        if not global_sdk_config.sdk_enabled():
            return DummySubsegment(DummySegment(global_sdk_config.DISABLED_ENTITY_NAME))

        entity = self.get_trace_entity()
        if self._is_subsegment(entity):
            return entity
        else:
            return None

    def end_subsegment(self, end_time=None):
        """
        End the current active subsegment. If this is the last one open
        under its parent segment, the entire segment will be sent.

        :param float end_time: subsegment compeletion in unix epoch in seconds.
        """
        if not global_sdk_config.sdk_enabled():
            return

        if not self.context.end_subsegment(end_time):
            return

        # if segment is already close, we check if we can send entire segment
        # otherwise we check if we need to stream some subsegments
        if self.current_segment().ready_to_send():
            self._send_segment()
        else:
            self.stream_subsegments()

    def put_annotation(self, key, value):
        """
        Annotate current active trace entity with a key-value pair.
        Annotations will be indexed for later search query.

        :param str key: annotation key
        :param object value: annotation value. Any type other than
            string/number/bool will be dropped
        """
        if not global_sdk_config.sdk_enabled():
            return
        entity = self.get_trace_entity()
        if entity and entity.sampled:
            entity.put_annotation(key, value)

    def put_metadata(self, key, value, namespace='default'):
        """
        Add metadata to the current active trace entity.
        Metadata is not indexed but can be later retrieved
        by BatchGetTraces API.

        :param str namespace: optional. Default namespace is `default`.
            It must be a string and prefix `AWS.` is reserved.
        :param str key: metadata key under specified namespace
        :param object value: any object that can be serialized into JSON string
        """
        if not global_sdk_config.sdk_enabled():
            return
        entity = self.get_trace_entity()
        if entity and entity.sampled:
            entity.put_metadata(key, value, namespace)

    def is_sampled(self):
        """
        Check if the current trace entity is sampled or not.
        Return `False` if no active entity found.
        """
        if not global_sdk_config.sdk_enabled():
            # Disabled SDK is never sampled
            return False
        entity = self.get_trace_entity()
        if entity:
            return entity.sampled
        return False

    def get_trace_entity(self):
        """
        A pass through method to ``context.get_trace_entity()``.
        """
        return self.context.get_trace_entity()

    def set_trace_entity(self, trace_entity):
        """
        A pass through method to ``context.set_trace_entity()``.
        """
        self.context.set_trace_entity(trace_entity)

    def clear_trace_entities(self):
        """
        A pass through method to ``context.clear_trace_entities()``.
        """
        self.context.clear_trace_entities()

    def stream_subsegments(self):
        """
        Stream all closed subsegments to the daemon
        and remove reference to the parent segment.
        No-op for a not sampled segment.
        """
        segment = self.current_segment()

        if self.streaming.is_eligible(segment):
            self.streaming.stream(segment, self._stream_subsegment_out)

    def capture(self, name=None):
        """
        A decorator that records enclosed function in a subsegment.
        It only works with synchronous functions.

        params str name: The name of the subsegment. If not specified
        the function name will be used.
        """
        return self.in_subsegment(name=name)

    def record_subsegment(self, wrapped, instance, args, kwargs, name,
                          namespace, meta_processor):

        subsegment = self.begin_subsegment(name, namespace)

        exception = None
        stack = None
        return_value = None

        try:
            return_value = wrapped(*args, **kwargs)
            return return_value
        except Exception as e:
            exception = e
            stack = stacktrace.get_stacktrace(limit=self.max_trace_back)
            raise
        finally:
            # No-op if subsegment is `None` due to `LOG_ERROR`.
            if subsegment is not None:
                end_time = time.time()
                if callable(meta_processor):
                    meta_processor(
                        wrapped=wrapped,
                        instance=instance,
                        args=args,
                        kwargs=kwargs,
                        return_value=return_value,
                        exception=exception,
                        subsegment=subsegment,
                        stack=stack,
                    )
                elif exception:
                    subsegment.add_exception(exception, stack)

                self.end_subsegment(end_time)

    def _populate_runtime_context(self, segment, sampling_decision):
        if self._origin:
            setattr(segment, 'origin', self._origin)

        segment.set_aws(copy.deepcopy(self._aws_metadata))
        segment.set_service(SERVICE_INFO)

        if isinstance(sampling_decision, str):
            segment.set_rule_name(sampling_decision)

    def _send_segment(self):
        """
        Send the current segment to X-Ray daemon if it is present and
        sampled, then clean up context storage.
        The emitter will handle failures.
        """
        segment = self.current_segment()

        if not segment:
            return

        if segment.sampled:
            self.emitter.send_entity(segment)
        self.clear_trace_entities()

    def _stream_subsegment_out(self, subsegment):
        log.debug("streaming subsegments...")
        if subsegment.sampled:
            self.emitter.send_entity(subsegment)

    def _load_sampling_rules(self, sampling_rules):

        if not sampling_rules:
            return

        if isinstance(sampling_rules, dict):
            self.sampler.load_local_rules(sampling_rules)
        else:
            with open(sampling_rules) as f:
                self.sampler.load_local_rules(json.load(f))

    def _is_subsegment(self, entity):

        return (hasattr(entity, 'type') and entity.type == 'subsegment')

    @property
    def enabled(self):
        return self._enabled

    @enabled.setter
    def enabled(self, value):
        self._enabled = value

    @property
    def sampling(self):
        return self._sampling

    @sampling.setter
    def sampling(self, value):
        self._sampling = value

    @property
    def sampler(self):
        return self._sampler

    @sampler.setter
    def sampler(self, value):
        self._sampler = value

    @property
    def service(self):
        return self._service

    @service.setter
    def service(self, value):
        self._service = value

    @property
    def dynamic_naming(self):
        return self._dynamic_naming

    @dynamic_naming.setter
    def dynamic_naming(self, value):
        if isinstance(value, str):
            self._dynamic_naming = DefaultDynamicNaming(value, self.service)
        else:
            self._dynamic_naming = value

    @property
    def context(self):
        return self._context

    @context.setter
    def context(self, cxt):
        self._context = cxt

    @property
    def emitter(self):
        return self._emitter

    @emitter.setter
    def emitter(self, value):
        self._emitter = value

    @property
    def streaming(self):
        return self._streaming

    @streaming.setter
    def streaming(self, value):
        self._streaming = value

    @property
    def streaming_threshold(self):
        """
        Proxy method to Streaming module's `streaming_threshold` property.
        """
        return self.streaming.streaming_threshold

    @streaming_threshold.setter
    def streaming_threshold(self, value):
        """
        Proxy method to Streaming module's `streaming_threshold` property.
        """
        self.streaming.streaming_threshold = value

    @property
    def max_trace_back(self):
        return self._max_trace_back

    @max_trace_back.setter
    def max_trace_back(self, value):
        self._max_trace_back = value

    @property
    def stream_sql(self):
        return self._stream_sql

    @stream_sql.setter
    def stream_sql(self, value):
        self._stream_sql = value
