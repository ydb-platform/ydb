import time

from aws_xray_sdk.core.recorder import AWSXRayRecorder
from aws_xray_sdk.core.utils import stacktrace
from aws_xray_sdk.core.models.subsegment import SubsegmentContextManager, is_already_recording, subsegment_decorator
from aws_xray_sdk.core.models.segment import SegmentContextManager


class AsyncSegmentContextManager(SegmentContextManager):
    async def __aenter__(self):
        return self.__enter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return self.__exit__(exc_type, exc_val, exc_tb)

class AsyncSubsegmentContextManager(SubsegmentContextManager):

    @subsegment_decorator
    async def __call__(self, wrapped, instance, args, kwargs):
        if is_already_recording(wrapped):
            # The wrapped function is already decorated, the subsegment will be created later,
            # just return the result
            return await wrapped(*args, **kwargs)

        func_name = self.name
        if not func_name:
            func_name = wrapped.__name__

        return await self.recorder.record_subsegment_async(
            wrapped, instance, args, kwargs,
            name=func_name,
            namespace='local',
            meta_processor=None,
        )

    async def __aenter__(self):
        return self.__enter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return self.__exit__(exc_type, exc_val, exc_tb)


class AsyncAWSXRayRecorder(AWSXRayRecorder):
    def capture_async(self, name=None):
        """
        A decorator that records enclosed function in a subsegment.
        It only works with asynchronous functions.

        params str name: The name of the subsegment. If not specified
        the function name will be used.
        """
        return self.in_subsegment_async(name=name)

    def in_segment_async(self, name=None, **segment_kwargs):
        """
        Return a segment async context manager.

        :param str name: the name of the segment
        :param dict segment_kwargs: remaining arguments passed directly to `begin_segment`
        """
        return AsyncSegmentContextManager(self, name=name, **segment_kwargs)

    def in_subsegment_async(self, name=None, **subsegment_kwargs):
        """
        Return a subsegment async context manager.

        :param str name: the name of the segment
        :param dict segment_kwargs: remaining arguments passed directly to `begin_segment`
        """
        return AsyncSubsegmentContextManager(self, name=name, **subsegment_kwargs)

    async def record_subsegment_async(self, wrapped, instance, args, kwargs, name,
                                      namespace, meta_processor):

        subsegment = self.begin_subsegment(name, namespace)

        exception = None
        stack = None
        return_value = None

        try:
            return_value = await wrapped(*args, **kwargs)
            return return_value
        except Exception as e:
            exception = e
            stack = stacktrace.get_stacktrace(limit=self._max_trace_back)
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
                    if subsegment:
                        subsegment.add_exception(exception, stack)

                self.end_subsegment(end_time)
