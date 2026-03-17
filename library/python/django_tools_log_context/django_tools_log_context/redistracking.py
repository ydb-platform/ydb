import logging
import six
import traceback

from django.conf import settings
from functools import wraps
from monotonic import monotonic
from ylog.context import log_context

from .state import get_state
from .utils import dynamic_import

_redis_execute_command_ref = None


def get_redis_class():
    return dynamic_import('redis.client', 'Redis')


def enable_instrumentation():
    global _redis_execute_command_ref
    if _redis_execute_command_ref is None:
        redis_class = get_redis_class()
        if redis_class:
            redis_execute_command = getattr(redis_class, 'execute_command')
            setattr(redis_class, 'execute_command', fake_execute_command(redis_execute_command))
            _redis_execute_command_ref = redis_execute_command


def disable_instrumentation():
    global _redis_execute_command_ref
    if _redis_execute_command_ref is not None:
        redis_class = get_redis_class()
        if redis_class:
            setattr(redis_class, 'execute_command', _redis_execute_command_ref)
            _redis_execute_command_ref = None


def fake_execute_command(old_execute_command):
    state = get_state()
    logger = logging.getLogger(__name__)

    @wraps(old_execute_command)
    def wrapped(self, *args, **kwargs):
        from django.conf import settings
        start_time = monotonic()
        response = None
        try:
            response = old_execute_command(self, *args, **kwargs)
            return response
        finally:
            duration = (monotonic() - start_time) * 1000
            state.add_redis_time(duration)
            command = args[0] if args else '?'

            if settings.TOOLS_LOG_CONTEXT_ENABLE_REDIS_TRACKING and state.is_enabled():
                profiling = {
                    'command': command,
                    'vendor': 'redis',
                }
                if command.endswith('GET'):
                    profiling['hit'] = response is not None
                    if len(args) > 1:
                        profiling['key'] = args[1]
                if settings.TOOLS_LOG_CONTEXT_ENABLE_STACKTRACES:
                    profiling['stacktrace'] = ''.join(i.decode('utf-8') if six.PY2 else i for i in traceback.format_stack()[:-1])

                with log_context(execution_time=int(duration), profiling=profiling):
                    logger.info('(%.3f msec) redis command %s', duration, command)
    return wrapped
