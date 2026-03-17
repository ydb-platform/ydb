import asyncio
import copy

from .context import Context as _Context


class AsyncContext(_Context):
    """
    Async Context for storing segments.

    Inherits nearly everything from the main Context class.
    Replaces threading.local with a task based local storage class,
    Also overrides clear_trace_entities
    """
    def __init__(self, *args, loop=None, use_task_factory=True, **kwargs):
        super().__init__(*args, **kwargs)

        self._loop = loop
        if loop is None:
            self._loop = asyncio.get_event_loop()

        if use_task_factory:
            self._loop.set_task_factory(task_factory)

        self._local = TaskLocalStorage(loop=loop)

    def clear_trace_entities(self):
        """
        Clear all trace_entities stored in the task local context.
        """
        if self._local is not None:
            self._local.clear()


class TaskLocalStorage:
    """
    Simple task local storage
    """
    def __init__(self, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self._loop = loop

    def __setattr__(self, name, value):
        if name in ('_loop',):
            # Set normal attributes
            object.__setattr__(self, name, value)

        else:
            # Set task local attributes
            task = asyncio.current_task(loop=self._loop)
            if task is None:
                return None

            if not hasattr(task, 'context'):
                task.context = {}

            task.context[name] = value

    def __getattribute__(self, item):
        if item in ('_loop', 'clear'):
            # Return references to local objects
            return object.__getattribute__(self, item)

        task = asyncio.current_task(loop=self._loop)
        if task is None:
            return None

        if hasattr(task, 'context') and item in task.context:
            return task.context[item]

        raise AttributeError('Task context does not have attribute {0}'.format(item))

    def clear(self):
        # If were in a task, clear the context dictionary
        task = asyncio.current_task(loop=self._loop)
        if task is not None and hasattr(task, 'context'):
            task.context.clear()


def task_factory(loop, coro):
    """
    Task factory function

    Fuction closely mirrors the logic inside of
    asyncio.BaseEventLoop.create_task. Then if there is a current
    task and the current task has a context then share that context
    with the new task
    """
    task = asyncio.Task(coro, loop=loop)
    if task._source_traceback:  # flake8: noqa
        del task._source_traceback[-1]  # flake8: noqa

    # Share context with new task if possible
    current_task = asyncio.current_task(loop=loop)
    if current_task is not None and hasattr(current_task, 'context'):
        if current_task.context.get('entities'):
            # NOTE: (enowell) Because the `AWSXRayRecorder`'s `Context` decides
            # the parent by looking at its `_local.entities`, we must copy the entities
            # for concurrent subsegments. Otherwise, the subsegments would be
            # modifying the same `entities` list and sugsegments would take other
            # subsegments as parents instead of the original `segment`.
            #
            # See more: https://github.com/aws/aws-xray-sdk-python/blob/0f13101e4dba7b5c735371cb922f727b1d9f46d8/aws_xray_sdk/core/context.py#L90-L101
            new_context = copy.copy(current_task.context)
            new_context['entities'] = [item for item in current_task.context['entities']]
        else:
            new_context = current_task.context
        setattr(task, 'context', new_context)

    return task
