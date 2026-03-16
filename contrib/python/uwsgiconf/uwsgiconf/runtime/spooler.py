import os
import pickle
from calendar import timegm
from datetime import datetime, timedelta
from typing import List, Type, Callable, Dict, Union, Any, Optional

from .. import uwsgi
from ..utils import listify, get_logger, encode, decode, decode_deep

_LOG = get_logger(__name__)
_MSG_MAX_SIZE = 64 * 1024  # 64 Kb https://uwsgi-docs.readthedocs.io/en/latest/Spooler.html#spool-files

TypeTaskResult = Optional[Union['TaskResult', bool]]

_task_functions: Dict[str, Callable] = {}

spooler_task_types = {}
"""Known task types handlers will store here runtime.

SpoolerTask heirs are automatically registered 
in runtime by SpoolerTask.__init_subclass__.

"""


def _get_spoolers() -> List[str]:
    return decode_deep(listify(uwsgi.opt.get(b'spooler', [])))


def _register_task(spooler_obj: 'Spooler', spooler_cls: Type['Spooler']) -> Callable:
    """

    :param spooler_obj:
    :param spooler_cls:

    """
    def _register_task_(priority=None, postpone=None):
        # This one is returned by `_HandlerRegisterer`

        def task_decorator(func: Callable) -> Callable:
            # This one handles decoration with `Spooler.register_handler`.

            func_name = func.__name__

            _task_functions[func_name] = func

            _LOG.debug(f'Spooler. Registered function: {func_name}')

            def task_call(*args, **kwargs) -> str:
                # Redirect task (function call) into spooler.

                return spooler_cls.send_message_raw(**SpoolerFunctionCallTask.build_message(
                    spooler=spooler_obj.name if spooler_obj else None,
                    priority=priority,
                    postpone=postpone,
                    payload={
                        'func': func_name,
                        'arg': args,
                        'kwg': kwargs,
                    }
                ))

            return task_call

        return task_decorator

    return _register_task_


class _TaskRegisterer:
    # Allows task decoration using both Spooler class and object.

    def __get__(self, instance: 'Spooler', owner: Type['Spooler']):
        return _register_task(instance, owner)

    def __call__(self, *, priority: int = None, postpone: Union[datetime, timedelta] = None):
        """Decorator. Used to register a function which should be run in Spooler.

        :param priority: Number. The priority of the message. Larger - less important.

            .. warning:: This works only if you enable `order_tasks` option in `spooler.set_basic_params()`.

        :param postpone: Postpone message processing till.

        """
        # Mirrors `_register_task_` arguments for IDEs ho get proper hints.


class Spooler:
    """Gives an access to uWSGI Spooler related functions.

    .. warning:: To use this helper one needs
        to configure spooler(s) in uWSGI config beforehand.

    .. code-block:: python

        my_spooler = Spooler.get_by_basename('myspooler')

        # @Spooler.task() to  run on first available or to run on `my_spooler`:
        @my_spooler.task(postpone=timedelta(seconds=1))
        def run_me(a, b='c'):
            ...

        # Now call this function as usual and it'll run in a spooler.
        ...
        run_me('some', b='other')
        ...


    """
    def __init__(self, name: str):
        self.name = name

    def __str__(self):
        return self.name

    task = _TaskRegisterer()
    """Decorator. Used to register a function which should be run in Spooler.
    
    .. code-block:: python
       
        my_spooler = Spooler.get_by_basename('myspooler')
        
        # @Spooler.task() to  run on first available or to run on `my_spooler`:
        @my_spooler.task(postpone=timedelta(seconds=1))  
        def run_me(a, b='c'):
            ...
    
    """

    @classmethod
    def send_message_raw(
            cls,
            message: str,
            *,
            spooler: Union[str, 'Spooler'] = None,
            priority: int = None,
            postpone: Union[datetime, timedelta] = None,
            payload: Any = None
    ) -> str:
        """Sends a message to a spooler.

        :param message: Message to pass using spooler.

        :param spooler: The spooler (id or directory) to use.
            Specify the ABSOLUTE path of the spooler that has to manage this task

        :param priority: Number. The priority of the message. Larger - less important.

            .. warning:: This works only if you enable `order_tasks` option in `spooler.set_basic_params()`.

        :param postpone: Postpone message processing till.

        :param payload: Object to pickle and pass within message.

        """
        msg = {
            'msg': message,
        }

        if spooler:

            if isinstance(spooler, Spooler):
                spooler = spooler.name

            msg['spooler'] = spooler

        if priority:
            msg['priority'] = str(priority)

        if postpone:

            if isinstance(postpone, timedelta):
                postpone += datetime.utcnow()

            if isinstance(postpone, datetime):
                postpone = timegm(postpone.timetuple())

            msg['at'] = postpone

        body = {}

        if payload:
            body['payload'] = payload

        if len(repr(msg)) >= _MSG_MAX_SIZE:
            # Message is too large move into body.
            body['msg'] = msg.pop('msg')

        if body:
            msg['body'] = pickle.dumps(body)

        return uwsgi.send_to_spooler(_msg_encode(msg))

    _valid_task_results = {uwsgi.SPOOL_OK, uwsgi.SPOOL_RETRY, uwsgi.SPOOL_IGNORE}

    @classmethod
    def _process_message_raw(cls, envelope) -> int:
        decoded = _msg_decode(envelope)

        task_name = decoded['spooler_task_name']
        msg = decoded.get('msg')
        body = decoded.get('body')

        payload = None

        if body:
            body = pickle.loads(body)
            if not msg:
                msg = body.get('msg')
            payload = body.get('payload')

        task_cls = SpoolerTask

        if msg.startswith('ucfg_'):
            message_type_id = msg.replace('ucfg_', '', 1)
            task_cls = spooler_task_types.get(message_type_id, SpoolerTask)

        task = task_cls(
            name=task_name,
            message=msg,
            payload=payload,
        )

        try:
            result = task.process()

        except Exception as e:
            _LOG.exception(f"Spooler. Unhandled exception in task '{task_name}'")
            result = ResultRescheduled(exception=e)

        if result is None:
            result = ResultSkipped(result)

        elif not isinstance(result, TaskResult):
            result = ResultProcessed(result)

        return result.code_uwsgi

    @classmethod
    def get_spoolers(cls) -> List['Spooler']:
        """Returns a list of registered spoolers."""
        return [Spooler(spooler_dir) for spooler_dir in _get_spoolers()]

    @classmethod
    def get_by_basename(cls, name: str) -> Optional['Spooler']:
        """Returns spooler object for a given directory name.

        If there is more than one spooler with the same directory base name,
        the first one is returned.

        If not found `None` is returned.

        :param name: Directory base name. E.g.: 'mydir' to get spooler for '/somewhere/here/is/mydir'

        """
        spooler = None
        basename = os.path.basename

        for spooler_dir in _get_spoolers():
            if basename(spooler_dir) == name:
                spooler = Spooler(spooler_dir)
                break

        return spooler

    @classmethod
    def get_pids(cls) -> List[int]:
        """Returns a list of all spooler processes IDs."""
        return uwsgi.spooler_pids()

    @classmethod
    def set_period(cls, seconds: int) -> bool:
        """Sets how often the spooler runs.

        :param seconds:

        """
        return uwsgi.set_spooler_frequency(seconds)

    @classmethod
    def get_tasks(cls) -> List[str]:
        """Returns a list of spooler jobs (filenames in spooler directory)."""
        return uwsgi.spooler_jobs()

    @classmethod
    def read_task_file(cls, path: str) -> dict:
        """Returns a spooler task information.

        :param path: The relative or absolute path to the task to read.

        """
        return uwsgi.spooler_get_task(path) or {}


class TaskResult:
    """Represents a task processing result."""

    code_uwsgi: str = None

    def __init__(self, result: Any = None, *, exception: Exception = None):
        self.result = result
        self.exception = exception


class ResultProcessed(TaskResult):
    """Treat task as processed."""

    code_uwsgi = uwsgi.SPOOL_OK


class ResultSkipped(TaskResult):
    """Treat task as skipped (ignored)."""

    code_uwsgi = uwsgi.SPOOL_IGNORE


class ResultRescheduled(TaskResult):
    """Treat task as rescheduled (being due to retry)."""

    code_uwsgi = uwsgi.SPOOL_RETRY


class SpoolerTask:
    """Consolidates information for a spooler task."""

    mark_processed = ResultProcessed
    mark_skipped = ResultSkipped
    mark_rescheduled = ResultRescheduled

    __slots__ = ['name', 'message', 'payload']

    type_id: str = ''

    def __init__(self, name: str, message: str, payload: Any):
        self.name = name
        self.message = message
        self.payload = payload

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__()
        spooler_task_types[cls.type_id] = cls

    def process(self) -> TypeTaskResult:
        """Processes the task.

        Supported results:
            * `None` - mark as ignored (skipped)
            * `TaskResult` - result type logic
            * exception - mark to retry
            * other - mark as processed

        """
        return True

    @classmethod
    def build_message(
            cls,
            spooler: Optional[str],
            priority: int,
            postpone: Union[datetime, timedelta],
            payload: Any = None
    ) -> dict:
        payload_ = {
            'spooler': spooler,
            'priority': priority,
            'postpone': postpone,
        }

        payload_.update(payload or {})

        msg = dict(
            message=f'ucfg_{cls.type_id}',
            spooler=spooler,
            priority=priority,
            postpone=postpone,
            payload=payload_,
        )
        return msg


class SpoolerFunctionCallTask(SpoolerTask):
    """Function call type. Allows delegating function calls to spoolers."""

    type_id = 'fcall'

    __slots__ = ['name', 'message', 'payload']

    def process(self) -> TypeTaskResult:
        payload = self.payload
        func = _task_functions[payload['func']]  # We expect an exception here if not registered.
        result = func(*payload['arg'], **payload['kwg'])
        return result


def _msg_encode(msg: dict) -> dict:
    """
    :param msg:

    """
    return {encode(k): encode(v) if isinstance(v, str) else v for k, v in msg.items()}


def _msg_decode(msg: dict) -> dict:
    """
    :param msg:

    """
    decoded = {}

    for k, v in msg.items():
        # Builtin keys are already strings. Our custom - bytes.
        k = decode(k) if isinstance(k, bytes) else k

        if k != 'body':
            # Consider body always pickled.
            v = decode(v)

        decoded[k] = v

    return decoded


uwsgi.spooler = Spooler._process_message_raw
