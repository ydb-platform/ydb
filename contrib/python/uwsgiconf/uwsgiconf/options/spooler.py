from os import path

from ..base import OptionsGroup
from ..typehints import Strlist
from ..utils import listify


class Spooler(OptionsGroup):
    """Spooler.

    .. note:: Supported on: Perl, Python, Ruby.

    .. note:: Be sure the ``spooler`` plugin is loaded in your instance,
        but generally it is built in by default.

    The Spooler is a queue manager built into uWSGI that works like a printing/mail system.
    You can enqueue massive sending of emails, image processing, video encoding, etc.
    and let the spooler do the hard work in background while your users
    get their requests served by normal workers.

    http://uwsgi-docs.readthedocs.io/en/latest/Spooler.html

    """

    def __init__(self, *args, **kwargs):
        self._base_dir = ''
        super().__init__(*args, **kwargs)

    def set_basic_params(
            self,
            *,
            touch_reload: Strlist = None,
            quiet: bool = None,
            process_count: int = None,
            max_tasks: int = None,
            order_tasks: int = None,
            harakiri: int = None,
            change_dir: str = None,
            poll_interval: int = None,
            signal_as_task: bool = None,
            cheap: bool = None,
            base_dir: str = None
    ):
        """

        :param touch_reload: reload spoolers if the specified file is modified/touched

        :param quiet: Do not log spooler related messages.

        :param process_count: Set the number of processes for spoolers.

        :param max_tasks: Set the maximum number of tasks to run before recycling
            a spooler (to help alleviate memory leaks).

        :param order_tasks: Try to order the execution of spooler tasks (uses scandir instead of readdir).

        :param harakiri: Set harakiri timeout for spooler tasks.

        :param change_dir: chdir() to specified directory before each spooler task.

        :param poll_interval: Spooler poll frequency in seconds. Default: 30.

        :param signal_as_task: Treat signal events as tasks in spooler.
            To be used with ``spooler-max-tasks``. If enabled spooler will treat signal
            events as task. Run signal handler will also increase the spooler task count.

        :param cheap: Use spooler cheap mode.

        :param base_dir: Base directory to prepend to `work_dir` argument of `.add()`.

        """
        self._set('touch-spoolers-reload', touch_reload, multi=True)
        self._set('spooler-quiet', quiet, cast=bool)
        self._set('spooler-processes', process_count)
        self._set('spooler-max-tasks', max_tasks)
        self._set('spooler-ordered', order_tasks, cast=bool)
        self._set('spooler-harakiri', harakiri)
        self._set('spooler-chdir', change_dir)
        self._set('spooler-frequency', poll_interval)
        self._set('spooler-signal-as-task', signal_as_task, cast=bool)
        self._set('spooler-cheap', cheap, cast=bool)

        if base_dir is not None:
            self._base_dir = base_dir

        return self._section

    def add(self, work_dir: Strlist, *, external: bool = False):
        """Run a spooler on the specified directory.

        :param work_dir: Spooler working directory path or it's name if
            `base_dir` argument of `spooler.set_basic_params()` is set.

            .. note:: Placeholders can be used to build paths, e.g.: {project_runtime_dir}/spool/
              See ``Section.project_name`` and ``Section.runtime_dir``.

        :param external: map spoolers requests to a spooler directory managed by an external instance

        """
        command = 'spooler'

        base_dir = self._base_dir
        if base_dir is not None:
            work_dir = [path.join(base_dir, work_dir_) for work_dir_ in listify(work_dir)]

        if external:
            command += '-external'

        self._set(command, self._section.replace_placeholders(work_dir), multi=True)

        return self._section
