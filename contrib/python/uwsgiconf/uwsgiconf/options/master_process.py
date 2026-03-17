from ..base import OptionsGroup
from ..exceptions import ConfigurationError
from ..typehints import Strlist, Strint
from ..utils import KeyValue


class MasterProcess(OptionsGroup):
    """Master process is a separate process offering mentoring capabilities
    for other processes. Only one master process per uWSGI instance.

    uWSGI's built-in prefork+threading multi-worker management mode,
    activated by flicking the master switch on.

    .. note:: For all practical serving deployments it's not really a good idea not to use master mode.

    """

    def set_basic_params(
            self,
            *,
            enable: bool = None,
            name: str = None,
            no_orphans: bool = None,
            as_root: bool = None,
            subproc_check_interval: int = None,
            fifo_file: str = None
    ):
        """

        :param enable: Enable uWSGI master process.

        :param name: Set master process name to given value.

        :param no_orphans: Automatically kill workers if master dies (can be dangerous for availability).

        :param as_root: Leave master process running as root.

        :param subproc_check_interval: Set the interval (in seconds) of master checks. Default: 1
            The master process makes a scan of subprocesses, etc. every N seconds.

            .. warning:: You can increase this time if you need to, but it's DISCOURAGED.

        :param fifo_file: Enables the master FIFO.

            .. note:: Placeholders can be used to build paths, e.g.: {project_runtime_dir}.fifo
              See ``Section.project_name`` and ``Section.runtime_dir``.

            Instead of signals, you can tell the master to create a UNIX named pipe (FIFO)
            that you may use to issue commands to the master.

            Up to 10 different FIFO files supported. By default the first specified is bound (mapped as '0').

            * http://uwsgi.readthedocs.io/en/latest/MasterFIFO.html#the-master-fifo

            .. note:: Since 1.9.17

        """
        self._set('master', enable, cast=bool)
        self._set('procname-master', name)
        self._set('no-orphans', no_orphans)
        self._set('master-as-root', as_root)
        self._set('check-interval', subproc_check_interval)
        self._set('master-fifo', self._section.replace_placeholders(fifo_file), multi=True)

        return self._section

    def set_exit_events(
            self,
            *,
            no_workers: bool = None,
            idle: bool = None,
            reload: bool = None,
            sig_term: bool = None
    ):
        """Do exit on certain events

        :param bool no_workers: Shutdown uWSGI when no workers are running.

        :param bool idle: Shutdown uWSGI when idle.

        :param bool reload: Force exit even if a reload is requested.

        :param bool sig_term: Exit on SIGTERM instead of brutal workers reload.

            .. note:: Before 2.1 SIGTERM reloaded the stack while SIGINT/SIGQUIT shut it down.

        """
        self._set('die-on-no-workers', no_workers, cast=bool)
        self._set('exit-on-reload', reload, cast=bool)
        self._set('die-on-term', sig_term, cast=bool)
        self.set_idle_params(exit=idle)

        return self._section

    def set_exception_handling_params(
            self,
            *,
            handler: Strlist = None,
            catch: bool = None,
            no_write_exception: bool = None
    ):
        """Exception handling related params.

        :param handler: Register one or more exception handling C-functions.

        :param catch: Catch exceptions and report them as http output (including stack trace and env params).

            .. warning:: Use only for testing purposes.

        :param no_write_exception: Disable exception generation on write()/writev().

            .. note:: This can be combined with ``logging.set_filters(write_errors=False, sigpipe=False)``.

            .. note: Currently available for Python.

        """
        self._set('exception-handler', handler, multi=True)
        self._set('catch-exceptions', catch, cast=bool)
        self._set('disable-write-exception', no_write_exception, cast=bool)

        return self._section

    def set_idle_params(self, *, timeout: int = None, exit: bool = None):
        """Activate idle mode - put uWSGI in cheap mode after inactivity timeout.

        :param timeout: Inactivity timeout in seconds.

        :param exit: Shutdown uWSGI when idle.

        """
        self._set('idle', timeout)
        self._set('die-on-idle', exit, cast=bool)

        return self._section

    def set_reload_params(self, *, mercy: int = None, exit: bool = None):
        """Set reload related params.

        :param mercy: Set the maximum time (in seconds) we wait
            for workers and other processes to die during reload/shutdown.

        :param exit: Force exit even if a reload is requested.

        """
        self._set('reload-mercy', mercy)
        self.set_exit_events(reload=exit)

        return self._section

    def add_cron_task(
            self,
            command: str,
            *,
            weekday: Strint = None,
            month: Strint = None,
            day: Strint = None,
            hour: Strint = None,
            minute: Strint = None,
            legion: str = None,
            unique: bool = None,
            harakiri: int = None
    ):
        """Adds a cron task running the given command on the given schedule.
        http://uwsgi.readthedocs.io/en/latest/Cron.html

        HINTS:
            * Use negative values to say `every`:
                hour=-3  stands for `every 3 hours`

            * Use - (minus) to make interval:
                minute='13-18'  stands for `from minute 13 to 18`

        .. note:: We use cron2 option available since 1.9.11.

        :param command: Command to execute on schedule (with or without path).

        :param weekday: Day of a the week number. Defaults to `each`.
            0 - Sunday  1 - Monday  2 - Tuesday  3 - Wednesday
            4 - Thursday  5 - Friday  6 - Saturday

        :param month: Month number 1-12. Defaults to `each`.

        :param day: Day of the month number 1-31. Defaults to `each`.

        :param hour: Hour 0-23. Defaults to `each`.

        :param minute: Minute 0-59. Defaults to `each`.

        :param legion: Set legion (cluster) name to use this cron command against.
            Such commands are only executed by legion lord node.

        :param unique: Marks command as unique. Default to not unique.
            Some commands can take a long time to finish or just hang doing their thing.
            Sometimes this is okay, but there are also cases when running multiple instances
            of the same command can be dangerous.

        :param harakiri: Enforce a time limit (in seconds) on executed commands.
            If a command is taking longer it will be killed.

        """
        rule = KeyValue(
            locals(),
            keys=['weekday', 'month', 'day', 'hour', 'minute', 'harakiri', 'legion', 'unique'],
            aliases={'weekday': 'week'},
            bool_keys=['unique'],
        )

        self._set('cron2', (f'{rule} {command}').strip(), multi=True)

        return self._section

    def attach_process_classic(
            self,
            command_or_pid_path: str,
            *,
            background: bool,
            control: bool = False,
            for_legion: bool = False
    ):
        """Attaches a command/daemon to the master process optionally managed by a pidfile.

        This will allow the uWSGI master to control/monitor/respawn this process.

        .. note:: This uses old classic uWSGI means of process attaching
            To have more control use ``.attach_process()`` method (requires  uWSGI 2.0+)

        http://uwsgi-docs.readthedocs.io/en/latest/AttachingDaemons.html

        :param command_or_pid_path:

        :param background: Must indicate whether process is in background.

        :param control: Consider this process a control: when the daemon dies, the master exits.

            .. note:: pidfile managed processed not supported.

        :param for_legion: Legion daemons will be executed only on the legion lord node,
            so there will always be a single daemon instance running in each legion.
            Once the lord dies a daemon will be spawned on another node.

            .. note:: uWSGI 1.9.9+ required.

        """
        prefix = 'legion-' if for_legion else ''

        if '.pid' in command_or_pid_path:

            if background:
                # Attach a command/daemon to the master process managed by a pidfile (the command must daemonize)
                self._set(f'{prefix}smart-attach-daemon', command_or_pid_path, multi=True)

            else:
                # Attach a command/daemon to the master process managed by a pidfile (the command must NOT daemonize)
                self._set(f'{prefix}smart-attach-daemon2', command_or_pid_path, multi=True)

        else:
            if background:
                raise ConfigurationError('Background flag is only supported for pid-governed commands')

            if control:
                # todo needs check
                self._set('attach-control-daemon', command_or_pid_path, multi=True)

            else:
                # Attach a command/daemon to the master process (the command has to remain in foreground)
                self._set(f'{prefix}attach-daemon', command_or_pid_path, multi=True)

        return self._section

    def attach_process(
            self,
            command: str,
            *,
            for_legion: bool = False,
            broken_counter: int = None,
            pidfile: str = None,
            control: bool = None,
            daemonize: bool = None,
            touch_reload: Strlist = None,
            signal_stop: int = None,
            signal_reload: int = None,
            honour_stdin: bool = None,
            uid: Strint = None,
            gid: Strint = None,
            new_pid_ns: bool = None,
            change_dir: str = None
    ):
        """Attaches a command/daemon to the master process.

        This will allow the uWSGI master to control/monitor/respawn this process.

        http://uwsgi-docs.readthedocs.io/en/latest/AttachingDaemons.html

        :param command: The command line to execute.

        :param for_legion: Legion daemons will be executed only on the legion lord node,
            so there will always be a single daemon instance running in each legion.
            Once the lord dies a daemon will be spawned on another node.

        :param broken_counter: Maximum attempts before considering a daemon "broken".

        :param pidfile: The pidfile path to check (enable smart mode).

        :param control: If True, the daemon becomes a `control` one:
            if it dies the whole uWSGI instance dies.

        :param daemonize: Daemonize the process (enable smart2 mode).

        :param touch_reload: List of files to check:
            whenever they are 'touched', the daemon is restarted

        :param signal_stop: The signal number to send to the daemon when uWSGI is stopped.

        :param signal_reload: The signal number to send to the daemon when uWSGI is reloaded.

        :param honour_stdin: The signal number to send to the daemon when uWSGI is reloaded.

        :param uid: Drop privileges to the specified uid.

            .. note:: Requires master running as root.

        :param gid: Drop privileges to the specified gid.

            .. note:: Requires master running as root.

        :param new_pid_ns: Spawn the process in a new pid namespace.

            .. note:: Requires master running as root.

            .. note:: Linux only.

        :param change_dir: Use chdir() to the specified directory
            before running the command.

        """
        rule = KeyValue(
            locals(),
            keys=[
                'command', 'broken_counter', 'pidfile', 'control', 'daemonize', 'touch_reload',
                'signal_stop', 'signal_reload', 'honour_stdin',
                'uid', 'gid', 'new_pid_ns', 'change_dir',
            ],
            aliases={
                'command': 'cmd',
                'broken_counter': 'freq',
                'touch_reload': 'touch',
                'signal_stop': 'stopsignal',
                'signal_reload': 'reloadsignal',
                'honour_stdin': 'stdin',
                'new_pid_ns': 'ns_pid',
                'change_dir': 'chdir',
            },
            bool_keys=['control', 'daemonize', 'honour_stdin'],
            list_keys=['touch_reload'],
        )

        prefix = 'legion-' if for_legion else ''

        self._set(prefix + 'attach-daemon2', rule, multi=True)

        return self._section
