import os
from pathlib import Path
from typing import Optional, List, Union, Tuple

from .main_process_actions import *
from ..base import OptionsGroup
from ..typehints import Strint, Strlist

TypeActionExt = Union[str, 'HookAction', List[Union[str, 'HookAction']]]


class MainProcess(OptionsGroup):
    """Main process is the uWSGI process.

    .. warning:: Do not run uWSGI instances as root.
        You can start your uWSGIs as root, but be sure to drop privileges
        with the ``uid`` and ``gid`` options from ``set_owner_params``.

    """

    class actions:
        """Actions available for ``.set_hook()``."""

        alarm = ActionAlarm
        call = ActionCall
        dir_change = ActionDirChange
        dir_create = ActionDirCreate
        execute = ActionExecute
        exit = ActionExit
        fifo_write = ActionFifoWrite
        file_create = ActionFileCreate
        file_write = ActionFileWrite
        mount = ActionMount
        printout = ActionPrintout
        set_host_name = ActionSetHostName
        unlink = ActionUnlink

        # todo consider adding:
        # putenv, chmod/sticky, chown/chown2, rpc/retryrpc, unix_signal
        # wait_for_fs/wait_for_file/wait_for_dir, wait_for_socket

    class phases:
        """Phases available for hooking using ``.set_hook()``.

        Some of them may be **fatal** - a failing hook for them
        will mean failing of the whole uWSGI instance (generally calling exit(1)).

        """

        ASAP = 'asap'
        """As soon as possible. **Fatal**
        
        Run directly after configuration file has been parsed, before anything else is done.
        
        """

        JAIL_PRE = 'pre-jail'
        """Before jailing. **Fatal** 
        
        Run before any attempt to drop privileges or put the process in some form of jail.        
        
        """

        JAIL_IN = 'in-jail'
        """In jail after initialization. **Fatal** 

        Run soon after jayling, but after post-jail. 
        If jailing requires fork(), the chidlren run this phase.
        
        """

        JAIL_POST = 'post-jail'
        """After jailing. **Fatal**
        
        Run soon after any jailing, but before privileges drop. 
        If jailing requires fork(), the parent process run this phase.
        
        """

        PRIV_DROP_PRE = 'as-root'
        """Before privileges drop. **Fatal**
        
        Last chance to run something as root.
        
        """

        PRIV_DROP_POST = 'as-user'
        """After privileges drop. **Fatal**"""

        MASTER_START = 'master-start'
        """When Master starts."""

        EMPEROR_START = 'emperor-start'
        """When Emperor starts."""

        EMPEROR_STOP = 'emperor-stop'
        """When Emperor sent a stop message."""

        EMPEROR_RELOAD = 'emperor-reload'
        """When Emperor sent a reload message."""

        EMPEROR_LOST = 'emperor-lost'
        """When Emperor connection is lost."""

        EXIT = 'as-user-atexit'
        """Before app exit and reload."""

        APP_LOAD_PRE = 'pre-app'
        """Before app loading. **Fatal**"""

        APP_LOAD_POST = 'post-app'
        """After app loading. **Fatal**"""

        VASSAL_ON_DEMAND_IN = 'as-on-demand-vassal'
        """Whenever a vassal enters on-demand mode."""

        VASSAL_CONFIG_CHANGE_POST = 'as-on-config-vassal'
        """Whenever the emperor detects a config change for an on-demand vassal."""

        VASSAL_START_PRE = 'as-emperor-before-vassal'
        """Before the new vassal is spawned."""

        VASSAL_PRIV_DRP_PRE = 'as-vassal-before-drop'
        """In vassal, before dropping its privileges."""

        VASSAL_SET_NAMESPACE = 'as-emperor-setns'
        """In the emperor entering vassal namespace."""

        VASSAL_START_IN = 'as-vassal'
        """In the vassal before executing the uwsgi binary. **Fatal** 
        
        In vassal on start just before calling exec() directly in the new namespace.
        
        """

        VASSAL_START_POST = 'as-emperor'
        """In the emperor soon after a vassal has been spawn.
         
        Setting 4 env vars:
            * UWSGI_VASSAL_CONFIG 
            * UWSGI_VASSAL_PID
            * UWSGI_VASSAL_UID 
            * UWSGI_VASSAL_GID

        """

        GATEWAY_START_IN_EACH = 'as-gateway'
        """In each gateway on start."""

        MULE_START_IN_EACH = 'as-mule'
        """In each mule on start."""

        WORKER_ACCEPTING_PRE_EACH = 'accepting'
        """Before the each worker starts accepting requests. 
        
        .. note:: Since 1.9.21
        
        """

        WORKER_ACCEPTING_PRE_FIRST = 'accepting1'
        """Before the first worker starts accepting requests.
        
        .. note:: Since 1.9.21
        
        """

        WORKER_ACCEPTING_PRE_EACH_ONCE = 'accepting-once'
        """Before the each worker starts accepting requests, one time per instance. 
        
        .. note:: Since 1.9.21
        
        """

        WORKER_ACCEPTING_PRE_FIRST_ONCE = 'accepting1-once'
        """Before the first worker starts accepting requests, one time per instance.
        
        .. note:: Since 1.9.21
        
        """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.owner: List[Optional[Strint], Optional[Strint]] = [None, None]

    def set_basic_params(
            self,
            *,
            touch_reload: Strlist = None,
            priority: int = None,
            vacuum: bool = None,
            binary_path: str = None,
            honour_stdin: bool = None
    ):
        """

        :param touch_reload: Reload uWSGI if the specified file or directory is modified/touched.

        :param priority: Set processes/threads priority (``nice``) value.

        :param vacuum: Try to remove all of the generated files/sockets
            (UNIX sockets and pidfiles) upon exit.

        :param binary_path: Force uWSGI binary path.
            If you do not have uWSGI in the system path you can force its path with this option
            to permit the reloading system and the Emperor to easily find the binary to execute.

        :param honour_stdin: Do not remap stdin to ``/dev/null``.
            By default, ``stdin`` is remapped to ``/dev/null`` on uWSGI startup.
            If you need a valid stdin (for debugging, piping and so on) use this option.

        """
        self._set('touch-reload', touch_reload, multi=True)
        self._set('prio', priority)
        self._set('vacuum', vacuum, cast=bool)
        self._set('binary-path', binary_path)
        self._set('honour-stdin', honour_stdin, cast=bool)

        return self._section

    def set_memory_params(self, *, ksm_interval: int = None, no_swap: bool = None):
        """Set memory related parameters.

        :param ksm_interval: Kernel Samepage Merging frequency option, that can reduce memory usage.
            Accepts a number of requests (or master process cycles) to run page scanner after.

            .. note:: Linux only.

            * http://uwsgi.readthedocs.io/en/latest/KSM.html

        :param no_swap: Lock all memory pages avoiding swapping.

        """
        self._set('ksm', ksm_interval)
        self._set('never_swap', no_swap, cast=bool)

        return self._section

    def daemonize(self, log_into: str, *, after_app_loading: bool = False):
        """Daemonize uWSGI.

        :param str log_into: Logging destination:

            * File: /tmp/mylog.log

            * UPD: 192.168.1.2:1717

                .. note:: This will require an UDP server to manage log messages.
                    Use ``networking.register_socket('192.168.1.2:1717, type=networking.SOCK_UDP)``
                    to start uWSGI UDP server.

        :param after_app_loading: Whether to daemonize after
            or before applications loading.

        """
        self._set('daemonize2' if after_app_loading else 'daemonize', log_into)

        return self._section

    def change_dir(self, to: str, *, after_app_loading: bool = False):
        """Chdir to specified directory before or after apps loading.

        :param to: Target directory.

        :param after_app_loading:
                *True* - after load
                *False* - before load

        """
        self._set('chdir2' if after_app_loading else 'chdir', to)

        return self._section

    def set_owner_params(
            self,
            *,
            uid: Strint = None,
            gid: Strint = None,
            add_gids: Union[Strint, List[Strint]] = None,
            set_asap: bool = False
    ):
        """Set process owner params - user, group.

        :param uid: Set uid to the specified username or uid.

        :param gid: Set gid to the specified groupname or gid.

        :param add_gids: Add the specified group id to the process credentials.
            This options allows you to add additional group ids to the current process.
            You can specify it multiple times.

        :param set_asap: Set as soon as possible.
            Setting them on top of your vassal file will force the instance to setuid()/setgid()
            as soon as possible and without the (theoretical) possibility to override them.

        """
        prefix = 'immediate-' if set_asap else ''

        self._set(prefix + 'uid', uid)
        self._set(prefix + 'gid', gid)
        self._set('add-gid', add_gids, multi=True)

        # This may be wrong for subsequent method calls.
        self.owner = [uid, gid]

        return self._section

    def get_owner(self, *, default: bool = True) -> Tuple[Optional[Strint], Optional[Strint]]:
        """Return (User ID, Group ID) tuple

        :param default: Whether to return default if not set.

        """
        uid, gid = self.owner

        if not uid and default:
            uid = os.getuid()

        if not gid and default:
            gid = os.getgid()

        return uid, gid

    def set_hook(self, phase: str, action: TypeActionExt):
        """Allows setting hooks (attaching actions) for various uWSGI phases.

        :param phase: See constants in ``.phases``.

        :param action:

        """
        self._set(f'hook-{phase}', action, multi=True)

        return self._section

    def set_hook_touch(self, fpath: Union[str, Path], action: TypeActionExt):
        """Allows running certain action when the specified file is touched.

        :param fpath: File path.

        :param action:

        """
        self._set('hook-touch', f'{fpath} {action}', multi=True)

        return self._section

    def set_hook_after_request(self, func: str):
        """Run the specified function/symbol (C level) after each request.

        :param func:

        """
        self._set('after-request-hook', func, multi=True)

        return self._section

    def set_on_exit_params(self, *, skip_hooks: bool = None, skip_teardown: bool = None):
        """Set params related to process exit procedure.

        :param skip_hooks: Skip ``EXIT`` phase hook.

            .. note:: Ignored by the master.

        :param skip_teardown: Allows skipping teardown (finalization) processes for some plugins.

            .. note:: Ignored by the master.

            Supported by:
                * Perl
                * Python

        """
        self._set('skip-atexit', skip_hooks, cast=bool)
        self._set('skip-atexit-teardown', skip_teardown, cast=bool)

        return self._section

    def run_command_on_event(self, command: str, *, phase: str = phases.ASAP):
        """Run the given command on a given phase.

        :param command:

        :param phase: See constants in ``Phases`` class.

        """
        self._set(f'exec-{phase}', command, multi=True)

        return self._section

    def run_command_on_touch(self, command: str, *, target: str):
        """Run command when the specified file is modified/touched.

        :param command:

        :param target: File path.

        """
        self._set('touch-exec', f'{target} {command}', multi=True)

        return self._section

    def set_pid_file(self, fpath: Union[str, Path], *, before_priv_drop: bool = True, safe: bool = False):
        """Creates pidfile before or after privileges drop.

        :param fpath: File path.

        :param before_priv_drop: Whether to create pidfile before privileges are dropped.

            .. note:: Vacuum is made after privileges drop, so it may not be able
                to delete PID file if it was created before dropping.

        :param safe: The safe-pidfile works similar to pidfile
            but performs the write a little later in the loading process.
            This avoids overwriting the value when app loading fails,
            with the consequent loss of a valid PID number.

        """
        command = 'pidfile'

        if not before_priv_drop:
            command += '2'

        if safe:
            command = 'safe-' + command

        self._set(command, str(fpath))

        return self._section

    def set_naming_params(self, *, autonaming: bool = None, prefix: str = None, suffix: str = None, name: str = None):
        """Setups processes naming parameters.

        :param autonaming: Automatically set process name to something meaningful.
            Generated process names may be 'uWSGI Master', 'uWSGI Worker #', etc.

        :param prefix: Add prefix to process names.

        :param suffix: Append string to process names.

        :param name: Set process names to given static value.

        """
        self._set('auto-procname', autonaming, cast=bool)
        self._set('procname-prefix%s' % ('-spaced' if prefix and prefix.endswith(' ') else ''), prefix)
        self._set('procname-append', suffix)
        self._set('procname', name)

        return self._section
