from ..base import OptionsGroup


class Empire(OptionsGroup):
    """Emperor and his vassals.

    If you need to deploy a big number of apps on a single server,
    or a group of servers, the Emperor mode is just the ticket.

    * http://uwsgi-docs.readthedocs.io/en/latest/Emperor.html

    """

    def set_emperor_params(
            self, *, vassals_home=None, name=None, scan_interval=None, pid_file=None, spawn_asap=None,
            stats_address=None, trigger_socket=None, links_no_follow=None):
        """

        .. note:: The emperor should generally not be run with master, unless master features like advanced
            logging are specifically needed.

        .. note:: The emperor should generally be started at server boot time and left alone,
            not reloaded/restarted except for uWSGI upgrades;
            emperor reloads are a bit drastic, reloading all vassals at once.
            Instead vassals should be reloaded individually when needed, in the manner of the imperial monitor in use.

        :param str|list[str] vassals_home: Set vassals home and enable Emperor mode.

        :param str name: Set the Emperor process name.

        :param int scan_interval: Set the Emperor scan frequency. Default: 3 seconds.

        :param str pid_file: Write the Emperor pid in the specified file.

        :param bool spawn_asap: Spawn the Emperor as soon as possible.

        :param str stats_address: Run the Emperor stats server on specified address.

        :param str trigger_socket: Enable the Emperor trigger socket.

        :param bool links_no_follow: Do not follow symlinks when checking for mtime.

        """
        self._set('emperor', vassals_home, multi=True)
        self._set('emperor-procname', name)
        self._set('emperor-freq', scan_interval)
        self._set('emperor-pidfile', pid_file)
        self._set('early-emperor', spawn_asap, cast=bool)
        self._set('emperor-stats-server', stats_address)
        self._set('emperor-trigger-socket', trigger_socket)
        self._set('emperor-nofollow', links_no_follow)

        return self._section

    def print_monitors(self):
        """Print out enabled imperial monitors."""

        self._set('imperial-monitor-list', True, cast=bool)

        return self._section

    def set_emperor_command_params(
            self, command_socket=None, *,
            wait_for_command=None, wait_for_command_exclude=None):
        """Emperor commands related parameters.

        * http://uwsgi-docs.readthedocs.io/en/latest/tutorials/EmperorSubscriptions.html

        :param str command_socket: Enable the Emperor command socket.
            It is a channel allowing external process to govern vassals.

        :param bool wait_for_command: Always wait for a 'spawn' Emperor command before starting a vassal.

        :param str|list[str] wait_for_command_exclude: Vassals that will ignore ``wait_for_command``.

        """
        self._set('emperor-command-socket', command_socket)
        self._set('emperor-wait-for-command', wait_for_command, cast=bool)
        self._set('emperor-wait-for-command-ignore', wait_for_command_exclude, multi=True)

        return self._section

    def set_vassals_wrapper_params(self, *, wrapper=None, overrides=None, fallbacks=None):
        """Binary wrapper for vassals parameters.

        :param str wrapper: Set a binary wrapper for vassals.

        :param str|list[str] overrides: Set a binary wrapper for vassals to try before the default one

        :param str|list[str] fallbacks: Set a binary wrapper for vassals to try as a last resort.
            Allows you to specify an alternative binary to execute when running a vassal
            and the default binary_path is not found (or returns an error).

        """
        self._set('emperor-wrapper', wrapper)
        self._set('emperor-wrapper-override', overrides, multi=True)
        self._set('emperor-wrapper-fallback', fallbacks, multi=True)

        return self._section

    def set_throttle_params(self, *, level=None, level_max=None):
        """Throttling options.

        * http://uwsgi-docs.readthedocs.io/en/latest/Emperor.html#throttling
        * http://uwsgi-docs.readthedocs.io/en/latest/Emperor.html#loyalty

        :param int level: Set throttling level (in milliseconds) for bad behaving vassals. Default: 1000.

        :param int level_max: Set maximum throttling level (in milliseconds)
            for bad behaving vassals. Default: 3 minutes.

        """
        self._set('emperor-throttle', level)
        self._set('emperor-max-throttle', level_max)

        return self._section

    def set_tolerance_params(self, *, for_heartbeat=None, for_cursed_vassals=None):
        """Various tolerance options.

        :param int for_heartbeat: Set the Emperor tolerance about heartbeats.

            * http://uwsgi-docs.readthedocs.io/en/latest/Emperor.html#heartbeat-system

        :param int for_cursed_vassals: Set the Emperor tolerance about cursed vassals.

            * http://uwsgi-docs.readthedocs.io/en/latest/Emperor.html#blacklist-system

        """
        self._set('emperor-required-heartbeat', for_heartbeat)
        self._set('emperor-curse-tolerance', for_cursed_vassals)

        return self._section

    def set_mode_tyrant_params(self, enable=None, *, links_no_follow=None, use_initgroups=None):
        """Tyrant mode (secure multi-user hosting).

        In Tyrant mode the Emperor will run the vassal using the UID/GID of the vassal
        configuration file.

        * http://uwsgi-docs.readthedocs.io/en/latest/Emperor.html#tyrant-mode-secure-multi-user-hosting

        :param enable: Puts the Emperor in Tyrant mode.

        :param bool links_no_follow: Do not follow symlinks when checking for uid/gid in Tyrant mode.

        :param bool use_initgroups: Add additional groups set via initgroups() in Tyrant mode.

        """
        self._set('emperor-tyrant', enable, cast=bool)
        self._set('emperor-tyrant-nofollow', links_no_follow, cast=bool)
        self._set('emperor-tyrant-initgroups', use_initgroups, cast=bool)

        return self._section

    def set_mode_broodlord_params(
            self, zerg_count=None, *,
            vassal_overload_sos_interval=None, vassal_queue_items_sos=None):
        """This mode is a way for a vassal to ask for reinforcements to the Emperor.

        Reinforcements are new vassals spawned on demand generally bound on the same socket.

        .. warning:: If you are looking for a way to dynamically adapt the number
            of workers of an instance, check the Cheaper subsystem - adaptive process spawning mode.

            *Broodlord mode is for spawning totally new instances.*

        :param int zerg_count: Maximum number of zergs to spawn.

        :param int vassal_overload_sos_interval: Ask emperor for reinforcement when overloaded.
            Accepts the number of seconds to wait between asking for a new reinforcements.

        :param int vassal_queue_items_sos: Ask emperor for sos if listen queue (backlog) has more
            items than the value specified

        """
        self._set('emperor-broodlord', zerg_count)
        self._set('vassal-sos', vassal_overload_sos_interval)
        self._set('vassal-sos-backlog', vassal_queue_items_sos)

        return self._section
