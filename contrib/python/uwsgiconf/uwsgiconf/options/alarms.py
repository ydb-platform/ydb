from operator import attrgetter
from typing import List

from .alarm_types import AlarmType, AlarmCommand, AlarmSignal, AlarmCurl, AlarmLog, AlarmMule, AlarmXmpp
from ..base import OptionsGroup
from ..utils import listify

TypeAlarmExt = List[AlarmType]


class Alarms(OptionsGroup):
    """Alarms.

    This subsystem allows the developer/sysadmin to "announce"
    special conditions of an app via various channels.

    * http://uwsgi-docs.readthedocs.io/en/latest/AlarmSubsystem.html

    """
    class alarm_types:
        """Alarm types available for ``.register_alarm()``."""

        command = AlarmCommand
        curl = AlarmCurl
        log = AlarmLog
        mule = AlarmMule
        signal = AlarmSignal
        xmpp = AlarmXmpp

    def __init__(self, *args, **kwargs):
        self._alarms = []
        super().__init__(*args, **kwargs)

    def set_basic_params(self, *, msg_size: int = None, cheap: bool = None, anti_loop_timeout: int = None):
        """
        :param msg_size: Set the max size of an alarm message in bytes. Default: 8192.

        :param cheap: Use main alarm thread rather than create dedicated
            threads for curl-based alarms

        :param anti_loop_timeout: Tune the anti-loop alarm system. Default: 3 seconds.

        """
        self._set('alarm-msg-size', msg_size)
        self._set('alarm-cheap', cheap, cast=bool)
        self._set('alarm-freq', anti_loop_timeout)

        return self._section

    def print_alarms(self):
        """Print out enabled alarms."""

        self._set('alarm-list', True, cast=bool)

        return self._section

    def register_alarm(self, alarm: TypeAlarmExt):
        """Register (create) an alarm.

        :param alarm: Alarm.

        """
        for alarm in listify(alarm):
            if alarm not in self._alarms:
                self._set('alarm', alarm, multi=True)
                self._alarms.append(alarm)

        return self._section

    def alarm_on_log(self, alarm: TypeAlarmExt, matcher: str, *, skip: bool = False):
        """Raise (or skip) the specified alarm when a log line matches the specified regexp.

        :param alarm: Alarm.

        :param matcher: Regular expression to match log line.

        :param skip:

        """
        self.register_alarm(alarm)

        value = f"{','.join(map(attrgetter('alias'), listify(alarm)))} {matcher}"

        self._set('not-alarm-log' if skip else 'alarm-log', value)

        return self._section

    def alarm_on_fd_ready(self, alarm: TypeAlarmExt, *, fd: str, message: str, byte_count: int = None):
        """Triggers the alarm when the specified file descriptor is ready for read.

        This is really useful for integration with the Linux eventfd() facility.
        Pretty low-level and the basis of most of the alarm plugins.

        * http://uwsgi-docs.readthedocs.io/en/latest/Changelog-1.9.7.html#alarm-fd

        :param alarm: Alarm.

        :param fd: File descriptor.

        :param message: Message to send.

        :param byte_count: Files to read. Default: 1 byte.

            .. note:: For ``eventfd`` set 8.

        """
        self.register_alarm(alarm)

        value = fd

        if byte_count:
            value += f':{byte_count}'

        value += f' {message}'

        for alarm_ in listify(alarm):
            self._set('alarm-fd', f'{alarm_.alias} {value}', multi=True)

        return self._section

    def alarm_on_queue_full(self, alarm: TypeAlarmExt):
        """Raise the specified alarm when the socket backlog queue is full.

        :param alarm: Alarm.

        """
        self.register_alarm(alarm)

        for alarm_ in listify(alarm):
            self._set('alarm-backlog', alarm_.alias, multi=True)

        return self._section

    def alarm_on_segfault(self, alarm: TypeAlarmExt):
        """Raise the specified alarm when the segmentation fault handler is executed.

        Sends a backtrace.

        :param alarm: Alarm.

        """
        self.register_alarm(alarm)

        for alarm_ in listify(alarm):
            self._set('alarm-segfault', alarm_.alias, multi=True)

        return self._section
