from ..base import ParametrizedValue
from ..typehints import Strlist
from ..utils import listify, filter_locals, KeyValue


class AlarmType(ParametrizedValue):

    alias: str = ''

    def __init__(self, alias: str, *args, **kwargs):
        self.alias = alias or ''
        super().__init__(*args)


class AlarmCommand(AlarmType):
    """Run a shell command, passing info into its stdin."""

    name = 'cmd'

    def __init__(self, alias: str, *, command: str):
        super().__init__(alias, command)


class AlarmSignal(AlarmType):
    """Raise an uWSGI signal."""

    name = 'signal'

    def __init__(self, alias: str, *, sig: int):
        super().__init__(alias, sig)


class AlarmLog(AlarmType):
    """Print line into log."""

    name = 'log'

    def __init__(self, alias: str):
        super().__init__(alias)


class AlarmMule(AlarmType):
    """Send info to a mule waiting for messages."""

    name = 'mule'

    def __init__(self, alias: str, *, mule: int):
        super().__init__(alias, mule)


class AlarmCurl(AlarmType):
    """Send info to a cURL-able URL."""

    name = 'curl'
    plugin = 'alarm_curl'
    args_joiner = ';'

    def __init__(
            self,
            alias: str,
            url: str,
            *,
            method: str = None,
            ssl: bool = None,
            ssl_insecure: bool = None,
            auth_user: str = None,
            auth_pass: str = None,
            timeout: int = None,
            conn_timeout: int = None,
            mail_from: str = None,
            mail_to: str = None,
            subject: str = None
    ):
        opts = KeyValue(
            filter_locals(locals(), drop=['alias', 'url']),
            bool_keys=['ssl', 'ssl_insecure'],
            items_separator=self.args_joiner,
        )
        super().__init__(alias, url, opts)


class AlarmXmpp(AlarmType):
    """Send info via XMPP/jabber."""

    name = 'xmpp'
    plugin = 'alarm_xmpp'
    args_joiner = ';'

    def __init__(self, alias: str, *, jid: str, password: str, recipients: Strlist):
        super().__init__(alias, jid, password, ','.join(listify(recipients)))
