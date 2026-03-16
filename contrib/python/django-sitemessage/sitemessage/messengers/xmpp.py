from typing import Type, List

from django.utils.translation import gettext as _

from .base import MessengerBase, Dispatch, Message
from ..exceptions import MessengerWarmupException

if False:  # pragma: nocover
    from ..messages.base import MessageBase  # noqa


class XMPPSleekMessenger(MessengerBase):
    """Implements XMPP message delivery using `sleekxmpp` module.

    http://sleekxmpp.com/

    """

    alias = 'xmppsleek'
    xmpp = None
    title = _('XMPP')

    address_attr = 'jabber'

    _session_started = False

    def __init__(
            self,
            from_jid: str,
            password: str,
            host: str = 'localhost',
            port: int = 5222,
            use_tls: bool = True,
            use_ssl: bool = False
    ):
        """Configures messenger.

        :param from_jid: Jabber ID to send messages from
        :param password: password to log into XMPP server
        :param host: XMPP server host
        :param port: XMPP server port
        :param use_tls: whether to use TLS
        :param use_ssl: whether to use SSL

        """
        import sleekxmpp

        self.lib = sleekxmpp

        self.from_jid = from_jid
        self.password = password
        self.host = host
        self.port = port
        self.use_tls = use_tls
        self.use_ssl = use_ssl

    def _test_message(self, to: str, text: str):
        return self._send_message(to, text)

    def _send_message(self, to: str, text: str):
        return self.xmpp.send_message(mfrom=self.from_jid, mto=to, mbody=text, mtype='chat')

    def before_send(self):
        def on_session_start(event):
            try:
                self.xmpp.send_presence()
                self.xmpp.get_roster()
                self._session_started = True

            except self.lib.exceptions.XMPPError as e:
                raise MessengerWarmupException(f'XMPP Error: {e}')

        self.xmpp = self.lib.ClientXMPP(self.from_jid, self.password)
        self.xmpp.add_event_handler('session_start', on_session_start)

        result = self.xmpp.connect(
            address=(self.host, self.port),
            reattempt=False,
            use_tls=self.use_tls,
            use_ssl=self.use_ssl
        )

        if result:
            self.xmpp.process(block=False)

    def after_send(self):
        if self._session_started:
            self.xmpp.disconnect(wait=True)  # Wait for a send queue.
            self._session_started = False

    def send(self, message_cls: Type['MessageBase'], message_model: Message, dispatch_models: List[Dispatch]):
        if self._session_started:
            for dispatch_model in dispatch_models:
                try:
                    self._send_message(dispatch_model.address, dispatch_model.message_cache)
                    self.mark_sent(dispatch_model)
                except Exception as e:
                    self.mark_error(dispatch_model, e, message_cls)
