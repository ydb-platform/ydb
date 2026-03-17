from typing import Type, List

from django.utils.translation import gettext as _

from .base import RequestsMessengerBase, TypeProxy, Message, Dispatch
from ..exceptions import MessengerWarmupException, MessengerException

if False:  # pragma: nocover
    from ..messages.base import MessageBase  # noqa


class TelegramMessengerException(MessengerException):
    """Exceptions raised by Telegram messenger."""


class TelegramMessenger(RequestsMessengerBase):
    """Implements Telegram message delivery via Telegram Bot API."""

    alias = 'telegram'
    title = _('Telegram')

    address_attr = 'telegram'

    _session_started = False
    _tpl_url = 'https://api.telegram.org/bot%(token)s/%(method)s'

    def __init__(self, auth_token: str, proxy: TypeProxy = None):
        """Configures messenger.

        Register a Telegram Bot using instructions from https://core.telegram.org/bots/api

        :param auth_token: Bot unique authentication token
        """
        super().__init__(proxy=proxy)
        self.auth_token = auth_token

    def _verify_bot(self):
        """Sends an API command to test whether bot is authorized."""
        self._send_command('getMe')

    def get_updates(self) -> list:
        """Returns new messages addressed to bot."""
        with self.before_after_send_handling():
            result = self._send_command('getUpdates')
        return result

    def get_chat_ids(self) -> list:
        """Returns unique chat IDs from `/start` command messages sent to our bot by users.
        Those chat IDs can be used to send messages to chats.

        """
        updates = self.get_updates()
        chat_ids = []
        if updates:
            for update in updates:
                message = update['message']
                if message['text'] == '/start':
                    chat_ids.append(message['chat']['id'])
        return list(set(chat_ids))

    def before_send(self):
        if self._session_started:
            return

        try:
            self._verify_bot()
            self._session_started = True

        except TelegramMessengerException as e:
            raise MessengerWarmupException(f'Telegram Error: {e}')

    def _build_message(self, text: str, to: str = None) -> dict:
        return {'chat_id': to, 'text': text}

    def _send_command(self, method_name: str, data: dict = None):
        """Sends a command to API.

        :param method_name:
        :param data:

        """
        json = self.post(url=self._tpl_url % {'token': self.auth_token, 'method': method_name}, data=data)

        if not json['ok']:
            raise TelegramMessengerException(json['description'])

        return json['result']

    def _send_message(self, msg: dict, to=None):
        return self._send_command('sendMessage', msg)

    def send(self, message_cls: Type['MessageBase'], message_model: Message, dispatch_models: List[Dispatch]):
        if not self._session_started:
            return
        super().send(message_cls, message_model, dispatch_models)
