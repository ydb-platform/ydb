from typing import Type, List

from django.utils.translation import gettext as _

from .base import MessengerBase, Message, Dispatch
from ..exceptions import MessengerWarmupException

if False:  # pragma: nocover
    from ..messages.base import MessageBase  # noqa


class TwitterMessenger(MessengerBase):
    """Implements to Twitter message delivery using `twitter` module.

    https://github.com/sixohsix/twitter

    """

    alias = 'twitter'
    title = _('Tweet')

    address_attr = 'twitter'

    _session_started = False

    def __init__(self, api_key: str, api_secret: str, access_token: str, access_token_secret: str):
        """Configures messenger.

        Register Twitter application here - https://apps.twitter.com/

        :param api_key: API key for Twitter client
        :param api_secret: API secret for Twitter client
        :param access_token: Access token for an account to tweet from
        :param access_token_secret: Access token secret for an account to tweet from
        """
        import twitter

        self.lib = twitter
        self.api_key = api_key
        self.api_secret = api_secret
        self.access_token = access_token
        self.access_token_secret = access_token_secret

    def _test_message(self, to: str, text: str):
        return self._send_message(self._build_message(to, text))

    def before_send(self):
        try:
            self.api = self.lib.Twitter(
                auth=self.lib.OAuth(self.access_token, self.access_token_secret, self.api_key, self.api_secret))
            self._session_started = True

        except self.lib.api.TwitterError as e:
            raise MessengerWarmupException(f'Twitter Error: {e}')
    
    @classmethod
    def _build_message(cls, to: str, text: str):
        if to:
            if not to.startswith('@'):
                to = f'@{to}'

            to = f'{to} '

        else:
            to = ''

        return f'{to}{text}'

    def _send_message(self, msg: str):
        return self.api.statuses.update(status=msg)

    def send(self, message_cls: Type['MessageBase'], message_model: Message, dispatch_models: List[Dispatch]):
        if self._session_started:
            for dispatch_model in dispatch_models:
                msg = self._build_message(dispatch_model.address, dispatch_model.message_cache)
                try:
                    self._send_message(msg)
                    self.mark_sent(dispatch_model)
                except Exception as e:
                    self.mark_error(dispatch_model, e, message_cls)
