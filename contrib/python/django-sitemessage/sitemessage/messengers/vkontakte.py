from django.utils.translation import gettext as _

from .base import RequestsMessengerBase, TypeProxy
from ..exceptions import MessengerException


class VKontakteMessengerException(MessengerException):
    """Exceptions raised by VKontakte messenger."""


class VKontakteMessenger(RequestsMessengerBase):
    """Implements VKontakte page wall message publishing.

    Steps to be done:

    1. Create a user/community page.
    2. Create `Standalone` application at http://vk.com/apps?act=manage
    3. Get your Application ID (under Settings menu item in left menu)
    4. To generate an access token:

        VKontakteMessenger.get_access_token(app_id=APP_ID)

        * Replace APP_ID with actual application ID.
        * This will open browser window.

    5. Confirm and copy token from URL in browser (symbols after `access_token=` but before &)
    6. Use this token.

    """
    alias = 'vk'
    title = _('VKontakte')

    address_attr = 'vkontakte'

    _url_wall = 'https://api.vk.com/method/wall.post'
    _api_version = '5.131'

    def __init__(self, access_token: str, proxy: TypeProxy = None):
        """Configures messenger.

        :param access_token: Unique authentication token to access your VK user/community page.
        :param proxy: Dictionary of proxy settings,
            or a callable returning such a dictionary.

        """
        super().__init__(proxy=proxy)
        self.access_token = access_token

    @classmethod
    def get_access_token(self, *, app_id: str) -> str:
        """Return an URL to get access token.

        Opens browser, trying to redirect to a page
        from location URL of which one can extract access_token
        (see the messenger class docstring).

        :param app_id: Application ID.

        """
        url = (
            'https://oauth.vk.com/authorize?'
            'client_id=%(app_id)s&'
            'scope=wall,offline&'
            'display=page&'
            'response_type=token&'
            'v=%(api_version)s&'
            'redirect_uri=https://oauth.vk.com/blank.html'

        ) % {'app_id': app_id, 'api_version': self._api_version}

        import webbrowser
        webbrowser.open(url)

        return url

    def _send_message(self, msg: str, to: str = None):

        # Automatically deduce message type.
        message_type = 'attachments' if msg.startswith('http') else 'message'

        json = self.post(
            url=self._url_wall,
            data={
                message_type: msg,
                'owner_id': to,
                'from_group': 1,
                'access_token': self.access_token,
                'v': self._api_version,
            })

        if 'error' in json:
            error = json['error']
            raise VKontakteMessengerException(f"{error['error_code']}: {error['error_msg']}")

        return json['response']['post_id']  # Returns post ID.
