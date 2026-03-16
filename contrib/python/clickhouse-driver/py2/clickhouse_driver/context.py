
class Context(object):
    def __init__(self):
        self._server_info = None
        self._settings = None
        self._client_settings = None
        super(Context, self).__init__()

    @property
    def server_info(self):
        return self._server_info

    @server_info.setter
    def server_info(self, value):
        self._server_info = value

    @property
    def settings(self):
        return self._settings.copy()

    @settings.setter
    def settings(self, value):
        self._settings = value.copy()

    @property
    def client_settings(self):
        return self._client_settings.copy()

    @client_settings.setter
    def client_settings(self, value):
        self._client_settings = value.copy()

    def __repr__(self):
        return '<Context(server_info=%s, client_settings=%s, settings=%s)>' % (
            self._server_info, self._client_settings, self._settings
        )
