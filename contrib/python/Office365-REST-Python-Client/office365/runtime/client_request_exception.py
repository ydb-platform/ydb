from requests import RequestException


class ClientRequestException(RequestException):
    def __init__(self, *args, **kwargs):
        super(ClientRequestException, self).__init__(*args, **kwargs)
        content_type = (
            self.response.headers.get("Content-Type", "").lower().split(";")[0]
        )
        if self.response.content and content_type == "application/json":
            self.payload = self.response.json()
        else:
            self.payload = None
        args = (self.code, self.message) + args
        self.args = args

    @property
    def code(self):
        if self.payload:
            error = self.payload.get("error")
            if error:
                return error.get("code")

    @property
    def message_lang(self):
        if self.payload:
            error = self.payload.get("error")
            if error:
                message = error.get("message")
                if isinstance(message, dict):
                    return message.get("lang")

    @property
    def message(self):
        # type: () -> str
        if self.payload:
            error = self.payload.get("error")
            if error:
                message = error.get("message")
                if isinstance(message, dict):
                    return message.get("value")
                return message
