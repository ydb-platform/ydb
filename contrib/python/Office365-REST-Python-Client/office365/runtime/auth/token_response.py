class TokenResponse(object):
    def __init__(self, access_token=None, token_type=None, **kwargs):
        self.accessToken = access_token
        self.tokenType = token_type
        for key, value in kwargs.items():
            setattr(self, key, value)

    @property
    def is_valid(self):
        return self.accessToken is not None and self.tokenType == "Bearer"

    @property
    def authorization_header(self):
        return "Bearer {0}".format(self.accessToken)

    @staticmethod
    def from_json(value):
        error = value.get("error", None)
        if error:
            raise ValueError(value)

        def _normalize_key(name):
            key_parts = name.split("_")
            if len(key_parts) >= 2:
                names = [n.title() for n in key_parts[1:]]
                return key_parts[0] + "".join(names)
            return name

        json = {_normalize_key(k): v for k, v in value.items()}
        return TokenResponse(**json)
