import ad_api.version as vd


class BaseClient:
    scheme = 'https://'
    method = 'GET'
    content_type = 'application/x-www-form-urlencoded;charset=UTF-8'
    user_agent = 'python-ad-api'

    def __init__(self):
        try:
            version = vd.__version__
            self.user_agent += f'-{version}'
        except Exception:
            pass
