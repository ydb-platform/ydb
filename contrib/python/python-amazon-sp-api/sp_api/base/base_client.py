from sp_api.__version__ import __version__


class BaseClient:
    scheme = 'https://'
    method = 'GET'
    content_type = 'application/x-www-form-urlencoded;charset=UTF-8'
    user_agent = f'python-sp-api-{__version__}'
