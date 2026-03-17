import os
import wsgi_intercept

# Ensure that our test apps are sending strict headers.
wsgi_intercept.STRICT_RESPONSE_HEADERS = True


if os.environ.get('USER') == 'cdent':
    import warnings
    warnings.simplefilter('error')
