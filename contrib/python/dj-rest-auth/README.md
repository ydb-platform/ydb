# dj-rest-auth

[![CI](https://github.com/iMerica/dj-rest-auth/actions/workflows/main.yml/badge.svg)](https://github.com/iMerica/dj-rest-auth/actions/workflows/main.yml)
[![PyPI](https://img.shields.io/pypi/v/dj-rest-auth)](https://pypi.org/project/dj-rest-auth/)
[![Python](https://img.shields.io/badge/python-3.10%E2%80%933.14-blue)](https://pypi.org/project/dj-rest-auth/)
[![Django](https://img.shields.io/badge/django-4.2%E2%80%936.0-green)](https://pypi.org/project/dj-rest-auth/)

Drop-in authentication endpoints for Django REST Framework. Works seamlessly with SPAs and mobile apps.

**[Documentation](https://dj-rest-auth.readthedocs.io/)** | **[PyPI](https://pypi.org/project/dj-rest-auth/)**

## Features

- Login, logout, password change, password reset
- User registration with email verification
- JWT authentication with HTTP-only cookies
- Social auth (Google, GitHub, Facebook) via django-allauth
- Fully customizable serializers

## Architecture

```mermaid
flowchart LR
    Client[Client<br/>React / Vue / Mobile]
    
    subgraph Django
        subgraph dj-rest-auth
            Auth[Login / Logout]
            Reg[Registration]
            PW[Password Reset]
        end
        
        DRF[Django REST Framework]
        DJAuth[django.contrib.auth]
        AA[django-allauth]
        JWT[simplejwt]
    end
    
    Client <--> dj-rest-auth
    
    Auth --> DRF
    Auth --> DJAuth
    Auth -.-> JWT
    Reg -.-> AA
    PW --> DJAuth
```

## Quick Start

```bash
pip install dj-rest-auth
```

```python
# settings.py
INSTALLED_APPS = [
    ...
    'rest_framework',
    'rest_framework.authtoken',
    'dj_rest_auth',
]
```

```python
# urls.py
urlpatterns = [
    path('auth/', include('dj_rest_auth.urls')),
]
```

You now have:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/auth/login/` | POST | Obtain auth token |
| `/auth/logout/` | POST | Revoke token |
| `/auth/user/` | GET, PUT | User details |
| `/auth/password/change/` | POST | Change password |
| `/auth/password/reset/` | POST | Request reset email |
| `/auth/password/reset/confirm/` | POST | Confirm reset |

## JWT with HTTP-only Cookies

```bash
pip install dj-rest-auth djangorestframework-simplejwt
```

```python
# settings.py
REST_FRAMEWORK = {
    'DEFAULT_AUTHENTICATION_CLASSES': [
        'dj_rest_auth.jwt_auth.JWTCookieAuthentication',
    ],
}

REST_AUTH = {
    'USE_JWT': True,
    'JWT_AUTH_COOKIE': 'access',
    'JWT_AUTH_REFRESH_COOKIE': 'refresh',
    'JWT_AUTH_HTTPONLY': True,
}
```

## Registration

```bash
pip install 'dj-rest-auth[with-social]'
```

```python
# settings.py
INSTALLED_APPS = [
    ...
    'django.contrib.sites',
    'allauth',
    'allauth.account',
    'dj_rest_auth.registration',
]

SITE_ID = 1
```

```python
# urls.py
urlpatterns = [
    path('auth/', include('dj_rest_auth.urls')),
    path('auth/registration/', include('dj_rest_auth.registration.urls')),
]
```

## Documentation

Full documentation at **[dj-rest-auth.readthedocs.io](https://dj-rest-auth.readthedocs.io/)**

- [Installation & Configuration](https://dj-rest-auth.readthedocs.io/en/latest/getting-started/installation/)
- [API Endpoints](https://dj-rest-auth.readthedocs.io/en/latest/api/endpoints/)
- [JWT & Cookies Guide](https://dj-rest-auth.readthedocs.io/en/latest/guides/jwt-cookies/)
- [Social Authentication](https://dj-rest-auth.readthedocs.io/en/latest/guides/social-auth/)

## Contributing

```bash
pip install -r dj_rest_auth/tests/requirements.txt
python runtests.py
```

See [Contributing Guide](https://dj-rest-auth.readthedocs.io/en/latest/contributing/) for details.

## License

MIT
