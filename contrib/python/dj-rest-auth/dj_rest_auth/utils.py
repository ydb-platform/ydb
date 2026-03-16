from django.utils.functional import lazy


def default_create_token(token_model, user, serializer):
    token, _ = token_model.objects.get_or_create(user=user)
    return token


def jwt_encode(user):
    from dj_rest_auth.app_settings import api_settings

    JWTTokenClaimsSerializer = api_settings.JWT_TOKEN_CLAIMS_SERIALIZER

    refresh = JWTTokenClaimsSerializer.get_token(user)
    return refresh.access_token, refresh


def format_lazy(s, *args, **kwargs):
    return s.format(*args, **kwargs)


format_lazy = lazy(format_lazy, str)
