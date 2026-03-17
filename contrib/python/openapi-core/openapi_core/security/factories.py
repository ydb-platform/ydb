from openapi_core.security.providers import (
    ApiKeyProvider, HttpProvider, UnsupportedProvider,
)


class SecurityProviderFactory(object):

    PROVIDERS = {
        'apiKey': ApiKeyProvider,
        'http': HttpProvider,
    }

    def create(self, scheme):
        scheme_type = scheme['type']
        if scheme_type == 'apiKey':
            return ApiKeyProvider(scheme)
        elif scheme_type == 'http':
            return HttpProvider(scheme)
        return UnsupportedProvider(scheme)
