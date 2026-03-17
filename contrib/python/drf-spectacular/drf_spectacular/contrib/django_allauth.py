from drf_spectacular.extensions import OpenApiAuthenticationExtension


class XSessionTokenAuthenticationScheme(OpenApiAuthenticationExtension):
    target_class = 'allauth.headless.contrib.rest_framework.authentication.XSessionTokenAuthentication'
    name = 'XSessionTokenAuth'
    optional = True

    def get_security_definition(self, auto_schema):
        return {
            "type": "apiKey",
            "in": "header",
            "name": "X-Session-Token",
            "description": "X-Session-Token authentication",
        }
