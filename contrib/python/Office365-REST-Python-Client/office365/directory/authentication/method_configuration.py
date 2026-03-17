from office365.entity import Entity


class AuthenticationMethodConfiguration(Entity):
    """
    This is an abstract type that represents the settings for each authentication method. It has the configuration
    of whether a specific authentication method is enabled or disabled for the tenant and which users and groups
    can register and use that method.
    """
