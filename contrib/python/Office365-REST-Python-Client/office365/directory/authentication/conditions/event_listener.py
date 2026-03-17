from office365.entity import Entity


class AuthenticationEventListener(Entity):
    """
    Defines a listener to evaluate when an authentication event happens in an authentication experience.
    An authenticationListener is abstract and is the base class of the various types of listeners you can evaluate
    during an authentication event.
    """
