# -*- coding: utf-8 -*-


class BaseMatcher(object):

    """
    Base class that ensures sub-classes that implement custom matchers can be
    registered and have the only method that is required.

    Usage:

    .. code-block:: python

        from betamax import Betamax, BaseMatcher

        class MyMatcher(BaseMatcher):
            name = 'my'

            def match(self, request, recorded_request):
                # My fancy matching algorithm

        Betamax.register_request_matcher(MyMatcher)

    The last line is absolutely necessary.

    The `match` method will be given a `requests.PreparedRequest` object and a
    dictionary. The dictionary always has the following keys:

    - url
    - method
    - body
    - headers

    """

    name = None

    def __init__(self):
        if not self.name:
            raise ValueError('Matchers require names')
        self.on_init()

    def on_init(self):
        """Method to implement if you wish something to happen in ``__init__``.

        The return value is not checked and this is called at the end of
        ``__init__``. It is meant to provide the matcher author a way to
        perform things during initialization of the instance that would
        otherwise require them to override ``BaseMatcher.__init__``.
        """
        return None

    def match(self, request, recorded_request):
        """A method that must be implemented by the user.

        :param PreparedRequest request: A requests PreparedRequest object
        :param dict recorded_request: A dictionary containing the serialized
            request in the cassette
        :returns bool: True if they match else False
        """
        raise NotImplementedError('The match method must be implemented on'
                                  ' %s' % self.__class__.__name__)
