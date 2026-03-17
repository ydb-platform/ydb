# -*- coding: utf-8 -*-
from . import matchers, serializers
from .adapter import BetamaxAdapter
from .cassette import Cassette
from .configure import Configuration
from .options import Options


class Betamax(object):

    """This object contains the main API of the request-vcr library.

    This object is entirely a context manager so all you have to do is:

    .. code::

        s = requests.Session()
        with Betamax(s) as vcr:
            vcr.use_cassette('example')
            r = s.get('https://httpbin.org/get')

    Or more concisely, you can do:

    .. code::

        s = requests.Session()
        with Betamax(s).use_cassette('example') as vcr:
            r = s.get('https://httpbin.org/get')

    This object allows for the user to specify the cassette library directory
    and default cassette options.

    .. code::

        s = requests.Session()
        with Betamax(s, cassette_library_dir='tests/cassettes') as vcr:
            vcr.use_cassette('example')
            r = s.get('https://httpbin.org/get')

        with Betamax(s, default_cassette_options={
                're_record_interval': 1000
                }) as vcr:
            vcr.use_cassette('example')
            r = s.get('https://httpbin.org/get')

    """

    def __init__(self, session, cassette_library_dir=None,
                 default_cassette_options={}):
        #: Store the requests.Session object being wrapped.
        self.session = session
        #: Store the session's original adapters.
        self.http_adapters = session.adapters.copy()
        #: Create a new adapter to replace the existing ones
        self.betamax_adapter = BetamaxAdapter(old_adapters=self.http_adapters)
        # We need a configuration instance to make life easier
        self.config = Configuration()
        # Merge the new cassette options with the default ones
        self.config.default_cassette_options.update(
            default_cassette_options or {}
        )

        # If it was passed in, use that instead.
        if cassette_library_dir:
            self.config.cassette_library_dir = cassette_library_dir

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *ex_args):
        self.stop()
        # ex_args comes through as the exception type, exception value and
        # exception traceback. If any of them are not None, we should probably
        # try to raise the exception and not muffle anything.
        if any(ex_args):
            # If you return False, Python will re-raise the exception for you
            return False

    @staticmethod
    def configure():
        """Help to configure the library as a whole.

        .. code::

            with Betamax.configure() as config:
                config.cassette_library_dir = 'tests/cassettes/'
                config.default_cassette_options['match_options'] = [
                    'method', 'uri', 'headers'
                    ]
        """
        return Configuration()

    @property
    def current_cassette(self):
        """Return the cassette that is currently in use.

        :returns: :class:`Cassette <betamax.cassette.Cassette>`
        """
        return self.betamax_adapter.cassette

    @staticmethod
    def register_request_matcher(matcher_class):
        """Register a new request matcher.

        :param matcher_class: (required), this must sub-class
            :class:`BaseMatcher <betamax.matchers.BaseMatcher>`
        """
        matchers.matcher_registry[matcher_class.name] = matcher_class()

    @staticmethod
    def register_serializer(serializer_class):
        """Register a new serializer.

        :param matcher_class: (required), this must sub-class
            :class:`BaseSerializer <betamax.serializers.BaseSerializer>`
        """
        name = serializer_class.name
        serializers.serializer_registry[name] = serializer_class()

    # ▶
    def start(self):
        """Start recording or replaying interactions."""
        for k in self.http_adapters:
            self.session.mount(k, self.betamax_adapter)
        dispatch_hooks('after_start', self.betamax_adapter.cassette)

    # ■
    def stop(self):
        """Stop recording or replaying interactions."""
        dispatch_hooks('before_stop', self.betamax_adapter.cassette)

        # No need to keep the cassette in memory any longer.
        self.betamax_adapter.eject_cassette()
        # On exit, we no longer wish to use our adapter and we want the
        # session to behave normally! Woooo!
        self.betamax_adapter.close()
        for (k, v) in self.http_adapters.items():
            self.session.mount(k, v)

    def use_cassette(self, cassette_name, **kwargs):
        """Tell Betamax which cassette you wish to use for the context.

        :param str cassette_name: relative name, without the serialization
            format, of the cassette you wish Betamax would use
        :param str serialize_with: the format you want Betamax to serialize
            the cassette with
        :param str serialize: DEPRECATED the format you want Betamax to
            serialize the request and response data to and from
        """
        kwargs = Options(kwargs)
        serialize = kwargs['serialize'] or kwargs['serialize_with']
        kwargs['cassette_library_dir'] = self.config.cassette_library_dir

        can_load = Cassette.can_be_loaded(
            self.config.cassette_library_dir,
            cassette_name,
            serialize,
            kwargs['record']
            )

        if can_load:
            self.betamax_adapter.load_cassette(cassette_name, serialize,
                                               kwargs)
        else:
            # If we're not recording or replaying an existing cassette, we
            # should tell the user/developer that there is no cassette, only
            # Zuul
            raise ValueError('Cassette must have a valid name and may not be'
                             ' None.')
        return self


def dispatch_hooks(hook_name, *args):
    """Dispatch registered hooks."""
    hooks = Configuration.recording_hooks[hook_name]
    for hook in hooks:
        hook(*args)
