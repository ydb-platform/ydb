from collections import defaultdict

from .cassette import Cassette


class Configuration(object):
    """This object acts as a proxy to configure different parts of Betamax.

    You should only ever encounter this object when configuring the library as
    a whole. For example:

    .. code::

        with Betamax.configure() as config:
            config.cassette_library_dir = 'tests/cassettes/'
            config.default_cassette_options['record_mode'] = 'once'
            config.default_cassette_options['match_requests_on'] = ['uri']
            config.define_cassette_placeholder('<URI>', 'http://httpbin.org')
            config.preserve_exact_body_bytes = True

    """

    CASSETTE_LIBRARY_DIR = 'vcr/cassettes'
    recording_hooks = defaultdict(list)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def __setattr__(self, prop, value):
        if prop == 'preserve_exact_body_bytes':
            self.default_cassette_options[prop] = True
        else:
            super(Configuration, self).__setattr__(prop, value)

    def after_start(self, callback=None):
        """Register a function to call after Betamax is started.

        Example usage:

        .. code-block:: python

            def on_betamax_start(cassette):
                if cassette.is_recording():
                    print("Setting up authentication...")

            with Betamax.configure() as config:
                config.cassette_load(callback=on_cassette_load)

        :param callable callback:
            The function which accepts a cassette and might mutate
            it before returning.
        """
        self.recording_hooks['after_start'].append(callback)

    def before_playback(self, tag=None, callback=None):
        """Register a function to call before playing back an interaction.

        Example usage:

        .. code-block:: python

            def before_playback(interaction, cassette):
                pass

            with Betamax.configure() as config:
                config.before_playback(callback=before_playback)

        :param str tag:
            Limits the interactions passed to the function based on the
            interaction's tag (currently unsupported).
        :param callable callback:
            The function which either accepts just an interaction or an
            interaction and a cassette and mutates the interaction before
            returning.
        """
        Cassette.hooks['before_playback'].append(callback)

    def before_record(self, tag=None, callback=None):
        """Register a function to call before recording an interaction.

        Example usage:

        .. code-block:: python

            def before_record(interaction, cassette):
                pass

            with Betamax.configure() as config:
                config.before_record(callback=before_record)

        :param str tag:
            Limits the interactions passed to the function based on the
            interaction's tag (currently unsupported).
        :param callable callback:
            The function which either accepts just an interaction or an
            interaction and a cassette and mutates the interaction before
            returning.
        """
        Cassette.hooks['before_record'].append(callback)

    def before_stop(self, callback=None):
        """Register a function to call before Betamax stops.

        Example usage:

        .. code-block:: python

            def on_betamax_stop(cassette):
                if not cassette.is_recording():
                    print("Playback completed.")

            with Betamax.configure() as config:
                config.cassette_eject(callback=on_betamax_stop)

        :param callable callback:
            The function which accepts a cassette and might mutate
            it before returning.
        """
        self.recording_hooks['before_stop'].append(callback)

    @property
    def cassette_library_dir(self):
        """Retrieve and set the directory to store the cassettes in."""
        return Configuration.CASSETTE_LIBRARY_DIR

    @cassette_library_dir.setter
    def cassette_library_dir(self, value):
        Configuration.CASSETTE_LIBRARY_DIR = value

    @property
    def default_cassette_options(self):
        """Retrieve and set the default cassette options.

        The options include:

        - ``match_requests_on``
        - ``placeholders``
        - ``re_record_interval``
        - ``record_mode``
        - ``preserve_exact_body_bytes``

        Other options will be ignored.
        """
        return Cassette.default_cassette_options

    @default_cassette_options.setter
    def default_cassette_options(self, value):
        Cassette.default_cassette_options = value

    def define_cassette_placeholder(self, placeholder, replace):
        """Define a placeholder value for some text.

        This also will replace the placeholder text with the text you wish it
        to use when replaying interactions from cassettes.

        :param str placeholder: (required), text to be used as a placeholder
        :param str replace: (required), text to be replaced or replacing the
            placeholder
        """
        self.default_cassette_options['placeholders'].append({
            'placeholder': placeholder,
            'replace': replace,
        })
