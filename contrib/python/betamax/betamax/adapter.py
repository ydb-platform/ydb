"""
betamax.adapter.

==============

adapter for betamax
"""
import os

from . import cassette
from .exceptions import BetamaxError
from datetime import datetime, timedelta
from requests.adapters import BaseAdapter, HTTPAdapter

_SENTINEL = object()


class BetamaxAdapter(BaseAdapter):
    """This object is an implementation detail of the library.

    It is not meant to be a public API and is not exported as such.

    """

    def __init__(self, **kwargs):
        super(BetamaxAdapter, self).__init__()
        self.cassette = None
        self.cassette_name = None
        self.old_adapters = kwargs.pop('old_adapters', {})
        self.http_adapter = HTTPAdapter(**kwargs)
        self.serialize = None
        self.options = {}

    def cassette_exists(self):
        """Check if cassette exists on file system.

        :returns: bool -- True if exists, False otherwise
        """
        if self.cassette_name and os.path.exists(self.cassette_name):
            return True
        return False

    def close(self):
        """Propagate close to underlying adapter."""
        self.http_adapter.close()

    def eject_cassette(self):
        """Eject currently loaded cassette."""
        if self.cassette:
            self.cassette.eject()
        self.cassette = None  # Allow self.cassette to be garbage-collected

    def load_cassette(self, cassette_name, serialize, options):
        """Load cassette.

        Loads a previously serialized http response as a cassette

        :param str cassette_name: (required), name of cassette
        :param str serialize: (required), type of serialization i.e 'json'
        :options dict options: (required), options for cassette
        """
        self.cassette_name = cassette_name
        self.serialize = serialize
        self.options.update(options.items())
        placeholders = self.options.get('placeholders', {})
        cassette_options = {}

        default_options = cassette.Cassette.default_cassette_options

        match_requests_on = self.options.get(
            'match_requests_on', default_options['match_requests_on']
            )

        cassette_options['preserve_exact_body_bytes'] = self.options.get(
            'preserve_exact_body_bytes',
            )

        cassette_options['allow_playback_repeats'] = self.options.get(
            'allow_playback_repeats'
            )

        cassette_options['record_mode'] = self.options.get('record')

        for option, value in list(cassette_options.items()):
            if value is None:
                cassette_options.pop(option)

        self.cassette = cassette.Cassette(
            cassette_name, serialize, placeholders=placeholders,
            cassette_library_dir=self.options.get('cassette_library_dir'),
            **cassette_options
            )

        if 'record' in self.options:
            self.cassette.record_mode = self.options['record']

        # NOTE(sigmavirus24): Cassette.match_options is a set, might as well
        # use that instead of overriding it.
        self.cassette.match_options.update(match_requests_on)

        re_record_interval = timedelta.max
        if self.options.get('re_record_interval'):
            re_record_interval = timedelta(self.options['re_record_interval'])

        now = datetime.utcnow()
        if re_record_interval < (now - self.cassette.earliest_recorded_date):
            self.cassette.clear()

    def send(self, request, stream=False, timeout=None, verify=True,
             cert=None, proxies=None):
        """Send request.

        :param request request: request
        :returns: A Response object
        """
        interaction = None
        current_cassette = self.cassette

        if not current_cassette:
            raise BetamaxError('No cassette was specified or found.')

        if current_cassette.interactions:
            interaction = current_cassette.find_match(request)

        if not interaction and current_cassette.is_recording():
            interaction = self.send_and_record(
                request, stream, timeout, verify, cert, proxies
                )

        if not interaction:
            raise BetamaxError(unhandled_request_message(request,
                                                         current_cassette))

        resp = interaction.as_response()
        resp.connection = self
        return resp

    def send_and_record(self, request, stream=False, timeout=None,
                        verify=True, cert=None, proxies=None):
        """Send request and record response.

        The response will be serialized and saved to a
        cassette which can be replayed in the future.

        :param request request: request
        :param bool stream: (optional) defer download until content is accessed
        :param float timeout: (optional) time to wait for a response
        :param bool verify: (optional) verify SSL certificate
        :param str cert: (optional) path to SSL client
        :param proxies dict: (optional) mapping protocol to URL of the proxy
        :return: Interaction
        :rtype: class:`betamax.cassette.Interaction`
        """
        adapter = self.find_adapter(request.url)
        response = adapter.send(
            request, stream=True, timeout=timeout, verify=verify,
            cert=cert, proxies=proxies
            )
        return self.cassette.save_interaction(response, request)

    def find_adapter(self, url):
        """Find adapter.

        Searches for an existing adapter where the url and prefix match.

        :param url str: (required) url of the adapter
        :returns: betamax adapter
        """
        for (prefix, adapter) in self.old_adapters.items():

            if url.lower().startswith(prefix):
                return adapter

        # Unlike in requests, we cannot possibly get this far.


UNHANDLED_REQUEST_EXCEPTION = """A request was made that could not be handled.

A request was made to {url} that could not be found in {cassette_file_path}.

The settings on the cassette are:

    - record_mode: {cassette_record_mode}
    - match_options {cassette_match_options}.
"""


def unhandled_request_message(request, cassette):
    """Generate exception for unhandled requests."""
    return UNHANDLED_REQUEST_EXCEPTION.format(
        url=request.url, cassette_file_path=cassette.cassette_name,
        cassette_record_mode=cassette.record_mode,
        cassette_match_options=cassette.match_options
        )
