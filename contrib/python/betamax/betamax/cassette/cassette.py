# -*- coding: utf-8 -*-
import collections
from datetime import datetime
from functools import partial
import os.path

from .interaction import Interaction

from .. import matchers
from .. import serializers
from betamax.util import (_option_from, serialize_prepared_request,
                          serialize_response, timestamp)


class Cassette(object):

    default_cassette_options = {
        'record_mode': 'once',
        'match_requests_on': ['method', 'uri'],
        're_record_interval': None,
        'placeholders': [],
        'preserve_exact_body_bytes': False,
        'allow_playback_repeats': False,
    }

    hooks = collections.defaultdict(list)

    def __init__(self, cassette_name, serialization_format, **kwargs):
        #: Short name of the cassette
        self.cassette_name = cassette_name

        self.serialized = None

        defaults = Cassette.default_cassette_options

        # Determine the record mode
        self.record_mode = _option_from('record_mode', kwargs, defaults)

        # Retrieve the serializer for this cassette
        self.serializer = serializers.SerializerProxy.find(
            serialization_format, kwargs.get('cassette_library_dir'),
            cassette_name
            )
        self.cassette_path = self.serializer.cassette_path

        # Determine which placeholders to use
        default_placeholders = defaults['placeholders'][:]
        cassette_placeholders = kwargs.get('placeholders', [])
        self.placeholders = merge_placeholder_lists(default_placeholders,
                                                    cassette_placeholders)

        # Determine whether to preserve exact body bytes
        self.preserve_exact_body_bytes = _option_from(
            'preserve_exact_body_bytes', kwargs, defaults
            )

        self.allow_playback_repeats = _option_from(
            'allow_playback_repeats', kwargs, defaults
            )

        # Initialize the interactions
        self.interactions = []

        # Initialize the match options
        self.match_options = set()

        self.load_interactions()
        self.serializer.allow_serialization = self.is_recording()

    @staticmethod
    def can_be_loaded(cassette_library_dir, cassette_name, serialize_with,
                      record_mode):
        # If we want to record a cassette we don't care if the file exists
        # yet
        recording = False
        if record_mode in ['once', 'all', 'new_episodes']:
            recording = True

        serializer = serializers.serializer_registry.get(
            serialize_with
        )
        if not serializer:
            raise ValueError(
                'Serializer {0} is not registered with Betamax'.format(
                    serialize_with
                    ))

        cassette_path = serializer.generate_cassette_name(
            cassette_library_dir, cassette_name
            )
        # Otherwise if we're only replaying responses, we should probably
        # have the cassette the user expects us to load and raise.
        return os.path.exists(cassette_path) or recording

    def clear(self):
        # Clear out the interactions
        self.interactions = []
        # Serialize to the cassette file
        self._save_cassette()

    @property
    def earliest_recorded_date(self):
        """The earliest date of all of the interactions this cassette."""
        if self.interactions:
            i = sorted(self.interactions, key=lambda i: i.recorded_at)[0]
            return i.recorded_at
        return datetime.now()

    def eject(self):
        self._save_cassette()

    def find_match(self, request):
        """Find a matching interaction based on the matchers and request.

        This uses all of the matchers selected via configuration or
        ``use_cassette`` and passes in the request currently in progress.

        :param request: ``requests.PreparedRequest``
        :returns: :class:`~betamax.cassette.Interaction`
        """
        # if we are recording, do not filter by match
        if self.is_recording():
            if ((self.record_mode == 'new_episodes' and
                 all(i.used is True for i in self.interactions)) or
                    self.record_mode in ('once', 'none')):
                return None

        opts = self.match_options
        # Curry those matchers
        curried_matchers = [
            partial(matchers.matcher_registry[o].match, request)
            for o in opts
        ]

        for interaction in self.interactions:
            if not interaction.match(curried_matchers):
                continue

            if interaction.used or interaction.ignored:
                continue

            # If the interaction matches everything
            if self.record_mode == 'all':
                # If we're recording everything and there's a matching
                # interaction we want to overwrite it, so we remove it.
                self.interactions.remove(interaction)
                break

            # set interaction as used before returning
            if not self.allow_playback_repeats:
                interaction.used = True
            return interaction

        # No matches. So sad.
        return None

    def is_empty(self):
        """Determine if the cassette was empty when loaded."""
        return not self.serialized

    def is_recording(self):
        """Return whether the cassette is recording."""
        values = {
            'none': False,
            'once': self.is_empty(),
        }
        return values.get(self.record_mode, True)

    def load_interactions(self):
        if self.serialized is None:
            self.serialized = self.serializer.deserialize()

        interactions = self.serialized.get('http_interactions', [])
        self.interactions = [Interaction(i) for i in interactions]

        for i in self.interactions:
            dispatch_hooks('before_playback', i, self)
            i.replace_all(self.placeholders, False)

    def sanitize_interactions(self):
        for i in self.interactions:
            i.replace_all(self.placeholders, True)

    def save_interaction(self, response, request):
        serialized_data = self.serialize_interaction(response, request)
        interaction = Interaction(serialized_data, response)
        dispatch_hooks('before_record', interaction, self)
        if not interaction.ignored:  # If a hook caused this to be ignored
            self.interactions.append(interaction)
        return interaction

    def serialize_interaction(self, response, request):
        return {
            'request': serialize_prepared_request(
                request,
                self.preserve_exact_body_bytes
                ),
            'response': serialize_response(
                response,
                self.preserve_exact_body_bytes
                ),
            'recorded_at': timestamp(),
        }

    # Private methods
    def _save_cassette(self):
        from .. import __version__
        self.sanitize_interactions()

        cassette_data = {
            'http_interactions': [i.data for i in self.interactions],
            'recorded_with': 'betamax/{0}'.format(__version__)
        }
        self.serializer.serialize(cassette_data)


class Placeholder(collections.namedtuple('Placeholder',
                                         'placeholder replace')):
    """Encapsulate some logic about Placeholders."""

    @classmethod
    def from_dict(cls, dictionary):
        return cls(**dictionary)

    def unpack(self, serializing):
        if serializing:
            return self.replace, self.placeholder
        else:
            return self.placeholder, self.replace


def merge_placeholder_lists(defaults, overrides):
    overrides = [Placeholder.from_dict(override) for override in overrides]
    overrides_dict = dict((p.placeholder, p) for p in overrides)
    placeholders = [overrides_dict.pop(p.placeholder, p)
                    for p in map(Placeholder.from_dict, defaults)]
    return placeholders + [p for p in overrides
                           if p.placeholder in overrides_dict]


def dispatch_hooks(hook_name, *args):
    """Dispatch registered hooks."""
    # Cassette.hooks is a dictionary that defaults to an empty list,
    # we neither need to check for the presence of hook_name in it, nor
    # need to worry about whether the return value will be iterable
    hooks = Cassette.hooks[hook_name]
    for hook in hooks:
        hook(*args)
