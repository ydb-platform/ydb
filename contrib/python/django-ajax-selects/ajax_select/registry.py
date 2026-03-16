from django.apps import apps
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import autodiscover_modules


class LookupChannelRegistry:
    """
    Registry for LookupChannels activated for your django project.

    This includes any installed apps that contain lookup.py modules (django 1.7+)
    and any lookups that are explicitly declared in `settings.AJAX_LOOKUP_CHANNELS`
    """

    _registry = {}

    def load_channels(self):
        """
        Called when loading the application. Cannot be called a second time,
        (eg. for testing) as Django will not re-import and re-register anything.
        """
        autodiscover_modules("lookups")

        if hasattr(settings, "AJAX_LOOKUP_CHANNELS"):
            self.register(settings.AJAX_LOOKUP_CHANNELS)

    def register(self, lookup_specs):
        """
        Register a set of lookup definitions.

        Args:
            lookup_specs (dict): One or more LookupChannel specifications
                - `{'channel': LookupChannelSubclass}`
                - `{'channel': ('module.of.lookups', 'MyLookupClass')}`
                - `{'channel': {'model': 'MyModelToBeLookedUp', 'search_field': 'field_to_search'}}`

        """
        for channel, spec in lookup_specs.items():
            if spec is None:  # unset
                if channel in self._registry:
                    del self._registry[channel]
            else:
                self._registry[channel] = spec

    def get(self, channel):
        """
        Find the LookupChannel class for the named channel and instantiate it.

        Args:
            channel (string):  - name that the lookup channel was registered at
        Returns:
            LookupChannel
        Raises:
            ImproperlyConfigured - if channel is not found.
            Exception - invalid lookup_spec was stored in registery

        """
        from ajax_select import LookupChannel

        try:
            lookup_spec = self._registry[channel]
        except KeyError as e:
            msg = f"No ajax_select LookupChannel named {channel!r} is registered."
            raise ImproperlyConfigured(msg) from e

        if (type(lookup_spec) is type) and issubclass(lookup_spec, LookupChannel):
            return lookup_spec()
            # damnit python.
            # ideally this would match regardless of how you imported the parent class
            # but these are different classes:
            # from ajax_select.lookup_channel import LookupChannel
            # from ajax_select import LookupChannel
        if isinstance(lookup_spec, dict):
            # 'channel' : dict(model='app.model', search_field='title' )
            #  generate a simple channel dynamically
            return self.make_channel(lookup_spec["model"], lookup_spec["search_field"])
        if isinstance(lookup_spec, tuple):
            # a tuple
            # 'channel' : ('app.module','LookupClass')
            #  from app.module load LookupClass and instantiate
            lookup_module = __import__(lookup_spec[0], {}, {}, [""])
            lookup_class = getattr(lookup_module, lookup_spec[1])
            return lookup_class()
        msg = f"Invalid lookup spec: {lookup_spec}"
        raise Exception(msg)

    def is_registered(self, channel):
        return channel in self._registry

    def make_channel(self, app_model, arg_search_field):
        """
        Automatically make a LookupChannel.

        Args:
            app_model (str):   app_name.ModelName
            arg_search_field (str):  the field to search against and to display in search results
        Returns:
            LookupChannel

        """
        from ajax_select import LookupChannel

        app_label, model_name = app_model.split(".")

        class MadeLookupChannel(LookupChannel):
            model = get_model(app_label, model_name)
            search_field = arg_search_field

        return MadeLookupChannel()


registry = LookupChannelRegistry()


def get_model(app_label, model_name):
    """Loads the model given an 'app_label' 'ModelName'."""
    return apps.get_model(app_label, model_name)


def register(channel):
    """
    Decorator to register a LookupClass.

    Example::
        from ajax_select import LookupChannel, register

        @register('agent')
        class AgentLookup(LookupClass):

            def get_query(self):
                ...
            def format_item(self):
                ...

    """

    def _wrapper(lookup_class):
        if not channel:
            msg = "Lookup Channel must have a channel name"
            raise ValueError(msg)

        registry.register({channel: lookup_class})

        return lookup_class

    return _wrapper
