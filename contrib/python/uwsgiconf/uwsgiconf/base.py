from typing import Type, Any, Union, Optional, Callable

from .utils import listify


if False:  # pragma: nocover
    from .config import Section  # noqa


class Options:
    """Options descriptor. Allows option."""

    def __init__(self, opt_type: Type['OptionsGroup']):
        """
        :param opt_type:

        """
        self.opt_type = opt_type

    def __get__(self, section: 'Section', section_cls: Type['Section']) -> Union['OptionsGroup', Type['OptionsGroup']]:
        """

        :param section:
        :param section_cls:

        """
        key = self.opt_type.__name__

        try:
            options_obj = section._options_objects.get(key)

        except AttributeError:
            # Allow easy access to option group static params:
            # Section.networking.socket_types.DEFAULT
            return self.opt_type

        if not options_obj:
            options_obj = self.opt_type(_section=section)

        section._options_objects[key] = options_obj

        return options_obj


class OptionKey:

    __slots__ = ['key']

    def __init__(self, key: str):
        self.key = key

    def swap(self, new_key: Any):

        if new_key:
            self.key = f'{new_key}'

    def __str__(self):
        return self.key

    def __eq__(self, obj):
        return self.key == obj

    def __hash__(self):
        return hash(self.key)


class OptionsGroup:
    """Introduces group of options.

    Basic group parameters may be passed to initializer
    or `set_basic_params` method.

    Usually methods setting parameters are named `set_***_params`.

    Methods ending with `_params` return section object and may be chained.

    """
    _section: 'Section' = None
    """Section this option group belongs to."""

    plugin: Union[bool, str] = False
    """Indication this option group belongs to a plugin."""

    name: str = ''
    """Name to represent the group."""

    def __init__(self, *args, **kwargs):
        if self._section is None:
            self._section = kwargs.pop('_section', None)

        self.set_basic_params(*args, **kwargs)

    def _get_name(self, *args, **kwargs) -> str:
        return self.name

    def __str__(self):
        return self._get_name()

    def __call__(self, *args, **kwargs) -> 'Section':
        """The call is translated into ``set_basic_params``` call.

        This approach is much more convenient yet IDE most probably won't
        give you a hint on what arguments are accepted.

        :param args:
        :param kwargs:

        """
        return self.set_basic_params(*args, **kwargs)

    def __eq__(self, obj):
        return f'{self}' == obj

    def __hash__(self):
        return hash(f'{self}')

    def set_basic_params(self, *args, **kwargs) -> 'Section':
        return self._section

    def _set(
            self,
            key: str,
            value: Any,
            *,
            condition: Optional[Union[bool, str]] = True,
            cast: Callable = None,
            multi: bool = False,
            plugin: str = None,
            priority: int = None
    ):
        """

        :param key: Option name

        :param value: Option value. Can be a lis if ``multi``.

        :param condition: Condition to test whether this option should be added to section.
            * True - test value is not None.

        :param cast: Value type caster.
            * bool - treat value as a flag

        :param multi: Indicate that many options can use the same name.

        :param plugin: Plugin this option exposed by. Activated automatically.

        :param priority: Option priority indicator. Options with lower numbers will come first.

        """
        key = OptionKey(key)

        def set_plugin(plugin):
            self._section.set_plugins_params(plugins=plugin)

        def handle_priority(value, *, use_list=False):

            if priority is not None:
                # Restructure options.
                opts_copy = opts.copy()
                opts.clear()

                existing_value = opts_copy.pop(key, [])

                if use_list:
                    existing_value.extend(value)
                    value = existing_value

                for pos, (item_key, item_val) in enumerate(opts_copy.items()):

                    if priority == pos:
                        opts[key] = value

                    opts[item_key] = item_val

                return True

        def handle_plugin_required(val):

            if isinstance(val, ParametrizedValue):
                key.swap(val.opt_key or key)  # todo

                if val.plugin:
                    # Automatic plugin activation.
                    set_plugin(val.plugin)

                if val._opts:
                    opts.update(val._opts)

        if condition is True:
            condition = value is not None

        if condition is None or condition:

            opts = self._section._opts

            if cast is bool:
                if value:
                    value = 'true'

                else:
                    try:
                        del opts[key]
                    except KeyError:
                        pass

                    return

            if self.plugin is True:
                # Automatic plugin activation when option from it is used.
                set_plugin(self)

            if plugin:
                set_plugin(plugin)

            if isinstance(value, tuple):  # Tuple - expect ParametrizedValue.
                list(map(handle_plugin_required, value))
                value = ' '.join(map(str, value))

            if multi:
                values = []

                # First activate plugin if required.
                for value in listify(value):
                    handle_plugin_required(value)
                    values.append(value)

                # Second: list in new option.
                if not handle_priority(values, use_list=True):
                    opts.setdefault(key, []).extend(values)

            else:
                handle_plugin_required(value)

                if not handle_priority(value):
                    opts[key] = value

    def _make_section_like(self):
        self._section = type('SectionLike', (object,), {'_opts': {}})

    def _contribute_to_opts(self, target):
        target_section = target._section
        target_section._opts.update(self._section._opts)


class ParametrizedValue(OptionsGroup):
    """Represents parametrized option value."""

    alias: str = ''
    """Alias to address this value."""

    args_joiner: str = ' '
    """Symbol to join arguments with."""

    name_separator: str = ':'
    """Separator to add after name portion."""

    name_separator_strip: bool = False
    """Strip leading and trailing name separator from the result."""

    opt_key: Optional[str] = None
    """Allows swapping default option key with custom value."""

    def __init__(self, *args):
        self.args = list(args)
        self._opts = {}
        super().__init__(_section=self)

    def __str__(self):
        args = [str(arg) for arg in self.args if arg is not None]

        result = ''

        if self.opt_key is None:
            result += self._get_name() + self.name_separator

        result += self.args_joiner.join(args)

        if self.alias:
            result = f'{self.alias} {result}'

        result = result.strip()

        if self.name_separator_strip:
            result = result.strip(self.name_separator)

        return result


class TemplatedValue(ParametrizedValue):

    tpl: str = '%s'

    def __init__(self, name: str):
        self._name = name
        super().__init__()

    def __str__(self):
        return self.tpl % self._name
