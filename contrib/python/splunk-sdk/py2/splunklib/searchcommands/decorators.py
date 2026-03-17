# coding=utf-8
#
# Copyright © 2011-2015 Splunk, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"): you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import, division, print_function, unicode_literals
from splunklib import six

from collections import OrderedDict  # must be python 2.7

from inspect import getmembers, isclass, isfunction
from splunklib.six.moves import map as imap

from .internals import ConfigurationSettingsType, json_encode_string
from .validators import OptionName


class Configuration(object):
    """ Defines the configuration settings for a search command.

    Documents, validates, and ensures that only relevant configuration settings are applied. Adds a :code:`name` class
    variable to search command classes that don't have one. The :code:`name` is derived from the name of the class.
    By convention command class names end with the word "Command". To derive :code:`name` the word "Command" is removed
    from the end of the class name and then converted to lower case for conformance with the `Search command style guide
    <http://docs.splunk.com/Documentation/Splunk/latest/Search/Searchcommandstyleguide>`__

    """
    def __init__(self, o=None, **kwargs):
        #
        # The o argument enables the configuration decorator to be used with or without parentheses. For example, it
        # enables you to write code that looks like this:
        #
        #   @Configuration
        #   class Foo(SearchCommand):
        #       ...
        #
        #   @Configuration()
        #   class Bar(SearchCommand):
        #       ...
        #
        # Without the o argument, the Python compiler will complain about the first form. With the o argument, both
        # forms work. The first form provides a value for o: Foo. The second form does does not provide a value for o.
        # The class or method decorated is not passed to the constructor. A value of None is passed instead.
        #
        self.settings = kwargs

    def __call__(self, o):

        if isfunction(o):
            # We must wait to finalize configuration as the class containing this function is under construction
            # at the time this call to decorate a member function. This will be handled in the call to
            # o.ConfigurationSettings.fix_up(o) in the elif clause of this code block.
            o._settings = self.settings
        elif isclass(o):

            # Set command name

            name = o.__name__
            if name.endswith('Command'):
                name = name[:-len('Command')]
            o.name = six.text_type(name.lower())

            # Construct ConfigurationSettings instance for the command class

            o.ConfigurationSettings = ConfigurationSettingsType(
                module=o.__module__ + '.' + o.__name__,
                name='ConfigurationSettings',
                bases=(o.ConfigurationSettings,))

            ConfigurationSetting.fix_up(o.ConfigurationSettings, self.settings)
            o.ConfigurationSettings.fix_up(o)
            Option.fix_up(o)
        else:
            raise TypeError('Incorrect usage: Configuration decorator applied to {0}'.format(type(o), o.__name__))

        return o


class ConfigurationSetting(property):
    """ Generates a :class:`property` representing the named configuration setting

    This is a convenience function designed to reduce the amount of boiler-plate code you must write; most notably for
    property setters.

    :param name: Configuration setting name.
    :type name: str or unicode

    :param doc: A documentation string.
    :type doc: bytes, unicode or NoneType

    :param readonly: If true, specifies that the configuration setting is fixed.
    :type name: bool or NoneType

    :param value: Configuration setting value.

    :return: A :class:`property` instance representing the configuration setting.
    :rtype: property

    """
    def __init__(self, fget=None, fset=None, fdel=None, doc=None, name=None, readonly=None, value=None):
        property.__init__(self, fget=fget, fset=fset, fdel=fdel, doc=doc)
        self._readonly = readonly
        self._value = value
        self._name = name

    def __call__(self, function):
        return self.getter(function)

    def deleter(self, function):
        return self._copy_extra_attributes(property.deleter(self, function))

    def getter(self, function):
        return self._copy_extra_attributes(property.getter(self, function))

    def setter(self, function):
        return self._copy_extra_attributes(property.setter(self, function))

    @staticmethod
    def fix_up(cls, values):

        is_configuration_setting = lambda attribute: isinstance(attribute, ConfigurationSetting)
        definitions = getmembers(cls, is_configuration_setting)
        i = 0

        for name, setting in definitions:

            if setting._name is None:
                setting._name = name = six.text_type(name)
            else:
                name = setting._name

            validate, specification = setting._get_specification()
            backing_field_name = '_' + name

            if setting.fget is None and setting.fset is None and setting.fdel is None:

                value = setting._value

                if setting._readonly or value is not None:
                    validate(specification, name, value)

                def fget(bfn, value):
                    return lambda this: getattr(this, bfn, value)

                setting = setting.getter(fget(backing_field_name, value))

                if not setting._readonly:

                    def fset(bfn, validate, specification, name):
                        return lambda this, value: setattr(this, bfn, validate(specification, name, value))

                    setting = setting.setter(fset(backing_field_name, validate, specification, name))

                setattr(cls, name, setting)

            def is_supported_by_protocol(supporting_protocols):

                def is_supported_by_protocol(version):
                    return version in supporting_protocols

                return is_supported_by_protocol

            del setting._name, setting._value, setting._readonly

            setting.is_supported_by_protocol = is_supported_by_protocol(specification.supporting_protocols)
            setting.supporting_protocols = specification.supporting_protocols
            setting.backing_field_name = backing_field_name
            definitions[i] = setting
            setting.name = name

            i += 1

            try:
                value = values[name]
            except KeyError:
                continue

            if setting.fset is None:
                raise ValueError('The value of configuration setting {} is fixed'.format(name))

            setattr(cls, backing_field_name, validate(specification, name, value))
            del values[name]

        if len(values) > 0:
            settings = sorted(list(six.iteritems(values)))
            settings = imap(lambda n_v: '{}={}'.format(n_v[0], repr(n_v[1])), settings)
            raise AttributeError('Inapplicable configuration settings: ' + ', '.join(settings))

        cls.configuration_setting_definitions = definitions

    def _copy_extra_attributes(self, other):
        other._readonly = self._readonly
        other._value = self._value
        other._name = self._name
        return other

    def _get_specification(self):

        name = self._name

        try:
            specification = ConfigurationSettingsType.specification_matrix[name]
        except KeyError:
            raise AttributeError('Unknown configuration setting: {}={}'.format(name, repr(self._value)))

        return ConfigurationSettingsType.validate_configuration_setting, specification


class Option(property):
    """ Represents a search command option.

    Required options must be specified on the search command line.

    **Example:**

    Short form (recommended). When you are satisfied with built-in or custom validation behaviors.

    ..  code-block:: python
        :linenos:

        from splunklib.searchcommands.decorators import Option
        from splunklib.searchcommands.validators import Fieldname

        total = Option(
            doc=''' **Syntax:** **total=***<fieldname>*
            **Description:** Name of the field that will hold the computed
            sum''',
            require=True, validate=Fieldname())

    **Example:**

    Long form. Useful when you wish to manage the option value and its deleter/getter/setter side-effects yourself. You
    must provide a getter and a setter. If your :code:`Option` requires `destruction <https://docs.python.org/2/reference/datamodel.html#object.__del__>`_ you must
    also provide a deleter. You must be prepared to accept a value of :const:`None` which indicates that your
    :code:`Option` is unset.

    ..  code-block:: python
        :linenos:

        from splunklib.searchcommands import Option

        @Option()
        def logging_configuration(self):
            \""" **Syntax:** logging_configuration=<path>
            **Description:** Loads an alternative logging configuration file for a command invocation. The logging
            configuration file must be in Python ConfigParser-format. The *<path>* name and all path names specified in
            configuration are relative to the app root directory.

            \"""
            return self._logging_configuration

        @logging_configuration.setter
        def logging_configuration(self, value):
            if value is not None
                logging.configure(value)
                self._logging_configuration = value

        def __init__(self)
            self._logging_configuration = None

    """
    def __init__(self, fget=None, fset=None, fdel=None, doc=None, name=None, default=None, require=None, validate=None):
        property.__init__(self, fget, fset, fdel, doc)
        self.name = name
        self.default = default
        self.validate = validate
        self.require = bool(require)

    def __call__(self, function):
        return self.getter(function)

    # region Methods

    def deleter(self, function):
        return self._copy_extra_attributes(property.deleter(self, function))

    def getter(self, function):
        return self._copy_extra_attributes(property.getter(self, function))

    def setter(self, function):
        return self._copy_extra_attributes(property.setter(self, function))

    @classmethod
    def fix_up(cls, command_class):

        is_option = lambda attribute: isinstance(attribute, Option)
        definitions = getmembers(command_class, is_option)
        validate_option_name = OptionName()
        i = 0

        for name, option in definitions:

            if option.name is None:
                option.name = name  # no validation required
            else:
                validate_option_name(option.name)

            if option.fget is None and option.fset is None and option.fdel is None:
                backing_field_name = '_' + name

                def fget(bfn):
                    return lambda this: getattr(this, bfn, None)

                option = option.getter(fget(backing_field_name))

                def fset(bfn, validate):
                    if validate is None:
                        return lambda this, value: setattr(this, bfn, value)
                    return lambda this, value: setattr(this, bfn, validate(value))

                option = option.setter(fset(backing_field_name, option.validate))
                setattr(command_class, name, option)

            elif option.validate is not None:

                def fset(function, validate):
                    return lambda this, value: function(this, validate(value))

                option = option.setter(fset(option.fset, option.validate))
                setattr(command_class, name, option)

            definitions[i] = name, option
            i += 1

        command_class.option_definitions = definitions

    def _copy_extra_attributes(self, other):
        other.name = self.name
        other.default = self.default
        other.require = self.require
        other.validate = self.validate
        return other

    # endregion

    # region Types

    class Item(object):
        """ Presents an instance/class view over a search command `Option`.

        This class is used by SearchCommand.process to parse and report on option values.

        """
        def __init__(self, command, option):
            self._command = command
            self._option = option
            self._is_set = False
            validator = self.validator
            self._format = six.text_type if validator is None else validator.format

        def __repr__(self):
            return '(' + repr(self.name) + ', ' + repr(self._format(self.value)) + ')'

        def __str__(self):
            value = self.value
            value = 'None' if value is None else json_encode_string(self._format(value))
            return self.name + '=' + value

        # region Properties

        @property
        def is_required(self):
            return bool(self._option.require)

        @property
        def is_set(self):
            """ Indicates whether an option value was provided as argument.

            """
            return self._is_set

        @property
        def name(self):
            return self._option.name

        @property
        def validator(self):
            return self._option.validate

        @property
        def value(self):
            return self._option.__get__(self._command)

        @value.setter
        def value(self, value):
            self._option.__set__(self._command, value)
            self._is_set = True

        # endregion

        # region Methods

        def reset(self):
            self._option.__set__(self._command, self._option.default)
            self._is_set = False

        pass
        # endregion

    class View(OrderedDict):
        """ Presents an ordered dictionary view of the set of :class:`Option` arguments to a search command.

        This class is used by SearchCommand.process to parse and report on option values.

        """
        def __init__(self, command):
            definitions = type(command).option_definitions
            item_class = Option.Item
            OrderedDict.__init__(self, ((option.name, item_class(command, option)) for (name, option) in definitions))

        def __repr__(self):
            text = 'Option.View([' + ','.join(imap(lambda item: repr(item), six.itervalues(self))) + '])'
            return text

        def __str__(self):
            text = ' '.join([str(item) for item in six.itervalues(self) if item.is_set])
            return text

        # region Methods

        def get_missing(self):
            missing = [item.name for item in six.itervalues(self) if item.is_required and not item.is_set]
            return missing if len(missing) > 0 else None

        def reset(self):
            for value in six.itervalues(self):
                value.reset()

        pass
        # endregion

    pass
    # endregion


__all__ = ['Configuration', 'Option']
