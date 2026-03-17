#
# Copyright (c) nexB Inc. and others.
# SPDX-License-Identifier: Apache-2.0
#
# Visit https://aboutcode.org and https://github.com/nexB/ for support and download.
# ScanCode is a trademark of nexB Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from collections import defaultdict
import sys

import click
from click.types import BoolParamType
from pluggy import HookimplMarker
from pluggy import HookspecMarker
from pluggy import PluginManager as PluggyPluginManager

from commoncode import cliutils

# Tracing flags
TRACE = False


def logger_debug(*args):
    pass


if TRACE:
    import logging

    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

    def logger_debug(*args):
        return logger.debug(" ".join(isinstance(a, str) and a or repr(a) for a in args))


class PlugincodeError(Exception):
    """Base exception for plugincode errors"""


class BasePlugin(object):
    """
    A base class for all ScanCode plugins.
    """

    # stage string for this plugin.
    # This is set automatically when a plugin class is loaded in its manager.
    # Subclasses must not set this.
    stage = None

    # name string under which this plugin is registered.
    # This is set automatically when a plugin class is loaded in its manager.
    # Subclasses must not set this.
    name = None

    # An ordered mapping of attr attributes that specifies the Codebase
    # attributes data returned by this plugin. These attributes will be added to
    # a Codebase class. The position of these attributes in the returned
    # serialized data is determined by the sort_order then the plugin name
    codebase_attributes = dict()

    # An ordered mapping of attr attributes that specifies the Resource data
    # returned by this plugin. These attributes will be added to a Resource
    # subclass. The position of these attributes in the returned serialized data
    # is determined by the sort_order then the plugin name
    resource_attributes = dict()

    # List of PluggableCommandLineOption CLI options for this plugin.
    # Subclasses should set this as needed
    options = []

    # List of stage:name plugins that this plugin requires.
    # required plugins are guaranteed to be loaded and initialized before
    # a plugin is called to run.
    # Subclasses should set this as needed
    required_plugins = []

    # A list of Codebase attribute name strings that this plugin need to
    # be able to run.
    # A ScanCode run will fail with an error if these attributes are not
    # provided either as part of the scan data if resuing an existing scan or by
    # another plugin.
    # Subclasses should set this as needed.
    required_codebase_attributes = []

    # A list of Resource attribute name strings that this plugin need to
    # be able to run.
    # A ScanCode run will fail with an error if these attributes are not
    # provided either as part of the scan data if resuing an existing scan or by
    # another plugin.
    # Subclasses should set this as needed.
    required_resource_attributes = []

    # A relative sort order number (integer or float).
    # This is used to compute the order in which a plugin runs before
    # another plugin in a given stage
    # This is also used in scan results, results from scanners are sorted by
    # this sort_order then by plugin "name".
    sort_order = 100

    # flag set to True once this plugin class has been initialized by calling it
    # setup() class method.
    # This is set automatically when a plugin class is loaded in its manager.
    # Subclasses must not set this.
    initialized = False

    def __init__(self, *args, **kwargs):
        pass

    # TODO: pass own command options name/values as concrete kwargs
    def is_enabled(self, **kwargs):
        """
        Return True if this plugin is enabled by user-selected options.
        Subclasses must override.
        This receives all the ScanCode call arguments as kwargs.
        """
        raise NotImplementedError

    # TODO: pass own command options name/values as concrete kwargs
    def setup(self, **kwargs):
        """
        Execute some setup for this plugin. This is guaranteed to be called
        exactly one time at initialization if this plugin is enabled.
        Must raise an Exception on failure.
        Subclasses can override as needed.
        This receives all the ScanCode call arguments as kwargs.
        """
        pass

    # NOTE: Other methods below should NOT be overriden.

    @classmethod
    def qname(cls):
        """
        Return the qualified name of this plugin.
        """
        return "{cls.stage}:{cls.name}".format(cls=cls)

    def __repr__(self, *args, **kwargs):
        return self.qname()


class CodebasePlugin(BasePlugin):
    """
    Base class for plugins that process a whole codebase at once.
    """

    def process_codebase(self, codebase, **kwargs):
        """
        Process a `codebase` Codebase object updating its Resources as needed.
        Subclasses should override.
        This receives all the ScanCode call arguments as kwargs.
        """
        raise NotImplementedError


class PluginManager(object):
    """
    A PluginManager class for scanning-related plugins.
    """

    # a global managers cache as a mapping of {stage: manager instance}
    managers = {}

    def __init__(self, stage, module_qname, entrypoint, plugin_base_class):
        """
        Initialize this plugin manager for the `stage` specified in the fully
        qualified Python module name `module_qname` with plugins loaded from the
        setuptools `entrypoint` that must subclass `plugin_base_class`.
        """
        self.manager = PluggyPluginManager(project_name=stage)
        self.managers[stage] = self

        self.stage = stage
        self.entrypoint = entrypoint
        self.plugin_base_class = plugin_base_class
        self.manager.add_hookspecs(sys.modules[module_qname])

        # set to True once this manager is initialized by running its setup()
        self.initialized = False

        # list of plugin_class for all the plugins of this manager
        self.plugin_classes = []

    @classmethod
    def load_plugins(cls):
        """
        Setup the plugins enviroment.
        Must be called once to initialize all the plugins of all managers.
        """
        plugin_classes = []
        plugin_options = []
        for stage, manager in cls.managers.items():
            mgr_setup = manager.setup()
            if not mgr_setup:
                msg = "Cannot load plugins for stage: %(stage)s" % locals()
                raise PlugincodeError(msg)
            mplugin_classes, mplugin_options = mgr_setup
            plugin_classes.extend(mplugin_classes)
            plugin_options.extend(mplugin_options)
        return plugin_classes, plugin_options

    def setup(self):
        """
        Return a tuple of (list of all plugin classes, list of all options of
        all plugin classes).

        Load and validate available plugins for this PluginManager from its
        assigned `entrypoint`. Raise a PlugincodeError if a plugin is not valid such
        that when it does not subcclass the manager `plugin_base_class`.
        Must be called once to setup the plugins of this manager.
        """
        if self.initialized:
            return

        entrypoint = self.entrypoint
        try:
            self.manager.load_setuptools_entrypoints(entrypoint)
        except ImportError as e:
            raise e
        stage = self.stage

        plugin_options = []
        plugin_classes = []
        required_plugins = set()
        for name, plugin_class in self.manager.list_name_plugin():

            if not issubclass(plugin_class, self.plugin_base_class):
                qname = "%(stage)s:%(name)s" % locals()
                plugin_base_class = self.plugin_base_class
                raise PlugincodeError(
                    "Invalid plugin: %(qname)r: %(plugin_class)r "
                    "must extend %(plugin_base_class)r." % locals()
                )

            for option in plugin_class.options:
                if not isinstance(option, cliutils.PluggableCommandLineOption):
                    qname = "%(stage)s:%(name)s" % locals()
                    oname = option.name
                    clin = cliutils.PluggableCommandLineOption
                    raise PlugincodeError(
                        "Invalid plugin: %(qname)r: option %(oname)r "
                        "must extend %(clin)r." % locals()
                    )
                plugin_options.append(option)

            plugin_class.stage = stage
            plugin_class.name = name

            plugin_classes.append(plugin_class)

        self.plugin_classes = sorted(plugin_classes, key=lambda c: (c.sort_order, c.name))
        self.initialized = True
        return self.plugin_classes, plugin_options
