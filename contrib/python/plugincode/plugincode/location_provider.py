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

import logging
import os
import sys

from pluggy import PluginManager as PluggyPluginManager

from plugincode import HookimplMarker
from plugincode import HookspecMarker

"""
Support for plugins that provide one or more paths keys typically OS-specific
paths to bundled pre-built binaries provided as Python packages.

Plugin can either be enabled for very specific environment/platform markers (OS,
arch, etc) in their built wheels .... Or be smart about OS/ARCH/etc and provide
a path based on running some code.
"""

logger = logging.getLogger(__name__)

# uncomment to enable logging locally
# logging.basicConfig(stream=sys.stdout)
# logger.setLevel(logging.DEBUG)


def logger_debug(*args):
    return logger.debug(" ".join(isinstance(a, str) and a or repr(a) for a in args))


project_name = __name__
entrypoint = "scancode_location_provider"

location_provider_spec = HookspecMarker(project_name=project_name)
location_provider_impl = HookimplMarker(project_name=project_name)


@location_provider_spec
class LocationProviderPlugin(object):
    """
    Base plugin class for plugins that provide path locations for one or more
    keys such as the path location to a native binary executable or related
    system files.
    A plugin is configured as it own package with proper environemnt markers
    """

    # name string under which this plugin is registered.
    # This is set automatically when a plugin class is loaded in its manager.
    # Subclasses must not set this.
    name = None

    def get_locations(self):
        """
        Return a mapping of {key: location} where location is an absolute path
        to a file or directory referenced by a known key. The location should
        exist on a given platorm/OS where this plgin can be installed.
        """
        raise NotImplementedError


class SimplePluginManager(object):
    """
    A PluginManager class for simple, non-scanning related plugins.
    """

    def __init__(self, project_name, entrypoint, plugin_base_class):
        """
        Initialize this plugin manager for the fully qualified Python module
        name `module_qname` with plugins loaded from the setuptools `entrypoint`
        that must subclass `plugin_base_class`.
        """
        self.manager = PluggyPluginManager(project_name=project_name)
        self.entrypoint = entrypoint
        self.plugin_base_class = plugin_base_class
        self.manager.add_hookspecs(sys.modules[project_name])

        # set to True once this manager is initialized by running its setup()
        self.initialized = False

        # mapping of {plugin.name: plugin_class} for all the loaded plugins of
        # this manager
        self.plugin_classes = dict()

    def setup(self):
        """
        Load and validate available plugins for this PluginManager from its
        assigned `entrypoint`. Raise an Exception if a plugin is not valid such
        that when it does not subcclass the manager `plugin_base_class`.
        Must be called once to initialize the plugins if this manager.

        Return a list of all plugin classes for this manager.
        """
        if self.initialized:
            return self.plugin_classes.values()

        entrypoint = self.entrypoint
        self.manager.load_setuptools_entrypoints(entrypoint)

        plugin_classes = []
        for name, plugin_class in self.manager.list_name_plugin():
            if not issubclass(plugin_class, self.plugin_base_class):
                plugin_base_class = self.plugin_base_class
                raise Exception(
                    "Invalid plugin: %(name)r: %(plugin_class)r "
                    "must extend %(plugin_base_class)r." % locals()
                )

            plugin_class.name = name
            plugin_classes.append(plugin_class)

        self.plugin_classes = dict([(cls.name, cls) for cls in plugin_classes])
        self.initialized = True
        return self.plugin_classes.values()


location_provider_plugins = SimplePluginManager(
    project_name=project_name, entrypoint=entrypoint, plugin_base_class=LocationProviderPlugin
)


class ProvidedLocationError(Exception):
    pass


def get_location(location_key, _cached_locations={}):
    """
    Return the location for a `location_key` if available from plugins or None.
    """
    if not _cached_locations:
        location_provider_plugins.setup()

        unknown_locations = {}

        for k, plugin_class in location_provider_plugins.plugin_classes.items():
            pc = plugin_class()
            provided_locs = pc.get_locations() or {}
            for loc_key, location in provided_locs.items():
                if not os.path.exists(location):
                    unknown_locations[loc_key] = location

                if loc_key in _cached_locations:
                    existing = _cached_locations[loc_key]
                    msg = (
                        "Duplicate location key provided: {loc_key}: "
                        "new: {location}, existing:{existing}"
                    )
                    msg = msg.format(**locals())
                    raise ProvidedLocationError(msg)

                _cached_locations[loc_key] = location

        if unknown_locations:
            msg = "Non-existing locations provided:\n:"
            msg += "\n".join("key:{}, loc: {}".format(k, l) for k, l in unknown_locations.items())
            raise ProvidedLocationError(msg)

    return _cached_locations.get(location_key)
