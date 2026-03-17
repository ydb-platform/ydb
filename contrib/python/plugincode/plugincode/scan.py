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

from plugincode import CodebasePlugin
from plugincode import PluginManager
from plugincode import HookimplMarker
from plugincode import HookspecMarker

stage = "scan"
entrypoint = "scancode_scan"

scan_spec = HookspecMarker(stage)
scan_impl = HookimplMarker(stage)


@scan_spec
class ScanPlugin(CodebasePlugin):
    """
    A scan plugin base class that all scan plugins must extend. A scan plugin
    provides a  `get_scanner()` method that returns a scanner function. This
    function must return an ordered mapping of attributes that will be attached
    to the resource. If a mapping key starts with "extra_data." the key value
    will be added instead ot the Resource.extra_data mapping.

    In addition to the get_scanner() method, a ScanPlugin is also a
    CodebasePlugin that can implement its own process_codebase() method. This
    method will be called after all the scans are completed and before the next
    stage (post-scans) plugins are called.
    """

    def get_scanner(self, **kwargs):
        """
        Return a scanner callable, receiving all the scancode call arguments as
        kwargs.

        The returned callable MUST be a top-level module importable function
        (e.g. that is picklable and it can be possibly closed on arguments with
        functools.partial) and accept these arguments:

        - a first `location` argument that is always an absolute path string to
          a file. This string is using the filesystem encoding (e.g. bytes on
          Linux and Unicode elsewhere).

        - other **kwargs that will be all the scancode call arguments.

        The returned callable MUST RETURN an ordered mapping of key/values that
        must be serializable to JSON.

        All mapping keys must be strings, including for any nested
        mappings.

        The first level mapping keys must be valid Python identifiers and MUST
        be declared as "resource_attributes" Resource-level attributes
        (otherwise this will fail at run time).

        If a mapping key starts with "extra_data." it does not need to
        be declared as a "resource_attributes" and will be saved as a
        key under the Reousrce.extra_data mapping.

        Values must be JSON-serializable types, such as one of:
        - None, unicode, int, float or long.
        - iterable/list/tuple/generator or dict/mapping preferrably ordered.

        This callable (typically a bare function) should carry as little state
        as possible as it may be executed through multiprocessing.

        Subclasses must override and implement.
        """
        raise NotImplementedError

    def process_codebase(self, codebase, **kwargs):
        """
        Process a `codebase` Codebase object updating its Resource as needed.
        Subclasses can override optionally.
        This receives all the ScanCode call arguments as kwargs.
        """
        pass


scan_plugins = PluginManager(
    stage=stage, module_qname=__name__, entrypoint=entrypoint, plugin_base_class=ScanPlugin
)
