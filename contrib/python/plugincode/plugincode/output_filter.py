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

stage = "output_filter"
entrypoint = "scancode_output_filter"

output_filter_spec = HookspecMarker(project_name=stage)
output_filter_impl = HookimplMarker(project_name=stage)


@output_filter_spec
class OutputFilterPlugin(CodebasePlugin):
    """
    Base plugin class for Resource output filter plugins that all output filter
    plugins must extend.

    Filter plugins MUST NOT modify the codebase beyond setting the
    Resource.is_filtered flag on resources.
    """

    pass


output_filter_plugins = PluginManager(
    stage=stage, module_qname=__name__, entrypoint=entrypoint, plugin_base_class=OutputFilterPlugin
)
