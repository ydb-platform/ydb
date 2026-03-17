# Copyright 2016 The Kubernetes Authors.
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

__project__ = "kubernetes_asyncio"
# The version is auto-updated. Please do not edit.
__version__ = "35.0.1"

import kubernetes_asyncio.client as client
import kubernetes_asyncio.config as config
import kubernetes_asyncio.dynamic as dynamic
import kubernetes_asyncio.stream as stream
import kubernetes_asyncio.utils as utils
import kubernetes_asyncio.watch as watch

__all__ = ["client", "config", "dynamic", "stream", "utils", "watch"]
