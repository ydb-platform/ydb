# SPDX-License-Identifier: Apache-2.0
#
# Copyright 2017 Hynek Schlawack
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

import attr

from .._environ_config import Raise
from ..exceptions import MissingSecretError


def _get_default_secret(var, default):
    """
    Get default or raise MissingSecretError.
    """
    if isinstance(default, attr.Factory):
        return attr.NOTHING

    if isinstance(default, Raise):
        raise MissingSecretError(var)

    return default
