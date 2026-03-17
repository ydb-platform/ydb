# SPDX-License-Identifier: Apache-2.0
#
# Copyright 2016 Hynek Schlawack
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

__title__ = "prometheus_async"

__author__ = "Hynek Schlawack"

__license__ = "Apache License, Version 2.0"
__copyright__ = f"Copyright (c) 2016 {__author__}"


from . import aio


__all__ = ["aio"]

try:
    from . import tx

    __all__ += ["tx"]
except ImportError:
    pass


def __getattr__(name: str) -> str:
    if name != "__version__":
        msg = f"module {__name__} has no attribute {name}"
        raise AttributeError(msg)

    from importlib.metadata import version

    return version("prometheus-async")
