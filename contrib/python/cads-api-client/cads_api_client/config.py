# Copyright 2022, European Union.
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

from __future__ import annotations

import json
import os
from typing import Any

SUPPORTED_API_VERSION = "v1"


def read_configuration_file(config_path: str | None = None) -> dict[Any, Any]:
    if config_path is None:
        config_path = os.getenv("CADS_API_RC", "~/.cads-api-client.json")
    config_path = os.path.expanduser(config_path)
    try:
        with open(config_path) as fin:
            config = json.load(fin)
        assert isinstance(config, dict)
    except FileNotFoundError:
        raise
    except Exception:
        raise ValueError(f"failed to parse {config_path!r} file")
    return config


def get_config(key: str, config_path: str | None = None) -> Any:
    return (
        os.getenv(f"CADS_API_{key.upper()}")
        or read_configuration_file(config_path)[key]
    )
