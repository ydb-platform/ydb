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

import os

SUPPORTED_API_VERSION = "v1"
CONFIG_PREFIX = "ecmwf_datastores"


def read_config(path: str | None = None) -> dict[str, str]:
    if path is None:
        path = os.getenv(
            f"{CONFIG_PREFIX}_RC_FILE".upper(),
            f"~/.{CONFIG_PREFIX}rc".replace("_", "").lower(),
        )
    path = os.path.expanduser(path)
    try:
        config = {}
        with open(path) as f:
            for line in f.readlines():
                if ":" in line:
                    key, value = line.strip().split(":", 1)
                    config[key] = value.strip()
        return config
    except FileNotFoundError:
        raise
    except Exception:
        raise ValueError(f"Failed to parse {path!r} file")


def get_config(key: str, config_path: str | None = None) -> str:
    return os.getenv(f"{CONFIG_PREFIX}_{key}".upper()) or read_config(config_path)[key]
