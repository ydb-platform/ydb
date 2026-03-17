# Copyright 1 dic. 2017 luisza
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

import ssl


class HttpConfig(dict):
    """
    Configuration settings for httplib.

    See: https://docs.python.org/3.4/library/http.client.html#http.client.HTTPSConnection

    available settings

    - http_key_file
    - http_cert_file
    - http_source_address
    - http_timeout

    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self["timeout"] = 5

    def save_keys(self, name, value):
        if name == "nosslcheck":
            self["context"] = ssl._create_unverified_context()
        else:
            self[name] = value

    def is_http_config(self, name, value):
        if name.startswith("--"):
            name = name[2:]
        elif name.startswith("-"):
            name = name[1:]

        if "http_" in name:
            name = name.replace("http_", "")
            self.save_keys(name, value)
            return True
        return False

    def __repr__(self) -> str:
        dev = ""
        for key, value in self.items():
            dev += f"{key!r} = {value!r}, "
        return dev


httpConfig = HttpConfig()
