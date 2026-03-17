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

import environ


@environ.config(prefix="APP")
class AppConfig:
    host = environ.var("127.0.0.1")
    port = environ.var(5000, converter=int)


@environ.config(prefix="APP", from_environ="from_env")
class ConfigRenamed:
    host = environ.var("127.0.0.1", help="host help")
    port = environ.var(5000, converter=int, help="port help")


@environ.config(prefix="APP", from_environ="")
class ConfigEmptyName:
    host = environ.var("127.0.0.1", help="host help")
    port = environ.var(5000, converter=int, help="port help")


@environ.config(prefix="APP", from_environ=None)
class ConfigNoneName:
    host = environ.var("127.0.0.1", help="host help")
    port = environ.var(5000, converter=int, help="port help")


def test_has_classmethod():
    """
    Class based `from_environ` classmethod exists
    """
    assert hasattr(AppConfig, "from_environ")
    assert hasattr(ConfigRenamed, "from_env")
    assert not hasattr(ConfigEmptyName, "from_environ")
    assert not hasattr(ConfigNoneName, "from_environ")


def test_default():
    """
    Class based `from_environ` without `environ` argument.
    """
    cfg = AppConfig.from_environ()

    assert cfg.host == "127.0.0.1"
    assert cfg.port == 5000

    assert environ.to_config(AppConfig) == AppConfig.from_environ()
    assert environ.to_config(ConfigRenamed) == ConfigRenamed.from_env()


def test_env():
    """
    Class based `from_environ`  with explicit `environ` argument.
    """
    env = {"APP_HOST": "0.0.0.0"}
    cfg = AppConfig.from_environ(environ=env)

    assert cfg.host == "0.0.0.0"
    assert cfg.port == 5000

    assert environ.to_config(AppConfig, environ=env) == AppConfig.from_environ(
        environ=env
    )

    assert environ.to_config(
        ConfigRenamed, environ=env
    ) == ConfigRenamed.from_env(environ=env)


def test_factory_default():
    """
    Class based ``from_environ`` allows ``attr.Factory`` defaults.
    """

    @environ.config()
    class FactoryConfig:
        x = environ.var(attr.Factory(list))
        y = environ.var("bar")

    cfg = FactoryConfig.from_environ({"APP_Y": "baz"})

    assert cfg.x == []
    assert cfg.y == "baz"
