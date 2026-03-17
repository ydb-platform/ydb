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

import pytest

import environ


@environ.config(prefix="APP")
class AppConfig:
    host = environ.var("127.0.0.1", help="host help")
    port = environ.var(5000, converter=int, help="port help")


@environ.config(prefix="APP", generate_help="gen_help")
class ConfigRenamed:
    host = environ.var("127.0.0.1", help="host help")
    port = environ.var(5000, converter=int, help="port help")


@environ.config(prefix="APP", generate_help="")
class ConfigEmptyName:
    host = environ.var("127.0.0.1", help="host help")
    port = environ.var(5000, converter=int, help="port help")


@environ.config(prefix="APP", generate_help=None)
class ConfigNoneName:
    host = environ.var("127.0.0.1", help="host help")
    port = environ.var(5000, converter=int, help="port help")


def test_has_classmethod():
    """
    Class based `generate_help` classmethod exists
    """
    sentinel = object()
    # getattr returning default sentinel value means the att is missing
    # some attributes are expected
    assert getattr(AppConfig, "generate_help", sentinel) is not sentinel
    assert getattr(ConfigRenamed, "gen_help", sentinel) is not sentinel
    # another attributes shall be missing
    assert getattr(ConfigEmptyName, "generate_help", sentinel) is sentinel
    assert getattr(ConfigNoneName, "generate_help", sentinel) is sentinel


def test_generated_helps_equals():
    """
    Text by classmethod and environ.generate_help equals
    """
    assert environ.generate_help(AppConfig) == AppConfig.generate_help()
    assert environ.generate_help(ConfigRenamed) == ConfigRenamed.gen_help()


@pytest.mark.parametrize("display_defaults", [True, False])
def test_generated_helps_equals_display_defaults(display_defaults):
    """
    Text by classmethod and environ.generate_help equals with display_defaults
    """
    assert environ.generate_help(
        AppConfig, display_defaults=display_defaults
    ) == AppConfig.generate_help(display_defaults=display_defaults)
    assert environ.generate_help(
        ConfigRenamed, display_defaults=display_defaults
    ) == ConfigRenamed.gen_help(display_defaults=display_defaults)
