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

from importlib import metadata

import pytest

import environ


class TestLegacyMetadataHack:
    def test_version(self):
        """
        environ.__version__ returns the correct version.
        """
        with pytest.deprecated_call():
            assert metadata.version("environ-config") == environ.__version__

    def test_description(self):
        """
        environ.__description__ returns the correct description.
        """
        with pytest.deprecated_call():
            assert (
                "Boilerplate-free configuration with env variables."
                == environ.__description__
            )

    def test_uri(self):
        """
        environ.__uri__ returns the correct project URL.
        """
        with pytest.deprecated_call():
            assert "https://environ-config.readthedocs.io/" == environ.__uri__

    def test_email(self):
        """
        environ.__email__ returns Hynek's email address.
        """
        with pytest.deprecated_call():
            assert "hs@ox.cx" == environ.__email__

    def test_does_not_exist(self):
        """
        Asking for unsupported dunders raises an AttributeError.
        """
        with pytest.raises(
            AttributeError, match="module environ has no attribute __yolo__"
        ):
            environ.__yolo__
