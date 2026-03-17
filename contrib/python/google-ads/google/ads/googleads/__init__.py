# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import warnings

import google.ads.googleads.client
import google.ads.googleads.errors
import google.ads.googleads.util

VERSION = "29.2.0"

# Checks if the current runtime is Python 3.9.
if sys.version_info.major == 3 and sys.version_info.minor <= 9:
    warnings.warn(
        "Support for Python versions less than 3.9 is deprecated in the "
        "google-ads package. Please upgrade to Python 3.10 or higher.",
        category=DeprecationWarning,
    )
