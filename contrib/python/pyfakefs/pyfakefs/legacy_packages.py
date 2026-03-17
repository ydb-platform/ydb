# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Imports external packages that replace or emulate internal packages.
These packages are not needed with any current Python version,
and their support in pyfakefs will be removed in a an upcoming release.
"""

try:
    import pathlib2
except ImportError:
    pathlib2 = None

try:
    import scandir as scandir
except ImportError:
    scandir = None
