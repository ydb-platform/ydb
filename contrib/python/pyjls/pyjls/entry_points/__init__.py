# Copyright 2018-2025 Jetperch LLC
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

from . import annotate, copy, csv, export, extract, info, plot, timestamp_patch

__all__ = [annotate, copy, csv, export, extract, info, plot, timestamp_patch]
"""This list of available command modules.  Each module must contain a 
parser_config(subparser) function.  The function must return the callable(args)
that will be executed for the command."""
