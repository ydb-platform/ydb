# Copyright 2018-2022 Jetperch LLC
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

from . import api_timeout, gpi, info, measure, program, record, scan, \
    set_parameter, statistics, threads

__all__ = [api_timeout, gpi, info, measure, program, record, scan,
           set_parameter, statistics, threads]
"""This list of available command modules.  Each module must contain a 
parser_config(subparser) function.  The function must return the callable(args)
that will be executed for the command."""
