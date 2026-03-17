# Copyright 2011 Tomo Krajina
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import IO, Union, Optional, AnyStr

from . import gpx as mod_gpx

__version__ = '1.6.2'

def parse(xml_or_file: Union[AnyStr, IO[str]], version: Optional[str] = None) -> mod_gpx.GPX:
    """
    Parse xml (string) or file object. This is just an wrapper for
    GPXParser.parse() function.

    parser may be 'lxml', 'minidom' or None (then it will be automatically
    detected, lxml if possible).

    xml_or_file must be the xml to parse or a file-object with the XML.

    version may be '1.0', '1.1' or None (then it will be read from the gpx
    xml node if possible, if not then version 1.0 will be used).
    """

    from . import parser as mod_parser

    parser = mod_parser.GPXParser(xml_or_file)

    return parser.parse(version)
