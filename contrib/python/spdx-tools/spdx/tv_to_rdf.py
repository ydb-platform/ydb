#!/usr/bin/env python
# Copyright (C) 2017 BMW AG
# Author: Thomas Hafner
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import sys

from spdx.parsers.loggers import StandardLogger
from spdx.writers.rdf import write_document
from spdx.parsers.tagvalue import Parser
from spdx.parsers.tagvaluebuilders import Builder


def tv_to_rdf(infile_name, outfile_name):
    """
    Convert a SPDX file from tag/value format to RDF format.
    Return True on sucess, False otherwise.
    """
    parser = Parser(Builder(), StandardLogger())
    parser.build()
    with open(infile_name) as infile:
        data = infile.read()
        document, error = parser.parse(data)
        if not error:
            with open(outfile_name, mode='w') as outfile:
                write_document(document, outfile)
            return True
        else:
            print('Errors encountered while parsing RDF file.')
            messages = []
            document.validate(messages)
            print('\n'.join(messages))
            return False


def main():
    args = sys.argv[1:]
    if not args:
        print(
            'Usage: spdx-tv2rdf <tag-value-file> <rdf-file>\n'
            'Convert an SPDX tag/value document to RDF.'
        )
        sys.exit(1)

    tvfile = args[0]
    rdffile = args[1]
    success = tv_to_rdf(tvfile, rdffile)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
