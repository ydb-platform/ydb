# All rights reserved.
# SPDX-License-Identifier: Apache-2.0 AND CC-BY-4.0
#
# Visit https://aboutcode.org and https://github.com/nexB/scancode-toolkit for
# support and download. ScanCode is a trademark of nexB Inc.
#
# The ScanCode software is licensed under the Apache License version 2.0.
# The ScanCode open data is licensed under CC-BY-4.0.
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
#

import io
import logging
import re

from gemfileparser import GemfileParser

"""
Handle Cocoapods(.podspec) and Ruby(.gemspec) files.
"""


TRACE = False

logger = logging.getLogger(__name__)

if TRACE:
    import sys
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)


class Spec():
    parse_name = re.compile(r'.*\.name(\s*)=(?P<name>.*)')
    parse_version = re.compile(r'.*\.version(\s*)=(?P<version>.*)')
    parse_license = re.compile(r'.*\.license(\s*)=(?P<license>.*)')
    parse_summary = re.compile(r'.*\.summary(\s*)=(?P<summary>.*)')
    parse_description = re.compile(r'.*\.description(\s*)=(?P<description>.*)')
    parse_homepage = re.compile(r'.*\.homepage(\s*)=(?P<homepage>.*)')
    parse_source = re.compile(r'.*\.source(\s*)=(?P<source>.*)')

    def parse_spec(self, location):
        """
        Return dictionary contains podspec or gemspec file data.
        """
        with io.open(location, encoding='utf-8', closefd=True) as data:
            lines = data.readlines()

        spec_data = {}

        for line in lines:
            line = pre_process(line)
            match = self.parse_name.match(line)
            if match:
                name = match.group('name')
                spec_data['name'] = get_stripped_data(name)
            match = self.parse_version.match(line)
            if match:
                version = match.group('version')
                spec_data['version'] = get_stripped_data(version)
            match = self.parse_license.match(line)
            if match:
                license_value = match.group('license')
                spec_data['license'] = get_stripped_data(license_value)
            match = self.parse_summary.match(line)
            if match:
                summary = match.group('summary')
                spec_data['summary'] = get_stripped_data(summary)
            match = self.parse_homepage.match(line)
            if match:
                homepage = match.group('homepage')
                spec_data['homepage_url'] = get_stripped_data(homepage)
            match = self.parse_source.match(line)
            if match:
                source = re.sub(r'/*.*source.*?>', '', line)
                stripped_source = re.sub(r',.*', '', source)
                spec_data['source'] = get_stripped_data(stripped_source)
            match = self.parse_description.match(line)
            if match:
                if location.endswith('.gemspec'):
                    # FIXME: description can be in single or multi-lines
                    # There are many different ways to write description.
                    description = match.group('description')
                    spec_data['description'] = get_stripped_data(description)
                else:
                    spec_data['description'] = get_description(location)
            if '.email' in line:
                _key, _sep, value = line.rpartition('=')
                stripped_emails = get_stripped_data(value)
                stripped_emails = stripped_emails.strip()
                stripped_emails = stripped_emails.split(',')
                spec_data['email'] = stripped_emails
            elif '.author' in line:
                authors = re.sub(r'/*.*author.*?=', '', line)
                stripped_authors = get_stripped_data(authors)
                stripped_authors = re.sub(r'(\s*=>\s*)', '=>', stripped_authors)
                stripped_authors = stripped_authors.strip()
                stripped_authors = stripped_authors.split(',')
                spec_data['author'] = stripped_authors

        parser = GemfileParser(location)
        deps = parser.parse()
        dependencies = {}
        for key in deps:
            depends = deps.get(key, []) or []
            for dep in depends:
                    dependencies[dep.name] = dep.requirement
        spec_data['dependencies'] = dependencies

        return spec_data


def pre_process(line):
    """
    Return line after comments and space.
    """
    if '#' in line:
        line = line[:line.index('#')]
    stripped_data = line.strip()

    return stripped_data

def get_stripped_data(data):
    """
    Return data after removing unnecessary special character
    """
    for strippable in ("'",'"', '{', '}', '[', ']', '%q',):
        data = data.replace(strippable, '')

    return data.strip()


def get_description(location):
    """
    Return description from podspec.

    https://guides.cocoapods.org/syntax/podspec.html#description
    description is in the form:
    spec.description = <<-DESC
                     Computes the meaning of life.
                     Features:
                     1. Is self aware
                     ...
                     42. Likes candies.
                    DESC
    """
    with io.open(location, encoding='utf-8', closefd=True) as data:
        lines = data.readlines()
    description = ''
    for i, content in enumerate(lines):
        if '.description' in content:
            for cont in lines[i+1:]:
                if 'DESC' in cont:
                    break
                description += ' '.join([description, cont.strip()])
            break
    description.strip()
    return description