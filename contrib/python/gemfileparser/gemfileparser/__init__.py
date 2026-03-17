#!/usr/bin/env python
#
# Copyright (c) Balasankar C <balasankarc@autistici.org> and others
# SPDX-License-Identifier: GPL-3.0-or-later OR MIT

"""
Python library to parse Ruby Gemfiles, gemspec and Cocoapods podspec files.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import collections
import csv
import glob
import io
import os
import re


class Dependency(object):
    """
    A class to hold information about a dependency gem.
    """

    def __init__(self):
        self.name = ''
        self.requirement = []
        self.autorequire = ''
        self.source = ''
        self.parent = []
        self.group = ''

    def to_dict(self):
        return dict(
            name=self.name,
            requirement=self.requirement,
            autorequire=self.autorequire,
            source=self.source,
            parent=self.parent,
            group=self.group,
        )


class GemfileParser(object):
    """
    Create a GemfileParser object to perform operations.
    """
    gemfile_regexes = collections.OrderedDict()
    gemfile_regexes['source'] = re.compile(r"source:[ ]?(?P<source>[a-zA-Z:\/\.-]+)")
    gemfile_regexes['git'] = re.compile(r"git:[ ]?(?P<git>[a-zA-Z:\/\.-]+)")
    gemfile_regexes['platform'] = re.compile(r"platform:[ ]?(?P<platform>[a-zA-Z:\/\.-]+)")
    gemfile_regexes['path'] = re.compile(r"path:[ ]?(?P<path>[a-zA-Z:\/\.-]+)")
    gemfile_regexes['branch'] = re.compile(r"branch:[ ]?(?P<branch>[a-zA-Z:\/\.-]+)")
    gemfile_regexes['autorequire'] = re.compile(r"require:[ ]?(?P<autorequire>[a-zA-Z:\/\.-]+)")
    gemfile_regexes['group'] = re.compile(r"group:[ ]?(?P<group>[a-zA-Z:\/\.-]+)")
    gemfile_regexes['name'] = re.compile(r"(?P<name>[a-zA-Z]+[\.0-9a-zA-Z _-]*)")
    gemfile_regexes['requirement'] = re.compile(r"(?P<requirement>([>|<|=|~>|\d]+[ ]*[0-9\.\w]+[ ,]*)+)")

    group_block_regex = re.compile(r"group[ ]?:[ ]?(?P<groupblock>.*?) do")

    gemspec_add_dvtdep_regex = re.compile(r".*add_development_dependency(?P<line>.*)")
    gemspec_add_rundep_regex = re.compile(r".*add_runtime_dependency(?P<line>.*)")
    gemspec_add_dep_regex = re.compile(r".*dependency(?P<line>.*)")

    def __init__(self, filepath, appname=''):
        self.filepath = filepath

        self.current_group = 'runtime'

        self.appname = appname
        self.dependencies = {
            'development': [],
            'runtime': [],
            'dependency': [],
            'test': [],
            'production': [],
            'metrics': [],
        }
        with open(filepath) as gf:
            self.contents = gf.readlines()

        self.gemspec = filepath.endswith(('.gemspec', '.podspec'))

    @staticmethod
    def preprocess(line):
        """
        Remove the comment portion and excess spaces.
        """

        if "#" in line:
            line = line[:line.index('#')]
        line = line.strip()
        return line

    def parse_line(self, line):
        """
        Parse a line and return a Dependency object.
        """

        # csv requires a file-like object
        linefile = io.StringIO(line)
        for line in csv.reader(linefile, delimiter=','):
            column_list = []
            for column in line:
                stripped_column = (
                    column.replace("'", "")
                    .replace('"', "")
                    .replace("%q<", "")
                    .replace("(", "")
                    .replace(")", "")
                    .replace("[", "")
                    .replace("]", "")
                    .strip()
                )
                column_list.append(stripped_column)

            dep = Dependency()
            dep.group = self.current_group
            dep.parent.append(self.appname)
            for column in column_list:
                # Check for a match in each regex and assign to
                # corresponding variables
                for criteria, criteria_regex in GemfileParser.gemfile_regexes.items():
                    match = criteria_regex.match(column)
                    if match:
                        if criteria == 'requirement':
                            dep.requirement.append(match.group(criteria))
                        else:
                            setattr(dep, criteria, match.group(criteria))
                        break
            if dep.group in self.dependencies:
                self.dependencies[dep.group].append(dep)
            else:
                self.dependencies[dep.group] = [dep]

    def parse_gemfile(self):
        """
        Parse a Gemfile and returns a mapping of categorized dependencies.
        """

        for line in self.contents:
            line = self.preprocess(line)
            if line == '' or line.startswith('source'):
                continue

            elif line.startswith('group'):
                match = self.group_block_regex.match(line)
                if match:
                    self.current_group = match.group('groupblock')

            elif line.startswith('end'):
                self.current_group = 'runtime'

            elif line.startswith('gemspec'):
                # Gemfile contains a call to gemspec
                gemfiledir = os.path.dirname(self.filepath)
                gemspec_list = glob.glob(os.path.join(gemfiledir, "*.gemspec"))
                if len(gemspec_list) > 1:
                    print("Multiple gemspec files found")
                    continue
                gemspec_file = gemspec_list[0]
                self.parse_gemspec(path=os.path.join(gemfiledir, gemspec_file))

            elif line.startswith('gem '):
                line = line[3:]
                self.parse_line(line)

        return self.dependencies

    def parse_gemspec(self, path=None):
        """
        Parse a .gemspec or .podspec and return a mapping of categorized
        dependencies.
        """

        for line in self.contents:
            line = self.preprocess(line)
            match = self.gemspec_add_dvtdep_regex.match(line)
            if match:
                self.current_group = 'development'
            else:
                match = self.gemspec_add_rundep_regex.match(line)
                if match:
                    self.current_group = 'runtime'
                else:
                    match = self.gemspec_add_dep_regex.match(line)
                    if match:
                        self.current_group = 'dependency'
            if match:
                line = match.group('line')
                self.parse_line(line)
        return self.dependencies

    def parse(self):
        """
        Return a mapping of dependencies parsed from the Gemfile or gemspec.
        """
        if self.gemspec:
            return self.parse_gemspec()
        else:
            return self.parse_gemfile()


def command_line():
    """
    A minimal command line entry point.
    """
    import sys
    if len(sys.argv) < 2:
        print("Usage : parsegemfile <input file>")
        sys.exit(0)

    parsed = GemfileParser(sys.argv[1])
    output = parsed.parse()
    for key, value in list(output.items()):
        print(key, ":")
        for item in value:
            print("\t", item)
