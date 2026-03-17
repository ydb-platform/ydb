# -*- coding: utf-8 -*-
# Copyright (c) 2013-2014 Will Thames <will@thames.id.au>
# Modified work Copyright (c) 2020 Warpnet B.V.

import os

from saltlint.utils import get_file_type


class Runner(object):

    def __init__(self, collection, file_name, config, checked_files=None):
        self.collection = collection

        self.files = set()
        # Assume the provided file name is a directory
        if os.path.isdir(file_name):
            self.files.add((os.path.join(file_name, 'init.sls'), 'state'))
        else:
            self.files.add((file_name, get_file_type(file_name)))

        # Get configuration options
        self.config = config
        self.tags = config.tags
        self.skip_list = config.skip_list
        self.verbosity = config.verbosity
        self._update_exclude_paths(config.exclude_paths)

        # Set the checked files
        if checked_files is None:
            checked_files = set()
        self.checked_files = checked_files

    def _update_exclude_paths(self, exclude_paths):
        if exclude_paths:
            # These will be (potentially) relative paths
            paths = [path.strip() for path in exclude_paths]
            self.exclude_paths = paths + [os.path.abspath(path) for path in paths]
        else:
            self.exclude_paths = []

    def is_excluded(self, file_path):
        # Any will short-circuit as soon as something returns True, but will
        # be poor performance for the case where the path under question is
        # not excluded.
        return any(file_path.startswith(path) for path in self.exclude_paths)

    def is_already_checked(self, file_path):
        return file_path in self.checked_files

    def run(self):
        files = []
        for index, file in enumerate(self.files):
            file_path = file[0]
            file_type = file[1]
            file_dict = {'path': file_path, 'type': file_type}
            # Skip excluded files
            if self.is_excluded(file_path):
                continue
            # Skip already checked files
            if self.is_already_checked(file_path):
                continue
            # Skip duplicates
            if file_dict in files[:index]:
                continue
            files.append(file_dict)

        problems = []
        for file in files:
            if self.verbosity > 0:
                print("Examining %s of type %s" % (file['path'], file['type']))
            problems.extend(self.collection.run(file, tags=set(self.tags),
                                                skip_list=self.skip_list))
        # Update list of checked files
        self.checked_files.update([file_dict['path'] for file_dict in files])

        return problems
