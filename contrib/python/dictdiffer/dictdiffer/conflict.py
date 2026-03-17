# This file is part of Dictdiffer.
#
# Copyright (C) 2015 CERN.
#
# Dictdiffer is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more
# details.

"""Sub module to recognize conflicts in dictdiffer patches."""

import itertools

from .utils import get_path, is_super_path


class Conflict(object):
    """Wrapper class to store and handle two conflicting patches."""

    def __init__(self, patch1, patch2):
        """Initialize Conflict object.

        :param patch1: First patch tuple
        :param patch2: Second patch tuple
        """
        self.first_patch = patch1
        self.second_patch = patch2
        self.take = None

    def take_patch(self):
        """Return the patch determined by the *take* attribute."""
        if self.take:
            return self.first_patch if self.take == 'f' else self.second_patch
        raise Exception('Take attribute not set.')

    def __repr__(self):
        """Return string representation."""
        return 'Conflict({0}, {1})'.format(self.first_patch, self.second_patch)


class ConflictFinder(object):
    """Responsible for finding conflicting patches."""

    def _is_conflict(self, patch1, patch2):
        """Decide on a conflict between two patches.

        The conditions are:
        1. The paths are identical
        2. On of the paths is the super path of the other

        :param patch1: First patch tuple
        :param patch2: First patch tuple
        """
        path1 = get_path(patch1)
        path2 = get_path(patch2)

        if path1 == path2:
            return True
        elif is_super_path(path1, path2) and patch1[0] == 'remove':
            return True
        elif is_super_path(path2, path1) and patch2[0] == 'remove':
            return True

        return False

    def find_conflicts(self, first_patches, second_patches):
        """Find all conflicts between two lists of patches.

        Iterates over the lists of patches, comparing each patch from list
        one to each patch from list two.

        :param first_patches: List of patch tuples
        :param second_patches: List of patch tuples
        """
        self.conflicts = [Conflict(patch1, patch2) for patch1, patch2
                          in itertools.product(first_patches,
                                               second_patches)
                          if self._is_conflict(patch1, patch2)]

        return self.conflicts
