# This file is part of Dictdiffer.
#
# Copyright (C) 2015 CERN.
#
# Dictdiffer is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more
# details.

"""Sub module to handle the unification of patches after the merge."""

from .utils import get_path, nested_hash


class Unifier(object):
    """Class handling the unification process after the merge."""

    def unify(self, first_patches, second_patches, conflicts):
        """Unify two lists of patches into one.

        Takes into account their appearance in the given list of conflicts.

        :param first_patches: list of dictdiffer.diff patches
        :param second_patches: list of dictdiffer.diff patches
        :param conflicts: list of Conflict objects
        """
        self.unified_patches = []
        self._build_index(conflicts)

        sorted_patches = sorted(first_patches + second_patches, key=get_path)

        for patch in sorted_patches:
            conflict = self._index.get(nested_hash(patch))

            # Apply only the patches that were taken as part of conflict
            # resolution.
            if conflict:
                if conflict.take_patch() != patch:
                    continue

            self.unified_patches.append(patch)

        return self.unified_patches

    def _build_index(self, conflicts):
        """Create a dictionary attribute mapping patches to conflicts.

        Creates a dictionary attribute mapping the tuplefied version of each
        patch to it's containing Conflict object.
        """
        self._index = {}
        for conflict in conflicts:
            self._index[nested_hash(conflict.first_patch)] = conflict
            self._index[nested_hash(conflict.second_patch)] = conflict
