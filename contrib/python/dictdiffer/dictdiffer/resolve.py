# This file is part of Dictdiffer.
#
# Copyright (C) 2015 CERN.
# Copyright (C) 2017 ETH Zurich, Swiss Data Science Center, Jiri Kuncar.
#
# Dictdiffer is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more
# details.

"""Sub module to handle the conflict resolution."""

from .utils import get_path


class UnresolvedConflictsException(Exception):
    """Exception raised in case of an unresolveable conflict.

    Exception raised in case of conflicts, that can not be resolved using
    the provided actions in the automated merging process.
    """

    def __init__(self, unresolved_conflicts):
        """Initialize the UnresolvedConflictsException.

        :param unresolved_conflicts: list of unresolved conflicts.
                                     dictdiffer.conflict.Conflict objects.
        """
        self.message = ("The unresolved conflicts are stored in the *content* "
                        "attribute of this exception or in the "
                        "*unresolved_conflicts* attribute of the "
                        "dictdiffer.merge.Merger object.")
        self.content = unresolved_conflicts

    def __repr__(self):
        """Return the object representation."""
        return self.message

    def __str__(self):
        """Return the string representation."""
        return self.message


class NoFurtherResolutionException(Exception):
    """Exception raised to stop the automated resolution process.

    Raised in case that the automatic conflict resolution process should stop
    trying more general keys.
    """

    pass


class Resolver(object):
    """Class handling the conflict resolution process.

    Presents the given conflicts to actions designed to solve them.
    """

    def __init__(self, actions, additional_info=None):
        """Initialize the Resolver.

        :param action: dict object containing the necessary resolution
                       functions
        :param additional_info: any additional information required by the
                                actions
        """
        self.actions = actions
        self.additional_info = additional_info

        self.unresolved_conflicts = []

    def _auto_resolve(self, conflict):
        """Try to auto resolve conflicts.

        Method trying to auto resolve conflicts in case that the perform the
        same amendment.
        """
        if conflict.first_patch == conflict.second_patch:
            conflict.take = 'f'
            return True
        return False

    def _find_conflicting_path(self, conflict):
        """Return the shortest path commown to two patches."""
        p1p = get_path(conflict.first_patch)
        p2p = get_path(conflict.second_patch)

        # This returns the shortest path
        return p1p if len(p1p) <= len(p2p) else p2p

    def _consecutive_slices(self, iterable):
        """Build a list of consecutive slices of a given path.

        >>> r = Resolver(None, None)
        >>> list(r._consecutive_slices([1, 2, 3]))
        [[1, 2, 3], [1, 2], [1]]
        """
        return (iterable[:i] for i in reversed(range(1, len(iterable)+1)))

    def resolve_conflicts(self, first_patches, second_patches, conflicts):
        """Convert the given conflicts to the actions.

        The method, will map the conflicts to an actions based on the path of
        the conflict. In case that the resolution attempt is not successful, it
        will strip the last element of the path and try again, until the
        resolution is just not possible.

        :param first_patches: list of dictdiffer.diff patches
        :param second_patches: list of dictdiffer.diff patches
        :param conflicts: list of Conflict objects
        """
        for conflict in conflicts:
            conflict_path = self._find_conflicting_path(conflict)

            if self._auto_resolve(conflict):
                continue
            # Let's do some cascading here
            for sub_path in self._consecutive_slices(conflict_path):
                try:
                    if self.actions[sub_path](conflict,
                                              first_patches,
                                              second_patches,
                                              self.additional_info):
                        break
                except NoFurtherResolutionException:
                    self.unresolved_conflicts.append(conflict)
                    break
                except KeyError:
                    pass
            else:
                # The conflict could not be resolved
                self.unresolved_conflicts.append(conflict)

        if self.unresolved_conflicts:
            raise UnresolvedConflictsException(self.unresolved_conflicts)

    def manual_resolve_conflicts(self, picks):
        """Resolve manually the conflicts.

        This method resolves conflicts that could not be resolved in an
        automatic way. The picks parameter utilized the *take* attribute of the
        Conflict objects.

        :param picks: list of 'f' or 's' strings, utilizing the *take*
                      parameter of each Conflict object
        """
        if len(picks) != len(self.unresolved_conflicts):
            raise UnresolvedConflictsException(self.unresolved_conflicts)
        for pick, conflict in zip(picks, self.unresolved_conflicts):
            conflict.take = pick

        self.unresolved_conflicts = []
