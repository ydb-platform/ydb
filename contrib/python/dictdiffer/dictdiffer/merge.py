# This file is part of Dictdiffer.
#
# Copyright (C) 2015 CERN.
# Copyright (C) 2017 ETH Zurich, Swiss Data Science Center, Jiri Kuncar.
#
# Dictdiffer is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more
# details.

"""Sub module to handle the merging of dictdiffer patches."""

from . import diff
from .conflict import ConflictFinder
from .resolve import Resolver, UnresolvedConflictsException
from .unify import Unifier
from .utils import PathLimit


class Merger(object):
    """Class wrapping steps of the automated merging process.

    Usage:
        >>> lca = {}
        >>> first = {'foo': 'bar'}
        >>> second = {'bar': 'foo'}
        >>> path_limits = []
        >>> actions = {}
        >>> additional_info = {}
        >>> m = Merger(lca, first, second, actions,
        ...            path_limits, additional_info)
        >>> try:
        ...     m.run()
        ... except UnresolvedConflictsException:
        ...     # fix the conflicts
        ...     m.continue_run()
    """

    def __init__(self,
                 lca, first, second, actions,
                 path_limits=[], additional_info=None):
        """Initialize the Merger object.

        :param lca: latest common ancestor of the two diverging data structures
        :param first: first data structure
        :param second: second data structure
        :param path_limits: list of paths, utilized to instantiate a
                            dictdiffer.utils.PathLimit object
        :param additional_info: Any object containing additional information
                                used by the resolution functions
        """
        self.lca = lca
        self.first = first
        self.second = second
        self.path_limit = PathLimit(path_limits)

        self.actions = actions
        self.additional_info = additional_info

        self.conflict_finder = ConflictFinder()

        self.resolver = Resolver(self.actions,
                                 self.additional_info)

        self.unifier = Unifier()

        self.conflicts = []
        self.unresolved_conflicts = []

    def run(self):
        """Run the automated merging process.

        Runs every step necessary for the automated merging process, raising
        an UnresolvedConflictsException in case that the provided resolution
        actions can not solve a given conflict.

        After every performed step, the results are stored inside attributes of
        the merger object.
        """
        self.extract_patches()
        self.find_conflicts()
        self.resolve_conflicts()

        if self.unresolved_conflicts:
            raise UnresolvedConflictsException(self.unresolved_conflicts)

        self.unify_patches()

    def continue_run(self, picks):
        """Continue the merge after an UnresolvedConflictsException.

        :param picks: a list of 'f' or 's' strings, which utilize the Conflicts
                      class *take* attribute
        """
        self.resolver.manual_resolve_conflicts(picks)
        self.unresolved_conflicts = []
        self.unify_patches()

    def extract_patches(self):
        """Extract the patches.

        Extracts the differences between the *lca* and the *first* and
        *second* data structure and stores them in the attributes
        *first_patches* and *second_patches*.
        """
        self.first_patches = list(diff(self.lca, self.first,
                                       path_limit=self.path_limit,
                                       expand=True))
        self.second_patches = list(diff(self.lca, self.second,
                                        path_limit=self.path_limit,
                                        expand=True))

    def find_conflicts(self):
        """Find conflicts between the tow lists of patches.

        Finds the conflicts between the two difference lists and stores
        them in the *conflicts* attribute.
        """
        self.conflicts = (self
                          .conflict_finder
                          .find_conflicts(self.first_patches,
                                          self.second_patches))

    def resolve_conflicts(self):
        """Resolve the conflicts.

        Runs the automated conflict resolution process.
        Occurring unresolvable conflicts are stored in *unresolved_conflicts*.
        """
        try:
            self.resolver.resolve_conflicts(self.first_patches,
                                            self.second_patches,
                                            self.conflicts)
        except UnresolvedConflictsException as e:
            self.unresolved_conflicts = e.content

    def unify_patches(self):
        """Unify the patches after the conflict resolution.

        Unifies the patches after a successful merge and stores them in
        *unified_patches*.
        """
        self.unified_patches = self.unifier.unify(self.first_patches,
                                                  self.second_patches,
                                                  self.conflicts)
