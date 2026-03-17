#
# Copyright (c) nexB Inc. and others. All rights reserved.
# Copyright (c) 2018 Peter Odding
# Author: Peter Odding <peter@peterodding.com>
# URL: https://github.com/xolox/python-deb-pkg-tools
# SPDX-License-Identifier: Apache-2.0 AND MIT
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/debian-inspector for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.

import re

from attr import asdict
from attr import attrs
from attr import attrib

from debian_inspector import version as dversion

"""
Parse and evaluate Debian package relationship aka. dependencies.

This module provides functions to parse and evaluate Debian package relationship
declarations as defined in `chapter 7` of the Debian policy manual.
http://www.debian.org/doc/debian-policy/ch-relationships.html#s-depsyntax
"""

# Define a compiled regular expression pattern that we will use to match
# package relationship expressions consisting of a package name followed by
# optional version and architecture restrictions.
parse_package_relationship_expression = re.compile(
    r"""
    # Capture all leading characters up to (but not including)
    # the first parenthesis, bracket or space.
    (?P<name> [^\(\[ ]+ )
    # Ignore any whitespace.
    \s*
    # Optionally capture version restriction inside parentheses.
    ( \( (?P<version> [^)]+ ) \) )?
    # Ignore any whitespace.
    \s*
    # Optionally capture architecture restriction inside brackets.
    ( \[ (?P<architectures> [^\]]+ ) \] )?
""",
    re.VERBOSE,
).match

ARCHITECTURE_RESTRICTIONS_MESSAGE = "Architecture constraint is not implemented."


def parse_depends(relationships):
    """
    Return an AndRelationships from a Debian package relationship declaration
    line. Raise ValueError when parsing fails.

    `relationships` is a string containing one or more comma separated package
    relationships or a list of strings with package relationships.

    The input has a form such as ``python (>= 2.6), python (<< 3)``, i.e. a
    comma-separated list of expressions.

    Each expression is a package name/versions/arch constraint or a pipe-
    separated list of alternative.
    """

    if isinstance(relationships, str):
        relationships = (r.strip() for r in relationships.split(",") if r.strip())

    return AndRelationships.from_relationships(*(map(parse_alternatives, relationships)))


def parse_alternatives(expression):
    """
    Return a Relationship for an expression string that contains one or more
    alternative relationships. Raise ValueError when parsing fails.

    An expression with alternative is something such as as ``python2.6 |
    python2.7.``, i.e. a list of relationship expressions separated by ``|``
    tokens.

    Each pipe-separated sub-expression is parsed with `parse_relationship()`
    """
    if "|" in expression:
        alternatives = (a.strip() for a in expression.split("|") if a.strip())
        alternatives = (parse_relationship(a) for a in alternatives)
        return OrRelationships.from_relationships(*alternatives)
    else:
        return parse_relationship(expression)


# split on operators
split_on_ops = re.compile("([<>=]+)").split


def parse_relationship(expression):
    """
    Parse an expression containing a package name and optional version and
    architecture constraints.

    :param expression: A relationship expression (a string).
    :returns: A :class:`Relationship` object.
    :raises: :exc:`~exceptions.ValueError` when parsing fails.

    This function parses relationship expressions containing a package name and
    (optionally) a version relation of the form ``python (>= 2.6)`` and/or an
    architecture restriction (refer to the Debian policy manual's documentation
    on the syntax of relationship fields for details).
    https://www.debian.org/doc/debian-policy/ch-relationships.html
    """
    try:
        pre = parse_package_relationship_expression(expression)
        name = pre.group("name")
        version = pre.group("version")
    except AttributeError:
        print("gd:", pre.groupdict())
        print("this:", repr(expression))
        raise

    # Split the architecture restrictions into a tuple of strings.
    architectures = tuple((pre.group("architectures") or "").split())

    if name and not version:
        # A package name (and optional architecture restrictions) without
        # version relation.
        return Relationship(name=name, architectures=architectures)

    else:
        # A package name (and optional architecture restrictions) followed by a
        # relationship to specific version(s) of the package.
        tokens = [t.strip() for t in split_on_ops(version) if t and t.strip()]
        if len(tokens) != 2:
            # Encountered something unexpected!
            raise ValueError(
                "Corrupt package relationship expression: Splitting operator "
                "from version resulted in more than two tokens! "
                "(expression: {e}, tokens: {t})".format(e=expression, t=tokens)
            )
        return VersionedRelationship(
            name=name, architectures=architectures, operator=tokens[0], version=tokens[1]
        )


@attrs
class AbstractRelationship(object):
    @property
    def names(self):
        """
        A set of package names (strings) in the relationship.
        """
        raise NotImplementedError

    def matches(self, name, version=None, architecture=None):
        """
        Check if the relationship matches a given package name and version.

        Return True, False or None:
          - True if the name and version match,
          - False if the name matches but not the version
          - None if the name does not match
        """
        raise NotImplementedError


@attrs
class Relationship(AbstractRelationship):
    """
    A simple package relationship referring only to the name of a package.
    """

    name = attrib()
    architectures = attrib(default=tuple())

    @property
    def names(self):
        return set([self.name])

    def matches(self, name, version=None, architecture=None):
        """
        Return True if the relationship matches a given package name ignoring
        version or None.
        """
        if self.name == name:
            # TODO: support architectures
            if self.architectures:
                raise NotImplementedError(ARCHITECTURE_RESTRICTIONS_MESSAGE)
            return True
        else:
            return None

    def __str__(self, *args, **kwargs):
        if not self.architectures:
            return self.name

        return "{name} {arches}".format(
            name=self.name, arches="[{}]".format(" ".join(self.architectures))
        )

    def to_dict(self):
        return asdict(self)


@attrs
class VersionedRelationship(Relationship):
    """
    A conditional package relationship that refers to a package and certain
    versions of that package.
    """

    name = attrib()
    operator = attrib()
    version = attrib()
    architectures = attrib(default=tuple())

    def matches(self, name, version=None, architecture=None):
        """
        Check if the relationship matches a given package name and version.
        """
        if self.name == name:
            if version:
                if self.architectures:
                    raise NotImplementedError(ARCHITECTURE_RESTRICTIONS_MESSAGE)
                # TODO: check if this is the correct order
                return dversion.eval_constraint(version, self.operator, self.version)
            else:
                return False
        else:
            return None

    def __str__(self, *args, **kwargs):
        s = f"{self.name} ({self.operator} {self.version})"
        if self.architectures:
            s += " [{}]".format(" ".join(self.architectures))
        return s


@attrs
class MultipleRelationship(AbstractRelationship):
    relationships = attrib(default=tuple())

    @classmethod
    def from_relationships(cls, *relationships):
        return cls(relationships=tuple(relationships))

    @property
    def names(self):
        """
        Get the name(s) of the packages in the relationship set.

        :returns: A set of package names (strings).
        """
        names = set()
        for relationship in self.relationships:
            names |= relationship.names
        return names

    def __iter__(self):
        return iter(self.relationships)


class OrRelationships(MultipleRelationship):
    """
    A multi relationship where one of the relationships must be satisfied.
    """

    def matches(self, name, version=None, architecture=None):
        """
        Return True if at least one of the relationships match a given package
        name and version strings.

        Return False if at least one of the relationships name are matched
        without the version or None otherwise.
        """
        matches = None
        for alternative in self.relationships:
            alternative_matches = alternative.matches(name, version, architecture)
            if alternative_matches is True:
                return True
            elif alternative_matches is False:
                # Keep looking for a match but return False if we don't find one.
                matches = False
        return matches

    def __str__(self, *args, **kwargs):
        return " | ".join(str(r) for r in self.relationships)


@attrs
class AndRelationships(MultipleRelationship):
    """
    A multi relationship where all relationships must be satisfied.
    """

    def matches(self, name, version=None, architecture=None):
        """
        Return True if all the the relationships match a given package name and
        version strings.

        Return False if at least one relationship matches to False or None
        otherwise.
        """
        results = (r.matches(name, version) for r in self.relationships)
        matches = [r for r in results if r is not None]
        if matches:
            return all(matches)
        else:
            return None

    def __str__(self, *args, **kwargs):
        return ", ".join(str(r) for r in self.relationships)
