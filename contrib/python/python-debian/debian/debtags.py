""" Facilities to work with debtags - tags for Debian packages """

# Copyright (C) 2006-2007  Enrico Zini <enrico@enricozini.org>
# Copyright (C) 2018-2023  Stuart Prescott <stuart@debian.org>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

import pickle
import re

from typing import (
    Callable,
    Dict,
    IO,
    Iterable,
    Iterator,
    Set,
    Tuple,
)
PkgTagDbType = Dict[str, Set[str]]
TagPkgDbType = Dict[str, Set[str]]
PkgFilterType = Callable[[str], bool]
TagFilterType = Callable[[str], bool]
PkgTagFilterType = Callable[[Tuple[str, Set[str]]], bool]


def parse_tags(input_data: Iterator[str]) -> Iterator[tuple[set[str], set[str]]]:
    lre = re.compile(r"^(.+?)(?::?\s*|:\s+(.+?)\s*)$")
    for line in input_data:
        # Is there a way to remove the last character of a line that does not
        # make a copy of the entire line?
        m = lre.match(line)
        if not m:
            continue

        pkgs = set(m.group(1).split(', '))
        if m.group(2):
            tags = set(m.group(2).split(', '))
        else:
            tags = set()
        yield pkgs, tags


def read_tag_database(input_data: Iterator[str]) -> PkgTagDbType:
    """Read the tag database, returning a pkg->tags dictionary"""
    db: PkgTagDbType = {}
    for pkgs, tags in parse_tags(input_data):
        # Create the tag set using the native set
        for p in pkgs:
            db[p] = tags.copy()
    return db


def read_tag_database_reversed(input_data: Iterator[str]) -> TagPkgDbType:
    """Read the tag database, returning a tag->pkgs dictionary"""
    db: TagPkgDbType = {}
    for pkgs, tags in parse_tags(input_data):
        # Create the tag set using the native set
        for tag in tags:
            if tag in db:
                db[tag] |= pkgs
            else:
                db[tag] = pkgs.copy()
    return db


def read_tag_database_both_ways(
        input_data: Iterator[str],
        tag_filter: TagFilterType | None = None,
    ) -> tuple[PkgTagDbType, TagPkgDbType]:
    "Read the tag database, returning a pkg->tags and a tag->pkgs dictionary"
    db: PkgTagDbType = {}
    dbr: TagPkgDbType = {}
    for pkgs, tags in parse_tags(input_data):
        # Create the tag set using the native set
        if tag_filter is None:
            tags = set(tags)
        else:
            tags = set(filter(tag_filter, tags))
        for pkg in pkgs:
            db[pkg] = tags.copy()
        for tag in tags:
            if tag in dbr:
                dbr[tag] |= pkgs
            else:
                dbr[tag] = pkgs.copy()
    return db, dbr


def reverse(db: PkgTagDbType) -> TagPkgDbType:
    """Reverse a tag database, from package -> tags to tag->packages"""
    res: dict[str, set[str]] = {}
    for pkg, tags in db.items():
        for tag in tags:
            if tag not in res:
                res[tag] = set()
            res[tag].add(pkg)
    return res


def output(db: PkgTagDbType) -> None:
    "Write the tag database"
    for pkg, tags in db.items():
        # Using % here seems awkward to me, but if I use calls to
        # sys.stdout.write it becomes a bit slower
        print("%s:" % (pkg), ", ".join(tags))


def relevance_index_function(full, sub):   #type: ignore
    #return (float(sub.card(tag)) / float(sub.tag_count())) / \
    #       (float(full.card(tag)) / float(full.tag_count()))
    #return sub.card(tag) * full.card(tag) / sub.tag_count()

    # New cardinality divided by the old cardinality
    #return float(sub.card(tag)) / float(full.card(tag))

    ## Same as before, but weighted by the relevance the tag had in the
    ## full collection, to downplay the importance of rare tags
    #return float(sub.card(tag) * full.card(tag)) / float(full.card(tag) * full.tag_count())
    # Simplified version:
    # return float(sub.card(tag)) / float(full.tag_count())

    # Weighted by the square root of the relevance, to downplay the very
    # common tags a bit
    # return lambda tag: float(sub.card(tag)) / float(full.card(tag)) *
    # math.sqrt(full.card(tag) / float(full.tag_count()))
    # return lambda tag: float(sub.card(tag)) / float(full.card(tag)) *
    # math.sqrt(full.card(tag) / float(full.package_count()))
    # One useless factor removed, and simplified further, thanks to Benjamin Mesing
    return lambda tag: float(sub.card(tag)**2) / float(full.card(tag))

    # The difference between how many packages are in and how many packages are out
    # (problems: tags that mean many different things can be very much out
    # as well.  In the case of 'image editor', for example, there will be
    # lots of editors not for images in the outside group.
    # It is very, very good for nonambiguous keywords like 'image'.
    # return lambda tag: 2 * sub.card(tag) - full.card(tag)
    # Same but it tries to downplay the 'how many are out' value in the
    # case of popular tags, to mitigate the 'there will always be popular
    # tags left out' cases.  Does not seem to be much of an improvement.
    # return lambda tag: sub.card(tag) - float(full.card(tag) - sub.card(tag))/
    # (math.sin(float(full.card(tag))*3.1415/full.package_count())/4 + 0.75)


class DB:
    """
    In-memory database mapping packages to tags and tags to packages.
    """

    def __init__(self) -> None:
        self.db: PkgTagDbType = {}
        self.rdb: TagPkgDbType = {}

    def read(self,
             input_data: Iterator[str],
             tag_filter: TagFilterType | None = None,
            ) -> None:
        """
        Read the database from a file.

        Example::
            # Read the system Debtags database
            db.read(open("/var/lib/debtags/package-tags", "r"))
        """
        self.db, self.rdb = read_tag_database_both_ways(input_data, tag_filter)

    def qwrite(self, file: IO[bytes]) -> None:
        """Quickly write the data to a pickled file"""
        pickle.dump(self.db, file)
        pickle.dump(self.rdb, file)

    def qread(self, file: IO[bytes]) -> None:
        """Quickly read the data from a pickled file"""
        self.db = pickle.load(file)
        self.rdb = pickle.load(file)

    def insert(self, pkg: str, tags: set[str]) -> None:
        self.db[pkg] = tags.copy()
        for tag in tags:
            if tag in self.rdb:
                self.rdb[tag].add(pkg)
            else:
                self.rdb[tag] = {pkg}

    def dump(self) -> None:
        output(self.db)

    def dump_reverse(self) -> None:
        output(self.rdb)

    def reverse(self) -> DB:
        "Return the reverse collection, sharing tagsets with this one"
        res = DB()
        res.db = self.rdb
        res.rdb = self.db
        return res

    def facet_collection(self) -> DB:
        """
        Return a copy of this collection, but replaces the tag names
        with only their facets.
        """
        fcoll = DB()
        tofacet = re.compile(r"^([^:]+).+")
        for pkg, tags in self.iter_packages_tags():
            ftags = {tofacet.sub(r"\1", t) for t in tags}
            fcoll.insert(pkg, ftags)
        return fcoll

    def copy(self) -> DB:
        """
        Return a copy of this collection, with the tagsets copied as
        well.
        """
        res = DB()
        res.db = self.db.copy()
        res.rdb = self.rdb.copy()
        return res

    def reverse_copy(self) -> DB:
        """
        Return the reverse collection, with a copy of the tagsets of
        this one.
        """
        res = DB()
        res.db = self.rdb.copy()
        res.rdb = self.db.copy()
        return res

    def choose_packages(self, package_iter: Iterable[str]) -> DB:
        """
        Return a collection with only the packages in package_iter,
        sharing tagsets with this one
        """
        res = DB()
        db = {}
        for pkg in package_iter:
            if pkg in self.db:
                db[pkg] = self.db[pkg]
        res.db = db
        res.rdb = reverse(db)
        return res

    def choose_packages_copy(self, package_iter: Iterable[str]) -> DB:
        """
        Return a collection with only the packages in package_iter,
        with a copy of the tagsets of this one
        """
        res = DB()
        db = {}
        for pkg in package_iter:
            db[pkg] = self.db[pkg]
        res.db = db
        res.rdb = reverse(db)
        return res

    def filter_packages(self, package_filter: PkgFilterType) -> DB:
        """
        Return a collection with only those packages that match a
        filter, sharing tagsets with this one.  The filter will match
        on the package.
        """
        res = DB()
        db = {}
        for pkg in filter(package_filter, self.db.keys()):
            db[pkg] = self.db[pkg]
        res.db = db
        res.rdb = reverse(db)
        return res

    def filter_packages_copy(self, filter_data: PkgFilterType) -> DB:
        """
        Return a collection with only those packages that match a
        filter, with a copy of the tagsets of this one.  The filter
        will match on the package.
        """
        res = DB()
        db = {}
        for pkg in filter(filter_data, self.db.keys()):
            db[pkg] = self.db[pkg].copy()
        res.db = db
        res.rdb = reverse(db)
        return res

    def filter_packages_tags(self, package_tag_filter: PkgTagFilterType) -> DB:
        """
        Return a collection with only those packages that match a
        filter, sharing tagsets with this one.  The filter will match
        on (package, tags).
        """
        res = DB()
        db = {}
        for pkg, _ in filter(package_tag_filter, self.db.items()):
            db[pkg] = self.db[pkg]
        res.db = db
        res.rdb = reverse(db)
        return res

    def filter_packages_tags_copy(self, package_tag_filter: PkgTagFilterType) -> DB:
        """
        Return a collection with only those packages that match a
        filter, with a copy of the tagsets of this one.  The filter
        will match on (package, tags).
        """
        res = DB()
        db = {}
        for pkg, _ in filter(package_tag_filter, self.db.items()):
            db[pkg] = self.db[pkg].copy()
        res.db = db
        res.rdb = reverse(db)
        return res

    def filter_tags(self, tag_filter: TagFilterType) -> DB:
        """
        Return a collection with only those tags that match a
        filter, sharing package sets with this one.  The filter will match
        on the tag.
        """
        res = DB()
        rdb = {}
        for tag in filter(tag_filter, self.rdb.keys()):
            rdb[tag] = self.rdb[tag]
        res.rdb = rdb
        res.db = reverse(rdb)
        return res

    def filter_tags_copy(self, tag_filter: TagFilterType) -> DB:
        """
        Return a collection with only those tags that match a
        filter, with a copy of the package sets of this one.  The
        filter will match on the tag.
        """
        res = DB()
        rdb = {}
        for tag in filter(tag_filter, self.rdb.keys()):
            rdb[tag] = self.rdb[tag].copy()
        res.rdb = rdb
        res.db = reverse(rdb)
        return res

    def has_package(self, pkg: str) -> bool:
        """Check if the collection contains the given package"""
        return pkg in self.db

    def has_tag(self, tag: str) -> bool:
        """Check if the collection contains packages tagged with tag"""
        return tag in self.rdb

    def tags_of_package(self, pkg: str) -> set[str]:
        """Return the tag set of a package"""
        return self.db[pkg] if pkg in self.db else set()

    def packages_of_tag(self, tag: str) -> set[str]:
        """Return the package set of a tag"""
        return self.rdb[tag] if tag in self.rdb else set()

    def tags_of_packages(self, pkgs: Iterable[str]) -> set[str]:
        """Return the set of tags that have all the packages in ``pkgs``"""
        return set.union(*(self.tags_of_package(p) for p in pkgs))

    def packages_of_tags(self, tags: Iterable[str]) -> set[str]:
        """Return the set of packages that have all the tags in ``tags``"""
        return set.union(*(self.packages_of_tag(t) for t in tags))

    def card(self, tag: str) -> int:
        """
        Return the cardinality of a tag
        """
        return len(self.rdb[tag]) if tag in self.rdb else 0

    def discriminance(self, tag: str) -> int:
        """
        Return the discriminance index if the tag.

        Th discriminance index of the tag is defined as the minimum
        number of packages that would be eliminated by selecting only
        those tagged with this tag or only those not tagged with this
        tag.
        """
        n = self.card(tag)
        tot = self.package_count()
        return min(n, tot - n)

    def iter_packages(self) -> Iterable[str]:
        """Iterate over the packages"""
        return self.db.keys()

    def iter_tags(self) -> Iterable[str]:
        """Iterate over the tags"""
        return self.rdb.keys()

    def iter_packages_tags(self) -> Iterable[tuple[str, set[str]]]:
        """Iterate over 2-tuples of (pkg, tags)"""
        return self.db.items()

    def iter_tags_packages(self) -> Iterable[tuple[str, set[str]]]:
        """Iterate over 2-tuples of (tag, pkgs)"""
        return self.rdb.items()

    def package_count(self) -> int:
        """Return the number of packages"""
        return len(self.db)

    def tag_count(self) -> int:
        """Return the number of tags"""
        return len(self.rdb)

    def ideal_tagset(self, tags: list[str]) -> set[str]:
        """
        Return an ideal selection of the top tags in a list of tags.

        Return the tagset made of the highest number of tags taken in
        consecutive sequence from the beginning of the given vector,
        that would intersect with the tagset of a comfortable amount
        of packages.

        Comfortable is defined in terms of how far it is from 7.
        """

        # TODO: the scoring function is quite ok, but may need more
        # tuning.  I also center it on 15 instead of 7 since we're
        # setting a starting point for the search, not a target point
        def score_fun(x: float) -> float:
            return float((x-15)*(x-15))/x

        tagset: set[str] = set()
        min_score = 3.
        for i in range(len(tags)):
            pkgs = self.packages_of_tags(tags[:i+1])
            card = len(pkgs)
            if card == 0:
                break
            score = score_fun(card)
            if score < min_score:
                min_score = score
                tagset = set(tags[:i+1])

        # Return always at least the first tag
        if not tagset:
            return set(tags[:1])
        return tagset

    def correlations(self) -> Iterator[tuple[str, str, float]]:
        """
        Generate the list of correlation as a tuple (hastag, hasalsotag, score).

        Every tuple will indicate that the tag 'hastag' tends to also
        have 'hasalsotag' with a score of 'score'.
        """
        for pivot in self.iter_tags():
            # pylint: disable=cell-var-from-loop
            with_ = self.filter_packages_tags(lambda pt: pivot in pt[1])
            without = self.filter_packages_tags(lambda pt: pivot not in pt[1])
            for tag in with_.iter_tags():
                if tag == pivot:
                    continue
                has = float(with_.card(tag)) / float(with_.package_count())
                hasnt = float(without.card(tag)) / float(without.package_count())
                yield pivot, tag, has - hasnt
