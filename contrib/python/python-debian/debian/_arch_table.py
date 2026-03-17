"""architecture matching

This leverages code from dpkg's Dpkg::Arch as well as python rewrites from
other people.  Copyright years imported from the sources.

@copyright: 2006-2015 Guillem Jover <guillem@debian.org>
@copyright: 2014, Ansgar Burchardt <ansgar@debian.org>
@copyright: 2014-2017, Johannes Schauer Marin Rodrigues <josch@debian.org>
@copyright: 2022, Niels Thykier <niels@thykier.net>
@license: GPL-2+
"""

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

from __future__ import annotations

import os
from os import PathLike
from typing import Iterable, IO

import collections.abc


def _parse_table_file(fd: IO[str]) -> Iterable[list[str]]:
    for line in fd:
        line = line.rstrip()
        if not line or line.startswith("#"):
            continue
        yield line.split()


_QuadTuple = collections.namedtuple("_QuadTuple", ['api_name', 'libc_name', 'os_name', 'cpu_name'])


class QuadTupleDpkgArchitecture(_QuadTuple):
    """Implementation detail of ArchTable"""

    def __contains__(self, item: object) -> bool:
        if isinstance(item, QuadTupleDpkgArchitecture):
            # This covers both equal and wildcard matches and semantically matches how dpkg does it
            return self.api_name in ('any', item.api_name) \
                   and self.libc_name in ('any', item.libc_name) \
                   and self.os_name in ('any', item.os_name) \
                   and self.cpu_name in ('any', item.cpu_name)
        return super().__contains__(item)

    @property
    def is_wildcard(self) -> bool:
        return any(x == 'any' for x in self)


class DpkgArchTable:

    def __init__(self,
                 arch2tuple: dict[str, QuadTupleDpkgArchitecture],
                 cputable: dict[str, tuple[str, ...]],
                 ostable: dict[str, tuple[str, ...]]
                ) -> None:
        self._arch2table = arch2tuple
        self._cputable = cputable
        self._ostable = ostable
        self._wildcard_cache: dict[str, QuadTupleDpkgArchitecture] = {
            'any': QuadTupleDpkgArchitecture('any', 'any', 'any', 'any')
        }

    @classmethod
    def load_arch_table(cls,
                        path: str | PathLike[str] = '/usr/share/dpkg') -> DpkgArchTable:
        # NOTE! This method is stubbed in including doctests to support non-Debian systems
        #   See conftest.py for the concrete implementation and the limited data set available.
        """Load the Dpkg Architecture Table

        This class method loads the architecture table from dpkg, so it can be used.

        >>> arch_table = DpkgArchTable.load_arch_table()
        >>> arch_table.matches_architecture("amd64", "any")
        True

        The method assumes the dpkg "tuple arch" format version 1.0 or the older triplet format.

        :param path: Choose a different directory for loading the architecture data.  The provided
          directory must contain the architecture data files from dpkg (such as "tupletable" and
          "cputable")
        """
        tupletable_path = os.path.join(path, 'tupletable')
        ostable_path = os.path.join(path, 'ostable')
        cputable_path = os.path.join(path, 'cputable')
        triplet_compat = False
        if not os.path.exists(tupletable_path):
            triplettable_path = os.path.join(path, 'triplettable')
            if os.path.join(triplettable_path):
                triplet_compat = True
                tupletable_path = triplettable_path

        with open(tupletable_path, encoding='utf-8') as tuple_fd,\
                open(ostable_path, encoding='utf-8') as os_fd,\
                open(cputable_path, encoding='utf-8') as cpu_fd:
            return cls._from_file(tuple_fd, os_fd, cpu_fd, triplet_compat=triplet_compat)

    @classmethod
    def _from_file(
        cls,
        tuple_table_fd: IO[str],
        os_table_fd: IO[str],
        cpu_table_fd: IO[str],
        triplet_compat: bool = False
    ) -> DpkgArchTable:
        arch2tuple: dict[str, QuadTupleDpkgArchitecture] = {}
        cputable = {} # Dict[str, Tuple[str, ...]]
        ostable = {} # Dict[str, Tuple[str, ...]]

        for row in _parse_table_file(os_table_fd):
            ostable[row[0]] = tuple(row[1:])
        for row in _parse_table_file(cpu_table_fd):
            cputable[row[0]] = tuple(row[1:])

        cpu_list = list(cputable.keys())
        for row in _parse_table_file(tuple_table_fd):
            # Manual unpack (so we support new columns)
            dpkg_tuple = row[0]
            dpkg_arch = row[1]

            if triplet_compat:
                dpkg_tuple = "base-" + dpkg_tuple

            if '<cpu>' in dpkg_tuple:
                for cpu_name in cpu_list:
                    debtuple_cpu = dpkg_tuple.replace('<cpu>', cpu_name)
                    dpkg_arch_cpu = dpkg_arch.replace('<cpu>', cpu_name)
                    if dpkg_arch_cpu not in arch2tuple:
                        arch2tuple[dpkg_arch_cpu] = QuadTupleDpkgArchitecture(
                            *debtuple_cpu.split('-', 3)
                        )
            else:
                arch2tuple[dpkg_arch] = QuadTupleDpkgArchitecture(*dpkg_tuple.split('-', 3))
        return DpkgArchTable(arch2tuple, cputable, ostable)

    def _dpkg_wildcard_to_tuple(self, arch: str) -> QuadTupleDpkgArchitecture:
        try:
            return self._wildcard_cache[arch]
        except KeyError:
            pass

        arch_tuple = arch.split('-', 3)
        if 'any' in arch_tuple:
            # This loop was written with the wildcard 'any' is always pre-cached.
            # (it might still work)
            while len(arch_tuple) < 4:
                arch_tuple.insert(0, 'any')
            result = QuadTupleDpkgArchitecture(*arch_tuple)
        else:
            result = self._dpkg_arch_to_tuple(arch)
        self._wildcard_cache[arch] = result
        return result

    def _dpkg_arch_to_tuple(self, dpkg_arch: str) -> QuadTupleDpkgArchitecture:
        if dpkg_arch.startswith("linux-"):
            dpkg_arch = dpkg_arch[6:]

        return self._arch2table[dpkg_arch]

    def dpkg_arch_to_multiarch(self, dpkg_arch: str) -> str:
        """Return the multiarch name for a given dpkg architecture [debarch_to_multiarch]

        This method is the closest match to dpkg's Dpkg::Arch::debarch_to_multiarch function.

        >>> arch_table = DpkgArchTable.load_arch_table()
        >>> arch_table.dpkg_arch_to_multiarch("amd64")
        'x86_64-linux-gnu'
        >>> arch_table.dpkg_arch_to_multiarch("armhf")
        'arm-linux-gnueabihf'

        :param dpkg_arch: A string representing a dpkg architecture.
        :returns: The multiarch name corresponding to the dpkg architecture.
        """
        debtuple = self._arch2table[dpkg_arch]
        abi = debtuple.api_name
        libc = debtuple.libc_name
        osname = debtuple.os_name
        cpu = debtuple.cpu_name

        assert cpu in self._cputable
        assert f'{abi}-{libc}-{osname}' in self._ostable
        gnutriplet = '-'.join((
                             self._cputable[cpu][0],
                             self._ostable[f'{abi}-{libc}-{osname}'][0]
                         ))
        return gnutriplet.replace('i686', 'i386')

    def matches_architecture(self, architecture: str, alias: str) -> bool:
        """Determine if a dpkg architecture matches another architecture or a wildcard [debarch_is]

        This method is the closest match to dpkg's Dpkg::Arch::debarch_is function.

        >>> arch_table = DpkgArchTable.load_arch_table()
        >>> arch_table.matches_architecture("amd64", "linux-any")
        True
        >>> arch_table.matches_architecture("i386", "linux-any")
        True
        >>> arch_table.matches_architecture("amd64", "amd64")
        True
        >>> arch_table.matches_architecture("i386", "amd64")
        False
        >>> arch_table.matches_architecture("all", "amd64")
        False
        >>> arch_table.matches_architecture("all", "all")
        True
        >>> # i386 is the short form of linux-i386. Therefore, it does not match kfreebsd-i386
        >>> arch_table.matches_architecture("i386", "kfreebsd-i386")
        False
        >>> # Note that "armel" and "armhf" are "arm" CPUs, so it is matched by "any-arm"
        >>> # (similar holds for some other architecture <-> CPU name combinations)
        >>> all(arch_table.matches_architecture(n, 'any-arm') for n in ['armel', 'armhf'])
        True
        >>> # Since "armel" is not a valid CPU name, this returns False (the correct would be
        >>> # any-arm as noted above)
        >>> arch_table.matches_architecture("armel", "any-armel")
        False
        >>> # Wildcards used as architecture always fail (except for special cases noted in the
        >>> # compatibility notes below)
        >>> arch_table.matches_architecture("any-i386", "i386")
        False
        >>> # any-i386 is not a subset of linux-any (they only have i386/linux-i386 as overlap)
        >>> arch_table.matches_architecture("any-i386", "linux-any")
        False
        >>> # Compatibility with dpkg - if alias is `any` then it always returns True
        >>> # even if the input otherwise would not make sense.
        >>> arch_table.matches_architecture("any-unknown", "any")
        True
        >>> # Another side effect of the dpkg compatibility
        >>> arch_table.matches_architecture("all", "any")
        True

        Compatibility note: The method emulates Dpkg::Arch::debarch_is function and therefore
        returns True if both parameters are the same even though they are wildcards or not known
        to be architectures. Additionally, if `alias` is `any`, then this method always returns
        True as `any` is the "match-everything-wildcard".

        :param architecture: A string representing a dpkg architecture.
        :param alias: A string representing a dpkg architecture or wildcard
               to match with.
        :returns: True if the `architecture` parameter is (logically) the same as the `alias`
                  parameter OR, if `alias` is a wildcard, the `architecture` parameter is a
                  subset of the wildcard.
                  The method returns False if `architecture` is not a known dpkg architecture,
                  or it is a wildcard.
        """
        if alias in ('any', architecture):
            # Dpkg::Arch has this shortcut too, which does not check whether they are valid
            # architectures.
            return True
        try:
            dpkg_arch = self._dpkg_arch_to_tuple(architecture)
            dpkg_wildcard = self._dpkg_wildcard_to_tuple(alias)
        except KeyError:
            return False
        return dpkg_arch in dpkg_wildcard

    def architecture_equals(self, arch1: str, arch2: str) -> bool:
        """Determine whether two dpkg architecture are exactly the same [debarch_eq]

        Unlike Python's `==` operator, this method also accounts for things like `linux-amd64` is
        a valid spelling of the dpkg architecture `amd64` (i.e.,
        `architecture_equals("linux-amd64", "amd64")` is True).

        This method is the closest match to dpkg's Dpkg::Arch::debarch_eq function.

        >>> arch_table = DpkgArchTable.load_arch_table()
        >>> arch_table.architecture_equals("linux-amd64", "amd64")
        True
        >>> arch_table.architecture_equals("amd64", "linux-i386")
        False
        >>> arch_table.architecture_equals("i386", "linux-amd64")
        False
        >>> arch_table.architecture_equals("amd64", "amd64")
        True
        >>> arch_table.architecture_equals("i386", "amd64")
        False
        >>> # Compatibility with dpkg: if the parameters are equal, then it always return True
        >>> arch_table.architecture_equals("unknown", "unknown")
        True

        Compatibility note: The method emulates Dpkg::Arch::debarch_eq function and therefore
        returns True if both parameters are the same even though they are wildcards or not known
        to be architectures.

        :param arch1: A string representing a dpkg architecture.
        :param arch2: A string representing a dpkg architecture.
        :returns: True if the dpkg architecture parameters are (logically) the exact same.
        """
        if arch1 == arch2:
            # Dpkg::Arch has this shortcut too, which does not check whether they are valid
            # architectures.
            return True
        try:
            dpkg_arch1 = self._dpkg_arch_to_tuple(arch1)
            dpkg_arch2 = self._dpkg_arch_to_tuple(arch2)
        except KeyError:
            return False
        return dpkg_arch1 == dpkg_arch2

    def architecture_is_concerned(self, architecture: str, architecture_restrictions: Iterable[str],
                                  allow_mixing_positive_and_negative: bool = False,
                                  ) -> bool:
        """Determine if a dpkg architecture is part of a list of restrictions [debarch_is_concerned]

        This method is the closest match to dpkg's Dpkg::Arch::debarch_is_concerned function.

        Compatibility notes:
          * The Dpkg::Arch::debarch_is_concerned function allow matching of negative and positive
            restrictions by default.  Often, this behaviour is not allowed nor recommended and the
            Debian Policy does not allow this practice in e.g., Build-Depends.  Therefore, this
            implementation defaults to raising ValueError when this occurs.  If the original
            behaviour is needed, set `allow_mixing_positive_and_negative` to True.
          * The Dpkg::Arch::debarch_is_concerned function is lazy and exits as soon as it finds a
            match. This means that if negative and positive restrictions are mixed, then order of
            the matches are important. This adaption matches that behaviour (provided that
            `allow_mixing_positive_and_negative` is set to True)

        >>> arch_table = DpkgArchTable.load_arch_table()
        >>> arch_table.architecture_is_concerned("linux-amd64", ["amd64", "i386"])
        True
        >>> arch_table.architecture_is_concerned("amd64", ["!amd64", "!i386"])
        False
        >>> # This is False because the "!amd64" is matched first.
        >>> arch_table.architecture_is_concerned("linux-amd64", ["!linux-amd64", "linux-any"],
        ...                                      allow_mixing_positive_and_negative=True)
        False
        >>> # This is True because the "linux-any" is matched first.
        >>> arch_table.architecture_is_concerned("linux-amd64", ["linux-any", "!linux-amd64"],
        ...                                      allow_mixing_positive_and_negative=True)
        True

        :param architecture: A string representing a dpkg architecture/wildcard.
        :param architecture_restrictions: A list of positive (amd64) or negative (!amd64) dpkg
                                          architectures or/and wildcards.
        :param allow_mixing_positive_and_negative: If True, the `architecture_restrictions` list
                                       can mix positive and negative (e.g., ["!any-amd64", "any"])
                                       restrictions. If False, mixing will trigger a ValueError.
        :returns: True if `architecture` is accepted by the `architecture_restrictions`.
        """

        # Our implementation diverges a bit from the Dpkg::Arch one because we want to enforce
        # allow_mixing_positive_and_negative=False even if the input matches before the
        # inconsistency is detected.

        verdict: bool | None = None
        positive_match_seen = False
        negative_match_seen = False
        arch_restriction_iter = iter(architecture_restrictions)

        try:
            dpkg_arch = self._dpkg_arch_to_tuple(architecture)
        except KeyError:
            return False

        for arch_restriction in arch_restriction_iter:
            # Should not happen in practice, but remove the special-case to avoid IndexError
            # (dpkg assumes invalid/unknown input does not match, so we can use a "continue" here)
            if arch_restriction == '':
                continue

            if arch_restriction[0] == '!':
                negative_match_seen = True
            else:
                positive_match_seen = True

            if verdict is not None:
                # We already know what the answer is. However, we are running through the remaining
                # input to ensure there is mixing of positive and negative restrictions.
                continue

            # Blindly matching Dpkg::Arch here, which also forgives uppercase letters.
            arch_restriction = arch_restriction.lower()
            verdict_if_matched = True
            arch_restriction_positive = arch_restriction

            if arch_restriction[0] == '!':
                verdict_if_matched = False
                arch_restriction_positive = arch_restriction[1:]

            dpkg_wildcard = self._dpkg_wildcard_to_tuple(arch_restriction_positive)

            # Inlined version of self.matches_architecture to reduce the number of lookups
            if dpkg_arch in dpkg_wildcard:
                verdict = verdict_if_matched
                if allow_mixing_positive_and_negative:
                    # If we do not care about the mixing, then we can closer emulate the dpkg
                    # implementation by existing early now
                    return verdict

        if not allow_mixing_positive_and_negative and positive_match_seen and negative_match_seen:
            raise ValueError("architecture_restrictions contained mixed positive and negative"
                             "restrictions (and allow_mixing_positive_and_negative was not True)")

        # If none of the restrictions directly matched the architecture, then this is now
        # a question of whether there was a negative match. If there was a negative match,
        # then it would have included the input as it is basically "any except <this>"
        if verdict is None:
            verdict = negative_match_seen
        return verdict

    def is_wildcard(self, wildcard: str) -> bool:
        """Determine if a given string is a dpkg wildcard [debarch_is_wildcard]

        This method is the closest match to dpkg's Dpkg::Arch::debarch_is_wildcard function.

        >>> arch_table = DpkgArchTable.load_arch_table()
        >>> arch_table.is_wildcard("linux-any")
        True
        >>> arch_table.is_wildcard("amd64")
        False
        >>> arch_table.is_wildcard("unknown")
        False
        >>> # Compatibility with the dpkg version of the function.
        >>> arch_table.is_wildcard("unknown-any")
        True

        Compatibility note: The original dpkg function does not ensure that the wildcard matches
          any supported architecture and this re-implementation matches that behaviour.  Therefore,
          this method can return True for a wildcard that can never match anything in practice.

        :param wildcard: A string that might represent a dpkg architecture or wildcard.
        :returns: True the parameter is a known dpkg wildcard.
        """
        try:
            dpkg_arch = self._dpkg_wildcard_to_tuple(wildcard)
        except KeyError:
            return False

        # _dpkg_wildcard_to_tuple falls back to concrete architectures so this can be False
        return dpkg_arch.is_wildcard
