# deltas.py - utilities to parse and combine deltas
#
# Copyright 2023 Laurent Bulteau <laurent.bulteau@univ-eiffel.fr>
# Copyright 2023 Octobus <contact@octobus.net>
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.
"""This module gather function to parse and combine deltas.

The `mpatch` module also contains function dealing with similar structure.
However while `mpatch` focus on efficient Python code, this module focus on
having Python version easy to read and clear to understand.
"""

from __future__ import annotations

import struct
import typing

from typing import (
    Iterator,
    Sequence,
    Tuple,
)

from ..thirdparty import attr

if typing.TYPE_CHECKING:
    import attr


@attr.s(slots=True, frozen=True, repr=False)
class DeltaPiece:
    """represent a piece of delta, replacing one section with another

    Used for debug, not intended to be performant.
    """

    start = attr.ib(type=int)
    old_end = attr.ib(type=int)
    size = attr.ib(type=int)
    new_end = attr.ib(
        type=int,
        default=attr.Factory(
            lambda s: s.start + s.size,
            takes_self=True,
        ),
    )
    storage_size = attr.ib(
        type=int,
        default=attr.Factory(
            lambda s: 12 + s.size,
            takes_self=True,
        ),
    )
    offset = attr.ib(
        type=int,
        default=attr.Factory(
            lambda s: s.size - (s.old_end - s.start),
            takes_self=True,
        ),
    )

    def __le__(self, other):
        return self.start <= other.start

    def __repr__(self) -> str:
        return f"DeltaPiece({self.start},{self.old_end},{self.size})"

    def offsetted(self, offset):
        """return a new version of this DeltaPiece moved by <offset> bytes


        >>> DeltaPiece(0, 10, 15).offsetted(7)
        DeltaPiece(7,17,15)
        >>> DeltaPiece(20, 30, 12).offsetted(-5)
        DeltaPiece(15,25,12)
        """
        if offset == 0:
            return self
        assert 0 <= self.start + offset, (self.start, offset)
        return DeltaPiece(self.start + offset, self.old_end + offset, self.size)

    def truncate(self, amount):
        """return a new version of this DeltaPiece, removing the <amount> first bytes change to the base

        >>> DeltaPiece(10, 20, 3).truncate(5)
        DeltaPiece(15,20,0)
        >>> DeltaPiece(10, 20, 5).truncate(5)
        DeltaPiece(15,20,0)
        >>> DeltaPiece(10, 20, 10).truncate(5)
        DeltaPiece(15,20,5)
        >>> DeltaPiece(10, 20, 15).truncate(5)
        DeltaPiece(15,20,10)
        """
        assert amount < (self.old_end - self.start)
        return DeltaPiece(
            self.start + amount,
            self.old_end,
            self.size - min(amount, self.size),
        )


@attr.s(slots=True, init=False, repr=False)
class Delta:
    """Represent a series of change against a base."""

    _patches = attr.ib(type=list[DeltaPiece])
    _storage_size = attr.ib(type=int)

    def __init__(
        self,
        patches: list[DeltaPiece],
    ) -> None:
        self._storage_size = None
        self._patches = patches

    @classmethod
    def from_bytes(cls, binary: bytes):
        """build a Delta from its binary representation"""
        pos = 0
        patches = []
        while pos < len(binary):
            h = struct.unpack(b">lll", binary[pos : pos + 12])
            p_start, p_end, p_size = h
            assert p_size >= 0
            pos += 12
            pos += p_size
            patches.append(DeltaPiece(p_start, p_end, p_size))

        return cls(patches)

    def __repr__(self) -> str:
        self.storage_size
        patches = [(p.start, p.old_end, p.size) for p in self._patches]
        return f"Delta(size={self.storage_size}, {patches})"

    @property
    def storage_size(self):
        """How many bytes are needed to represent this delta in binary"""
        if self._storage_size is None:
            self._storage_size = sum(p.storage_size for p in self._patches)
        return self._storage_size

    def combine(self, other: Delta) -> Delta:
        """combined a delta with another one

        The `self` object should be the earlier delta in the chain, the `other`
        object applying over it.
        This produce a new delta that would result in `other` content when
        applied on `self` base.

        input:
        A --[self]--> B --[other]-->C
        output:
        A --[combine]--> C

        Resulting delta is always "correct", but there is some case where is it not "minimal"

        For example, if a line is reverted to a previous content (A → B → A),
        folding the two last patch would result in a patch requesting an
        unecessary replacement of the line with "A".


        ###### test case without offset

        ### Check that disjoint patches do not get merged
        >>> low = Delta([DeltaPiece(0, 10, 10)])
        >>> high = Delta([DeltaPiece(20, 30, 10)])
        >>> low.storage_size
        22
        >>> high.storage_size
        22
        >>> low.combine(high)
        Delta(size=44, [(0, 10, 10), (20, 30, 10)])

        ### Check that consecutive patches get merged
        >>> low = Delta([DeltaPiece(0, 10, 10)])
        >>> high = Delta([DeltaPiece(10, 20, 10)])
        >>> low.storage_size
        22
        >>> high.storage_size
        22
        >>> low.combine(high)
        Delta(size=32, [(0, 20, 20)])

        ### Check that overlapping patches get merged
        >>> low = Delta([DeltaPiece(0, 10, 10)])
        >>> high = Delta([DeltaPiece(5, 15, 10)])
        >>> low.storage_size
        22
        >>> high.storage_size
        22
        >>> low.combine(high)
        Delta(size=27, [(0, 15, 15)])

        ### Check that a chain overlapping patches get merged
        >>> low = Delta([DeltaPiece(0, 10, 10), DeltaPiece(15, 25, 10), DeltaPiece(30, 40, 10)])
        >>> high = Delta([DeltaPiece(8, 18, 10), DeltaPiece(23, 33, 10)])
        >>> low.storage_size
        66
        >>> high.storage_size
        44
        >>> low.combine(high)
        Delta(size=52, [(0, 40, 40)])

        ### Check that a chain with patch being a super set than other get merged fine
        >>> low = Delta([DeltaPiece(0, 30, 30), DeltaPiece(50, 60, 10)])
        >>> high = Delta([DeltaPiece(10, 20, 10), DeltaPiece(40, 70, 30)])
        >>> low.storage_size
        64
        >>> high.storage_size
        64
        >>> low.combine(high)
        Delta(size=84, [(0, 30, 30), (40, 70, 30)])

        ###### test case without offset

        ### Check that disjoint patches do not get merged
        >>> low = Delta([DeltaPiece(0, 10, 5)])
        >>> high = Delta([DeltaPiece(10, 15, 10)])
        >>> low.storage_size
        17
        >>> high.storage_size
        22
        >>> low.combine(high)
        Delta(size=39, [(0, 10, 5), (15, 20, 10)])

        ### Check that consecutive patches get merged
        >>> low = Delta([DeltaPiece(0, 10, 15)])
        >>> high = Delta([DeltaPiece(15, 20, 7)])
        >>> low.storage_size
        27
        >>> high.storage_size
        19
        >>> low.combine(high)
        Delta(size=34, [(0, 15, 22)])

        ### Check that overlapping patches get merged
        >>> low = Delta([DeltaPiece(10, 20, 20)])
        >>> high = Delta([DeltaPiece(25, 35, 3)])
        >>> low.storage_size
        32
        >>> high.storage_size
        15
        >>> low.combine(high)
        Delta(size=30, [(10, 25, 18)])

        ### Check that a chain overlapping patches get merged (patches around that)
        >>> low = Delta([
        ...     DeltaPiece(0, 0, 5),
        ...     DeltaPiece(5, 10, 15),
        ...     DeltaPiece(15, 20, 0),
        ...     DeltaPiece(30, 35, 15),
        ...     DeltaPiece(40, 80, 10),
        ... ])
        >>> high = Delta([
        ...     DeltaPiece(18, 45, 5),
        ...     DeltaPiece(50, 65, 30),
        ...     DeltaPiece(75, 75, 5)
        ... ])
        >>> low.storage_size
        105
        >>> high.storage_size
        76
        >>> low.combine(high)
        Delta(size=101, [(0, 0, 5), (5, 82, 55), (85, 85, 5)])

        ### Check that a chain with patch being a super set than other get merged fine
        >>> low = Delta([
        ...     DeltaPiece(5, 10, 15),    # shift things around at the start (+10)
        ...     DeltaPiece(20, 50, 10),   # Bigger range than the higher one (-20)
        ...     DeltaPiece(80, 90, 40),   # smaller range than the higher one (+30)
        ... ])
        >>> high = Delta([
        ...     DeltaPiece(33, 38, 65),
        ...     DeltaPiece(65, 120, 10),
        ...     DeltaPiece(150, 200, 66),
        ... ])
        >>> low.storage_size
        101
        >>> high.storage_size
        177
        >>> low.combine(high)
        Delta(size=209, [(5, 10, 15), (20, 50, 70), (75, 100, 10), (130, 180, 66)])

        #### complexe case flagged by the tests
        >>> high = Delta([
        ...     DeltaPiece(0, 1652, 1652),
        ...     DeltaPiece(9869, 9902, 33),
        ...     DeltaPiece(29669, 29702, 33),
        ...     DeltaPiece(49469, 49502, 33),
        ...     DeltaPiece(59369, 59402, 33),
        ...     DeltaPiece(69269, 69302, 33),
        ...     DeltaPiece(69335, 69368, 33),
        ...     DeltaPiece(79169, 79202, 33),
        ...     DeltaPiece(98969, 99002, 33),
        ...     DeltaPiece(108869, 108902, 33),
        ...     DeltaPiece(118769, 118802, 33),
        ...     DeltaPiece(138569, 138602, 33),
        ...     DeltaPiece(158369, 158402, 33),
        ...     DeltaPiece(168269, 168302, 33),
        ...     DeltaPiece(178169, 178202, 33),
        ...     DeltaPiece(178235, 178268, 33),
        ...     DeltaPiece(188069, 188102, 33),
        ...     DeltaPiece(207869, 207902, 33),
        ...     DeltaPiece(217769, 217802, 33),
        ...     DeltaPiece(227669, 227702, 33),
        ...     DeltaPiece(247469, 247502, 33),
        ...     DeltaPiece(267269, 267302, 33),
        ...     DeltaPiece(277169, 277202, 33),
        ...     DeltaPiece(287069, 287102, 33),
        ...     DeltaPiece(287135, 287168, 33),
        ...     DeltaPiece(296969, 297002, 33),
        ...     DeltaPiece(316769, 316802, 33),
        ...     DeltaPiece(326669, 326702, 33),
        ...     DeltaPiece(336569, 336602, 33)
        ... ])
        >>> low = Delta([
        ...     DeltaPiece(0, 1651, 1652),
        ...     DeltaPiece(9868, 9901, 33),
        ...     DeltaPiece(19768, 19801, 33),
        ...     DeltaPiece(29668, 29701, 33),
        ...     DeltaPiece(39568, 39601, 33),
        ...     DeltaPiece(49468, 49501, 33),
        ...     DeltaPiece(59368, 59401, 33),
        ...     DeltaPiece(69268, 69301, 33),
        ...     DeltaPiece(79168, 79201, 33),
        ...     DeltaPiece(89068, 89101, 33),
        ...     DeltaPiece(98968, 99001, 33),
        ...     DeltaPiece(108868, 108901, 33),
        ...     DeltaPiece(118768, 118801, 33),
        ...     DeltaPiece(128668, 128701, 33),
        ...     DeltaPiece(138568, 138601, 33),
        ...     DeltaPiece(148468, 148501, 33),
        ...     DeltaPiece(158368, 158401, 33),
        ...     DeltaPiece(168268, 168301, 33),
        ...     DeltaPiece(178168, 178201, 33),
        ...     DeltaPiece(188068, 188101, 33),
        ...     DeltaPiece(197968, 198001, 33),
        ...     DeltaPiece(207868, 207901, 33),
        ...     DeltaPiece(217768, 217801, 33),
        ...     DeltaPiece(227668, 227701, 33),
        ...     DeltaPiece(237568, 237601, 33),
        ...     DeltaPiece(247468, 247501, 33),
        ...     DeltaPiece(257368, 257401, 33),
        ...     DeltaPiece(267268, 267301, 33),
        ...     DeltaPiece(277168, 277201, 33),
        ...     DeltaPiece(287068, 287101, 33),
        ...     DeltaPiece(296968, 297001, 33),
        ...     DeltaPiece(306868, 306901, 33),
        ...     DeltaPiece(316768, 316801, 33),
        ...     DeltaPiece(326668, 326701, 33),
        ...     DeltaPiece(336568, 336601, 33)
        ... ])
        >>> low.storage_size
        3194
        >>> high.storage_size
        2924
        >>> low.combine(high)
        Delta(size=3329, [(0, 1651, 1652), (9868, 9901, 33), (19768, 19801, 33), (29668, 29701, 33), (39568, 39601, 33), (49468, 49501, 33), (59368, 59401, 33), (69268, 69301, 33), (69334, 69367, 33), (79168, 79201, 33), (89068, 89101, 33), (98968, 99001, 33), (108868, 108901, 33), (118768, 118801, 33), (128668, 128701, 33), (138568, 138601, 33), (148468, 148501, 33), (158368, 158401, 33), (168268, 168301, 33), (178168, 178201, 33), (178234, 178267, 33), (188068, 188101, 33), (197968, 198001, 33), (207868, 207901, 33), (217768, 217801, 33), (227668, 227701, 33), (237568, 237601, 33), (247468, 247501, 33), (257368, 257401, 33), (267268, 267301, 33), (277168, 277201, 33), (287068, 287101, 33), (287134, 287167, 33), (296968, 297001, 33), (306868, 306901, 33), (316768, 316801, 33), (326668, 326701, 33), (336568, 336601, 33)])
        """
        new_patches = []

        # == delta naming ==
        #
        # the lower delta is called "bottom",
        # the higher delta is called "top",
        # the final result if called "folded"
        #
        # == content name ==
        #
        # the unknown content modified by "lower" and "folded" is called "base" or "A"
        # the result of apply "bottom" on A is B
        # the result of apply "top" on B is C
        #
        #   A ─┬─[bottom]─> B ─[top]─┬─> C
        #      └────────[folded]─────┘

        iter_bottom = iter(self._patches)
        iter_top = iter(other._patches)
        current_bottom = next(iter_bottom, None)
        current_top = next(iter_top, None)

        # The amount of bytes added or removed by content from "bottom" so far
        #
        # The reverse of this value need to be apply to "top" patches "start"
        # when considering where they will apply in "base" (and, therefor, how
        # they need to be represented in "folded")
        #
        # When the "low" patch:
        # - add N extra bytes of content compared the "base", it goes up by N
        # - remove N bytes of content compared the "base", it goes down by N
        #
        # For a patch "b" from bottom:
        #   - start of the resulting area in B is b.start + offset_BA
        #   - end of the resulting area in B is b.start + b.size + offset_BA
        #
        # For a patch "t" from top:
        #   - start of the replaced area in A is t.start - offset_BA
        #   - end of the replaced area in A is t.old_end - offset_BA
        offset_BA = 0

        while current_bottom is not None or current_top is not None:
            if current_top is None:
                # There is no "top" patches anymore
                #
                # Let us consume the remaining "bottom" ones.
                # We will exit the loop in the next iteration.
                new_patches.append(current_bottom)
                new_patches.extend(iter_bottom)
                current_bottom = None
            elif current_bottom is None:
                # There is no "bottom" patches anymore
                #
                # Let us consume the remaining "top" ones.
                # We will exit the loop in the next iteration.
                new_patches.append(current_top.offsetted(-offset_BA))
                for p in iter_top:
                    new_patches.append(p.offsetted(-offset_BA))
                current_top = None
            elif (current_bottom.start + current_bottom.size) < (
                current_top.start - offset_BA
            ):
                # The next patch in "bottom" alter an area of "base" that is
                # untouched by the next patch in "top"
                #
                # we can simply copy that "bottom" patch in "folded"
                new_patches.append(current_bottom)
                offset_BA += current_bottom.offset
                current_bottom = next(iter_bottom, None)
            elif (current_top.old_end - offset_BA) < current_bottom.start:
                # The next patch in "top" alter an aread of base untouched by
                # the next patch in "top"
                #
                # we can copy over that "top" patch after adjusting it location
                p = current_top.offsetted(-offset_BA)
                new_patches.append(p)
                current_top = next(iter_top, None)
            else:
                # There is some overlap between the next patch in "bottom" and
                # the next patch in "top" and they need to be merged.

                # start with a zero size area from the first patch
                start_A = min(
                    current_bottom.start, current_top.start - offset_BA
                )
                end_A = start_A

                # check content offset from each layer
                #
                # We don't consume the initial patch yet, as consuming them is
                # no different than consuming the subsequent one.
                local_offset_from_bottom = 0
                local_offset_from_top = 0

                # We look for successive patches in either list the overlap
                # with the current one, and merge them (extend end points and
                # adapt offsets)
                while not (current_bottom is None and current_top is None):
                    if current_top is None:
                        # We no longer have patch from "bottom", merge everything we can from "top"
                        while current_bottom is not None and (
                            current_bottom.start <= end_A
                        ):
                            if current_bottom.old_end > end_A:
                                end_A = current_bottom.old_end
                            local_offset_from_bottom += current_bottom.offset
                            current_bottom = next(iter_bottom, None)
                        break
                    elif current_bottom is None:
                        local_BA = offset_BA + local_offset_from_bottom
                        # We no longer have patch from "bottom", merge everything we can from "top"
                        while (
                            current_top is not None
                            and current_top.start - local_BA <= end_A
                        ):
                            if current_top.old_end - local_BA > end_A:
                                end_A = current_top.old_end - local_BA
                            local_offset_from_top += current_top.offset
                            current_top = next(iter_top, None)
                        break

                    # compute where each current patch starts
                    start_bottom = current_bottom.start
                    start_top = (
                        current_top.start - offset_BA - local_offset_from_bottom
                    )

                    if start_bottom > end_A and start_top > end_A:
                        # No remaining overlap, go to conclusion of this merge
                        break

                    # check if any patch is ahead of the other and by how much.
                    #
                    # If one is ahead of the other, we process it first. Either
                    # by consuming it entirely if there is no overlap with the
                    # next one, or by processing the part ahead.
                    #
                    # If the two patch start at the same level, consume the one contained in the other
                    distance = start_bottom - start_top
                    if distance < 0:
                        distance *= -1
                        size_A = current_bottom.old_end - current_bottom.start
                        if size_A <= distance:
                            # "top" does not overlap with the current patch, merge wholly
                            if current_bottom.old_end > end_A:
                                end_A = current_bottom.old_end
                            local_offset_from_bottom += current_bottom.offset
                            current_bottom = next(iter_bottom, None)
                        else:
                            # truncate the start to align it with the other one
                            end_A += distance
                            local_offset_from_bottom += min(
                                0, current_bottom.size - distance
                            )
                            current_bottom = current_bottom.truncate(distance)
                    elif distance > 0:
                        size_B = current_top.old_end - current_top.start
                        if size_B <= distance:
                            # "bottom" patch does not overlap with the current patch, merge wholly
                            end = (
                                current_top.old_end
                                - offset_BA
                                - local_offset_from_bottom
                            )
                            if end > end_A:
                                end_A = end
                            local_offset_from_top += current_top.offset
                            current_top = next(iter_top, None)
                        else:
                            # truncate the current "top" patch top align it with the "bottom" one
                            end_A += distance
                            local_offset_from_top += min(
                                0, current_top.size - distance
                            )
                            current_top = current_top.truncate(distance)
                    else:
                        assert start_bottom == start_top
                        # Check if the "top" patch only replace "bottom" content
                        size_B = current_top.old_end - current_top.start
                        if size_B <= current_bottom.size:
                            # "top" only replace bottom content, adjust the
                            # final patch size and rely on "bottom" to update
                            # the `end_A` value.
                            local_offset_from_top += current_top.offset
                            current_top = next(iter_top, None)
                        else:
                            # "top" replace more that the current "bottom",
                            # truncate the part associated with that "bottom"
                            # patch and consume it.
                            local_offset_from_top += min(
                                0, current_top.size - current_bottom.size
                            )
                            current_top = current_top.truncate(
                                current_bottom.size
                            )

                            # consume the "bottom" patch
                            end_A += (
                                current_bottom.old_end - current_bottom.start
                            )
                            local_offset_from_bottom += current_bottom.offset
                            current_bottom = next(iter_bottom, None)
                size = (
                    end_A
                    - start_A
                    + local_offset_from_top
                    + local_offset_from_bottom
                )
                new_patches.append(DeltaPiece(start_A, end_A, size))
                offset_BA += local_offset_from_bottom
        return Delta(new_patches)


def estimate_combined_deltas_size(deltas: Sequence[bytes]) -> int:
    """estimate an upper bound of the size of combiner delta_low with delta_high"""
    # filter empty deltas
    deltas = [d for d in deltas if d]
    if not deltas:
        return 0
    high = Delta.from_bytes(deltas[-1])
    for low_data in deltas[-2::-1]:
        if not low_data:
            continue
        low = Delta.from_bytes(low_data)
        high = low.combine(high)
    return high.storage_size


def optimize_base(
    delta: bytes,
    next_deltas: Iterator[Tuple[int, bytes]],
    max_size: int,
) -> int | None:
    """Try to find a better base than the existing delta

    This function starts from <delta>, fold with other candidates from
    <next_deltas> until the estimated size gets over <max_size>.
    """
    if not delta:
        # if the delta is empty, we won't do better unless the rest of the
        # chain is also empty, and those should have already be optimized by
        # the search strategy
        return None
    current_delta = Delta.from_bytes(delta)
    best_base = None
    for base, data in next_deltas:
        current_delta = Delta.from_bytes(data).combine(current_delta)
        if current_delta.storage_size > max_size:
            break
        best_base = base
    return best_base
