"""
Finite state machine library, intended to be used by `greenery` only
"""

from __future__ import annotations

__all__ = (
    "Fsm",
    "StateType",
    "EPSILON",
    "NULL",
    "Charclass",
)

from dataclasses import dataclass
from typing import (
    Callable,
    ClassVar,
    Collection,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    TypeVar,
)

from .charclass import DOT, Charclass, repartition

AlphaType = Charclass
StateType = int
M = TypeVar("M")
"""Meta-state type for crawl(). Can be anything."""


def unify_alphabets(fsms: Iterable[Fsm], /) -> List[Fsm]:
    charclasses = set()
    for fsm in fsms:
        for charclass in fsm.alphabet:
            charclasses.add(charclass)

    partition = repartition(charclasses)
    # maps old Charclasses to collections of new Charclasses

    return [fsm.replace_alphabet(partition) for fsm in fsms]


# pylint: disable=too-many-public-methods,too-many-branches,fixme
@dataclass(frozen=True, init=False)
class Fsm:
    """
    A Finite State Machine or FSM has an alphabet and a set of states. At
    any given moment, the FSM is in one state. When passed a symbol from
    the alphabet, the FSM jumps to another state (or possibly the same
    state). A map (Python dictionary) indicates where to jump.
    One state is nominated as a starting state. Zero or more states are
    nominated as final states. If, after consuming a string of symbols,
    the FSM is in a final state, then it is said to "accept" the string.
    This class also has some pretty powerful methods which allow FSMs to
    be concatenated, alternated between, multiplied, looped (Kleene star
    closure), intersected, and simplified.
    The majority of these methods are available using operator overloads.
    """

    alphabet: frozenset[AlphaType]
    states: frozenset[StateType]
    initial: StateType
    finals: frozenset[StateType]
    map: Mapping[StateType, Mapping[AlphaType, StateType]]

    # noinspection PyShadowingBuiltins
    # pylint: disable=too-many-arguments
    def __init__(
        self,
        /,
        *,
        alphabet: Iterable[AlphaType],
        states: Iterable[StateType],
        initial: StateType,
        finals: Iterable[StateType],
        # pylint: disable=redefined-builtin
        map: Mapping[StateType, Mapping[AlphaType, StateType]],
    ) -> None:
        """
        `alphabet` is an iterable of symbols the FSM can be fed.
        `states` is the set of states for the FSM
        `initial` is the initial state
        `finals` is the set of accepting states
        `map` may be sparse (i.e. it may omit transitions). In the case of
        omitted transitions, a non-final "oblivion" state is simulated.
        """
        alphabet = frozenset(alphabet)
        states = frozenset(states)
        finals = frozenset(finals)

        # Validation. Thanks to immutability, this only needs to be carried out
        # once.
        if initial not in states:
            raise ValueError(f"Initial state {initial!r} must be one of {states!r}")
        if not finals.issubset(states):
            raise ValueError(f"Final states {finals!r} must be a subset of {states!r}")
        for state, state_trans in map.items():
            if state not in states:
                raise ValueError(f"Transition from unknown state {state!r}")
            for symbol, dest in state_trans.items():
                if symbol not in alphabet:
                    raise ValueError(
                        f"Invalid symbol {symbol!r}"
                        f" in transition from {state!r}"
                        f" to {dest!r}"
                    )
                if dest not in states:
                    raise ValueError(
                        f"Transition for state {state!r}"
                        f" and symbol {symbol!r}"
                        f" leads to {dest!r},"
                        " which is not a state"
                    )
        for state in states:
            if state not in map:
                raise ValueError(f"State {state!r} missing from map")
            for charclass in alphabet:
                if charclass not in map[state]:
                    raise ValueError(
                        f"Symbol {charclass!r} missing from map[{state!r}]"
                    )

        # Check that the charclasses form a proper partition of all of Unicode
        unified = Charclass()
        for charclass in alphabet:
            if unified & charclass != Charclass():
                raise ValueError(f"Alphabet {alphabet!r} has overlaps")
            unified |= charclass
        if unified != DOT:
            raise ValueError(f"Alphabet {alphabet!r} is not a proper partition")

        # Initialise the hard way due to immutability.
        object.__setattr__(self, "alphabet", alphabet)
        object.__setattr__(self, "states", states)
        object.__setattr__(self, "initial", initial)
        object.__setattr__(self, "finals", finals)
        object.__setattr__(self, "map", map)

    def accepts(self, string: str, /) -> bool:
        """
        Test whether the present FSM accepts the supplied string (iterable
        of symbols). Equivalently, consider `self` as a possibly-infinite
        set of strings and test whether `string` is a member of it. This is
        actually mainly used for unit testing purposes.
        """
        state = self.initial
        for char in string:
            for charclass in self.map[state]:
                if charclass.accepts(char):
                    state = self.map[state][charclass]
                    break
        return state in self.finals

    def __contains__(self, string: str, /) -> bool:
        """
        This lets you use the syntax `"a" in fsm1` to see whether the
        string "a" is in the set of strings accepted by `fsm1`.
        """
        return self.accepts(string)

    def reduce(self, /) -> Fsm:
        """
        A result by Brzozowski (1963) shows that a minimal finite state
        machine equivalent to the original can be obtained by reversing the
        original twice.
        """
        return self.reversed().reversed()

    def __repr__(self, /) -> str:
        args = ", ".join(
            [
                f"alphabet={self.alphabet!r}",
                f"states={self.states!r}",
                f"initial={self.initial!r}",
                f"finals={self.finals!r}",
                f"map={self.map!r}",
            ]
        )
        return f"Fsm({args})"

    # The Python `__eq__` + `__hash__` contract requires that value-equality
    # implies hash-equality. `Fsm` `__eq__` implementation currently represents
    # equality of the set of accepted strings, independent of specific state
    # labels or unused members of the alphabet. This is not trivial to hash.
    # Regarding the type suppression, see
    # https://github.com/python/mypy/issues/4266
    __hash__: ClassVar[None] = None  # type: ignore

    def __str__(self, /) -> str:
        rows = []

        sorted_alphabet = sorted(self.alphabet)

        # top row
        row = ["", "name", "final?"]
        row.extend(str(symbol) for symbol in sorted_alphabet)
        rows.append(row)

        # other rows
        for state in self.states:
            row = []
            if state == self.initial:
                row.append("*")
            else:
                row.append("")
            row.append(str(state))
            if state in self.finals:
                row.append("True")
            else:
                row.append("False")
            for symbol in sorted_alphabet:
                row.append(str(self.map[state][symbol]))
            rows.append(row)

        # column widths
        colwidths = []
        for x in range(len(rows[0])):
            colwidths.append(max(len(str(row[x])) for y, row in enumerate(rows)) + 1)

        # apply padding
        for y, row in enumerate(rows):
            for x, col in enumerate(row):
                rows[y][x] = col.ljust(colwidths[x])

        # horizontal line
        rows.insert(1, ["-" * colwidth for colwidth in colwidths])

        return "".join("".join(row) + "\n" for row in rows)

    def concatenate(*fsms: Fsm) -> Fsm:
        """
        Concatenate arbitrarily many finite state machines together.
        """
        unified_fsms = unify_alphabets(fsms)

        def connect_all(
            i: int,
            substate: StateType,
        ) -> Iterable[tuple[int, StateType]]:
            """
            Take a state in the numbered FSM and return a set containing
            it, plus (if it's final) the first state from the next FSM,
            plus (if that's final) the first state from the next but one
            FSM, plus...
            """
            result = {(i, substate)}
            while i < len(unified_fsms) - 1 and substate in unified_fsms[i].finals:
                i += 1
                substate = unified_fsms[i].initial
                result.add((i, substate))
            return result

        # Use a superset containing states from all FSMs at once.
        # We start at the start of the first FSM. If this state is final in the
        # first FSM, then we are also at the start of the second FSM. And so
        # on.
        initial = frozenset(
            connect_all(0, unified_fsms[0].initial) if unified_fsms else ()
        )

        def final(state: frozenset[tuple[int, StateType]]) -> bool:
            """If you're in a final state of the final FSM, it's final"""
            return any(
                i == len(unified_fsms) - 1 and substate in unified_fsms[i].finals
                for i, substate in state
            )

        def follow(
            current: frozenset[tuple[int, StateType]],
            symbol: AlphaType,
        ) -> frozenset[tuple[int, StateType]]:
            """
            Follow the collection of states through all FSMs at once,
            jumping to the next FSM if we reach the end of the current one
            """
            next_metastate: set[tuple[int, StateType]] = set()
            for i, substate in current:
                next_metastate.update(
                    connect_all(i, unified_fsms[i].map[substate][symbol])
                )

            return frozenset(next_metastate)

        alphabet = unified_fsms[0].alphabet if len(unified_fsms) > 0 else {~Charclass()}

        return crawl(alphabet, initial, final, follow).reduce()

    def __add__(self, other: Fsm, /) -> Fsm:
        """
        Concatenate two finite state machines together.
        For example, if self accepts "0*" and other accepts "1+(0|1)",
        will return a finite state machine accepting "0*1+(0|1)".
        Accomplished by effectively following non-deterministically.
        Call using "fsm3 = fsm1 + fsm2"
        """
        return self.concatenate(other)

    def star(self, /) -> Fsm:
        """
        If the present FSM accepts X, returns an FSM accepting X* (i.e. 0
        or more Xes). This is NOT as simple as naively connecting the final
        states back to the initial state: see (b*ab)* for example.
        """
        alphabet = self.alphabet

        initial: Collection[StateType] = {self.initial}

        def follow(
            state: Collection[StateType],
            symbol: AlphaType,
        ) -> Collection[StateType]:
            next_states = set()

            for substate in state:
                next_states.add(self.map[substate][symbol])

                # If one of our substates is final, then we can also consider
                # transitions from the initial state of the original FSM.
                if substate in self.finals:
                    next_states.add(self.map[self.initial][symbol])

            return frozenset(next_states)

        def final(state: Collection[StateType]) -> bool:
            return any(substate in self.finals for substate in state)

        return crawl(alphabet, initial, final, follow) | EPSILON

    def times(self, multiplier: int, /) -> Fsm:
        """
        Given an FSM and a multiplier, return the multiplied FSM.
        """
        if multiplier < 0:
            raise ArithmeticError(f"Can't multiply an FSM by {multiplier!r}")

        alphabet = self.alphabet

        # metastate is a set of iterations+states
        initial: Collection[tuple[StateType, int]] = {(self.initial, 0)}

        def final(state: Collection[tuple[StateType, int]]) -> bool:
            """
            If the initial state is final then multiplying doesn't alter
            that
            """
            return any(
                substate == self.initial
                and (self.initial in self.finals or iteration == multiplier)
                for substate, iteration in state
            )

        def follow(
            current: Collection[tuple[StateType, int]],
            symbol: AlphaType,
        ) -> Collection[tuple[StateType, int]]:
            next_metastate = []
            for substate, iteration in current:
                if iteration < multiplier:
                    next_metastate.append((self.map[substate][symbol], iteration))
                    # final of self? merge with initial on next iteration
                    if self.map[substate][symbol] in self.finals:
                        next_metastate.append((self.initial, iteration + 1))
            return frozenset(next_metastate)

        return crawl(alphabet, initial, final, follow).reduce()

    def __mul__(self, multiplier: int, /) -> Fsm:
        """
        Given an FSM and a multiplier, return the multiplied FSM.
        """
        return self.times(multiplier)

    def union(*fsms: Fsm) -> Fsm:
        """
        Treat `fsms` as a collection of arbitrary FSMs and return the union
        FSM. Can be used as `fsm1.union(fsm2, ...)` or
        `fsm.union(fsm1, ...)`. `fsms` may be empty.
        """
        return parallel(fsms, any)

    def __or__(self, other: Fsm, /) -> Fsm:
        """
        Alternation.
        Return a finite state machine which accepts any sequence of symbols
        that is accepted by either self or other. Note that the set of
        strings recognised by the two FSMs undergoes a set union.
        Call using "fsm3 = fsm1 | fsm2"
        """
        return self.union(other)

    def intersection(*fsms: Fsm) -> Fsm:
        """
        Intersection.
        Take FSMs and AND them together. That is, return an FSM which
        accepts any sequence of symbols that is accepted by both of the
        original FSMs. Note that the set of strings recognised by the two
        FSMs undergoes a set intersection operation.
        Call using "fsm3 = fsm1 & fsm2"
        """
        return parallel(fsms, all)

    def __and__(self, other: Fsm, /) -> Fsm:
        """
        Treat the FSMs as sets of strings and return the intersection of
        those sets in the form of a new FSM.
        """
        return self.intersection(other)

    def symmetric_difference(*fsms: Fsm) -> Fsm:
        """
        Treat `fsms` as a collection of sets of strings and compute the
        symmetric difference of them all. The python set method only allows
        two sets to be operated on at once, but we go the extra mile since
        it's not too hard.
        """
        return parallel(fsms, lambda accepts: (accepts.count(True) % 2) == 1)

    def __xor__(self, other: Fsm, /) -> Fsm:
        """
        Symmetric difference. Returns an FSM which recognises only the
        strings recognised by `self` or `other` but not both.
        """
        return self.symmetric_difference(other)

    def everythingbut(self, /) -> Fsm:
        """
        Return a finite state machine which will accept any string NOT
        accepted by self, and will not accept any string accepted by self.
        """
        alphabet = self.alphabet
        initial = self.initial

        def follow(
            current: StateType,
            symbol: AlphaType,
        ) -> StateType:
            return self.map[current][symbol]

        # state is final unless the original was
        def final(state: StateType) -> bool:
            return state not in self.finals

        return crawl(alphabet, initial, final, follow).reduce()

    def reversed(self, /) -> Fsm:
        """
        Return a new FSM such that for every string that self accepts (e.g.
        "beer", the new FSM accepts the reversed string ("reeb").
        """
        alphabet = self.alphabet

        # Start from a composite "state-set" consisting of all final states.
        # If there are no final states, this set is empty and we'll find that
        # no other states get generated.
        initial = frozenset(self.finals)

        # Find every possible way to reach the current state-set
        # using this symbol.
        def follow(
            current: frozenset[StateType],
            symbol: AlphaType,
        ) -> frozenset[StateType]:
            next_states = frozenset(
                [
                    prev
                    for prev in self.map
                    for state in current
                    if self.map[prev][symbol] == state
                ]
            )
            return next_states

        # A state-set is final if the initial state is in it.
        def final(state: frozenset[StateType]) -> bool:
            return self.initial in state

        # Man, crawl() is the best!
        return crawl(alphabet, initial, final, follow)
        # Do not reduce() the result, since reduce() calls us in turn

    def islive(self, /, state: StateType) -> bool:
        """A state is "live" if a final state can be reached from it."""
        reachable = [state]
        i = 0
        while i < len(reachable):
            current = reachable[i]
            if current in self.finals:
                return True
            for symbol in self.map[current]:
                next_state = self.map[current][symbol]
                if next_state not in reachable:
                    reachable.append(next_state)
            i += 1
        return False

    def empty(self, /) -> bool:
        """
        An FSM is empty if it recognises no strings. An FSM may be
        arbitrarily complicated and have arbitrarily many final states
        while still recognising no strings because those final states may
        all be inaccessible from the initial state. Equally, an FSM may be
        non-empty despite having an empty alphabet if the initial state is
        final.
        """
        return not self.islive(self.initial)

    def strings(self, otherchars: Iterable[str]) -> Iterator[str]:
        """
        Generate strings that this FSM accepts. Note that for our purposes a
        string is a sequence of Unicode characters, NOT a list of Charclasses.

        Since
        there may be infinitely many of these we use a generator instead of
        constructing a static list. Strings will be sorted in order of
        length and then lexically. This procedure uses arbitrary amounts of
        memory but is very fast. There may be more efficient ways to do
        this, that I haven't investigated yet. You can use this in list
        comprehensions.
        """

        # Most FSMs have at least one "dead state".
        # Once you reach a dead state, you can no
        # longer reach a final state. Since many strings may end up here, it's
        # advantageous to constrain our search to live states only.
        livestates = set(state for state in self.states if self.islive(state))

        # We store a list of tuples. Each tuple consists of an input string and
        # the state that this input string leads to. This means we don't have
        # to run the state machine from the very beginning every time we want
        # to check a new string.
        strings: list[tuple[str, StateType]] = []

        # Initial entry (or possibly not, in which case this is a short one)
        cstate: StateType = self.initial
        cstring: str = ""
        if cstate in livestates:
            if cstate in self.finals:
                yield cstring
            strings.append((cstring, cstate))

        # Fixed point calculation
        i = 0
        while i < len(strings):
            cstring, cstate = strings[i]

            for charclass in sorted(self.map[cstate]):
                # TODO: scrap otherchars as a concept?
                chars = otherchars if charclass.negated else charclass.get_chars()
                for char in chars:
                    nstate = self.map[cstate][charclass]
                    nstring = cstring + char
                    if nstate in livestates:
                        if nstate in self.finals:
                            yield nstring
                        strings.append((nstring, nstate))
            i += 1

    def __iter__(self, /) -> Iterator[str]:
        """
        This allows you to do `for string in fsm1` as a list comprehension!
        """
        return self.strings([])

    def equivalent(self, other: Fsm, /) -> bool:
        """
        Two FSMs are considered equivalent if they recognise the same
        strings. Or, to put it another way, if their symmetric difference
        recognises no strings.
        """
        return (self ^ other).empty()

    def __eq__(self, other: object, /) -> bool:
        """
        You can use `fsm1 == fsm2` to determine whether two FSMs recognise
        the same strings.
        """
        if not isinstance(other, Fsm):
            return NotImplemented
        return self.equivalent(other)

    def different(self, other: Fsm, /) -> bool:
        """
        Two FSMs are considered different if they have a non-empty
        symmetric difference.
        """
        return not (self ^ other).empty()

    def __ne__(self, other: object, /) -> bool:
        """
        Use `fsm1 != fsm2` to determine whether two FSMs recognise
        different strings.
        """
        return not self == other

    def difference(*fsms: Fsm) -> Fsm:
        """
        Difference. Returns an FSM which recognises only the strings
        recognised by the first FSM in the list, but none of the others.
        """
        return parallel(fsms, lambda accepts: accepts[0] and not any(accepts[1:]))

    def __sub__(self, other: Fsm, /) -> Fsm:
        return self.difference(other)

    def cardinality(self, /) -> int:
        """
        Consider the FSM as a set of strings and return the cardinality of
        that set, or raise an OverflowError if there are infinitely many
        """
        num_strings: dict[StateType, int | None] = {}

        def get_num_strings(state: StateType) -> int:
            # Most FSMs have at least one oblivion state
            if self.islive(state):
                if state in num_strings:
                    if num_strings[state] is None:  # "computing..."
                        # Recursion! There are infinitely many strings
                        # recognised
                        raise OverflowError(state)
                    return num_strings[state]  # type: ignore

                num_strings[state] = None  # i.e. "computing..."
                n = 0
                for charclass in self.map[state]:
                    num_transitions = charclass.num_chars()
                    nstate = self.map[state][charclass]
                    if nstate in self.finals:
                        n += num_transitions
                    n += num_transitions * get_num_strings(nstate)
                num_strings[state] = n

            else:
                # Dead state
                num_strings[state] = 0

            return num_strings[state]  # type: ignore

        n = 1 if self.initial in self.finals else 0
        return n + get_num_strings(self.initial)

    def __len__(self, /) -> int:
        """
        Consider the FSM as a set of strings and return the cardinality of
        that set, or raise an OverflowError if there are infinitely many
        """
        return self.cardinality()

    def isdisjoint(self, other: Fsm, /) -> bool:
        """
        Treat `self` and `other` as sets of strings and see if they are
        disjoint
        """
        return (self & other).empty()

    def issubset(self, other: Fsm, /) -> bool:
        """
        Treat `self` and `other` as sets of strings and see if `self` is a
        subset of `other`... `self` recognises no strings which `other`
        doesn't.
        """
        return (self - other).empty()

    def __le__(self, other: Fsm, /) -> bool:
        """
        Treat `self` and `other` as sets of strings and see if `self` is a
        subset of `other`... `self` recognises no strings which `other`
        doesn't.
        """
        return self.issubset(other)

    def ispropersubset(self, other: Fsm, /) -> bool:
        """
        Treat `self` and `other` as sets of strings and see if `self` is a
        proper subset of `other`.
        """
        return self <= other and self != other

    def __lt__(self, other: Fsm, /) -> bool:
        """
        Treat `self` and `other` as sets of strings and see if `self` is a
        strict subset of `other`.
        """
        return self.ispropersubset(other)

    def issuperset(self, other: Fsm, /) -> bool:
        """
        Treat `self` and `other` as sets of strings and see if `self` is a
        superset of `other`.
        """
        return (other - self).empty()

    def __ge__(self, other: Fsm, /) -> bool:
        """
        Treat `self` and `other` as sets of strings and see if `self` is a
        superset of `other`.
        """
        return self.issuperset(other)

    def ispropersuperset(self, other: Fsm, /) -> bool:
        """
        Treat `self` and `other` as sets of strings and see if `self` is a
        proper superset of `other`.
        """
        return self >= other and self != other

    def __gt__(self, other: Fsm, /) -> bool:
        """
        Treat `self` and `other` as sets of strings and see if `self` is a
        strict superset of `other`.
        """
        return self.ispropersuperset(other)

    def copy(self, /) -> Fsm:
        """
        For completeness only, since `set.copy()` and `frozenset.copy()` exist.
        FSM objects are immutable; like `frozenset`, this just returns `self`.
        """
        return self

    __copy__ = copy

    def derive(self, string: str, /) -> Fsm:
        """
        Compute the Brzozowski derivative of this FSM with respect to the
        input string. Note that the FSM uses Charclasses as symbols internally,
        but the input string is a sequence of Unicode characters
        <https://en.wikipedia.org/wiki/Brzozowski_derivative>
        """
        # Consume the input string.
        state = self.initial
        for char in string:
            for charclass in self.map[state]:
                if charclass.accepts(char):
                    state = self.map[state][charclass]
                    break

        # OK so now we have consumed that string, use the new location as
        # the starting point.
        return Fsm(
            alphabet=self.alphabet,
            states=self.states,
            initial=state,
            finals=self.finals,
            map=self.map,
        )

    def replace_alphabet(
        self, replacements: Mapping[AlphaType, Iterable[AlphaType]]
    ) -> Fsm:
        """
        Returns a new FSM which uses a different alphabet. If one original
        symbol converts to two new symbols, there will be multiple identical
        transitions; if none, the transitions will be omitted.
        """
        new_alphabet = set()
        for symbol in self.alphabet:
            for replacement in replacements[symbol]:
                new_alphabet.add(replacement)

        new_map: Dict[StateType, Dict[AlphaType, StateType]] = {}
        for state in self.map:
            new_map[state] = {}
            for symbol in self.alphabet:
                for replacement in replacements[symbol]:
                    new_map[state][replacement] = self.map[state][symbol]

        return Fsm(
            alphabet=new_alphabet,
            states=self.states,
            initial=self.initial,
            finals=self.finals,
            map=new_map,
        )


NULL = Fsm(
    alphabet={~Charclass()},
    states={0},
    initial=0,
    finals=(),
    map={
        0: {~Charclass(): 0},
    },
)
"""
An FSM accepting nothing (not even the empty string). This is
demonstrates that this is possible, and is also extremely useful
in some situations
"""

EPSILON = Fsm(
    alphabet={~Charclass()},
    states={0, 1},
    initial=0,
    finals={0},
    map={
        0: {~Charclass(): 1},
        1: {~Charclass(): 1},
    },
)
"""
An FSM matching an empty string, "", only.
This is very useful in many situations
"""


def parallel(
    fsms: tuple[Fsm, ...],
    test: Callable[[list[bool]], bool],
    /,
) -> Fsm:
    """
    Crawl several FSMs in parallel, mapping the states of a larger
    meta-FSM. To determine whether a state in the larger FSM is final, pass
    all of the finality statuses (e.g. [True, False, False] to `test`.
    """
    unified_fsms = unify_alphabets(fsms)

    initial: Mapping[int, StateType] = {
        i: fsm.initial for i, fsm in enumerate(unified_fsms)
    }

    # dedicated function accepts a "superset" and returns the next "superset"
    # obtained by following this transition in the new FSM
    def follow(
        current: Mapping[int, StateType],
        symbol: AlphaType,
    ) -> Mapping[int, StateType]:
        return {i: fsm.map[current[i]][symbol] for i, fsm in enumerate(unified_fsms)}

    # Determine the "is final?" condition of each substate, then pass it to the
    # test to determine finality of the overall FSM.
    def final(state: Mapping[int, StateType]) -> bool:
        return test([state[i] in fsm.finals for i, fsm in enumerate(unified_fsms)])

    alphabet = unified_fsms[0].alphabet if len(unified_fsms) > 0 else {~Charclass()}

    return crawl(alphabet, initial, final, follow).reduce()


def crawl(
    alphabet: Iterable[AlphaType],
    initial: M,
    final: Callable[[M], bool],
    follow: Callable[[M, AlphaType], M],
) -> Fsm:
    """
    Given the above conditions and instructions, crawl a new unknown FSM,
    mapping its states, final states and transitions. Return the new FSM.
    This is a pretty powerful procedure which could potentially go on
    forever if you supply an evil version of follow().
    """

    states: list[M] = [initial]
    finals: set[StateType] = set()
    transitions: dict[StateType, dict[AlphaType, StateType]] = {}

    # iterate over a growing list
    i = 0
    while i < len(states):
        state = states[i]

        # add to finals
        if final(state):
            finals.add(i)

        # compute map for this state
        transitions[i] = {}
        for symbol in sorted(alphabet):
            next_state = follow(state, symbol)

            try:
                j = states.index(next_state)
            except ValueError:
                j = len(states)
                states.append(next_state)

            transitions[i][symbol] = j

        i += 1

    return Fsm(
        alphabet=alphabet,
        states=set(range(len(states))),
        initial=0,
        finals=finals,
        map=transitions,
    )


def from_charclass(charclass: Charclass) -> Fsm:
    # 0 is initial, 1 is final, 2 is dead
    return Fsm(
        alphabet={charclass, ~charclass},
        states={0, 1, 2},
        initial=0,
        finals={1},
        map={
            0: {charclass: 1, ~charclass: 2},
            1: {charclass: 2, ~charclass: 2},
            2: {charclass: 2, ~charclass: 2},
        },
    )
