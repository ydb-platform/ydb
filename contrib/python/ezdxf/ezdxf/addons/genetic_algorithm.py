#  Copyright (c) 2022, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import (
    Sequence,
    Iterable,
    Optional,
    Callable,
    Union,
)
import abc
import copy
from dataclasses import dataclass
import json
import random
import time

# example usage:
# examples\addons\optimize\bin_packing_forms.py
# examples\addons\optimize\tsp.py


class DNA(abc.ABC):
    """Abstract DNA class."""

    fitness: Optional[float] = None
    _data: list

    @abc.abstractmethod
    def reset(self, values: Iterable):
        ...

    @property
    @abc.abstractmethod
    def is_valid(self) -> bool:
        ...

    def copy(self):
        return copy.deepcopy(self)

    def _taint(self):
        self.fitness = None

    def __repr__(self):
        return f"{self.__class__.__name__}({str(self._data)})"

    def __eq__(self, other):
        assert isinstance(other, self.__class__)
        return self._data == other._data

    def __len__(self):
        return len(self._data)

    def __getitem__(self, item):
        return self._data.__getitem__(item)

    def __setitem__(self, key, value):
        self._data.__setitem__(key, value)
        self._taint()

    def __iter__(self):
        return iter(self._data)

    @abc.abstractmethod
    def flip_mutate_at(self, index: int) -> None:
        ...


def dna_fitness(dna: DNA) -> float:
    return dna.fitness  # type: ignore


class Mutate(abc.ABC):
    """Abstract mutation."""

    @abc.abstractmethod
    def mutate(self, dna: DNA, rate: float):
        ...


class FlipMutate(Mutate):
    """Flip one bit mutation."""

    def mutate(self, dna: DNA, rate: float):
        for index in range(len(dna)):
            if random.random() < rate:
                dna.flip_mutate_at(index)


class NeighborSwapMutate(Mutate):
    """Swap two neighbors mutation."""

    def mutate(self, dna: DNA, rate: float):
        for index in range(len(dna)):
            if random.random() < rate:
                i2 = index - 1
                tmp = dna[i2]
                dna[i2] = dna[index]
                dna[index] = tmp


class RandomSwapMutate(Mutate):
    """Swap two random places mutation."""

    def mutate(self, dna: DNA, rate: float):
        length = len(dna)
        for index in range(length):
            if random.random() < rate:
                i2 = random.randrange(0, length)
                if i2 == index:
                    i2 -= 1
                tmp = dna[i2]
                dna[i2] = dna[index]
                dna[index] = tmp


class ReverseMutate(Mutate):
    """Reverse some consecutive bits mutation."""

    def __init__(self, bits: int = 3):
        self._bits = int(max(bits, 1))

    def mutate(self, dna: DNA, rate: float):
        length = len(dna)
        if random.random() < rate * (
            length / self._bits
        ):  # applied to all bits at ones
            i1 = random.randrange(length - self._bits)
            i2 = i1 + self._bits
            bits = dna[i1:i2]
            dna[i1:i2] = reversed(bits)


class ScrambleMutate(Mutate):
    """Scramble some consecutive bits mutation."""

    def __init__(self, bits: int = 3):
        self._bits = int(max(bits, 1))

    def mutate(self, dna: DNA, rate: float):
        length = len(dna)
        if random.random() < rate * (
            length / self._bits
        ):  # applied to all bits at ones
            i1 = random.randrange(length - self._bits)
            i2 = i1 + self._bits
            bits = dna[i1:i2]
            random.shuffle(bits)
            dna[i1:i2] = bits


class Mate(abc.ABC):
    """Abstract recombination."""

    @abc.abstractmethod
    def recombine(self, dna1: DNA, dna2: DNA):
        pass


class Mate1pCX(Mate):
    """One point crossover recombination."""

    def recombine(self, dna1: DNA, dna2: DNA):
        length = len(dna1)
        index = random.randrange(0, length)
        recombine_dna_2pcx(dna1, dna2, index, length)


class Mate2pCX(Mate):
    """Two point crossover recombination."""

    def recombine(self, dna1: DNA, dna2: DNA):
        length = len(dna1)
        i1 = random.randrange(0, length)
        i2 = random.randrange(0, length)
        if i1 > i2:
            i1, i2 = i2, i1
        recombine_dna_2pcx(dna1, dna2, i1, i2)


class MateUniformCX(Mate):
    """Uniform recombination."""

    def recombine(self, dna1: DNA, dna2: DNA):
        for index in range(len(dna1)):
            if random.random() > 0.5:
                tmp = dna1[index]
                dna1[index] = dna2[index]
                dna2[index] = tmp


class MateOrderedCX(Mate):
    """Recombination class for ordered DNA like UniqueIntDNA()."""

    def recombine(self, dna1: DNA, dna2: DNA):
        length = len(dna1)
        i1 = random.randrange(0, length)
        i2 = random.randrange(0, length)
        if i1 > i2:
            i1, i2 = i2, i1
        recombine_dna_ocx1(dna1, dna2, i1, i2)


def recombine_dna_2pcx(dna1: DNA, dna2: DNA, i1: int, i2: int) -> None:
    """Two point crossover."""
    part1 = dna1[i1:i2]
    part2 = dna2[i1:i2]
    dna1[i1:i2] = part2
    dna2[i1:i2] = part1


def recombine_dna_ocx1(dna1: DNA, dna2: DNA, i1: int, i2: int) -> None:
    """Ordered crossover."""
    copy1 = dna1.copy()
    replace_dna_ocx1(dna1, dna2, i1, i2)
    replace_dna_ocx1(dna2, copy1, i1, i2)


def replace_dna_ocx1(dna1: DNA, dna2: DNA, i1: int, i2: int) -> None:
    """Replace a part in dna1 by dna2 and preserve order of remaining values in
    dna1.
    """
    old = dna1.copy()
    new = dna2[i1:i2]
    dna1[i1:i2] = new
    index = 0
    new_set = set(new)
    for value in old:
        if value in new_set:
            continue
        if index == i1:
            index = i2
        dna1[index] = value
        index += 1


class FloatDNA(DNA):
    """Arbitrary float numbers in the range [0, 1]."""

    __slots__ = ("_data", "fitness")

    def __init__(self, values: Iterable[float]):
        self._data: list[float] = list(values)
        self._check_valid_data()
        self.fitness: Optional[float] = None

    @classmethod
    def random(cls, length: int) -> FloatDNA:
        return cls((random.random() for _ in range(length)))

    @classmethod
    def n_random(cls, n: int, length: int) -> list[FloatDNA]:
        return [cls.random(length) for _ in range(n)]

    @property
    def is_valid(self) -> bool:
        return all(0.0 <= v <= 1.0 for v in self._data)

    def _check_valid_data(self):
        if not self.is_valid:
            raise ValueError("data value out of range")

    def __str__(self):
        if self.fitness is None:
            fitness = ", fitness=None"
        else:
            fitness = f", fitness={self.fitness:.4f}"
        return f"{str([round(v, 4) for v in self._data])}{fitness}"

    def reset(self, values: Iterable[float]):
        self._data = list(values)
        self._check_valid_data()
        self._taint()

    def flip_mutate_at(self, index: int) -> None:
        self._data[index] = 1.0 - self._data[index]  # flip pick location


class BitDNA(DNA):
    """One bit DNA."""

    __slots__ = ("_data", "fitness")

    def __init__(self, values: Iterable):
        self._data: list[bool] = list(bool(v) for v in values)
        self.fitness: Optional[float] = None

    @property
    def is_valid(self) -> bool:
        return True  # everything can be evaluated to True/False

    @classmethod
    def random(cls, length: int) -> BitDNA:
        return cls(bool(random.randint(0, 1)) for _ in range(length))

    @classmethod
    def n_random(cls, n: int, length: int) -> list[BitDNA]:
        return [cls.random(length) for _ in range(n)]

    def __str__(self):
        if self.fitness is None:
            fitness = ", fitness=None"
        else:
            fitness = f", fitness={self.fitness:.4f}"
        return f"{str([int(v) for v in self._data])}{fitness}"

    def reset(self, values: Iterable) -> None:
        self._data = list(bool(v) for v in values)
        self._taint()

    def flip_mutate_at(self, index: int) -> None:
        self._data[index] = not self._data[index]


class UniqueIntDNA(DNA):
    """Unique integer values in the range from 0 to length-1.
    E.g. UniqueIntDNA(10) = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

    Requires MateOrderedCX() as recombination class to preserve order and
    validity after DNA recombination.

    Requires mutation by swapping like SwapRandom(), SwapNeighbors(),
    ReversMutate() or ScrambleMutate()
    """

    __slots__ = ("_data", "fitness")

    def __init__(self, values: Union[int, Iterable]):
        self._data: list[int]
        if isinstance(values, int):
            self._data = list(range(values))
        else:
            self._data = [int(v) for v in values]
            if not self.is_valid:
                raise TypeError(self._data)
        self.fitness: Optional[float] = None

    @classmethod
    def random(cls, length: int) -> UniqueIntDNA:
        dna = cls(length)
        random.shuffle(dna._data)
        return dna

    @classmethod
    def n_random(cls, n: int, length: int) -> list[UniqueIntDNA]:
        return [cls.random(length) for _ in range(n)]

    @property
    def is_valid(self) -> bool:
        return len(set(self._data)) == len(self._data)

    def __str__(self):
        if self.fitness is None:
            fitness = ", fitness=None"
        else:
            fitness = f", fitness={self.fitness:.4f}"
        return f"{str([int(v) for v in self._data])}{fitness}"

    def reset(self, values: Iterable) -> None:
        self._data = list(int(v) for v in values)
        self._taint()

    def flip_mutate_at(self, index: int) -> None:
        raise TypeError("flip mutation not supported")


class IntegerDNA(DNA):
    """Integer values in the range from 0 to max_ - 1.
    E.g. IntegerDNA([0, 1, 2, 3, 4, 0, 1, 2, 3, 4], 5)
    """

    __slots__ = ("_data", "fitness")

    def __init__(self, values: Iterable[int], max_: int):
        self._max = int(max_)
        self._data: list[int] = list(values)
        if not self.is_valid:
            raise TypeError(self._data)
        self.fitness: Optional[float] = None

    @classmethod
    def random(cls, length: int, max_: int) -> IntegerDNA:
        imax = int(max_)
        return cls((random.randrange(0, imax) for _ in range(length)), imax)

    @classmethod
    def n_random(cls, n: int, length: int, max_: int) -> list[IntegerDNA]:
        return [cls.random(length, max_) for _ in range(n)]

    @property
    def is_valid(self) -> bool:
        return all(0 <= v < self._max for v in self._data)

    def __repr__(self):
        return f"{self.__class__.__name__}({str(self._data)}, {self._max})"

    def __str__(self):
        if self.fitness is None:
            fitness = ", fitness=None"
        else:
            fitness = f", fitness={self.fitness:.4f}"
        return f"{str([int(v) for v in self._data])}{fitness}"

    def reset(self, values: Iterable) -> None:
        self._data = list(int(v) for v in values)
        self._taint()

    def flip_mutate_at(self, index: int) -> None:
        self._data[index] = self._max - self._data[index] - 1


class Selection(abc.ABC):
    """Abstract selection class."""

    @abc.abstractmethod
    def pick(self, count: int) -> Iterable[DNA]:
        ...

    @abc.abstractmethod
    def reset(self, candidates: Iterable[DNA]):
        ...


class Evaluator(abc.ABC):
    """Abstract evaluation class."""

    @abc.abstractmethod
    def evaluate(self, dna: DNA) -> float:
        ...


class Log:
    @dataclass
    class Entry:
        runtime: float
        fitness: float
        avg_fitness: float

    def __init__(self) -> None:
        self.entries: list[Log.Entry] = []

    def add(self, runtime: float, fitness: float, avg_fitness: float) -> None:
        self.entries.append(Log.Entry(runtime, fitness, avg_fitness))

    def dump(self, filename: str) -> None:
        data = [(e.runtime, e.fitness, e.avg_fitness) for e in self.entries]
        with open(filename, "wt") as fp:
            json.dump(data, fp, indent=4)

    @classmethod
    def load(cls, filename: str) -> Log:
        with open(filename, "rt") as fp:
            data = json.load(fp)
        log = Log()
        for runtime, fitness, avg_fitness in data:
            log.add(runtime, fitness, avg_fitness)
        return log


class HallOfFame:
    def __init__(self, count):
        self.count = count
        self._unique_entries = dict()

    def __iter__(self):
        return (
            self._unique_entries[k] for k in self._sorted_keys()[: self.count]
        )

    def _sorted_keys(self):
        return sorted(self._unique_entries.keys(), reverse=True)

    def add(self, dna: DNA):
        assert dna.fitness is not None
        self._unique_entries[dna.fitness] = dna

    def get(self, count: int) -> list[DNA]:
        entries = self._unique_entries
        keys = self._sorted_keys()
        return [entries[k] for k in keys[: min(count, self.count)]]

    def purge(self):
        if len(self._unique_entries) <= self.count:
            return
        entries = self._unique_entries
        keys = self._sorted_keys()
        self._unique_entries = {k: entries[k] for k in keys[: self.count]}


def threshold_filter(
    candidates: Sequence[DNA], best_fitness: float, threshold: float
) -> Iterable[DNA]:
    if best_fitness <= 0.0:
        minimum = min(candidates, key=dna_fitness)
        min_value = (1.0 - threshold) * minimum.fitness  # type: ignore
    else:
        min_value = best_fitness * threshold
    return (c for c in candidates if c.fitness > min_value)  # type: ignore


class GeneticOptimizer:
    """A genetic algorithm (GA) is a meta-heuristic inspired by the process of
    natural selection. Genetic algorithms are commonly used to generate
    high-quality solutions to optimization and search problems by relying on
    biologically inspired operators such as mutation, crossover and selection.

    Source: https://en.wikipedia.org/wiki/Genetic_algorithm

    This implementation searches always for the maximum fitness, fitness
    comparisons are always done by the "greater than" operator (">").
    The algorithm supports negative values to search for the minimum fitness
    (e.g. Travelling Salesmen Problem: -900 > -1000). Reset the start fitness
    by the method :meth:`reset_fitness` accordingly::

        optimizer.reset_fitness(-1e99)

    """

    def __init__(
        self,
        evaluator: Evaluator,
        max_generations: int,
        max_fitness: float = 1.0,
    ):
        if max_generations < 1:
            raise ValueError("requires max_generations > 0")
        # data:
        self.name = "GeneticOptimizer"
        self.log = Log()
        self.candidates: list[DNA] = []

        # core components:
        self.evaluator: Evaluator = evaluator
        self.selection: Selection = RouletteSelection()
        self.mate: Mate = Mate2pCX()
        self.mutation = FlipMutate()

        # options:
        self.max_generations = int(max_generations)
        self.max_fitness: float = float(max_fitness)
        self.max_runtime: float = 1e99
        self.max_stagnation = 100
        self.crossover_rate = 0.70
        self.mutation_rate = 0.01
        self.elitism: int = 2
        # percentage (0.1 = 10%) of candidates with least fitness to ignore in
        # next generation
        self.threshold: float = 0.0

        # state of last (current) generation:
        self.generation: int = 0
        self.start_time = 0.0
        self.runtime: float = 0.0
        self.best_dna: DNA = BitDNA([])
        self.best_fitness: float = 0.0
        self.stagnation: int = 0  # generations without improvement
        self.hall_of_fame = HallOfFame(10)

    def reset_fitness(self, value: float) -> None:
        self.best_fitness = float(value)

    @property
    def is_executed(self) -> bool:
        return bool(self.generation)

    @property
    def count(self) -> int:
        return len(self.candidates)

    def add_candidates(self, dna: Iterable[DNA]):
        if not self.is_executed:
            self.candidates.extend(dna)
        else:
            raise TypeError("already executed")

    def execute(
        self,
        feedback: Optional[Callable[[GeneticOptimizer], bool]] = None,
        interval: float = 1.0,
    ) -> None:
        if self.is_executed:
            raise TypeError("can only run once")
        if not self.candidates:
            print("no DNA defined!")
        t0 = time.perf_counter()
        self.start_time = t0
        for self.generation in range(1, self.max_generations + 1):
            self.measure_fitness()
            t1 = time.perf_counter()
            self.runtime = t1 - self.start_time
            if (
                self.best_fitness >= self.max_fitness
                or self.runtime >= self.max_runtime
                or self.stagnation >= self.max_stagnation
            ):
                break
            if feedback and t1 - t0 > interval:
                if feedback(self):  # stop if feedback() returns True
                    break
                t0 = t1
            self.next_generation()

    def measure_fitness(self) -> None:
        self.stagnation += 1
        fitness_sum: float = 0.0
        for dna in self.candidates:
            if dna.fitness is not None:
                fitness_sum += dna.fitness
                continue
            fitness = self.evaluator.evaluate(dna)
            dna.fitness = fitness
            fitness_sum += fitness
            self.hall_of_fame.add(dna)
            if fitness > self.best_fitness:
                self.best_fitness = fitness
                self.best_dna = dna
                self.stagnation = 0

        self.hall_of_fame.purge()
        try:
            avg_fitness = fitness_sum / len(self.candidates)
        except ZeroDivisionError:
            avg_fitness = 0.0
        self.log.add(
            time.perf_counter() - self.start_time,
            self.best_fitness,
            avg_fitness,
        )

    def next_generation(self) -> None:
        count = len(self.candidates)
        candidates: list[DNA] = []
        selector = self.selection
        selector.reset(self.filter_threshold(self.candidates))

        if self.elitism > 0:
            candidates.extend(self.hall_of_fame.get(self.elitism))

        while len(candidates) < count:
            dna1, dna2 = selector.pick(2)
            dna1 = dna1.copy()
            dna2 = dna2.copy()
            self.recombine(dna1, dna2)
            self.mutate(dna1, dna2)
            candidates.append(dna1)
            candidates.append(dna2)
        self.candidates = candidates

    def filter_threshold(self, candidates: Sequence[DNA]) -> Iterable[DNA]:
        if self.threshold > 0.0:
            return threshold_filter(
                candidates, self.best_fitness, self.threshold
            )
        else:
            return candidates

    def recombine(self, dna1: DNA, dna2: DNA):
        if random.random() < self.crossover_rate:
            self.mate.recombine(dna1, dna2)

    def mutate(self, dna1: DNA, dna2: DNA):
        self.mutation.mutate(dna1, self.mutation_rate)
        self.mutation.mutate(dna2, self.mutation_rate)


def conv_negative_weights(weights: Iterable[float]) -> Iterable[float]:
    # random.choices does not accept negative values: -100 -> 1/100, -10 -> 1/10
    return (0.0 if w == 0.0 else 1.0 / abs(w) for w in weights)


class RouletteSelection(Selection):
    """Selection by fitness values."""

    def __init__(self, negative_values: bool = False) -> None:
        self._candidates: list[DNA] = []
        self._weights: list[float] = []
        self._negative_values = bool(negative_values)

    def reset(self, candidates: Iterable[DNA]):
        # dna.fitness is not None here!
        self._candidates = list(candidates)
        if self._negative_values:
            self._weights = list(
                conv_negative_weights(dna.fitness for dna in self._candidates)  # type: ignore
            )
        else:
            self._weights = [dna.fitness for dna in self._candidates]  # type: ignore

    def pick(self, count: int) -> Iterable[DNA]:
        return random.choices(self._candidates, self._weights, k=count)


class RankBasedSelection(RouletteSelection):
    """Selection by rank of fitness."""

    def reset(self, candidates: Iterable[DNA]):
        # dna.fitness is not None here!
        self._candidates = list(candidates)
        self._candidates.sort(key=dna_fitness)
        # weight of best_fitness == len(strands)
        # and decreases until 1 for the least fitness
        self._weights = list(range(1, len(self._candidates) + 1))


class TournamentSelection(Selection):
    """Selection by choosing the best of a certain count of candidates."""

    def __init__(self, candidates: int):
        self._candidates: list[DNA] = []
        self.candidates = candidates

    def reset(self, candidates: Iterable[DNA]):
        self._candidates = list(candidates)

    def pick(self, count: int) -> Iterable[DNA]:
        for _ in range(count):
            values = [
                random.choice(self._candidates) for _ in range(self.candidates)
            ]
            values.sort(key=dna_fitness)
            yield values[-1]
