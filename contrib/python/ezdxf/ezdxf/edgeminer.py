# Copyright (c) 2024, Manfred Moitzi
# License: MIT License
"""
EdgeMiner
=========

A module for detecting linked edges.

The complementary module ezdxf.edgesmith can create entities from the output of this 
module.

Terminology
-----------

I try to use the terminology of `Graph Theory`_ but there are differences where I think
a different term is better suited for this module like loop for cycle.

Edge (in this module)
    Edge is an immutable class:
        - unique id
        - 3D start point (vertex)
        - 3D end point (vertex)
        - optional length
        - optional payload (arbitrary data)

    The geometry of an edge is not known.
    Intersection points of edges are not known and cannot be calculated.

Vertex
    A connection point of two or more edges. 
    The degree of a vertex is the number of connected edges.

Leaf
    A leaf is a vertex of degree 1.
    A leaf is a loose end of an edge, which is not connected to other edges.

Junction
    A junction is a vertex of degree greater 2.
    A junction has more than two adjacent edges.
    A junction is an ambiguity when searching for open chains or closed loops.
    Graph Theory: multiple adjacency

Chain
    A chain has sequential connected edges. 
    The end point of an edge is connected to the start point of the following edge. 
    A chain has unique edges, each edge appears only once in the chain.
    A chain can contain vertices of degree greater 2.
    A solitary edge is also a chain.
    Chains are represented as Sequence[Edge].
    Graph Theory: Trail - no edge is repeated, vertex is repeated

Simple Chain (special to this module)
    A simple chain contains only vertices of degree 2, except the start- and end vertex.
    The start- and end vertices are leafs (degree of 1) or junctions (degree greater 2).
    
Open Chain
    An open chain is a chain which starts and ends with at leaf.
    A solitary edge is also an open chain.
    Graph Theory: Path - no edge is repeated, no vertex is repeated, endings not connected

Loop
    A loop is a simple chain with connected start- and end vertices.
    A loop has two or more edges.
    A loop contains only vertices of degree 2.
    Graph Theory: Cycle - no edge is repeated, no vertex is repeated, endings connected; 
    a loop in Graph Theory is something different

Network
    A network has two or more edges that are directly and indirectly connected. 
    The edges in a network have no order.
    A network can contain vertices of degree greater 2 (junctions).
    A solitary edge is not a network. 
    A chain with two or more edges is a network. 
    Networks are represented as Sequence[Edge].
    Graph Theory: multigraph; a network in Graph Theory is something different

Gap Tolerance
    Maximum vertex distance to consider two edges as connected

Forward Connection
    An edge is forward connected when the end point of the edge is connected to the 
    start point of the following edge.

.. important::
    
    THIS MODULE IS WORK IN PROGRESS (ALPHA VERSION), EVERYTHING CAN CHANGE UNTIL 
    THE RELEASE IN EZDXF V1.4.

.. _Graph Theory: https://en.wikipedia.org/wiki/Glossary_of_graph_theory
.. _GeeksForGeeks: https://www.geeksforgeeks.org/graph-data-structure-and-algorithms/?ref=shm

"""
from __future__ import annotations
from typing import Any, Sequence, Iterator, Iterable, Dict, Tuple, NamedTuple, Callable
from typing_extensions import Self, TypeAlias
from collections import Counter
import functools
import time
import math

from ezdxf.math import UVec, Vec2, Vec3, distance_point_line_3d
from ezdxf.math import rtree


__all__ = [
    "Deposit",
    "Edge",
    "TimeoutError",
    "find_all_loops",
    "find_all_open_chains",
    "find_all_sequential_chains",
    "find_all_simple_chains",
    "find_loop",
    "find_loop_by_edge",
    "find_sequential_chain",
    "flatten",
    "is_chain",
    "is_loop",
    "length",
    "longest_chain",
    "reverse_chain",
    "shortest_chain",
    "subtract_edges",
    "unique_chains",
]
GAP_TOL = 1e-9
ABS_TOL = 1e-9
TIMEOUT = 60.0  # in seconds


class TimeoutError(Exception):  # noqa
    """
    Attributes:
        solutions: solutions found until time out occur

    """

    def __init__(self, msg: str, solutions: Sequence[Sequence[Edge]] = tuple()) -> None:
        super().__init__(msg)
        self.solutions = solutions


class Watchdog:
    def __init__(self, timeout=TIMEOUT) -> None:
        self.timeout: float = timeout
        self.start_time: float = time.perf_counter()

    def start(self, timeout: float):
        self.timeout = timeout
        self.start_time = time.perf_counter()

    @property
    def has_timed_out(self) -> bool:
        return time.perf_counter() - self.start_time > self.timeout


class Edge(NamedTuple):
    """Represents an immutable edge.

    An edge can represent any linear curve (line, elliptical arc, spline, ...), but it
    does not know this shape; only the start and end vertices are stored, not the shape
    itself.  Therefore, the length of the edge must be specified if the length
    calculation for a sequence of edges should be possible.  Intersection points between
    edges are not known and cannot be calculated.

    .. Important::

        Use only the :func:`make_edge` function to create new edges to get unique ids!

    Attributes:
        id: unique id
        start (Vec3): start vertex
        end (Vec3): end vertex
        is_reverse: flag to indicate that the edge is reversed compared to its initial state
        length: length of the edge, default is 1.0
        payload: arbitrary data attached to the edge, default is ``None``

    """

    id: int
    start: Vec3
    end: Vec3
    is_reverse: bool = False
    length: float = 1.0
    payload: Any = None

    def __eq__(self, other) -> bool:
        """Return ``True`` if the ids of the edges are equal.

        .. important::

            An edge is equal to its reversed copy!

        """
        if isinstance(other, Edge):
            return self.id == other.id
        return False

    def __repr__(self) -> str:
        payload = self.payload
        if payload is None:
            content = str(self.id)
        elif isinstance(payload, EdgeWrapper):
            content = "[" + (",".join(repr(e) for e in payload.edges)) + "]"
        else:
            content = str(payload)
        return f"Edge({content})"

    def __hash__(self) -> int:
        """The edge :attr:`id` is used as hash value.

        An Edge and its reversed edge have the same hash value and cannot both
        exist in the same :class:`set`.
        """
        return self.id

    def reversed(self) -> Self:
        """Returns a reversed copy.

        The reversed edge has the same :attr:`id` as the source edge, because they
        represent the same edge.
        """
        return self.__class__(  # noqa
            self.id,  # edge and its reversed edge must have the same id!
            self.end,
            self.start,
            not self.is_reverse,
            self.length,
            self.payload,
        )


def make_id_generator(start: int = 0) -> Callable[[], int]:
    next_edge_id = start

    def next_id() -> int:
        nonlocal next_edge_id
        next_edge_id += 1
        return next_edge_id

    return next_id


id_generator = make_id_generator()


def make_edge(
    start: UVec, end: UVec, length: float = -1.0, *, payload: Any = None
) -> Edge:
    """Creates a new :class:`Edge` with a unique id.

    Args:
        start: start point
        end: end point
        length: default is the distance between start and end
        payload: arbitrary data attached to the edge

    """
    start = Vec3(start)
    end = Vec3(end)
    if length < 0.0:
        length = start.distance(end)
    return Edge(id_generator(), start, end, False, length, payload)


def isclose(a: Vec3, b: Vec3, *, gap_tol=GAP_TOL) -> bool:
    """This function should be used to test whether two vertices are close to each other
    to get consistent results.
    """
    return a.distance(b) <= gap_tol


class Deposit:
    """The edge deposit stores all available edges for further searches.

    The edges and the search index are immutable after instantiation.
    The :attr:`gap_tol` attribute is mutable.

    Args:
        edges: sequence of :class:`Edge`
        gap_tol: maximum vertex distance to consider two edges as connected

    Attributes:
        gap_tol: maximum vertex distance to consider two edges as connected (mutable)

    """

    def __init__(self, edges: Sequence[Edge], *, gap_tol=GAP_TOL) -> None:
        self.gap_tol: float = gap_tol
        self._edges: Sequence[Edge] = type_check(edges)
        self._search_index = _SpatialSearchIndex(self._edges)

    @property
    def edges(self) -> Sequence[Edge]:
        """Sequence of edges stored in this deposit."""
        return self._edges

    def degree_counter(self) -> Counter[int]:
        """Returns a :class:`Counter` for the degree of all vertices.

        - :code:`Counter[degree]` returns the count of vertices of this degree.
        - :code:`Counter.keys()` returns all existing degrees in this deposit

        A new counter will be created for every method call! The :attr:`gap_tol`
        attribute is mutable and different gap tolerances may yield different results.

        """
        # no caching: result depends on gap_tol, which is mutable
        counter: Counter[int] = Counter()
        search = functools.partial(
            self._search_index.vertices_in_sphere, radius=self.gap_tol
        )
        for edge in self.edges:
            counter[len(search(edge.start))] += 1
            counter[len(search(edge.end))] += 1
        # remove duplicate counts:
        return Counter({k: v // k for k, v in counter.items()})

    @property
    def max_degree(self) -> int:
        """Returns the maximum degree of all vertices in this deposit."""
        return max(self.degree_counter().keys())

    def degree(self, vertex: UVec) -> int:
        """Returns the degree of the given vertex.

        - degree of 0: not in this deposit
        - degree of 1: one edge is connected to this vertex
        - degree of 2: two edges are connected to this vertex
        - degree of 3: three edges ... and so on


        Check if a vertex exist in a deposit::

            if deposit.degree(vertex): ...
        """
        return len(self._search_index.vertices_in_sphere(Vec3(vertex), self.gap_tol))

    def degrees(self, vertices: Iterable[UVec]) -> Sequence[int]:
        """Returns the degree of the given vertices."""
        search = functools.partial(
            self._search_index.vertices_in_sphere, radius=self.gap_tol
        )
        return tuple(len(search(vertex)) for vertex in Vec3.generate(vertices))

    def unique_vertices(self) -> set[Vec3]:
        """Returns all unique vertices from this deposit.

        Ignores vertices that are close to another vertex (within the range of gap_tol).
        It is not determined which of the close vertices is returned.

        e.g. if the vertices a, b are close together, you don't know if you get a or b,
        but it's guaranteed that you only get one of them
        """
        return filter_close_vertices(self._search_index.rtree, gap_tol=self.gap_tol)

    def edges_linked_to(self, vertex: UVec, radius: float = -1) -> Sequence[Edge]:
        """Returns all edges linked to `vertex` in range of `radius`.

        Args:
            vertex: 3D search location
            radius: search range, default radius is :attr:`Deposit.gap_tol`

        """
        if radius < 0:
            radius = self.gap_tol
        vertices = self._search_index.vertices_in_sphere(Vec3(vertex), radius)
        return tuple(v.edge for v in vertices)

    def find_nearest_edge(self, vertex: UVec) -> Edge | None:
        """Return the nearest edge to the given vertex.

        The distance is measured to the connection line from start to end of the edge.
        This is not correct for edges that represent arcs or splines.
        """

        def distance(edge: Edge) -> float:
            try:
                return distance_point_line_3d(vertex, edge.start, edge.end)
            except ZeroDivisionError:
                return edge.start.distance(vertex)

        vertex = Vec3(vertex)
        si = self._search_index
        nearest_vertex = si.nearest_vertex(vertex)
        edges = self.edges_linked_to(nearest_vertex)
        if edges:
            return min(edges, key=distance)
        return None

    def find_network(self, edge: Edge) -> set[Edge]:
        """Returns the network of all edges that are directly and indirectly linked to
        `edge`.  A network has two or more edges, a solitary edge is not a network.

        """

        def process(vertex: Vec3) -> None:
            linked_edges = set(self.edges_linked_to(vertex)) - network
            if linked_edges:
                network.update(linked_edges)
                todo.extend(linked_edges)

        todo: list[Edge] = [edge]
        network: set[Edge] = set(todo)
        while todo:
            edge = todo.pop()
            process(edge.start)
            process(edge.end)
        if len(network) > 1:  # a network requires two or more edges
            return network
        return set()

    def find_all_networks(self) -> Sequence[set[Edge]]:
        """Returns all separated networks in this deposit in ascending order of edge
        count.

        """
        edges = set(self.edges)
        networks: list[set[Edge]] = []
        while edges:
            edge = edges.pop()
            network = self.find_network(edge)
            if len(network):
                networks.append(network)
                edges -= network
            else:  # solitary edge
                edges.discard(edge)

        networks.sort(key=lambda n: len(n))
        return networks

    def find_leafs(self) -> Iterator[Edge]:
        """Yields all edges that have at least one end point without connection to other
        edges.
        """
        for edge in self.edges:
            if len(self.edges_linked_to(edge.start)) == 1:
                yield edge
            elif len(self.edges_linked_to(edge.end)) == 1:
                yield edge


def is_forward_connected(a: Edge, b: Edge, *, gap_tol=GAP_TOL) -> bool:
    """Returns ``True`` if the edges have a forward connection.

    Forward connection: distance from :attr:`a.end` to :attr:`b.start` <= gap_tol

    Args:
        a: first edge
        b: second edge
        gap_tol: maximum vertex distance to consider two edges as connected
    """
    return isclose(a.end, b.start, gap_tol=gap_tol)


def is_chain(edges: Sequence[Edge], *, gap_tol=GAP_TOL) -> bool:
    """Returns ``True`` if all edges in the sequence have a forward connection.

    Args:
        edges: sequence of edges
        gap_tol: maximum vertex distance to consider two edges as connected
    """
    return all(
        is_forward_connected(a, b, gap_tol=gap_tol) for a, b in zip(edges, edges[1:])
    )


def is_loop(edges: Sequence[Edge], *, gap_tol=GAP_TOL) -> bool:
    """Return ``True`` if the sequence of edges is a closed loop.

    Args:
        edges: sequence of edges
        gap_tol: maximum vertex distance to consider two edges as connected
    """
    if not is_chain(edges, gap_tol=gap_tol):
        return False
    return isclose(edges[-1].end, edges[0].start, gap_tol=gap_tol)


def is_loop_fast(edges: Sequence[Edge], *, gap_tol=GAP_TOL) -> bool:
    """Internal fast loop check."""
    return isclose(edges[-1].end, edges[0].start, gap_tol=gap_tol)


def length(edges: Sequence[Edge]) -> float:
    """Returns the length of a sequence of edges."""
    return sum(e.length for e in edges)


def shortest_chain(chains: Iterable[Sequence[Edge]]) -> Sequence[Edge]:
    """Returns the shortest chain of connected edges.

    .. note::

        This function does not verify if the input sequences are connected edges!

    """
    sorted_chains = sorted(chains, key=length)
    if sorted_chains:
        return sorted_chains[0]
    return tuple()


def longest_chain(chains: Iterable[Sequence[Edge]]) -> Sequence[Edge]:
    """Returns the longest chain of connected edges.

    .. Note::

        This function does not verify if the input sequences are connected edges!

    """
    sorted_chains = sorted(chains, key=length)
    if sorted_chains:
        return sorted_chains[-1]
    return tuple()


def reverse_chain(chain: Sequence[Edge]) -> list[Edge]:
    """Returns the reversed chain.

    The sequence order of the edges will be reversed as well as the start- and end
    points of the edges.
    """
    edges = list(chain)
    edges.reverse()
    return [edge.reversed() for edge in edges]


def find_sequential_chain(edges: Sequence[Edge], *, gap_tol=GAP_TOL) -> Sequence[Edge]:
    """Returns a simple chain beginning at the first edge.

    The search stops at the first edge without a forward connection from the previous
    edge.  Edges will be reversed if required to create connection.

    Args:
        edges: edges to be examined
        gap_tol: maximum vertex distance to consider two edges as connected

    Raises:
        TypeError: invalid data in sequence `edges`
    """
    edges = type_check(edges)
    if len(edges) < 2:
        return edges
    chain = [edges[0]]
    for edge in edges[1:]:
        last = chain[-1]
        if is_forward_connected(last, edge, gap_tol=gap_tol):
            chain.append(edge)
            continue
        reversed_edge = edge.reversed()
        if is_forward_connected(last, reversed_edge, gap_tol=gap_tol):
            chain.append(reversed_edge)
            continue
        break
    return chain


def find_all_sequential_chains(
    edges: Sequence[Edge], *, gap_tol=GAP_TOL
) -> Iterator[Sequence[Edge]]:
    """Yields all simple chains from sequence `edges`.

    The search progresses strictly in order of the input sequence. The search starts a
    new chain at every edge without a forward connection from the previous edge.
    Edges will be reversed if required to create connection.
    Each chain has one or more edges.

    Args:
        edges: sequence of edges
        gap_tol: maximum vertex distance to consider two edges as connected

    Raises:
        TypeError: invalid data in sequence `edges`
    """
    while edges:
        chain = find_sequential_chain(edges, gap_tol=gap_tol)
        edges = edges[len(chain) :]
        yield chain


def find_loop(deposit: Deposit, *, timeout: float = TIMEOUT) -> Sequence[Edge]:
    """Returns the first closed loop in `deposit`.

    Returns only simple loops, where all vertices have a degree of 2 (only two adjacent
    edges).

    .. note::

        This is a recursive backtracking algorithm with time complexity of O(n!).

    Args:
        deposit (Deposit): edge deposit
        timeout (float): timeout in seconds

    Raises:
        TimeoutError: search process has timed out
    """
    chains = find_all_simple_chains(deposit)
    if not chains:
        return tuple()

    gap_tol = deposit.gap_tol
    packed_edges: list[Edge] = []
    for chain in chains:
        if len(chain) > 1:
            if is_loop_fast(chain, gap_tol=gap_tol):
                return chain
            packed_edges.append(_wrap_simple_chain(chain))
        else:
            packed_edges.append(chain[0])
    deposit = Deposit(packed_edges, gap_tol=gap_tol)
    if len(deposit.edges) < 2:
        return tuple()
    return tuple(flatten(_find_loop_in_deposit(deposit, timeout=timeout)))


def _find_loop_in_deposit(deposit: Deposit, *, timeout=TIMEOUT) -> Sequence[Edge]:
    if len(deposit.edges) < 2:
        return tuple()

    finder = LoopFinder(deposit, timeout=timeout)
    loop = finder.find_any_loop()
    if loop:
        return loop
    return tuple()


def find_all_loops(
    deposit: Deposit, *, timeout: float = TIMEOUT
) -> Sequence[Sequence[Edge]]:
    """Returns all closed loops from `deposit`.

    Returns only simple loops, where all vertices have a degree of 2 (only two adjacent
    edges).  The result does not include reversed solutions.

    .. note::

        This is a recursive backtracking algorithm with time complexity of O(n!).

    Args:
        deposit (Deposit): edge deposit
        timeout (float): timeout in seconds

    Raises:
        TimeoutError: search process has timed out
    """
    chains = find_all_simple_chains(deposit)
    if not chains:
        return tuple()

    gap_tol = deposit.gap_tol
    solutions: list[Sequence[Edge]] = []
    packed_edges: list[Edge] = []
    for chain in chains:
        if len(chain) > 1:
            if is_loop_fast(chain, gap_tol=gap_tol):
                # these loops have no ambiguities (junctions)
                solutions.append(chain)
            else:
                packed_edges.append(_wrap_simple_chain(chain))
        else:
            packed_edges.append(chain[0])

    if not packed_edges:
        return solutions

    deposit = Deposit(packed_edges, gap_tol=gap_tol)
    if len(deposit.edges) < 2:
        return tuple()
    try:
        result = _find_all_loops_in_deposit(deposit, timeout=timeout)
    except TimeoutError as err:
        if err.solutions:
            solutions.extend(err.solutions)
            err.solutions = solutions
        raise
    solutions.extend(result)
    return _unwrap_simple_chains(solutions)


def _find_all_loops_in_deposit(
    deposit: Deposit, timeout=TIMEOUT
) -> Sequence[Sequence[Edge]]:
    solutions: list[Sequence[Edge]] = []
    finder = LoopFinder(deposit, timeout=timeout)
    for edge in deposit.edges:
        finder.search(edge)
    solutions.extend(finder)
    return solutions


def unique_chains(chains: Sequence[Sequence[Edge]]) -> Iterator[Sequence[Edge]]:
    """Filter duplicate chains and yields only unique chains.

    Yields the first chain for chains which have the same set of edges. The order of the
    edges is not important.
    """
    seen: set[frozenset[int]] = set()
    for chain in chains:
        key = frozenset(edge.id for edge in chain)
        if key not in seen:
            yield chain
            seen.add(key)


def type_check(edges: Sequence[Edge]) -> Sequence[Edge]:
    for edge in edges:
        if not isinstance(edge, Edge):
            raise TypeError(f"expected type <Edge>, got {str(type(edge))}")
    return edges


class _Vertex(Vec3):
    __slots__ = ("edge",)
    # for unknown reasons super().__init__(location) doesn't work, therefor no
    # _Vertex.__init__(self, location: Vec3, edge: Edge) constructor
    edge: Edge


def make_edge_vertex(location: Vec3, edge: Edge) -> _Vertex:
    vertex = _Vertex(location)
    vertex.edge = edge
    return vertex


class _SpatialSearchIndex:
    """Spatial search index of all edge vertices.

    (internal class)
    """

    def __init__(self, edges: Sequence[Edge]) -> None:
        vertices: list[_Vertex] = []
        for edge in edges:
            vertices.append(make_edge_vertex(edge.start, edge))
            vertices.append(make_edge_vertex(edge.end, edge))
        self._search_tree = rtree.RTree(vertices)

    @property
    def rtree(self) -> rtree.RTree[Vec3]:
        return self._search_tree

    def vertices_in_sphere(self, center: Vec3, radius: float) -> Sequence[_Vertex]:
        """Returns all vertices located around `center` with a max. distance of `radius`."""
        return tuple(self._search_tree.points_in_sphere(center, radius))

    def nearest_vertex(self, location: Vec3) -> _Vertex:
        """Returns the nearest vertex to the given location."""
        vertex, _ = self._search_tree.nearest_neighbor(location)
        return vertex


SearchSolutions: TypeAlias = Dict[Tuple[int, ...], Sequence[Edge]]


class LoopFinder:
    """Find closed loops in a :class:`Deposit` by a recursive backtracking algorithm.

    Finds only simple loops, where all vertices have only two adjacent edges.

    (internal class)
    """

    def __init__(self, deposit: Deposit, *, timeout=TIMEOUT) -> None:
        if len(deposit.edges) < 2:
            raise ValueError("two or more edges required")
        self._deposit = deposit
        self._timeout = timeout
        self._solutions: SearchSolutions = {}

    @property
    def gap_tol(self) -> float:
        return self._deposit.gap_tol

    def __iter__(self) -> Iterator[Sequence[Edge]]:
        return iter(self._solutions.values())

    def __len__(self) -> int:
        return len(self._solutions)

    def find_any_loop(self, start: Edge | None = None) -> Sequence[Edge]:
        """Returns the first loop found beginning with the given start edge or an
        arbitrary edge if `start` is None.
        """
        if start is None:
            start = self._deposit.edges[0]

        self.search(start, stop_at_first_loop=True)
        try:
            return next(iter(self._solutions.values()))
        except StopIteration:
            return tuple()

    def search(self, start: Edge, stop_at_first_loop: bool = False) -> None:
        """Searches for all loops that begin at the given start edge and contain
        only vertices of degree 2.

        These are not all possible loops in the edge deposit!

        Raises:
            TimeoutError: search process has timed out, intermediate results are attached
                TimeoutError.data

        """
        deposit = self._deposit
        gap_tol = self.gap_tol
        start_point = start.start
        watchdog = Watchdog(self._timeout)
        todo: list[tuple[Edge, ...]] = [(start,)]  # "unlimited" recursion stack
        while todo:
            if watchdog.has_timed_out:
                raise TimeoutError(
                    "search process has timed out",
                    solutions=tuple(self._solutions.values()),  # noqa
                )
            chain = todo.pop()
            last_edge = chain[-1]
            end_point = last_edge.end
            candidates = deposit.edges_linked_to(end_point, radius=gap_tol)
            # edges must be unique in a loop
            survivors = set(candidates) - set(chain)
            for edge in survivors:
                if isclose(end_point, edge.start, gap_tol=gap_tol):
                    next_edge = edge
                else:
                    next_edge = edge.reversed()
                last_point = next_edge.end
                if isclose(last_point, start_point, gap_tol=gap_tol):
                    self.add_solution(chain + (next_edge,))
                    if stop_at_first_loop:
                        return
                # Add only chains to the stack that have vertices of max degree 2.
                # If the new end point is in the chain, a vertex of degree 3 would be
                # created. (loop check is done)
                elif not any(
                    isclose(last_point, e.end, gap_tol=gap_tol) for e in chain
                ):
                    todo.append(chain + (next_edge,))

    def add_solution(self, loop: Sequence[Edge]) -> None:
        solutions = self._solutions
        key = loop_key(loop)
        if key in solutions or loop_key(loop, reverse=True) in solutions:
            return
        solutions[key] = loop


def loop_key(edges: Sequence[Edge], *, reverse=False) -> tuple[int, ...]:
    """Returns a normalized key.

    The key is rotated to begin with the smallest edge id.
    """
    if reverse:
        ids = tuple(edge.id for edge in reversed(edges))
    else:
        ids = tuple(edge.id for edge in edges)
    index = ids.index(min(ids))
    if index:
        ids = ids[index:] + ids[:index]
    return ids


def find_all_simple_chains(deposit: Deposit) -> Sequence[Sequence[Edge]]:
    """Returns all simple chains from `deposit`.

    Each chains starts and ends at a leaf (degree of 1) or a junction (degree greater 2).
    All vertices between the start- and end vertex have a degree of 2.
    The result doesn't include reversed solutions.
    """
    if len(deposit.edges) < 1:
        return tuple()
    solutions: list[Sequence[Edge]] = []
    edges = set(deposit.edges)
    while edges:
        chain = find_simple_chain(deposit, edges.pop())
        solutions.append(chain)
        edges -= set(chain)
    return solutions


def find_simple_chain(deposit: Deposit, start: Edge) -> Sequence[Edge]:
    """Returns a simple chain containing `start` edge.

    A simple chain start and ends at a leaf or a junction.

    All connected edges have vertices of degree 2, except the first and last vertex.
    The first and the last vertex have a degree of 1 (leaf) or greater 2 (junction).
    """
    forward_chain = _simple_forward_chain(deposit, start)
    if is_loop_fast(forward_chain, gap_tol=deposit.gap_tol):
        return forward_chain
    backwards_chain = _simple_forward_chain(deposit, start.reversed())
    if len(backwards_chain) == 1:
        return forward_chain
    backwards_chain.reverse()
    backwards_chain.pop()  # reversed start
    return [edge.reversed() for edge in backwards_chain] + forward_chain


def _simple_forward_chain(deposit: Deposit, edge: Edge) -> list[Edge]:
    """Returns a simple chain beginning with `edge`.

    All connected edges have vertices of degree 2, expect for the last edge.
    The last edge has an end-vertex of degree 1 (leaf) or greater 2 (junction).
    """
    gap_tol = deposit.gap_tol
    chain = [edge]
    while True:
        last = chain[-1]
        linked = deposit.edges_linked_to(last.end, gap_tol)
        if len(linked) != 2:  # no junctions allowed!
            return chain
        if linked[0] == last:
            edge = linked[1]
        else:
            edge = linked[0]
        if isclose(last.end, edge.start, gap_tol=gap_tol):
            chain.append(edge)
        else:
            chain.append(edge.reversed())
        if is_loop_fast(chain, gap_tol=gap_tol):
            return chain


def is_wrapped_chain(edge: Edge) -> bool:
    """Returns ``True`` if `edge` is a wrapper for linked edges."""
    return isinstance(edge.payload, EdgeWrapper)


def wrap_simple_chain(chain: Sequence[Edge], *, gap_tol=GAP_TOL) -> Edge:
    """Wraps a sequence of linked edges (simple chain) into a single edge.

    Two or more linked edges required. Closed loops cannot be wrapped into a single
    edge.

    Raises:
        ValueError: less than two edges; not a chain; chain is a closed loop

    """
    if len(chain) < 2:
        raise ValueError("two or more linked edges required")
    if is_chain(chain, gap_tol=gap_tol):
        if is_loop_fast(chain, gap_tol=gap_tol):
            raise ValueError("closed loop cannot be wrapped into a single edge")
        return _wrap_simple_chain(chain)
    raise ValueError("edges are not connected")


def unwrap_simple_chain(edge: Edge) -> Sequence[Edge]:
    """Unwraps a simple chain which is wrapped into a single edge."""
    if isinstance(edge.payload, EdgeWrapper):
        return _unwrap_simple_chain(edge)
    return (edge,)


class EdgeWrapper:
    """Internal class to wrap a sequence of linked edges."""

    __slots__ = ("edges",)

    def __init__(self, edges: Sequence[Edge]) -> None:
        self.edges: Sequence[Edge] = tuple(edges)


def _wrap_simple_chain(edges: Sequence[Edge]) -> Edge:
    return make_edge(
        edges[0].start, edges[-1].end, length(edges), payload=EdgeWrapper(edges)
    )


def _unwrap_simple_chain(edge: Edge) -> Sequence[Edge]:
    wrapper = edge.payload
    assert isinstance(wrapper, EdgeWrapper)
    if edge.is_reverse:
        return tuple(e.reversed() for e in reversed(wrapper.edges))
    else:
        return wrapper.edges


def _unwrap_simple_chains(chains: Iterable[Iterable[Edge]]) -> Sequence[Sequence[Edge]]:
    return tuple(tuple(flatten(chain)) for chain in chains)


def flatten(edges: Edge | Iterable[Edge]) -> Iterator[Edge]:
    """Yields all edges from any nested structure of edges as a flat stream of edges."""
    edge: Edge
    if not isinstance(edges, Edge):
        for edge in edges:
            yield from flatten(edge)
    else:
        edge = edges
        if is_wrapped_chain(edge):
            yield from flatten(_unwrap_simple_chain(edge))
        else:
            yield edge


def chain_key(edges: Sequence[Edge], reverse=False) -> tuple[int, ...]:
    """Returns a normalized key."""
    if reverse:
        return tuple(edge.id for edge in reversed(edges))
    else:
        return tuple(edge.id for edge in edges)


def find_all_open_chains(
    deposit: Deposit, timeout: float = TIMEOUT
) -> Sequence[Sequence[Edge]]:
    """Returns all open chains from `deposit`.

    Returns only simple chains ending on both sides with a leaf.
    A leaf is a vertex of degree 1 without further connections.
    All vertices have a degree of 2 except for the leafs at the start and end.
    The result does not include reversed solutions or closed loops.

    .. note::

        This is a recursive backtracking algorithm with time complexity of O(n!).

    Args:
        deposit (Deposit): edge deposit
        timeout (float): timeout in seconds

    Raises:
        TimeoutError: search process has timed out
    """

    finder = OpenChainFinder(deposit, timeout)
    for edge in deposit.find_leafs():
        finder.search(edge)
    solutions = finder.solutions
    solutions.sort(key=lambda x: len(x))
    return solutions


class OpenChainFinder:
    def __init__(self, deposit: Deposit, timeout=TIMEOUT):
        self.deposit = deposit
        self.solution_keys: set[tuple[int, ...]] = set()
        self.solutions: list[Sequence[Edge]] = []
        self.watchdog = Watchdog(timeout)

    def search(self, edge: Edge) -> None:
        forward_chains = self.forward_search(edge)
        self.reverse_search(forward_chains)

    def forward_search(self, edge: Edge) -> list[tuple[Edge, ...]]:
        deposit = self.deposit
        gap_tol = deposit.gap_tol
        watchdog = self.watchdog

        forward_chains: list[tuple[Edge, ...]] = []
        todo: list[tuple[Edge, ...]] = [(edge,)]
        while todo:
            if watchdog.has_timed_out:
                raise TimeoutError("search has timed out")
            chain = todo.pop()
            start_point = chain[-1].end
            candidates = deposit.edges_linked_to(start_point)
            backwards_edges = set(candidates) - set(chain)
            if backwards_edges:
                for edge in backwards_edges:
                    if not isclose(start_point, edge.start, gap_tol=gap_tol):
                        edge = edge.reversed()
                    # Add only chains to the stack that have vertices of max degree 2.
                    # If the new end point is in the chain, a vertex of degree 3 would be
                    # created. (loop check is done)
                    last_point = edge.end
                    if not any(
                        isclose(last_point, e.end, gap_tol=gap_tol) for e in chain
                    ):
                        todo.append(chain + (edge,))
            else:
                forward_chains.append(chain)
        return forward_chains

    def reverse_search(self, forward_chains: list[tuple[Edge, ...]]) -> None:
        deposit = self.deposit
        gap_tol = deposit.gap_tol
        watchdog = self.watchdog
        todo = forward_chains
        while todo:
            if watchdog.has_timed_out:
                raise TimeoutError("search has timed out", solutions=self.solutions)
            chain = todo.pop()
            start_point = chain[0].start
            candidates = deposit.edges_linked_to(start_point)
            backwards_edges = set(candidates) - set(chain)
            if backwards_edges:
                for edge in backwards_edges:
                    if not isclose(start_point, edge.end, gap_tol=gap_tol):
                        edge = edge.reversed()
                    # Add only chains to the stack that have vertices of max degree 2.
                    # If the new end point is in the chain, a vertex of degree 3 would be
                    # created.
                    new_start_point = edge.start
                    if not any(
                        isclose(new_start_point, e.end, gap_tol=gap_tol) for e in chain
                    ):
                        todo.append((edge,) + chain)
            else:
                self.add_solution(chain)

    def add_solution(self, solution: Sequence[Edge]) -> None:
        keys = self.solution_keys
        key = chain_key(solution)
        if key in keys:
            return
        keys.add(key)
        key = chain_key(solution, reverse=True)
        if key in keys:
            return
        keys.add(key)
        self.solutions.append(solution)


def count_checker(count: int):
    """Returns a function that checks if a given sequence of edges has at least `count`
    vertices.
    """

    def has_min_edge_count(edges: Sequence[Edge]) -> bool:
        return len(edges) >= count

    return has_min_edge_count


def line_checker(abs_tol=ABS_TOL):
    """Returns a function that checks if two input edges are congruent."""

    def is_congruent_line(a: Edge, b: Edge) -> bool:
        return (
            a.start.isclose(b.start, abs_tol=abs_tol)
            and a.end.isclose(b.end, abs_tol=abs_tol)
        ) or (
            a.start.isclose(b.end, abs_tol=abs_tol)
            and a.end.isclose(b.start, abs_tol=abs_tol)
        )

    return is_congruent_line


def filter_coincident_edges(
    deposit: Deposit, eq_fn: Callable[[Edge, Edge], bool] = line_checker()
) -> Sequence[Edge]:
    """Returns all edges from deposit that are not coincident to any other edge in the
    deposit. Coincident edges are detected by the given `eq_fn` function.
    """
    edges = set(deposit.edges)
    unique_edges: list[Edge] = []
    while edges:
        edge = edges.pop()
        unique_edges.append(edge)
        candidates = set(deposit.edges_linked_to(edge.start))
        candidates.update(deposit.edges_linked_to(edge.end))
        candidates.discard(edge)
        for candidate in candidates:
            if eq_fn(edge, candidate):
                edges.discard(candidate)
    return unique_edges


def filter_close_vertices(rt: rtree.RTree[Vec3], *, gap_tol: float) -> set[Vec3]:
    """Returns all vertices from a :class:`RTree` and filters vertices that are closer
    than radius `gap_tol` to another vertex.

    Vertices that are close to another vertex are filtered out, so none of the returned
    vertices has another vertex within the range of `gap_tol`.  It is not determined
    which of close vertices is returned.
    """
    # RTree cannot be empty!
    todo = set(rt)
    merged: set[Vec3] = set([todo.pop()])
    for vertex in todo:
        for candidate in rt.points_in_sphere(vertex, gap_tol):
            if candidate in merged:
                continue
            if not any(isclose(candidate, v, gap_tol=gap_tol) for v in merged):
                merged.add(candidate)
    return merged


def sort_edges_to_base(
    edges: Iterable[Edge], base: Edge, *, gap_tol=GAP_TOL
) -> list[Edge]:
    """Returns a list of `edges` sorted in counter-clockwise order in relation to the
    `base` edge.

    The `base` edge represents zero radians.
    All edges have to be connected to end-vertex of the `base` edge.
    This is a pure 2D algorithm and ignores all z-axis values.

    Args:
        edges: list of edges to sort
        base: base edge for sorting, represents 0-radian
        gap_tol: maximum vertex distance to consider two edges as connected

    Raises:
        ValueError: edge is not connected to center

    """

    def angle(edge: Edge) -> float:
        s = Vec2(edge.start)
        e = Vec2(edge.end)
        if connection_point.distance(s) <= gap_tol:
            edge_angle = (e - s).angle
        elif connection_point.distance(e) <= gap_tol:
            edge_angle = (s - e).angle
        else:
            raise ValueError(f"edge {edge!s} not connected to center")
        return (edge_angle - base_angle) % math.tau

    connection_point = Vec2(base.end)
    base_angle = (Vec2(base.start) - connection_point).angle
    return sorted(edges, key=angle)


def find_loop_by_edge(deposit: Deposit, start: Edge, clockwise=True) -> Sequence[Edge]:
    """Returns the first loop found including the given `start` edge.

    Returns an empty sequence if no loop was found.

    Args:
        deposit (Deposit): edge deposit
        start (Edge): starting edge of the search
        clockwise (bool): search orientation, counter-clockwise when ``False``

    """
    if len(deposit.edges) < 2:
        return tuple()
    gap_tol = deposit.gap_tol
    chain = [start]
    chain_set = set(chain)
    start_point = start.start

    while True:
        last_edge = chain[-1]
        end_point = last_edge.end
        candidates = deposit.edges_linked_to(end_point, radius=gap_tol)
        # edges must be unique in a loop
        survivors = set(candidates) - chain_set
        count = len(survivors)
        if count == 0:
            return tuple()  # dead end
        if count > 1:
            sorted_edges = sort_edges_to_base(survivors, last_edge, gap_tol=gap_tol)
            if clockwise:
                next_edge = sorted_edges[-1]  # first clockwise edge
            else:
                next_edge = sorted_edges[0]  # first counter-clockwise edge
        else:
            next_edge = survivors.pop()
        if not isclose(next_edge.start, end_point, gap_tol=gap_tol):
            next_edge = next_edge.reversed()
        chain.append(next_edge)
        if isclose(next_edge.end, start_point, gap_tol=gap_tol):
            return chain  # found a closed loop
        chain_set.add(next_edge)


def subtract_edges(base: Iterable[Edge], edges: Iterable[Edge]) -> list[Edge]:
    """Returns all edges from the iterable `base` that do not exist in the iterable
    `edges` e.g. remove solutions from the search space.
    """
    edge_ids = set(edge.id for edge in edges)
    return [edge for edge in base if edge.id not in edge_ids]
