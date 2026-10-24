# Changelog

All notable changes to this project will be documented in this file.


## [0.8.3](https://github.com/petgraph/petgraph/compare/petgraph@v0.8.2...petgraph@v0.8.3) - 2025-09-30

### Bug Fixes

- Infinite `subgraph_isomorphisms_iter` for empty isomorphisms ([#780](https://github.com/petgraph/petgraph/pull/780))
- Algos don't work on `UndirectedAdaptor` ([#870](https://github.com/petgraph/petgraph/pull/870)) ([#871](https://github.com/petgraph/petgraph/pull/871))
- use a queue for SPFA  ([#893](https://github.com/petgraph/petgraph/pull/893))
- `StableGraph::reverse` breaks free lists ([#890](https://github.com/petgraph/petgraph/pull/890))

### Documentation

- Fix examples link in README and unify typesetting of one word ([#823](https://github.com/petgraph/petgraph/pull/823))
- Add link to multigraph definition to isomorphism algos ([#824](https://github.com/petgraph/petgraph/pull/824))
- Fix auxiliary space (and time) complexity of bron-kerbosch ([#825](https://github.com/petgraph/petgraph/pull/825))
- Fix Typo in Operator Module Documentation ([#831](https://github.com/petgraph/petgraph/pull/831))
- Sync the crate feature flags in the README and docs ([#832](https://github.com/petgraph/petgraph/pull/832))
- Remove all \[Generic\] tags from algo docstrings ([#835](https://github.com/petgraph/petgraph/pull/835))
- Fix typos in comments ([#836](https://github.com/petgraph/petgraph/pull/836))
- Revamp CONTRIBUTING.md ([#833](https://github.com/petgraph/petgraph/pull/833))
- Update `GraphMap` link in README ([#857](https://github.com/petgraph/petgraph/pull/857))
- Add doc comment for `Dot::with_attr_getters` ([#850](https://github.com/petgraph/petgraph/pull/850))
- Specify iteration order for neighbors and edges and their variants ([#790](https://github.com/petgraph/petgraph/pull/790))
- Collection of Doc fixes ([#856](https://github.com/petgraph/petgraph/pull/856))

### New Features

- Add `into_nodes_edges_iters` to `StableGraph` ([#841](https://github.com/petgraph/petgraph/pull/841))
- Add methods to reserve & shrink `StableGraph` capacity ([#846](https://github.com/petgraph/petgraph/pull/846))
- Add Dinic's Maximum Flow Algorithm ([#739](https://github.com/petgraph/petgraph/pull/739))
- make Csr::from_sorted_edges generic over edge type and properly increase edge_count in Csr::from_sorted_edges ([#861](https://github.com/petgraph/petgraph/pull/861))
- Add `map_owned` and `filter_map_owned` for `Graph` and `StableGraph` ([#863](https://github.com/petgraph/petgraph/pull/863))
- Add dijkstra::with_dynamic_goal ([#855](https://github.com/petgraph/petgraph/pull/855))
- Fix self-loop bug in all_simple_paths and enable multiple targets ([#865](https://github.com/petgraph/petgraph/pull/865))
- mark petgraph::dot::Dot::graph_fmt as public ([#866](https://github.com/petgraph/petgraph/pull/866))
- Add bidirectional Dijkstra algorithm ([#782](https://github.com/petgraph/petgraph/pull/782))

### Performance

- Make A* tie break on lower h-values ([#882](https://github.com/petgraph/petgraph/pull/882))

### Refactor

- add examples for scc algorithms and reorganize into dedicated module ([#830](https://github.com/petgraph/petgraph/pull/830))
- Remove unnecessary trait bounds from impls/methods ([#828](https://github.com/petgraph/petgraph/pull/828))
- replace uses of 'crate::util::zip' with 'core::iter::zip' ([#849](https://github.com/petgraph/petgraph/pull/849))
- Fix clippy (and other) lints ([#851](https://github.com/petgraph/petgraph/pull/851))
- Cleanup repo ([#854](https://github.com/petgraph/petgraph/pull/854))
- replace crate::util::enumerate with Iterator::enumerate ([#881](https://github.com/petgraph/petgraph/pull/881))

### Testing

- Add dependency list for 'quickcheck' feature ([#822](https://github.com/petgraph/petgraph/pull/822))
- Fix feature cfg capitalization in doctest ([#852](https://github.com/petgraph/petgraph/pull/852))

## [0.8.2](https://github.com/petgraph/petgraph/compare/petgraph@v0.8.1...petgraph@v0.8.2) - 2025-06-06
This minor release fixes several bugs, adds two new algorithms, slightly improves the performance of `maximum_matching`, 
adds a tool for parsing graphs from Dot/Graphviz files, and improves the documentation, making it more complete and uniform, as well as clarifying several points.

### Bug Fixes

- Ford Fulkerson sometimes Panics on StableGraphs ([#793](https://github.com/petgraph/petgraph/pull/793))
- Run Maximal Cliques Quickcheck only on Digraphs which are symmetrical ([#800](https://github.com/petgraph/petgraph/pull/800))
- Run Steiner Tree Quickcheck on the connected components to properly support disconnected graphs ([#801](https://github.com/petgraph/petgraph/pull/801))
- Quickcheck random01 function only outputs 0 ([#798](https://github.com/petgraph/petgraph/pull/798))

### Documentation

- Specify that Acyclic::try_udpate_edge may add an edge ([#770](https://github.com/petgraph/petgraph/pull/770))
- Update remove_node doc comment in graphmap.rs ([#663](https://github.com/petgraph/petgraph/pull/663))
- Add examples to minimum spanning tree functions ([#808](https://github.com/petgraph/petgraph/pull/808))
- Minimal typo fix in comments ([#803](https://github.com/petgraph/petgraph/pull/803))
- Update docs.rs ([#807](https://github.com/petgraph/petgraph/pull/807))
- Add note about `StableGraph::edge_indices` behaviour ([#812](https://github.com/petgraph/petgraph/pull/812))
- Clarification of references to nodes and V (refresh #358) ([#814](https://github.com/petgraph/petgraph/pull/814))
- Fix link and mention Dfs and Bfs as special case in examples ([#816](https://github.com/petgraph/petgraph/pull/816))
- Unify algo docs ([#815](https://github.com/petgraph/petgraph/pull/815))

### New Features

- *(parser)* allow parsing graphs from Dot/Graphviz files ([#653](https://github.com/petgraph/petgraph/pull/653))
- Implement `DataMap` for `GraphMap` graphs ([#776](https://github.com/petgraph/petgraph/pull/776))
- Add Johnson's algorithm ([#741](https://github.com/petgraph/petgraph/pull/741))
- Add algorithm to find bridge edges ([#590](https://github.com/petgraph/petgraph/pull/590))

### Performance

- Reuse queue allocation in `maximum_matching` main loop ([#817](https://github.com/petgraph/petgraph/pull/817))

### Refactor

- Fix new clippy warnings ([#791](https://github.com/petgraph/petgraph/pull/791))

## [0.8.1](https://github.com/petgraph/petgraph/compare/petgraph@v0.8.0...petgraph@v0.8.1) - 2025-04-07

This patch release re-adds a missing `VisitMap` implementation that was dropped in the `0.8.0` release,
improves error messaging in panicking functions, and adds capacity management methods to `UnionFind`.

### Bug Fixes

- Bring back `VisitMap` impl for std `HashSet` ([#764](https://github.com/petgraph/petgraph/pull/764))

### New Features

- Add `UnionFind` capacity management methods ([#736](https://github.com/petgraph/petgraph/pull/736))
- add `#[track_caller]` to functions that panic  ([#748](https://github.com/petgraph/petgraph/pull/748))

## [0.8.0](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.8.0) - 2025-04-05

### Breaking changes

- Add ``no_std`` Support ([#747](https://github.com/petgraph/petgraph/issues/747))
- Add ``VisitMap::unvisit`` as proposed in #610 ([#611](https://github.com/petgraph/petgraph/issues/611))
- Add support for specifying rankdir on dot plots. ([#728](https://github.com/petgraph/petgraph/issues/728))
- Make ``dot::Config`` non_exhaustive ([#756](https://github.com/petgraph/petgraph/issues/756))
- Add ``from_f32/64`` methods for ``Float``, ``Unit``, and ``Bounded`` measures ([#733](https://github.com/petgraph/petgraph/issues/733))

### New algorithms

- Add articulation points implementation ([#681](https://github.com/petgraph/petgraph/issues/681))
- Add Prim's Algorithm for Minimum Spanning Tree ([#625](https://github.com/petgraph/petgraph/issues/625))
- Add Kou's algorithm for finding a MST ([#682](https://github.com/petgraph/petgraph/issues/682))
- Add Bron-Kerbosch algorithm for maximal cliques ([#662](https://github.com/petgraph/petgraph/issues/662))
- Add Shortest Path Faster Algorithm Implementation ([#686](https://github.com/petgraph/petgraph/issues/686))

### New features

- Add ``UnionFind::new_set`` ([#684](https://github.com/petgraph/petgraph/issues/684))
- Implement ``Csr::try_add_edge`` ([#719](https://github.com/petgraph/petgraph/issues/719))
- Add checked ``UnionFind`` methods ([#730](https://github.com/petgraph/petgraph/issues/730))
- Add ``MatrixGraph`` methods with recoverable errors ([#720](https://github.com/petgraph/petgraph/issues/720))
- Add methods with recoverable errors for ``Graph`` and ``StableGraph`` ([#718](https://github.com/petgraph/petgraph/issues/718))

### CI & fixes

- Fix all clippy lints and check them on CI ([#726](https://github.com/petgraph/petgraph/issues/726))
- Pin once_cell version for MSRV builds ([#750](https://github.com/petgraph/petgraph/issues/750))
- Require conventional commits tag in PR titles ([#734](https://github.com/petgraph/petgraph/issues/734))
- Fix wrong trigger for pr-title check ([#751](https://github.com/petgraph/petgraph/issues/751))
- Solve clippy warnings ([#749](https://github.com/petgraph/petgraph/issues/749))
- Fix github token in pr-title action ([#752](https://github.com/petgraph/petgraph/issues/752))
- Add new triggers for semver-checks ([#754](https://github.com/petgraph/petgraph/issues/754))

### Documentation

- Add some missed features into crate-lvl doc ([#758](https://github.com/petgraph/petgraph/issues/758))

## [0.7.1](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.7.1) - 2025-01-08

- Do not unnecessarily restrict ``indexmap`` version ([#714](https://github.com/petgraph/petgraph/issues/714))
- Export ``UndirectedAdaptor`` ([#717](https://github.com/petgraph/petgraph/issues/717))


## [0.7.0](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.7.0) - 2024-12-31

- Re-released version 0.6.6 with the correct version number, as it included a major update to an exposed crate ([#664](https://github.com/petgraph/petgraph/issues/664)).

## [0.6.6](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.6.6) - 2024-12-31

- Add graph6 format encoder and decoder ([#658](https://github.com/petgraph/petgraph/issues/658))
- Dynamic Topological Sort algorithm support ([#675](https://github.com/petgraph/petgraph/issues/675))
- Add ``UndirectedAdaptor`` ([#695](https://github.com/petgraph/petgraph/issues/695))
- Add ``LowerHex`` and ``UpperHex`` implementations for ``Dot`` ([#687](https://github.com/petgraph/petgraph/issues/687))
- Make ``serde`` support more complete ([#550](https://github.com/petgraph/petgraph/issues/550))
- Process multiple edges in the Floyd-Warshall implementation ([#685](https://github.com/petgraph/petgraph/issues/685))
- Update ``fixedbitset`` to 0.5.7 ([#664](https://github.com/petgraph/petgraph/issues/664))
- Fix ``immediately_dominated_by`` function called on root of graph returns root itself ([#670](https://github.com/petgraph/petgraph/issues/670))
- Fix adjacency matrix for ``Csr`` and ``List`` ([#648](https://github.com/petgraph/petgraph/issues/648))
- Fix clippy warnings ([#701](https://github.com/petgraph/petgraph/issues/701))
- Add performance note to the ``all_simple_paths`` function documentation ([#693](https://github.com/petgraph/petgraph/issues/693))


## [0.6.5](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.6.5) - 2024-05-06

- Add rayon support for ``GraphMap`` ([#573](https://github.com/petgraph/petgraph/issues/573), [#615](https://github.com/petgraph/petgraph/issues/615))
- Add ``Topo::with_initials`` method ([#585](https://github.com/petgraph/petgraph/issues/585))
- Add logo to the project ([#598](https://github.com/petgraph/petgraph/issues/598))
- Add Ford-Fulkerson algorithm ([#640](https://github.com/petgraph/petgraph/issues/640))
- Update ``itertools`` to 0.12.1 ([#628](https://github.com/petgraph/petgraph/issues/628))
- Update ``GraphMap`` to allow custom hash functions ([#622](https://github.com/petgraph/petgraph/issues/622))
- Fix documentation ([#630](https://github.com/petgraph/petgraph/issues/630))
- Fix clippy warnings ([#627](https://github.com/petgraph/petgraph/issues/627))
- (internal) Fix remove old ``copyclone`` macro ([#601](https://github.com/petgraph/petgraph/issues/601))
- (internal) Move minimum spanning tree into own module ([#624](https://github.com/petgraph/petgraph/issues/624))


## [0.6.4](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.6.4) - 2023-08-21

- Update ``indexmap`` to 2.0.0 ([#568](https://github.com/petgraph/petgraph/issues/568))
- Fix typos ([#544](https://github.com/petgraph/petgraph/issues/544))


## [0.6.3](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.6.3) - 2023-02-07

- Added an iterator over subgraph isomorphisms ([#500](https://github.com/petgraph/petgraph/issues/500))
- Added serde support on ``GraphMap`` ([#496](https://github.com/petgraph/petgraph/issues/496))
- Added ``reverse`` method for ``StableGraph`` ([#533](https://github.com/petgraph/petgraph/issues/533))
- Added ``edges_connecting`` iterator for ``StableGraph`` ([#521](https://github.com/petgraph/petgraph/issues/521))
- Fix Floyd-Warshall algorithm behaviour on undirected graphs (`487`_)
- Fix IntoEdgesDirected implementation for NodeFiltered when direction is Incoming (`476`_)
- Fix cardinality check in subgraph isomorphism (`472`_)
- Fix UB in ``MatrixGraph`` ([#505](https://github.com/petgraph/petgraph/issues/505))


## [0.6.2](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.6.2) - 2022-05-28

- Loosed the strict version dependency set in `493`_, to allow users to use newer versions of indexmap  (`495`_).


## [0.6.1](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.6.1) - 2022-05-22

- Added clarifications on Graph docs (`491`_).
- Fix build errors on rust 1.41 (`493`_).


## [0.6.0](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.6.0) - 2021-07-04

### Breaking changes

- MSRV is now 1.41 ([#444](https://github.com/petgraph/petgraph/issues/444)).
- Removed the ``NodeCompactIndexable`` trait impl for ``MatrixGraph`` ([#429](https://github.com/petgraph/petgraph/issues/429)).
- The ``IntoEdges::edges`` implementations are now required return edges with the passed node as source ([#433](https://github.com/petgraph/petgraph/issues/433)).

### New features

- Multiple documentation improvements ([#360](https://github.com/petgraph/petgraph/issues/360), [#383](https://github.com/petgraph/petgraph/issues/383), [#426](https://github.com/petgraph/petgraph/issues/426), [#433](https://github.com/petgraph/petgraph/issues/433), [#437](https://github.com/petgraph/petgraph/issues/437), [#443](https://github.com/petgraph/petgraph/issues/443), [#450](https://github.com/petgraph/petgraph/issues/450)).
- Added an ``immediately_dominated_by`` method to the dominators result ([#337](https://github.com/petgraph/petgraph/issues/337)).
- Added ``adj::List``, a new append-only graph type using a simple adjacency list with no node-weights ([#263](https://github.com/petgraph/petgraph/issues/263)).
- Added ``dag_to_toposorted_adjacency_list`` and ``dag_transitive_reduction_closure`` algorithms to transitively reduce an acyclic graph ([#263](https://github.com/petgraph/petgraph/issues/263)).
- Made the ``is_isomorphic`` algorithm generic on both graph types ([#369](https://github.com/petgraph/petgraph/issues/369)).
- Implement Debug and Clone for all the iterators ([#418](https://github.com/petgraph/petgraph/issues/418)).
- Implement multiple mising traits on graph implementations and adapters ([#405](https://github.com/petgraph/petgraph/issues/405), [#429](https://github.com/petgraph/petgraph/issues/429)).
- Add an EdgeIndexable public trait ([#402](https://github.com/petgraph/petgraph/issues/402)).
- Added immutable ``node_weights`` and ``edge_weights`` methods for ``Graph`` and ``StableGraph`` ([#363](https://github.com/petgraph/petgraph/issues/363)).

### New algorithms

- Added a k-shortest-path implementation ([#328](https://github.com/petgraph/petgraph/issues/328)).
- Added a generic graph complement implementation ([#371](https://github.com/petgraph/petgraph/issues/371)).
- Added a maximum matching implementation ([#400](https://github.com/petgraph/petgraph/issues/400)).
- Added a Floyd-Warshall shortest path algorithm ([#377](https://github.com/petgraph/petgraph/issues/377)).
- Added a greedy feedback arc set algorithm ([#386](https://github.com/petgraph/petgraph/issues/386)).
- Added a `find_negative_cycle` algorithm ([#434](https://github.com/petgraph/petgraph/issues/434)).

### Performance

- Reuse the internal state in ``tarjan_scc`` ([#313](https://github.com/petgraph/petgraph/issues/313))
- Reduce memory usage in ``tarjan_scc`` ([#413](https://github.com/petgraph/petgraph/issues/413)).
- Added tighter size hints to all iterators ([#380](https://github.com/petgraph/petgraph/issues/380)).
- Optimized ``petgraph::dot`` a bit ([#424](https://github.com/petgraph/petgraph/issues/424)).
- Optimized StableGraph de-serialization with holes ([#395](https://github.com/petgraph/petgraph/issues/395)).

### Bug fixes

- Fixed A* not producing optimal solutions with inconsistent heuristics ([#379](https://github.com/petgraph/petgraph/issues/379)).
- Fixed a stacked borrow violation ([#404](https://github.com/petgraph/petgraph/issues/404)).
- Fixed a panic in ``StableGraph::extend_with_edges`` ([#415](https://github.com/petgraph/petgraph/issues/415)).
- Fixed multiple bugs in the matrix graph implementation ([#427](https://github.com/petgraph/petgraph/issues/427)).
- Fixed ``GraphMap::remove_node`` not removing some edges ([#432](https://github.com/petgraph/petgraph/issues/432)).
- Fixed all clippy warnings ([#440](https://github.com/petgraph/petgraph/issues/440), [#449](https://github.com/petgraph/petgraph/issues/449)).

### Other changes

- Now using github actions as CI ([#391](https://github.com/petgraph/petgraph/issues/391)).
- Replace matchs on `Option<T>` with `map` ([#381](https://github.com/petgraph/petgraph/issues/381)).
- Added benchmarks for ``tarjan_scc`` ([#421](https://github.com/petgraph/petgraph/issues/421)).



## [0.5.1](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.5.1) - 2020-05-23

- Implement ``Default`` for traversals.
- Export ``EdgesConnecting`` publicly.
- Implement ``is_bipartite_graph``.
- Add ``FilterNode`` implementation for ``FixedBitSet`` and ``HashSet``.
- Implement ``node_weights_mut`` and ``edge_weights_mut`` for ``StableGraph``.
- Add configurable functions for adding attributes to dotfile features.

## [0.5.0](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.5.0) - 2019-12-25

### Breaking changes

- The iterative DFS implementation, ``Dfs``, now marks nodes visited when
  they are pushed onto the stack, not when they're popped off. This may
  require changes to callers that use ``Dfs::from_parts`` or manipulate
  its internals.
- The ``IntoEdgesDirected`` trait now has a stricter contract for
  undirected graphs. Custom implementations of this trait may have to be
  updated. See the `trait documentation`__ for more.

### Other changes

- Upgrade to Rust 2018 edition
- Fix clippy warnings and unify code formatting
- Improved and enhanced documentation
- Update dependencies including modern quickcheck
- Numerous bugfixes and refactorings
- Added ``MatrixGraph`` implementation

__ https://docs.rs/petgraph/0.5/petgraph/visit/trait.IntoEdgesDirected.html

## [0.4.13](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.4.13) - 2018-08-26

- Fix clippy warnings by @jonasbb
- Add docs for ``Csr`` by @ksadorf
- Fix conflict with new stable method ``find_map`` in new Rust

## [0.4.12](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.4.12) - 2018-03-26

- Newtype ``Time`` now also implements ``Hash``
- Documentation updates for ``Frozen``.

## [0.4.11](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.4.11) - 2018-01-07

- Fix ``petgraph::graph::NodeReferences`` to be publicly visible
- Small doc typo and code style files by @shepmaster and @waywardmonkeys
- Fix a future compat warning with pointer casts

## [0.4.10](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.4.10) - 2017-08-15

- Add graph trait ``IntoEdgesDirected``
- Update dependencies

## [0.4.9](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.4.9) - 2017-10-02

- Fix ``bellman_ford`` to work correctly with undirected graphs (#152) by
  @carrutstick
- Performance improvements for ``Graph, Stablegraph``'s ``.map()``.

## [0.4.8](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.4.8) - 2017-09-20

- ``StableGraph`` learned new methods nearing parity with ``Graph``.  Note
  that the ``StableGraph`` methods preserve index stability even in the batch
  removal methods like ``filter_map`` and ``retain_edges``.

  - Added ``.filter_map()``, which maps associated node and edge data
  - Added ``.retain_edges()``, ``.edge_indices()`` and ``.clear_edges()``

- Existing ``Graph`` iterators gained some trait impls:

  - ``.node_indices(), .edge_indices()`` are ``ExactSizeIterator``
  - ``.node_references()`` is now
    ``DoubleEndedIterator + ExactSizeIterator``.
  - ``.edge_references()`` is now ``ExactSizeIterator``.

- Implemented ``From<StableGraph>`` for ``Graph``.

## [0.4.7](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.4.7) - 2017-09-16

- New algorithm by @jmcomets: A* search algorithm in ``petgraph::algo::astar``
- One ``StableGraph`` bug fix whose patch was supposed to be in the previous
  version:
  - ``add_edge(m, n, _)`` now properly always panics if nodes m or n don't
    exist in the graph.

## [0.4.6](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.4.6) - 2017-09-12

- New optional crate feature: ``"serde-1"``, which enables serialization
  for ``Graph`` and ``StableGraph`` using serde.
- Add methods ``new``, ``add_node`` to ``Csr`` by @jmcomets
- Add indexing with ``[]`` by node index, ``NodeCompactIndexable`` for
  ``Csr`` by @jmcomets
- Amend doc for ``GraphMap::into_graph`` (it has a case where it can panic)
- Add implementation of ``From<Graph>`` for ``StableGraph``.
- Add implementation of ``IntoNodeReferences`` for ``&StableGraph``.
- Add method ``StableGraph::map`` that maps associated data
- Add method ``StableGraph::find_edge_undirected``
- Many ``StableGraph`` bug fixes involving node vacancies (holes left by
  deletions):

  - ``neighbors(n)`` and similar neighbor and edge iterator methods now
    handle n being a vacancy properly. (This produces an empty iterator.)
  - ``find_edge(m, n)`` now handles m being a vacancy correctly too
  - ``StableGraph::node_bound`` was fixed for empty graphs and returns 0

- Add implementation of ``DoubleEndedIterator`` to ``Graph, StableGraph``'s
  edge references iterators.
- Debug output for ``Graph`` now shows node and edge count. ``Graph, StableGraph``
  show nothing for the edges list if it's empty (no label).
- ``Arbitrary`` implementation for ``StableGraph`` now can produce graphs with
  vacancies (used by quickcheck)

## [0.4.5](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.4.5) - 2017-06-16

- Fix ``max`` ambiguity error with current rust nightly by @daboross (#153)

## [0.4.4](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.4.4) - 2017-03-14

- Add ``GraphMap::all_edges_mut()`` iterator by @Binero
- Add ``StableGraph::retain_nodes`` by @Rupsbant
- Add ``StableGraph::index_twice_mut`` by @christolliday

## [0.4.3](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.4.3) - 2017-01-21

- Add crate categories

## [0.4.2](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.4.2) - 2017-01-06

- Move the ``visit.rs`` file due to changed rules for a module’s directory
  ownership in Rust, resolving a future compat warning.
- The error types ``Cycle, NegativeCycle`` now implement ``PartialEq``.

## [0.4.1](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.4.1) - 2016-10-26

- Add new algorithm ``simple_fast`` for computing dominators in a control-flow
  graph.

## [0.4.0](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.4.0) - 2016-10-17

### Breaking changes in ``Graph``

- ``Graph::edges`` and the other edges methods now return an iterator of
  edge references

### Other breaking changes

- ``toposort`` now returns an error if the graph had a cycle.
- ``is_cyclic_directed`` no longer takes a dfs space argument. It is
  now recursive.
- ``scc`` was renamed to ``kosaraju_scc``.
- ``min_spanning_tree`` now returns an iterator that needs to be
  made into a specific graph type deliberately.
- ``dijkstra`` now uses the ``IntoEdges`` trait.
- ``NodeIndexable`` changed its method signatures.
- ``IntoExternals`` was removed, and many other smaller adjustments
  in graph traits. ``NodeId`` must now implement ``PartialEq``, for example.
- ``DfsIter, BfsIter`` were removed in favour of a more general approach
  with the ``Walker`` trait and its iterator conversion.

### New features

- New graph traits, for example ``IntoEdges`` which returns
  an iterator of edge references. Everything implements the graph traits
  much more consistently.
- Traits for associated data access and building graphs: ``DataMap``,
  ``Build, Create, FromElements``.
- Graph adaptors: ``EdgeFiltered``. ``Filtered`` was renamed to ``NodeFiltered``.
- New algorithms: bellman-ford
- New graph: compressed sparse row (``Csr``).
- ``GraphMap`` implements ``NodeIndexable``.
- ``Dot`` was generalized

## [0.3.2](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.3.2) - 2016-10-11

  - Add ``depth_first_search``, a recursive dfs visitor that emits discovery,
    finishing and edge classification events.
  - Add graph adaptor ``Filtered``.
  - impl ``Debug, NodeIndexable`` for ``Reversed``.

## [0.3.1](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.3.1) - 2016-10-05

- Add ``.edges(), .edges_directed()`` to ``StableGraph``. Note that these
  differ from ``Graph``, because this is the signature they will all use
  in the future.
- Add ``.update_edge()`` to ``StableGraph``.
- Add reexports of common items in ``stable_graph`` module (for example
  ``NodeIndex``).
- Minor performance improvements to graph iteration
- Improved docs for ``visit`` module.

## [0.3.0](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.3.0) - 2016-10-03

- Overhaul all graph visitor traits so that they use the ``IntoIterator``
  style. This makes them composable.

  - Multiple graph algorithms use new visitor traits.
  - **Help is welcome to port more algorithms (and create new graph traits in
    the process)!**

- ``GraphMap`` can now have directed edges. ``GraphMap::new`` is now generic
  in the edge type. ``DiGraphMap`` and ``UnGraphMap`` are new type aliases.
- Add type aliases ``DiGraph, UnGraph, StableDiGraph, StableUnGraph``
- ``GraphMap`` is based on the indexmap crate. Deterministic iteration
  order, faster iteration, no side tables needed to convert to ``Graph``.
- Improved docs for a lot of types and functions.
- Add graph visitor ``DfsPostOrder``
- ``Dfs`` gained new methods ``from_parts`` and ``reset``.
- New algo ``has_path_connecting``.
- New algo ``tarjan_scc``, a second scc implementation.
- Document traversal order in ``Dfs, DfsPostOrder, scc, tarjan_scc``.
- Optional graph visitor workspace reuse in ``has_path_connecting``,
  ``is_cyclic_directed, toposort``.
- Improved ``Debug`` formatting for ``Graph, StableGraph``.
- Add a prelude module
- ``GraphMap`` now has a method ``.into_graph()`` that makes a ``Graph``.
- ``Graph::retain_nodes, retain_edges`` now expose the self graph only
  as wrapped in ``Frozen``, so that weights can be mutated but the
  graph structure not.
- Enable ``StableGraph`` by default
- Add method ``Graph::contains_edge``.
- Renamed ``EdgeDirection`` → ``Direction``.
- Remove ``SubTopo``.
- Require Rust 1.12 or later

## [0.2.10](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.2.10) - 2016-07-27

- Fix compilation with rust nightly

## [0.2.9](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.2.9) - 2016-10-01

- Fix a bug in SubTopo (#81)

## [0.2.8](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.2.8) - 2016-09-12

- Add Graph methods reserve_nodes, reserve_edges, reserve_exact_nodes,
  reserve_exact_edges, shrink_to_fit_edges, shrink_to_fit_nodes, shrink_to_fit

## [0.2.7](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.2.7) - 2016-04-22

- Update URLs

## [0.2.6](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.2.6) - 2016-04-20

- Fix warning about type parameter defaults (no functional change)

## [0.2.5](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.2.5) - 2016-04-10

- Add SubTopo, a topo walker for the subgraph reachable from a starting point.
- Add condensation, which forms the graph of a graph’s strongly connected
  components.

## [0.2.4](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.2.4) - 2016-04-05

- Fix an algorithm error in scc (#61). This time we have a test that
  crosschecks the result of the algorithm vs another implementation, for
  greater confidence in its correctness.

## [0.2.3](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.2.3) - 2016-02-22

- Require Rust 1.6: Due to changes in how rust uses type parameter defaults.
- Implement Graph::clone_from.

## [0.2.2](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.2.2) - 2015-12-14

- Require Rust 1.5
- ``Dot`` passes on the alternate flag to node and edge label formatting
- Add ``Clone`` impl for some iterators
- Document edge iteration order for ``Graph::neighbors``
- Add *experimental feature* ``StableGraph``, using feature flag ``stable_graph``

## [0.2.1](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.2.1) - 2015-12-06

- Add algorithm ``is_isomorphic_matching``

## [0.2.0](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.2.0) - 2015-12-03

### New Features

- Add Graph::neighbors().detach() to step edges without borrowing.
  This is more general than, and replaces now deprecated
  walk_edges_directed. (#39)
- Implement Default for Graph, GraphMap
- Add method EdgeDirection::opposite()

### Breaking changes

- Graph::neighbors() for undirected graphs and Graph::neighbors_undirected
  for any graph now visit self loop edges once, not twice. (#31)
- Renamed Graph::without_edges to Graph::externals
- Removed Graph::edges_both
- GraphMap::add_edge now returns ``Option<E>``
- Element type of ``GraphMap<N, E>::all_edges()`` changed to ``(N, N, &E)``

### Minor breaking changes

- IntoWeightedEdge changed a type parameter to associated type
- IndexType is now an unsafe trait
- Removed IndexType::{one, zero}, use method new instead.
- Removed MinScored
- Ptr moved to the graphmap module.
- Directed, Undirected are now void enums.
- Fields of graphmap::Edges are now private (#19)

## [0.1.18](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.1.18) - 2015-11-30

- Fix bug on calling GraphMap::add_edge with existing edge (#35)

## [0.1.17](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.1.17) - 2015-11-25

- Add Graph::capacity(), GraphMap::capacity()
- Fix bug in Graph::reverse()
- Graph and GraphMap have `quickcheck::Arbitrary` implementations,
  if optional feature `check` is enabled.

## [0.1.16](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.1.16) - 2015-11-25

- Add Graph::node_indices(), Graph::edge_indices()
- Add Graph::retain_nodes(), Graph::retain_edges()
- Add Graph::extend_with_edges(), Graph::from_edges()
- Add functions petgraph::graph::{edge_index, node_index};
- Add GraphMap::extend(), GraphMap::from_edges()
- Add petgraph::dot::Dot for simple graphviz dot output

## [0.1.15](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.1.15) - 2015-11-20

- Add Graph::clear_edges()
- Add Graph::edge_endpoints()
- Add Graph::map() and Graph::filter_map()

## [0.1.14](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.1.14) - 2015-11-19

- Add new topological order visitor Topo
- New graph traits NeighborsDirected, Externals, Revisitable

## [0.1.13](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.1.13) - 2015-11-11

- Add iterator GraphMap::all_edges

## [0.1.12](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.1.12) - 2015-11-07

- Fix an algorithm error in scc (#14)

## [0.1.11](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.1.11) - 2015-08-16

- Update for well-formedness warnings (Rust RFC 1214), adding
  new lifetime bounds on NeighborIter and Dfs, impact should be minimal.

## [0.1.10](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.1.10) - 2015-06-22

- Fix bug in WalkEdges::next_neighbor()

## [0.1.9](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.1.9) - 2015-06-17

- Fix Dfs/Bfs for a rustc bugfix that disallowed them
- Add method next_neighbor() to WalkEdges

## [0.1.8](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.1.8) - 2015-06-08

- Add Graph::walk_edges_directed()
- Add Graph::index_twice_mut()

## [0.1.7](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.1.7) - 2015-06-08

- Add Graph::edges_directed()

## [0.1.6](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.1.6) - 2015-06-04

- Add Graph::node_weights_mut and Graph::edge_weights_mut

## [0.1.4](https://github.com/petgraph/petgraph/releases/tag/petgraph@v0.1.4) - 2015-05-20

- Add back DfsIter, BfsIter
