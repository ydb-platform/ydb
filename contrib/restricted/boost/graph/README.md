# Boost Graph Library

[![Docs](https://img.shields.io/badge/docs-boost.org-blue.svg)](https://www.boost.org/doc/libs/release/libs/graph/doc/index.html)
[![C++14](https://img.shields.io/badge/C%2B%2B-14-blue.svg)](https://en.cppreference.com/w/cpp/14)
[![CI](https://github.com/boostorg/graph/actions/workflows/ci.yml/badge.svg)](https://github.com/boostorg/graph/actions/workflows/ci.yml)
[![License: BSL-1.0](https://img.shields.io/badge/License-BSL_1.0-blue.svg)](https://www.boost.org/LICENSE_1_0.txt)
[![Boost release](https://img.shields.io/github/v/release/boostorg/boost?label=Boost&color=orange)](https://github.com/boostorg/boost/releases)

The Boost Graph Library (BGL) is a generic library that allows users to:

1. Represent graph data using different structures (adjacency matrix, adjacency list, compressed sparse row, vectors of vectors, user-defined data structures).
2. Attach user-defined data to vertices, edges, or the graph itself.
3. Run a large number of algorithms on the graph.
4. Inject user logic into algorithms using visitor hooks.

## Example

[Try it on Compiler Explorer](https://godbolt.org/z/9Esszr9Ga)

```cpp
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/dijkstra_shortest_paths.hpp>
#include <boost/graph/visitors.hpp>
#include <iostream>
#include <limits>
#include <vector>

struct City {};
struct Road { int cost; };

using namespace boost;
using Graph = adjacency_list<vecS, vecS, directedS, City, Road>;
using Vertex = graph_traits<Graph>::vertex_descriptor;

int main() {
    Graph g(4);
    add_edge(0, 1, Road{1}, g);
    add_edge(1, 2, Road{2}, g);
    add_edge(0, 2, Road{10}, g);
    add_edge(2, 3, Road{1}, g);

    // Storage: you control allocation, lifetime, and container type
    std::vector<Vertex> storage_pred(num_vertices(g));
    std::vector<int>    storage_dist(num_vertices(g));

    // Property maps: lightweight views into the storage
    auto index_map       = get(vertex_index, g);
    auto costs_map       = get(&Road::cost, g);
    auto predecessor_map = make_iterator_property_map(storage_pred.begin(), index_map);
    auto distance_map    = make_iterator_property_map(storage_dist.begin(), index_map);

    dijkstra_shortest_paths(g, vertex(0, g),
        predecessor_map, distance_map,
        costs_map, index_map,
        std::less<int>(), std::plus<int>(),
        std::numeric_limits<int>::max(), 0,
        dijkstra_visitor<null_visitor>());

    for (auto v : make_iterator_range(vertices(g)))
        std::cout << "distance to " << v << " = " << storage_dist[v] << "\n";
}
```
```
distance to 0 = 0
distance to 1 = 1
distance to 2 = 3
distance to 3 = 4
```

## Algorithms

BGL ships dozens of graph algorithms: shortest paths (Dijkstra, Bellman-Ford, A*, Floyd-Warshall, Johnson), spanning trees (Kruskal, Prim), maximum flow (Edmonds-Karp, push-relabel, Boykov-Kolmogorov), traversal (BFS, DFS, topological sort), planarity testing, isomorphism, component decomposition, and more.

See the [full algorithm reference](https://becheler.github.io/graph/graph/algorithms/overview.html) for the complete catalogue.

## Help and feedback

* **[GitHub Issues](https://github.com/boostorg/graph/issues)** for bug reports. Search before opening a new one.
* **[GitHub Discussions](https://github.com/boostorg/graph/discussions)** for questions, design ideas, and general conversation about the library.
* **[Boost mailing list](http://lists.boost.org/mailman/listinfo.cgi/boost-users)** for general Boost development. Use the `[graph]` tag in the subject line.
* **CppLang Slack** for real-time chat. [Request an invite](https://cppalliance.org/slack/), then join the `#boost` channel.
* **Direct contact with maintainers**: see [CONTRIBUTING.md#maintainers](CONTRIBUTING.md#maintainers).

## Using BGL

Install Boost via your package manager:

| Manager | Command |
|---|---|
| [vcpkg](https://vcpkg.io) | `vcpkg install boost-graph` |
| [Conan](https://conan.io) | `conan install --requires=boost/[*]` |
| apt (Debian/Ubuntu) | `sudo apt install libboost-graph-dev` |
| Homebrew (macOS) | `brew install boost` |

Then wire it into CMake:

```cmake
find_package(Boost REQUIRED COMPONENTS graph)
target_link_libraries(my_app PRIVATE Boost::graph)
```

Most of BGL is header-only. Linking `Boost::graph` is only required for the GraphViz and GraphML parsers.

## Building from source

For working on BGL itself (building Boost from source, running the test suite), see [CONTRIBUTING.md](CONTRIBUTING.md).
