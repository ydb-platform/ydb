#ifndef TOPOLOGICAL_ORDERING_INL_H_
#error "Direct inclusion of this file is not allowed, include topological_ordering.h"
// For the sake of sane code completion.
#include "topological_ordering.h"
#endif

#include "topological_ordering.h"

#include "serialize.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <typename TVertexDescriptor>
const std::vector<TVertexDescriptor>& TIncrementalTopologicalOrdering<TVertexDescriptor>::GetOrdering() const
{
    return TopologicalOrdering_;
}

template <typename TVertexDescriptor>
void TIncrementalTopologicalOrdering<TVertexDescriptor>::AddEdge(const TVertexDescriptor& from, const TVertexDescriptor& to)
{
    if (OutgoingEdges_[from].insert(to).second) {
        Rebuild();
    }
}

template <typename TVertexDescriptor>
void TIncrementalTopologicalOrdering<TVertexDescriptor>::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, OutgoingEdges_);
    Persist(context, TopologicalOrdering_);
}

template <typename TVertexDescriptor>
void TIncrementalTopologicalOrdering<TVertexDescriptor>::Rebuild()
{
    std::queue<TVertexDescriptor> queue;
    THashMap<TVertexDescriptor, int> inDegree;

    // Initialize in-degrees of all vertices.
    for (const auto& [srcVertex, dstVertices] : OutgoingEdges_) {
        // Make an entry for the vertex appear in the inDegree.
        inDegree[srcVertex];
        for (const auto& dstVertex : dstVertices) {
            ++inDegree[dstVertex];
        }
    }

    // Put all sources in the queue.
    for (const auto& [vertex, degree] : inDegree) {
        if (degree == 0) {
            queue.push(vertex);
        }
    }

    TopologicalOrdering_.clear();

    // Extract sources and put them into the ordering while graph is non-empty.
    while (!queue.empty()) {
        const auto& vertex = queue.front();
        queue.pop();

        auto it = inDegree.find(vertex);
        YT_VERIFY(it != inDegree.end() && it->second == 0);
        inDegree.erase(it);

        TopologicalOrdering_.push_back(vertex);

        for (const auto& nextVertex : OutgoingEdges_[vertex]) {
            if (--inDegree[nextVertex] == 0) {
                queue.push(nextVertex);
            }
        }
    }

    // Check that all vertices are visited.
    YT_VERIFY(inDegree.empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
