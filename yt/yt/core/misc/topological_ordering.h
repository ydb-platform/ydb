#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Class representing a directed acyclic graph with its topological ordering
//! allowing you to insert new edges in the graph.
/*!
 * Vertices of graph are represented with integers.
 * TODO(max42): http://people.cs.georgetown.edu/~jfineman/papers/topsort.pdf :)
 */
template <typename TVertexDescriptor>
class TIncrementalTopologicalOrdering
{
public:
    TIncrementalTopologicalOrdering() = default;

    const std::vector<TVertexDescriptor>& GetOrdering() const;

    void Persist(const TStreamPersistenceContext& context);

    void AddEdge(const TVertexDescriptor& from, const TVertexDescriptor& to);

private:
    std::vector<TVertexDescriptor> TopologicalOrdering_;
    THashMap<TVertexDescriptor, THashSet<TVertexDescriptor>> OutgoingEdges_;

    void Rebuild();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define TOPOLOGICAL_ORDERING_INL_H_
#include "topological_ordering-inl.h"
#undef TOPOLOGICAL_ORDERING_INL_H_
