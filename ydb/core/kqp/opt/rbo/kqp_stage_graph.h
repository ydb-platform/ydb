#pragma once

#include "kqp_info_unit.h"
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <yql/essentials/core/yql_statistics.h>

#include <optional>
#include <memory>

namespace NKikimr::NKqp {

struct TSortOrder {
    bool Ascending = true;
    bool NullsFirst = true;
};
using TSortIUs = TOrderedIUs<TSortOrder>;

// Polymorphic settings for transferring one producer output to a consumer.
// OutputIndex identifies an output of the producer, not an input of the consumer.
// Construct settings with the concrete connection constructors, then pass them to
// TStageGraph::Connect or TStageEdge::ReplaceConnection. Both clone the settings,
// so later changes to the caller's settings object do not change the graph.
// Expression nodes referenced by settings remain shared through intrusive pointers.
struct TConnection: TSimpleRefCount<TConnection> {
    TConnection(TString type, ui32 outputIndex)
        : Type(std::move(type))
        , OutputIndex(outputIndex) {
    }
    virtual ~TConnection() = default;
    // Copy the concrete settings object, preserving its dynamic type. Referenced
    // expression nodes are shared; this is not a deep copy of the expression tree.
    virtual TIntrusivePtr<TConnection> Clone() const = 0;

    // Lower these settings into a physical connection to inputStage (the producer).
    // names resolves information-unit IDs used by keys/order into physical columns.
    // Type-specific settings must be complete before lowering; for example, a
    // shuffle needs its hash function and a stream lookup needs its input type.
    virtual NYql::TExprNode::TPtr BuildConnection(NYql::TExprNode::TPtr inputStage, NYql::TPositionHandle pos, NYql::TExprContext& ctx, const TPhysicalNames& names) const = 0;
    // Build a connection whose only setting is the selected producer output.
    template <typename T>
    NYql::TExprNode::TPtr BuildConnectionImpl(NYql::TExprNode::TPtr inputStage, NYql::TPositionHandle pos, NYql::TExprContext& ctx) const;
    // Columns required by the exchange itself, even if the consumer does not use
    // them (for example, shuffle or merge keys). Used by liveness analysis.
    // The returned set is borrowed from these settings or is a static empty set.
    virtual const TUnorderedIUs& GetUsedIUs() const {
        static const TUnorderedIUs empty;
        return empty;
    }
    // Describe the connection for explain output, resolving IDs via the registry.
    virtual NJson::TJsonValue ToJson(const TInfoUnitRegistry& registry) const;

    // Index in the producer stage output list. It is local to that stage.
    ui32 GetOutputIndex() const { return OutputIndex; }

    TString Type;
    ui32 OutputIndex;
};

// Broadcast the selected producer output to the consumer tasks.
struct TBroadcastConnection: public TConnection {
    TIntrusivePtr<TConnection> Clone() const override { return MakeIntrusive<TBroadcastConnection>(*this); }
    explicit TBroadcastConnection(ui32 outputIndex)
        : TConnection("Broadcast", outputIndex) {
    }
    virtual NYql::TExprNode::TPtr BuildConnection(NYql::TExprNode::TPtr inputStage, NYql::TPositionHandle pos, NYql::TExprContext& ctx, const TPhysicalNames& names) const override;
};

// Map producer output partitions to consumer tasks without repartitioning.
struct TMapConnection: public TConnection {
    TIntrusivePtr<TConnection> Clone() const override { return MakeIntrusive<TMapConnection>(*this); }
    explicit TMapConnection(ui32 outputIndex)
        : TConnection("Map", outputIndex) {
    }
    virtual NYql::TExprNode::TPtr BuildConnection(NYql::TExprNode::TPtr inputStage, NYql::TPositionHandle pos, NYql::TExprContext& ctx, const TPhysicalNames& names) const override;
};

// Combine producer output streams using UnionAll; parallel selects ParallelUnionAll.
struct TUnionAllConnection: public TConnection {
    TIntrusivePtr<TConnection> Clone() const override { return MakeIntrusive<TUnionAllConnection>(*this); }
    TUnionAllConnection(ui32 outputIndex, bool parallel = false)
        : TConnection("UnionAll", outputIndex)
        , Parallel(parallel) {
    }
    virtual NYql::TExprNode::TPtr BuildConnection(NYql::TExprNode::TPtr inputStage, NYql::TPositionHandle pos, NYql::TExprContext& ctx, const TPhysicalNames& names) const override;
    virtual NJson::TJsonValue ToJson(const TInfoUnitRegistry& registry) const override;

private:
    bool Parallel{false};
};

// Partition rows by the ordered key columns using HashFuncType.
// Key order is significant for matching partitioning between stages.
// HashFuncType is assigned during hash propagation before physical lowering.
struct TShuffleConnection: public TConnection {
    TIntrusivePtr<TConnection> Clone() const override { return MakeIntrusive<TShuffleConnection>(*this); }
    TShuffleConnection(TOrderedIUs<> keys, ui32 outputIndex,
                       bool useSpilling = false)
        : TConnection("HashShuffle", outputIndex)
        , Keys(std::move(keys))
        , UseSpilling(useSpilling) {
    }

    virtual NYql::TExprNode::TPtr BuildConnection(NYql::TExprNode::TPtr inputStage, NYql::TPositionHandle pos, NYql::TExprContext& ctx, const TPhysicalNames& names) const override;
    const TUnorderedIUs& GetUsedIUs() const override { return Keys.Unordered(); }
    virtual NJson::TJsonValue ToJson(const TInfoUnitRegistry& registry) const override;

    TOrderedIUs<> Keys;
    std::optional<NYql::NDq::EHashShuffleFuncType> HashFuncType;
    bool UseSpilling = false;
};

// Merge streams already sorted by Order into a sorted consumer input.
// The ordering columns remain live even when they are absent from the result.
struct TMergeConnection: public TConnection {
    TIntrusivePtr<TConnection> Clone() const override { return MakeIntrusive<TMergeConnection>(*this); }
    TMergeConnection(TSortIUs order, ui32 outputIndex)
        : TConnection("Merge", outputIndex)
        , Order(std::move(order)) {
    }

    virtual NYql::TExprNode::TPtr BuildConnection(NYql::TExprNode::TPtr inputStage, NYql::TPositionHandle pos, NYql::TExprContext& ctx, const TPhysicalNames& names) const override;
    const TUnorderedIUs& GetUsedIUs() const override { return Order.Unordered(); }
    virtual NJson::TJsonValue ToJson(const TInfoUnitRegistry& registry) const override;

    TSortIUs Order;
};

// Represent a source input in the graph despite the special handling required
// by data shard sources. Uses output index zero; BuildConnection returns the
// supplied source expression unchanged instead of wrapping it in a DQ connection.
struct TSourceConnection: public TConnection {
    TIntrusivePtr<TConnection> Clone() const override { return MakeIntrusive<TSourceConnection>(*this); }
    TSourceConnection()
        : TConnection("Source", 0) {
    }
    virtual NYql::TExprNode::TPtr BuildConnection(NYql::TExprNode::TPtr inputStage, NYql::TPositionHandle pos, NYql::TExprContext& ctx, const TPhysicalNames& names) const override;
};

// Look up table rows using keys supplied by the selected producer output.
// Table, Columns and Settings describe the lookup; InputType describes the rows
// crossing the edge and may be filled in later during physical conversion.
struct TStreamLookupConnection: public TConnection {
    TIntrusivePtr<TConnection> Clone() const override { return MakeIntrusive<TStreamLookupConnection>(*this); }
    TStreamLookupConnection(ui32 outputIndex, NYql::TExprNode::TPtr table, NYql::TExprNode::TPtr columns,
                            NYql::TExprNode::TPtr inputType, NYql::TExprNode::TPtr settings)
        : TConnection("StreamLookup", outputIndex)
        , Table(table)
        , Columns(columns)
        , InputType(inputType)
        , Settings(settings) {
    }
    virtual NYql::TExprNode::TPtr BuildConnection(NYql::TExprNode::TPtr inputStage, NYql::TPositionHandle pos, NYql::TExprContext& ctx, const TPhysicalNames& names) const override;

    // In join mode the input type describes the tuples that the physical conversion builds at the
    // end of the input stage, so it can only be filled in once that expression exists.
    void SetInputType(NYql::TExprNode::TPtr inputType) {
        InputType = std::move(inputType);
    }

    NYql::TExprNode::TPtr Table;
    NYql::TExprNode::TPtr Columns;
    NYql::TExprNode::TPtr InputType;
    NYql::TExprNode::TPtr Settings;
};

// Pass producer rows through the sequencer for generated/default columns.
// Table and Columns identify the destination schema; GeneratedColumns and
// InputType describe the generated values and incoming row type.
struct TSequencerConnection: public TConnection {
    TIntrusivePtr<TConnection> Clone() const override { return MakeIntrusive<TSequencerConnection>(*this); }
    TSequencerConnection(ui32 outputIndex, NYql::TExprNode::TPtr table, NYql::TExprNode::TPtr columns,
        NYql::TExprNode::TPtr generatedColumns, NYql::TExprNode::TPtr inputType)
        : TConnection("Sequencer", outputIndex)
        , Table(std::move(table))
        , Columns(std::move(columns))
        , GeneratedColumns(std::move(generatedColumns))
        , InputType(std::move(inputType))
    {}

    NYql::TExprNode::TPtr BuildConnection(NYql::TExprNode::TPtr inputStage, NYql::TPositionHandle pos,
        NYql::TExprContext& ctx, const TPhysicalNames& names) const override;

    NYql::TExprNode::TPtr Table;
    NYql::TExprNode::TPtr Columns;
    NYql::TExprNode::TPtr GeneratedColumns;
    NYql::TExprNode::TPtr InputType;
};

// Test the concrete settings type without exposing mutable graph-owned settings.
template <typename T>
bool IsConnection(const TConnection& connection) {
    return dynamic_cast<const T*>(&connection);
}

// Stage IDs are unique within an allocator; graphs sharing an allocator therefore
// have disjoint stage IDs. Edge IDs are local to one graph and are a distinct type.
using TStageId = ui32;
enum class TStageEdgeId : ui32 {};

class TStageGraph;

// Monotonic stage-ID allocation shared by graphs belonging to one query.
// Only TStageGraph allocates IDs. Sharing this object does not merge graphs or
// allow stages from one graph to be used as endpoints in another.
class TStageIdAllocator {
private:
    friend class TStageGraph;
    TStageId Next_ = 0;
};

// A graph-owned stage: identity, source/sink properties and output reservations.
// Create stages through TStageGraph::Add*Stage so identity and indexes are set up.
// A stage stores no edges or graph pointer; all adjacency queries go through the
// owning graph. Source/sink flags describe stage roles, not vertex degree.
// Addresses remain stable when the graph grows or is moved.
class TStage {
public:
    TStage() = default;
    TStage(const TStage&) = delete;
    TStage& operator=(const TStage&) = delete;

    // Numeric identity allocated by the graph's TStageIdAllocator.
    TStageId Id() const { return Id_; }

    // Persistent GUID assigned at creation, used when building physical stages.
    const TString& Guid() const { return Guid_; }

    // True for a stage created with source storage metadata.
    bool IsSource() const { return StorageType_ != NYql::EStorageType::NA; }

    // True for a stage carrying sink settings.
    bool IsSink() const { return bool(SinkSettings_); }

    // Source storage kind, or NA for a stage without source metadata.
    NYql::EStorageType StorageType() const { return StorageType_; }

    // Return the shared settings expression. Requires IsSink(); otherwise throws.
    NYql::TExprNode::TPtr SinkSettings() const {
        Y_ENSURE(IsSink(), "Stage has no sink settings");
        return SinkSettings_;
    }

private:
    friend class TStageGraph;
    TStageId Id_ = 0;
    TString Guid_;
    NYql::EStorageType StorageType_ = NYql::EStorageType::NA;
    NYql::TExprNode::TPtr SinkSettings_;
    THashSet<ui32> Outputs_;
    ui32 NextOutput_ = 0;
};

// One directed producer -> consumer connection owned by TStageGraph.
// Parallel edges between the same stages are distinct objects with distinct IDs.
// Endpoints, identity and producer output index stay fixed; only connection
// settings can change. Keeping payload changes here preserves graph adjacency.
class TStageEdge {
public:
    // Stable identity within the owning graph; not interchangeable across graphs.
    TStageEdgeId Id() const { return Id_; }

    // Borrow the stages at the sending and receiving ends of this edge.
    const TStage& Producer() const { return *Producer_; }
    const TStage& Consumer() const { return *Consumer_; }

    // Registered output of Producer(). Multiple edges may use the same output.
    ui32 OutputIndex() const { return Connection_->GetOutputIndex(); }

    // Borrow the current settings. ReplaceConnection invalidates this reference;
    // the other setters update the existing settings object in place.
    const TConnection& Connection() const { return *Connection_; }

    // Clone new settings, possibly changing connection kind. Requires non-null
    // settings with the same output index; throws otherwise. Does not change
    // edge identity, endpoints, adjacency order or the stage output reservations.
    void ReplaceConnection(TIntrusivePtr<TConnection> connection);

    // Set the hash algorithm on a shuffle edge; throws for other connection kinds.
    void SetShuffleHashFunction(NYql::NDq::EHashShuffleFuncType type);

    // Fill in the physical input row type after constructing lookup keys.
    // Throws unless this is a stream lookup edge. A non-null type is required
    // when BuildConnection is eventually called.
    void SetStreamLookupInputType(NYql::TExprNode::TPtr type);

private:
    friend class TStageGraph;
    TStageEdge(TStageEdgeId id, const TStage& producer, const TStage& consumer,
        TIntrusivePtr<TConnection> connection)
        : Id_(id), Producer_(&producer), Consumer_(&consumer), Connection_(std::move(connection)) {}
    TStageEdgeId Id_;
    const TStage* Producer_;
    const TStage* Consumer_;
    TIntrusivePtr<TConnection> Connection_;
};

// Owns stages and directed edges, with adjacency in connection insertion order.
// Parallel edges are allowed. The graph must be a DAG: callers must preserve this
// invariant because Connect deliberately performs no cycle traversal. Optimizer
// boundaries use ContainsCycles in debug assertions. Connectivity can be checked
// separately; a graph under construction may contain disconnected components.
//
// Stage and edge addresses survive additions and moving the graph. They remain
// borrowed objects: destroying their owner (or overwriting it by move assignment)
// destroys them. Graph methods taking stages reject stages from another graph,
// even if their numeric IDs happen to match.
//
// Const edge-range methods borrow internal vectors without copying. Do not retain
// their iterators or references across structural graph changes. Mutable overloads
// return independent vectors of borrowed mutable pointers; editing those vectors
// does not edit graph topology. Copy pointers out of temporary vectors:
//     auto* edge = graph.IncomingEdges(stage).front();  // after checking nonempty
// Binding const auto& to that .front() would dangle at the end of the statement.
// Payload changes do not invalidate stage/edge pointers or adjacency order.
class TStageGraph {
public:
    TStageGraph() = default;
    TStageGraph(TStageGraph&&) = default;
    TStageGraph& operator=(TStageGraph&&) = default;
    TStageGraph(const TStageGraph&) = delete;
    TStageGraph& operator=(const TStageGraph&) = delete;

    // Create an isolated stage with a fresh numeric ID and GUID, no reserved
    // outputs, and no source/sink metadata. Appends to stage creation order.
    TStage& AddStage();

    // Create an isolated source stage. Throws if storage is NA.
    TStage& AddSourceStage(NYql::EStorageType storage);

    // Create an isolated sink stage retaining the supplied settings expression.
    // Throws for null settings.
    TStage& AddSinkStage(NYql::TExprNode::TPtr settings);

    // Borrow all stages in creation order, which need not be topological order.
    const TVector<const TStage*>& Stages() const { return StageOrder_; }

    // All edges in Connect order, including parallel edges. The mutable overload
    // copies the pointer vector; the const overload borrows it without copying.
    TVector<TStageEdge*> Edges();
    const TVector<const TStageEdge*>& Edges() const { return EdgeOrder_; }

    // Resolve an ID in this graph. Throws if no stage with that ID exists here.
    const TStage& GetStage(TStageId id) const;

    // Resolve a graph-local edge ID. Throws if unknown. The mutable overload
    // permits settings changes through the edge, but no endpoint changes.
    TStageEdge& GetEdge(TStageEdgeId id);
    const TStageEdge& GetEdge(TStageEdgeId id) const;

    // All edges whose consumer is stage, in Connect order. This ordering is used
    // for physical stage inputs. Returns an empty vector for a stage with no
    // inputs. Mutable overload: pointer-vector copy; const overload: borrowed vector.
    TVector<TStageEdge*> IncomingEdges(const TStage& stage);
    const TVector<const TStageEdge*>& IncomingEdges(const TStage& stage) const;

    // All edges whose producer is stage, in Connect order; empty if none.
    // Mutable overload: pointer-vector copy; const overload: borrowed vector.
    TVector<TStageEdge*> OutgoingEdges(const TStage& stage);
    const TVector<const TStageEdge*>& OutgoingEdges(const TStage& stage) const;

    // All direct producer -> consumer edges in Connect order; does not search
    // paths or reverse edges. Empty if none. Mutable overload copies the pointer
    // vector; const overload borrows it. Both endpoints must belong to this graph.
    TVector<TStageEdge*> EdgesBetween(const TStage& producer, const TStage& consumer);
    const TVector<const TStageEdge*>& EdgesBetween(const TStage& producer, const TStage& consumer) const;

    // Return the direct edge only if exactly one exists for these endpoints.
    // Returns nullptr for both missing and ambiguous (parallel) connections.
    // Both endpoints must belong to this graph.
    TStageEdge* FindUniqueEdge(const TStage& producer, const TStage& consumer);
    const TStageEdge* FindUniqueEdge(const TStage& producer, const TStage& consumer) const;

    // Reserve and return the next output index, starting at zero and advancing
    // beyond all explicit registrations. Does not reuse gaps or create an edge.
    ui32 AllocateOutput(const TStage& stage);

    // Reserve an explicit output index and return it. Re-registering an existing
    // index is allowed, enabling multiple edges to share an output. Advances
    // AllocateOutput past index. Max<ui32>() is rejected to prevent overflow.
    ui32 RegisterOutput(const TStage& stage, ui32 index);

    // Check that index is reserved on stage and return it; throws if unknown.
    // Does not reserve an output or require that an edge already uses it.
    ui32 GetOutput(const TStage& stage, ui32 index) const;

    // Append a producer -> consumer edge with its own copy of connection settings.
    // Registers the selected producer output (including explicit indices) and
    // updates all adjacency indexes. Both stages must belong to this graph and
    // settings must be non-null. Parallel edges and shared outputs are allowed.
    // Callers must ensure this creates no directed cycle, including a self-loop.
    // No cycle/connectivity scan is performed here; use debug checks at boundaries.
    TStageEdge& Connect(const TStage& producer, const TStage& consumer, TIntrusivePtr<TConnection> connection);

    // Return all stages with producers before consumers, including disconnected
    // components. Deterministic for a fixed insertion order; does not reorder
    // Stages(). Requires a DAG and asserts that requirement in debug builds.
    // Runs in O(stages + edges) time with O(stages) auxiliary storage.
    TVector<const TStage*> TopologicalOrder() const;

    // Test weak connectivity: ignore edge directions and check that every stage
    // is reachable from one stage. Empty graph: false; single stage: true.
    // Works even for cyclic graphs; does not imply acyclicity or a unique sink.
    // Runs in O(stages + edges) time with O(stages) auxiliary storage.
    bool IsConnected() const;

    // Detect any directed cycle, including self-loops and disconnected cycles.
    // Empty graph: false. Parallel edges alone do not constitute a cycle.
    // Runs in O(stages + edges) time with O(stages) auxiliary storage. Available
    // in all builds; intended for debug sanity checks rather than every Connect.
    bool ContainsCycles() const;

    // Install a shared allocator before adding stages, so separate query-root
    // graphs get disjoint stage IDs. Requires an empty graph and non-null allocator.
    // Without this call, each graph uses its own allocator starting at zero.
    void SetIdAllocator(std::shared_ptr<TStageIdAllocator> allocator);

private:
    // Check ownership before accessing graph-owned mutable stage properties.
    TStage& MutableStage(const TStage& stage);

    // Compare object identity as well as ID; matching numeric IDs are insufficient.
    void CheckStage(const TStage& stage) const;

    // Kahn traversal shared by ordering and cycle detection. Counts each parallel
    // edge separately. On a cyclic graph returns only the stages that can be
    // processed, allowing ContainsCycles to detect that some stages remain.
    TVector<const TStage*> OrderAcyclicStages() const;

    std::shared_ptr<TStageIdAllocator> IdAllocator_ = std::make_shared<TStageIdAllocator>();
    // Owning containers keep objects stable; ordered vectors and adjacency maps
    // below are non-owning indexes maintained together by AddStage and Connect.
    THashMap<TStageId, std::unique_ptr<TStage>> Stages_;
    TVector<const TStage*> StageOrder_;
    TVector<std::unique_ptr<TStageEdge>> Edges_;
    TVector<const TStageEdge*> EdgeOrder_;
    THashMap<TStageId, TVector<const TStageEdge*>> Incoming_;
    THashMap<TStageId, TVector<const TStageEdge*>> Outgoing_;
    THashMap<std::pair<TStageId, TStageId>, TVector<const TStageEdge*>> Between_;
};

} // namespace NKikimr::NKqp
