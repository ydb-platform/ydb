#include "kqp_stage_graph.h"

#include <yql/essentials/utils/log/log.h>

#include <util/generic/guid.h>

namespace NKikimr::NKqp {

namespace {

using namespace NKikimr;
using namespace NKqp;
using namespace NYql;
using namespace NNodes;

} // anonymous namespace

template <typename DqConnectionType>
TExprNode::TPtr TConnection::BuildConnectionImpl(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx) const {
    // clang-format off
    return Build<DqConnectionType>(ctx, pos)
        .Output()
            .Stage(inputStage)
            .Index().Build(ToString(OutputIndex))
        .Build()
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr TBroadcastConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx, const TPhysicalNames&) const {
    return BuildConnectionImpl<TDqCnBroadcast>(inputStage, pos, ctx);
}

TExprNode::TPtr TMapConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx, const TPhysicalNames&) const {
    return BuildConnectionImpl<TDqCnMap>(inputStage, pos, ctx);
}

TExprNode::TPtr TUnionAllConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx, const TPhysicalNames&) const {
    return Parallel ? BuildConnectionImpl<TDqCnParallelUnionAll>(inputStage, pos, ctx) : BuildConnectionImpl<TDqCnUnionAll>(inputStage, pos, ctx);
}

TExprNode::TPtr TShuffleConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx, const TPhysicalNames& names) const {
    Y_ENSURE(HashFuncType, "Hash function type must be assigned before building a shuffle connection.");

    TVector<TCoAtom> keyColumns;
    for (const auto id : Keys.Items()) {
        keyColumns.emplace_back(Build<TCoAtom>(ctx, pos).Value(names.Get(id)).Done());
    }

    // clang-format off
    return Build<TDqCnHashShuffle>(ctx, pos)
        .Output()
            .Stage(inputStage)
            .Index().Build(ToString(OutputIndex))
        .Build()
        .KeyColumns()
            .Add(keyColumns)
        .Build()
        .UseSpilling().Build(UseSpilling)
        .HashFunc().Build(ToString(*HashFuncType))
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr TMergeConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx, const TPhysicalNames& names) const {
    TVector<TExprNode::TPtr> sortColumns;
    for (const auto& [id, order] : Order.Items()) {
        // clang-format off
        sortColumns.push_back(Build<TDqSortColumn>(ctx, pos)
            .Column<TCoAtom>().Build(names.Get(id))
            .SortDirection().Build(order.Ascending ? TTopSortSettings::AscendingSort : TTopSortSettings::DescendingSort)
            .Done().Ptr());
        // clang-format on
    }

    // clang-format off
    return Build<TDqCnMerge>(ctx, pos)
        .Output()
            .Stage(inputStage)
            .Index().Build(ToString(OutputIndex))
        .Build()
        .SortColumns()
            .Add(sortColumns)
        .Build()
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr TSourceConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx, const TPhysicalNames&) const {
    Y_UNUSED(pos);
    Y_UNUSED(ctx);
    return inputStage;
}

TExprNode::TPtr TStreamLookupConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos, TExprContext& ctx, const TPhysicalNames&) const {
    Y_ENSURE(InputType, "Stream lookup input type has not been set");
    // clang-format off
    return Build<TKqpCnStreamLookup>(ctx, pos)
        .Output()
            .Stage(inputStage)
            .Index().Build(ToString(OutputIndex))
        .Build()
        .Table(Table)
        .Columns(Columns)
        .InputType(InputType)
        .Settings(Settings)
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr TSequencerConnection::BuildConnection(TExprNode::TPtr inputStage, TPositionHandle pos,
    TExprContext& ctx, const TPhysicalNames&) const {
    return Build<TKqpCnSequencer>(ctx, pos)
        .Output().Stage(inputStage).Index().Build(ToString(OutputIndex)).Build()
        .Table(Table)
        .Columns(Columns)
        .DefaultConstraintColumns(GeneratedColumns)
        .InputItemType(InputType)
        .Done().Ptr();
}

TStage& TStageGraph::AddStage() {
    auto stage = std::make_unique<TStage>();
    Y_ENSURE(IdAllocator_, "Cannot add stages to a moved-from graph");
    stage->Id_ = IdAllocator_->Next_++;
    stage->Guid_ = CreateGuidAsString();
    auto& result = *stage;
    Stages_.emplace(result.Id(), std::move(stage));
    StageOrder_.push_back(&result);
    Incoming_.emplace(result.Id(), TVector<const TStageEdge*>{});
    Outgoing_.emplace(result.Id(), TVector<const TStageEdge*>{});
    return result;
}

TStage& TStageGraph::AddSourceStage(NYql::EStorageType storage) {
    Y_ENSURE(storage != NYql::EStorageType::NA, "Source storage must be specified");
    auto& stage = AddStage();
    stage.StorageType_ = storage;
    return stage;
}

TStage& TStageGraph::AddSinkStage(TExprNode::TPtr settings) {
    Y_ENSURE(settings, "Sink settings must be specified");
    auto& stage = AddStage();
    stage.SinkSettings_ = std::move(settings);
    return stage;
}

void TStageGraph::SetIdAllocator(std::shared_ptr<TStageIdAllocator> allocator) {
    Y_ENSURE(Stages_.empty(), "Stage IDs must be reserved before graph construction");
    Y_ENSURE(allocator, "Stage ID allocator must be specified");
    IdAllocator_ = std::move(allocator);
}

const TStage& TStageGraph::GetStage(TStageId id) const {
    const auto it = Stages_.find(id);
    Y_ENSURE(it != Stages_.end(), "Unknown stage " << id);
    return *it->second;
}

TStageEdge& TStageGraph::GetEdge(TStageEdgeId id) {
    const auto index = static_cast<ui32>(id);
    Y_ENSURE(index < Edges_.size(), "Unknown stage edge " << index);
    return *Edges_[index];
}

TVector<TStageEdge*> TStageGraph::Edges() {
    const auto& edges = std::as_const(*this).Edges();
    TVector<TStageEdge*> result;
    result.reserve(edges.size());
    for (const auto* edge : edges) {
        result.push_back(&GetEdge(edge->Id()));
    }
    return result;
}

TVector<TStageEdge*> TStageGraph::IncomingEdges(const TStage& stage) {
    const auto& edges = std::as_const(*this).IncomingEdges(stage);
    TVector<TStageEdge*> result;
    result.reserve(edges.size());
    for (const auto* edge : edges) {
        result.push_back(&GetEdge(edge->Id()));
    }
    return result;
}

TVector<TStageEdge*> TStageGraph::OutgoingEdges(const TStage& stage) {
    const auto& edges = std::as_const(*this).OutgoingEdges(stage);
    TVector<TStageEdge*> result;
    result.reserve(edges.size());
    for (const auto* edge : edges) {
        result.push_back(&GetEdge(edge->Id()));
    }
    return result;
}

TVector<TStageEdge*> TStageGraph::EdgesBetween(const TStage& producer, const TStage& consumer) {
    const auto& edges = std::as_const(*this).EdgesBetween(producer, consumer);
    TVector<TStageEdge*> result;
    result.reserve(edges.size());
    for (const auto* edge : edges) {
        result.push_back(&GetEdge(edge->Id()));
    }
    return result;
}

TStageEdge* TStageGraph::FindUniqueEdge(const TStage& producer, const TStage& consumer) {
    const auto* edge = std::as_const(*this).FindUniqueEdge(producer, consumer);
    return edge ? &GetEdge(edge->Id()) : nullptr;
}

const TStageEdge& TStageGraph::GetEdge(TStageEdgeId id) const {
    const auto index = static_cast<ui32>(id);
    Y_ENSURE(index < Edges_.size(), "Unknown stage edge " << index);
    return *Edges_[index];
}

void TStageGraph::CheckStage(const TStage& stage) const {
    Y_ENSURE(&GetStage(stage.Id()) == &stage, "Stage belongs to another graph");
}

TStage& TStageGraph::MutableStage(const TStage& stage) {
    CheckStage(stage);
    return *Stages_.at(stage.Id());
}


const TVector<const TStageEdge*>& TStageGraph::IncomingEdges(const TStage& stage) const {
    CheckStage(stage);
    return Incoming_.at(stage.Id());
}

const TVector<const TStageEdge*>& TStageGraph::OutgoingEdges(const TStage& stage) const {
    CheckStage(stage);
    return Outgoing_.at(stage.Id());
}

const TVector<const TStageEdge*>& TStageGraph::EdgesBetween(const TStage& producer, const TStage& consumer) const {
    CheckStage(producer);
    CheckStage(consumer);
    const auto it = Between_.find(std::make_pair(producer.Id(), consumer.Id()));
    static const TVector<const TStageEdge*> empty;
    return it == Between_.end() ? empty : it->second;
}

const TStageEdge* TStageGraph::FindUniqueEdge(const TStage& producer, const TStage& consumer) const {
    const auto& edges = EdgesBetween(producer, consumer);
    return edges.size() == 1 ? edges.front() : nullptr;
}

ui32 TStageGraph::RegisterOutput(const TStage& stage, ui32 index) {
    auto& owned = MutableStage(stage);
    Y_ENSURE(index != Max<ui32>(), "Stage output index overflow");
    owned.Outputs_.insert(index);
    owned.NextOutput_ = Max(owned.NextOutput_, index + 1);
    return index;
}

ui32 TStageGraph::AllocateOutput(const TStage& stage) {
    return RegisterOutput(stage, MutableStage(stage).NextOutput_);
}

ui32 TStageGraph::GetOutput(const TStage& stage, ui32 index) const {
    CheckStage(stage);
    Y_ENSURE(stage.Outputs_.contains(index), "Unknown stage output " << index);
    return index;
}

TStageEdge& TStageGraph::Connect(const TStage& producer, const TStage& consumer, TIntrusivePtr<TConnection> connection) {
    CheckStage(producer);
    CheckStage(consumer);
    Y_ENSURE(connection, "Connection settings must be specified");
    RegisterOutput(producer, connection->GetOutputIndex());
    auto edge = std::unique_ptr<TStageEdge>(new TStageEdge(
        static_cast<TStageEdgeId>(Edges_.size()), producer, consumer, connection->Clone()));
    auto& result = *edge;
    Edges_.push_back(std::move(edge));
    EdgeOrder_.push_back(&result);
    Outgoing_.at(producer.Id()).push_back(&result);
    Incoming_.at(consumer.Id()).push_back(&result);
    Between_[std::make_pair(producer.Id(), consumer.Id())].push_back(&result);
    return result;
}

void TStageEdge::ReplaceConnection(TIntrusivePtr<TConnection> connection) {
    Y_ENSURE(connection, "Connection settings must be specified");
    Y_ENSURE(connection->GetOutputIndex() == OutputIndex(), "Replacing a connection must preserve its output index");
    Connection_ = connection->Clone();
}

void TStageEdge::SetShuffleHashFunction(NDq::EHashShuffleFuncType type) {
    auto* shuffle = dynamic_cast<TShuffleConnection*>(Connection_.Get());
    Y_ENSURE(shuffle, "Edge is not a hash shuffle");
    shuffle->HashFuncType = type;
}

void TStageEdge::SetStreamLookupInputType(TExprNode::TPtr type) {
    auto* lookup = dynamic_cast<TStreamLookupConnection*>(Connection_.Get());
    Y_ENSURE(lookup, "Edge is not a stream lookup");
    lookup->SetInputType(std::move(type));
}

TVector<const TStage*> TStageGraph::OrderAcyclicStages() const {
    THashMap<TStageId, size_t> pending;
    TVector<const TStage*> order;
    order.reserve(StageOrder_.size());
    // Seed the ready queue in stage creation order. Appending newly ready stages
    // while visiting outgoing edges in insertion order makes the result repeatable.
    for (const auto* stage : StageOrder_) {
        const auto count = Incoming_.at(stage->Id()).size();
        pending.emplace(stage->Id(), count);
        if (!count) {
            order.push_back(stage);
        }
    }
    for (size_t i = 0; i < order.size(); ++i) {
        for (const auto* edge : Outgoing_.at(order[i]->Id())) {
            // Count edges, not distinct producers: parallel inputs must each
            // discharge the dependency counted in Incoming_ above.
            if (--pending.at(edge->Consumer().Id()) == 0) {
                order.push_back(&edge->Consumer());
            }
        }
    }
    return order;
}

TVector<const TStage*> TStageGraph::TopologicalOrder() const {
    auto order = OrderAcyclicStages();
    Y_DEBUG_ABORT_UNLESS(order.size() == StageOrder_.size(), "Stage graph contains a cycle");
    return order;
}

bool TStageGraph::ContainsCycles() const {
    return OrderAcyclicStages().size() != StageOrder_.size();
}

bool TStageGraph::IsConnected() const {
    if (StageOrder_.empty()) {
        return false;
    }
    THashSet<TStageId> visited{StageOrder_.front()->Id()};
    TVector<const TStage*> pending{StageOrder_.front()};
    auto visit = [&](const TStage& stage) {
        if (visited.insert(stage.Id()).second) {
            pending.push_back(&stage);
        }
    };
    // Walk both directions for weak connectivity. The visited set also makes
    // this diagnostic safe on malformed cyclic graphs and parallel connections.
    for (size_t i = 0; i < pending.size(); ++i) {
        const auto id = pending[i]->Id();
        for (const auto* edge : Incoming_.at(id)) {
            visit(edge->Producer());
        }
        for (const auto* edge : Outgoing_.at(id)) {
            visit(edge->Consumer());
        }
    }
    return visited.size() == StageOrder_.size();
}

} // namespace NKikimr::NKqp
