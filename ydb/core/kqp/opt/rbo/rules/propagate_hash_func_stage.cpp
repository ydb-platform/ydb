#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

namespace NKikimr::NKqp {

namespace {

using namespace NKikimr::NKqp;
using namespace NYql;

void PropagateHashFuncs(TStageGraph& stageGraph, const TKikimrConfiguration::TPtr& config) {
    const auto defaultHashType = config->HashShuffleFuncType.Get().GetOrElse(config->GetDqDefaultHashShuffleFuncType());
    const auto columnShardHashType = config->ColumnShardHashShuffleFuncType.Get().GetOrElse(NDq::EHashShuffleFuncType::ColumnShardHashV1);

    // Source stages introduce ColumnShardHashV1 partitioning. Map connections
    // preserve the producer hash, so sibling HashShuffle connections must use
    // the same function.
    THashMap<ui32, NDq::EHashShuffleFuncType> hashTypeByStageId;
    for (const auto* stage : stageGraph.TopologicalOrder()) {
        auto stageHashType = defaultHashType;
        if (stage->IsSource()) {
            stageHashType = columnShardHashType;
        } else {
            for (auto* edge : stageGraph.IncomingEdges(*stage)) {
                if (IsConnection<TMapConnection>(edge->Connection())) {
                    stageHashType = hashTypeByStageId.at(edge->Producer().Id());
                }
            }
        }
        hashTypeByStageId[stage->Id()] = stageHashType;
        for (auto* edge : stageGraph.IncomingEdges(*stage)) {
            if (IsConnection<TShuffleConnection>(edge->Connection())) {
                edge->SetShuffleHashFunction(stageHashType);
            }
        }
    }
}

} // anonymous namespace

TPropagateHashFuncStage::TPropagateHashFuncStage()
    : IRBOStage("Hash function propagation")
{}

void TPropagateHashFuncStage::RunStage(TOpRoot& root, TRBOContext& ctx) {
    PropagateHashFuncs(root.PlanProps.StageGraph, ctx.KqpCtx.Config);
}

} // namespace NKikimr::NKqp
