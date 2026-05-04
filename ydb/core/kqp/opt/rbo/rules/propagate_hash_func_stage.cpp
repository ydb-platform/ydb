#include "kqp_rules_include.h"

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
    for (const auto stageId : stageGraph.GetTopologicalOrder()) {
        auto stageHashType = defaultHashType;

        if (stageGraph.IsSourceStage(stageId)) {
            stageHashType = columnShardHashType;
        } else {
            for (const auto inputStageId : stageGraph.StageInputs.at(stageId)) {
                for (const auto& connection : stageGraph.GetConnections(inputStageId, stageId)) {
                    if (!IsConnection<TMapConnection>(connection)) {
                        continue;
                    }

                    const auto it = hashTypeByStageId.find(inputStageId);
                    Y_ENSURE(it != hashTypeByStageId.end(), "Input stage hash function type is not initialized.");
                    stageHashType = it->second;
                }
            }
        }

        hashTypeByStageId[stageId] = stageHashType;

        for (const auto inputStageId : stageGraph.StageInputs.at(stageId)) {
            for (const auto& connection : stageGraph.Connections.at(std::make_pair(inputStageId, stageId))) {
                if (auto* shuffle = dynamic_cast<TShuffleConnection*>(connection.get())) {
                    shuffle->HashFuncType = stageHashType;
                }
            }
        }
    }
}

} // namespace

namespace NKikimr {
namespace NKqp {

TPropagateHashFuncStage::TPropagateHashFuncStage()
    : IRBOStage("Hash function propagation")
{
}

void TPropagateHashFuncStage::RunStage(TOpRoot& root, TRBOContext& ctx) {
    PropagateHashFuncs(root.PlanProps.StageGraph, ctx.KqpCtx.Config);
}

} // namespace NKqp
} // namespace NKikimr
