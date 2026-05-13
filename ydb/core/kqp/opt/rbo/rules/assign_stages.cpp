#include "kqp_rules_include.h"


namespace {
using namespace NKikimr;
using namespace NKikimr::NKqp;

void MaybeSetJoinAlgo(TPhysicalOpProps& props, const TRBOContext& rboCtx) {
    if (props.JoinAlgo.has_value()) {
        return;
    }

    auto joinMode = rboCtx.KqpCtx.Config->GetHashJoinMode();
    NKikimr::NKqp::EJoinAlgoType joinAlgo;
    switch (joinMode) {
        case NYql::NDq::EHashJoinMode::Map: {
            joinAlgo = NKikimr::NKqp::EJoinAlgoType::MapJoin;
            break;
        }
        default: {
            joinAlgo = NKikimr::NKqp::EJoinAlgoType::GraceJoin;
            break;
        }
    }
    props.JoinAlgo = joinAlgo;
}

// For row storage read we create a separate stage.
// TODO: We can also push to row storage stage, but it requires an implementation on physical plan generation.
void ProcessSource(TIntrusivePtr<IOperator> op, TIntrusivePtr<TOpRead> read, TPlanProps& props) {
    const auto readStageId = *read->Props.StageId;
    if (!op->IsSingleConsumer() || read->GetTableStorageType() == NYql::EStorageType::RowStorage) {
        const auto newStageId = props.StageGraph.AddStage();
        op->Props.StageId = newStageId;
        props.StageGraph.Connect(readStageId, newStageId, MakeIntrusive<TUnionAllConnection>(props.StageGraph.GetOutputIndex(readStageId)));
    } else {
        op->Props.StageId = readStageId;
    }
}
} // namespace

namespace NKikimr {
namespace NKqp {

/**
 * Assign stages and build stage graph in the process
 */
bool TAssignStagesRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    auto nodeName = input->ToString(ctx.ExprCtx);
    YQL_CLOG(TRACE, CoreDq) << "Assign stages: " << nodeName;

    if (input->Props.StageId.has_value()) {
        YQL_CLOG(TRACE, CoreDq) << "Assign stages: " << nodeName << " stage assigned already";
        return false;
    }

    for (const auto& child : input->Children) {
        if (!child->Props.StageId.has_value()) {
            YQL_CLOG(TRACE, CoreDq) << "Assign stages: " << nodeName << " child with unassigned stage";
            return false;
        }
    }

    if (input->Kind == EOperator::EmptySource || input->Kind == EOperator::Source) {
        auto opRead = CastOperator<TOpRead>(input);
        TString readName;
        if (input->Kind == EOperator::Source) {
            auto opRead = CastOperator<TOpRead>(input);
            const auto newStageId = props.StageGraph.AddSourceStage(opRead->StorageType);
            input->Props.StageId = newStageId;
            readName = opRead->Alias;
        } else {
            const auto newStageId = props.StageGraph.AddStage();
            input->Props.StageId = newStageId;
        }
        YQL_CLOG(TRACE, CoreDq) << "Assign stages source: " << readName;
    } else if (input->Kind == EOperator::Join) {
        const auto join = CastOperator<TOpJoin>(input);
        const auto leftStage = *join->GetLeftInput()->Props.StageId;
        const auto rightStage = *join->GetRightInput()->Props.StageId;
        const auto leftOutputIndex = props.StageGraph.GetOutputIndex(leftStage);
        const auto rightOutputIndex = props.StageGraph.GetOutputIndex(rightStage);

        const auto newStageId = props.StageGraph.AddStage();
        join->Props.StageId = newStageId;

        MaybeSetJoinAlgo(join->Props, ctx);

        // For cross-join or map join we build a stage with map and broadcast connections
        // FIXME: We assume that right side is small one, map join also can work with hash shuffle connections.
        if (join->JoinKind == "Cross" || join->Props.JoinAlgo == EJoinAlgoType::MapJoin) {
            props.StageGraph.Connect(leftStage, newStageId, MakeIntrusive<TMapConnection>(leftOutputIndex));
            props.StageGraph.Connect(rightStage, newStageId, MakeIntrusive<TBroadcastConnection>(rightOutputIndex));
        }
        else {
            TVector<TInfoUnit> leftShuffleKeys;
            TVector<TInfoUnit> rightShuffleKeys;
            for (const auto& key : join->JoinKeys) {
                leftShuffleKeys.push_back(key.first);
                rightShuffleKeys.push_back(key.second);
            }
            const TVector<TInfoUnit>& effectiveLeftShuffleKeys =
                join->Props.LeftShuffleBy ? *join->Props.LeftShuffleBy : leftShuffleKeys;
            const TVector<TInfoUnit>& effectiveRightShuffleKeys =
                join->Props.RightShuffleBy ? *join->Props.RightShuffleBy : rightShuffleKeys;
            const bool leftShuffleEliminated = join->Props.LeftShuffleBy && join->Props.LeftShuffleBy->empty();
            const bool rightShuffleEliminated = join->Props.RightShuffleBy && join->Props.RightShuffleBy->empty();

            // Channel spilling (UseSpilling) is opt-in: without a specific need, backpressure
            // is preferred. There are two exceptions to this:
            //
            // 1. GraceJoins. Because of the way GraceJoin algorithm is implemented, it tries to
            //    align left and right inputs. This may lead to a deadlock if two separate tasks
            //    wait for two different inputs. We explicitly set UseSpilling = true for those
            //
            // 2. MultiOutput. This is handled in tasks graph.
            //
            // All other things set UseSpilling = false instead (the default in TShuffleConnection)

            if (leftShuffleEliminated) {
                props.StageGraph.Connect(leftStage, newStageId, MakeIntrusive<TMapConnection>(leftOutputIndex));
            } else {
                auto shuffleConnection = MakeIntrusive<TShuffleConnection>(
                    effectiveLeftShuffleKeys,
                    leftOutputIndex,
                    /*useSpilling=*/true
                );
                props.StageGraph.Connect(leftStage, newStageId, std::move(shuffleConnection));
            }

            if (rightShuffleEliminated) {
                props.StageGraph.Connect(rightStage, newStageId, MakeIntrusive<TMapConnection>(rightOutputIndex));
            } else {
                auto shuffleConnection = MakeIntrusive<TShuffleConnection>(
                    effectiveRightShuffleKeys,
                    rightOutputIndex,
                    /*useSpilling=*/true
                );
                props.StageGraph.Connect(rightStage, newStageId, std::move(shuffleConnection));
            }
        }
        YQL_CLOG(TRACE, CoreDq) << "Assign stages join";
    } else if (input->Kind == EOperator::Filter || input->Kind == EOperator::Map) {
        auto childOp = CastOperator<IUnaryOperator>(input)->GetInput();
        const auto prevStageId = *(childOp->Props.StageId);

        if (childOp->GetKind() == EOperator::Source) {
            ProcessSource(input, CastOperator<TOpRead>(childOp), props);
        } else if (!childOp->IsSingleConsumer()) {
            auto newStageId = props.StageGraph.AddStage();
            input->Props.StageId = newStageId;
            props.StageGraph.Connect(prevStageId, newStageId, MakeIntrusive<TMapConnection>(props.StageGraph.GetOutputIndex(prevStageId)));
        } else {
            input->Props.StageId = prevStageId;
        }
        YQL_CLOG(TRACE, CoreDq) << "Assign stages map/filter";
    } else if (input->Kind == EOperator::Sort) {
        auto sort = CastOperator<TOpSort>(input);
        const auto newStageId = props.StageGraph.AddStage();
        input->Props.StageId = newStageId;
        const auto prevStageId = *(sort->GetInput()->Props.StageId);
        props.StageGraph.Connect(prevStageId, newStageId, MakeIntrusive<TUnionAllConnection>());
        YQL_CLOG(TRACE, CoreDq) << "Assign stages sort";
    } else if (input->Kind == EOperator::Limit) {
        auto limit = CastOperator<TOpLimit>(input);
        const auto newStageId = props.StageGraph.AddStage();
        input->Props.StageId = newStageId;
        const auto prevStageId = *(limit->GetInput()->Props.StageId);
        props.StageGraph.Connect(prevStageId, newStageId, MakeIntrusive<TUnionAllConnection>());
        YQL_CLOG(TRACE, CoreDq) << "Assign stages limit";
    } else if (input->Kind == EOperator::UnionAll) {
        auto unionAll = CastOperator<TOpUnionAll>(input);

        auto leftStage = unionAll->GetLeftInput()->Props.StageId;
        auto rightStage = unionAll->GetRightInput()->Props.StageId;

        const auto newStageId = props.StageGraph.AddStage();
        unionAll->Props.StageId = newStageId;
        const bool parallelUnionAllConnections = ctx.KqpCtx.Config->GetEnableParallelUnionAllConnectionsForExtend();

        props.StageGraph.Connect(*leftStage, newStageId,
                                 MakeIntrusive<TUnionAllConnection>(props.StageGraph.GetOutputIndex(*leftStage), parallelUnionAllConnections));
        props.StageGraph.Connect(*rightStage, newStageId,
                                 MakeIntrusive<TUnionAllConnection>(props.StageGraph.GetOutputIndex(*rightStage), parallelUnionAllConnections));

        YQL_CLOG(TRACE, CoreDq) << "Assign stages union_all";
    } else if (input->Kind == EOperator::Aggregate) {
        auto aggregate = CastOperator<TOpAggregate>(input);
        const auto inputStageId = *(aggregate->GetInput()->Props.StageId);

        const auto newStageId = props.StageGraph.AddStage();
        aggregate->Props.StageId = newStageId;
        if (!aggregate->KeyColumns.empty()) {
            const auto outputIndex = props.StageGraph.GetOutputIndex(inputStageId);
            auto connection = MakeIntrusive<TShuffleConnection>(
                aggregate->KeyColumns,
                outputIndex
            );

            props.StageGraph.Connect(inputStageId, newStageId, std::move(connection));
        } else {
            props.StageGraph.Connect(inputStageId, newStageId, MakeIntrusive<TUnionAllConnection>());
        }

        YQL_CLOG(TRACE, CoreDq) << "Assign stage to aggregation ";
    } else {
        Y_ENSURE(false, "Unknown operator encountered");
    }

    return true;
}
}
}
