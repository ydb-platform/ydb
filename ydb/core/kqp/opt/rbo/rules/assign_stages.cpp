#include "kqp_rules_include.h"


namespace {
using namespace NKikimr;
using namespace NKikimr::NKqp;

void UpdateNumOfConsumers(TIntrusivePtr<IOperator> &input) {
    auto &props = input->Props;
    if (props.NumOfConsumers.has_value()) {
        props.NumOfConsumers.value() += 1;
    } else {
        props.NumOfConsumers = 1;
    }
}

}

namespace NKikimr {
namespace NKqp {

/**
 * Assign stages and build stage graph in the process
 */
bool TAssignStagesRule::MatchAndApply(TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(props);

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
            const auto newStageId = props.StageGraph.AddSourceStage(opRead->Columns, opRead->GetOutputIUs(), opRead->StorageType, opRead->NeedsMap());
            input->Props.StageId = newStageId;
            readName = opRead->Alias;
        } else {
            const auto newStageId = props.StageGraph.AddStage();
            input->Props.StageId = newStageId;
        }
        YQL_CLOG(TRACE, CoreDq) << "Assign stages source: " << readName;
    } else if (input->Kind == EOperator::Join) {
        auto join = CastOperator<TOpJoin>(input);
        auto leftStage = join->GetLeftInput()->Props.StageId;
        auto rightStage = join->GetRightInput()->Props.StageId;

        const auto newStageId = props.StageGraph.AddStage();
        join->Props.StageId = newStageId;

        const auto leftInputStorageType = props.StageGraph.GetStorageType(*leftStage);
        const auto rightInputStorageType = props.StageGraph.GetStorageType(*rightStage);

        // For cross-join we build a stage with map and broadcast connections
        if (join->JoinKind == "Cross") {
            props.StageGraph.Connect(*leftStage, newStageId, MakeIntrusive<TMapConnection>(leftInputStorageType));
            props.StageGraph.Connect(*rightStage, newStageId, MakeIntrusive<TBroadcastConnection>(rightInputStorageType));
        }

        // For inner join (we don't support other joins yet) we build a new stage
        // with GraceJoinCore and connect inputs via Shuffle connections
        else {
            TVector<TInfoUnit> leftShuffleKeys;
            TVector<TInfoUnit> rightShuffleKeys;
            for (const auto& key : join->JoinKeys) {
                leftShuffleKeys.push_back(key.first);
                rightShuffleKeys.push_back(key.second);
            }

            props.StageGraph.Connect(*leftStage, newStageId, MakeIntrusive<TShuffleConnection>(leftShuffleKeys, leftInputStorageType));
            props.StageGraph.Connect(*rightStage, newStageId, MakeIntrusive<TShuffleConnection>(rightShuffleKeys, rightInputStorageType));
        }
        YQL_CLOG(TRACE, CoreDq) << "Assign stages join";
    } else if (input->Kind == EOperator::Filter || input->Kind == EOperator::Map) {
        auto childOp = CastOperator<IUnaryOperator>(input)->GetInput();
        const auto prevStageId = *(childOp->Props.StageId);
        UpdateNumOfConsumers(childOp);

        // If the child operator is a source, it requires its own stage
        // So we have build a new stage for current operator
        if (childOp->Kind == EOperator::Source) {
            auto opRead = CastOperator<TOpRead>(childOp);
            const auto newStageId = props.StageGraph.AddStage();
            input->Props.StageId = newStageId;
            TIntrusivePtr<TConnection> connection;
            // Type of connections depends on the storage type.
            switch (opRead->GetTableStorageType()) {
                case NYql::EStorageType::RowStorage: {
                    connection.Reset(MakeIntrusive<TSourceConnection>());
                    break;
                }
                case NYql::EStorageType::ColumnStorage: {
                    connection.Reset(MakeIntrusive<TUnionAllConnection>(NYql::EStorageType::ColumnStorage, props.StageGraph.GetOutputIndex(prevStageId)));
                    break;
                }
                default: {
                    Y_ENSURE(false, "Invalid storage type for op read");
                    break;
                }
            }
            props.StageGraph.Connect(prevStageId, newStageId, connection);
        }
        // If the child operator is not single use, we also need to create a new stage
        // for current operator with a map connection
        else if (!childOp->IsSingleConsumer()) {
            auto newStageId = props.StageGraph.AddStage();
            input->Props.StageId = newStageId;
            props.StageGraph.Connect(prevStageId, newStageId, MakeIntrusive<TMapConnection>());
        } else {
            input->Props.StageId = prevStageId;
        }
        YQL_CLOG(TRACE, CoreDq) << "Assign stages rest";
    } else if (input->Kind == EOperator::Sort) {
        auto sort = CastOperator<TOpSort>(input);
        const auto newStageId = props.StageGraph.AddStage();
        input->Props.StageId = newStageId;
        const auto prevStageId = *(sort->GetInput()->Props.StageId);
        props.StageGraph.Connect(prevStageId, newStageId, MakeIntrusive<TUnionAllConnection>(props.StageGraph.GetStorageType(prevStageId)));
    }
    else if (input->Kind == EOperator::Limit) {
        auto limit = CastOperator<TOpLimit>(input);
        const auto newStageId = props.StageGraph.AddStage();
        input->Props.StageId = newStageId;
        const auto prevStageId = *(limit->GetInput()->Props.StageId);
        props.StageGraph.Connect(prevStageId, newStageId, MakeIntrusive<TUnionAllConnection>(props.StageGraph.GetStorageType(prevStageId)));
    } else if (input->Kind == EOperator::UnionAll) {
        auto unionAll = CastOperator<TOpUnionAll>(input);
        UpdateNumOfConsumers(unionAll->GetLeftInput());
        UpdateNumOfConsumers(unionAll->GetRightInput());

        auto leftStage = unionAll->GetLeftInput()->Props.StageId;
        auto rightStage = unionAll->GetRightInput()->Props.StageId;

        const auto newStageId = props.StageGraph.AddStage();
        unionAll->Props.StageId = newStageId;

        props.StageGraph.Connect(
            *leftStage, newStageId,
            MakeIntrusive<TUnionAllConnection>(props.StageGraph.GetStorageType(*leftStage), props.StageGraph.GetOutputIndex(*leftStage)));
        props.StageGraph.Connect(
            *rightStage, newStageId,
            MakeIntrusive<TUnionAllConnection>(props.StageGraph.GetStorageType(*rightStage), props.StageGraph.GetOutputIndex(*rightStage)));

        YQL_CLOG(TRACE, CoreDq) << "Assign stages union_all";
    } else if (input->Kind == EOperator::Aggregate) {
        auto aggregate = CastOperator<TOpAggregate>(input);
        const auto inputStageId = *(aggregate->GetInput()->Props.StageId);

        const auto newStageId = props.StageGraph.AddStage();
        aggregate->Props.StageId = newStageId;
        if (!aggregate->KeyColumns.empty()) {
            props.StageGraph.Connect(inputStageId, newStageId,
                                     MakeIntrusive<TShuffleConnection>(aggregate->KeyColumns, props.StageGraph.GetStorageType(inputStageId)));
        } else {
            props.StageGraph.Connect(inputStageId, newStageId, MakeIntrusive<TUnionAllConnection>(props.StageGraph.GetStorageType(inputStageId)));
        }

        YQL_CLOG(TRACE, CoreDq) << "Assign stage to Aggregation ";
    } else {
        Y_ENSURE(false, "Unknown operator encountered");
    }

    return true;
}
}
}