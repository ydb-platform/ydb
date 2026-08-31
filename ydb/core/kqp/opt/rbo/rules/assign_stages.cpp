#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

#include <yql/essentials/core/yql_expr_type_annotation.h>

namespace NKikimr::NKqp {

namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;

void FinalizeJoinPhysicalProps(TOpJoin& join, const TRBOContext& rboCtx) {
    auto& props = join.Props;
    const auto& config = *rboCtx.KqpCtx.Config;
    if (!props.JoinAlgo.has_value()) {
        const auto joinMode = config.GetHashJoinMode();
        switch (joinMode) {
            case NYql::NDq::EHashJoinMode::Map: {
                props.JoinAlgo = EJoinAlgoType::MapJoin;
                break;
            }
            default: {
                props.JoinAlgo = EJoinAlgoType::GraceJoin;
                break;
            }
        }
    }

    const auto joinKind = GetValidJoinKind(join.JoinKind);
    if (joinKind == "Cross") {
        props.UseBlockHashJoin = config.GetUseBlockHashJoin() && config.GetUseBlockHashJoinForCross();
        if (props.UseBlockHashJoin) {
            props.JoinAlgo = EJoinAlgoType::GraceJoin;
        }
        return;
    }

    const auto joinAlgo = *props.JoinAlgo;
    props.UseBlockHashJoin = config.GetUseBlockHashJoin()
        && (joinAlgo == EJoinAlgoType::GraceJoin || joinAlgo == EJoinAlgoType::ReverseBlockJoin)
        && (joinKind == "Inner" || joinKind == "Left" || joinKind == "LeftSemi" || joinKind == "LeftOnly");
}

// For row storage read we create a separate stage.
// TODO: We can also push to row storage stage, but it requires an implementation on physical plan generation.
void ProcessSource(TIntrusivePtr<IOperator> op, TIntrusivePtr<TOpRead> read, TPlanProps& props) {
    const auto readStageId = *read->Props.StageId;
    if (!read->IsSingleConsumer() || read->GetTableStorageType() == NYql::EStorageType::RowStorage) {
        const auto newStageId = props.StageGraph.AddStage();
        op->Props.StageId = newStageId;
        props.StageGraph.Connect(readStageId, newStageId, MakeIntrusive<TUnionAllConnection>(props.StageGraph.GetOutputIndex(readStageId)));
    } else {
        op->Props.StageId = readStageId;
    }
}

} // anonymous namespace

/**
 * Assign stages and build stage graph in the process
 */
bool TAssignStagesRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    const auto nodeName = input->ToString(ctx.ExprCtx);
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
            const auto opRead = CastOperator<TOpRead>(input);
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

        FinalizeJoinPhysicalProps(*join, ctx);

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
        props.StageGraph.Connect(prevStageId, newStageId, MakeIntrusive<TUnionAllConnection>(props.StageGraph.GetOutputIndex(prevStageId)));
        YQL_CLOG(TRACE, CoreDq) << "Assign stages sort";
    } else if (input->Kind == EOperator::Limit) {
        const auto limit = CastOperator<TOpLimit>(input);
        const auto limitInput = limit->GetInput();
        const auto prevStageId = *limitInput->Props.StageId;
        if (limitInput->GetKind() == EOperator::Sort) {
            // Put limit to sort stage.
            limit->Props.StageId = prevStageId;
        } else {
            const auto newStageId = props.StageGraph.AddStage();
            const auto outputIndex = props.StageGraph.GetOutputIndex(prevStageId);
            input->Props.StageId = newStageId;
            props.StageGraph.Connect(prevStageId, newStageId, MakeIntrusive<TUnionAllConnection>(outputIndex));
        }
        YQL_CLOG(TRACE, CoreDq) << "Assign stages limit";
    } else if (input->Kind == EOperator::UnionAll) {
        auto unionAll = CastOperator<TOpUnionAll>(input);

        const auto newStageId = props.StageGraph.AddStage();
        unionAll->Props.StageId = newStageId;
        const bool parallelUnionAllConnections = ctx.KqpCtx.Config->GetEnableParallelUnionAllConnectionsForExtend();

        // Connect the inputs in child order: the physical conversion pairs stage arguments
        // with the connections of this stage.
        for (const auto& child : unionAll->Children) {
            const auto childStageId = *child->Props.StageId;
            props.StageGraph.Connect(childStageId, newStageId,
                                     MakeIntrusive<TUnionAllConnection>(props.StageGraph.GetOutputIndex(childStageId), parallelUnionAllConnections));
        }

        YQL_CLOG(TRACE, CoreDq) << "Assign stages union_all";
    } else if (input->Kind == EOperator::Aggregate) {
        auto aggregate = CastOperator<TOpAggregate>(input);
        const auto inputStageId = *(aggregate->GetInput()->Props.StageId);
        const auto outputIndex = props.StageGraph.GetOutputIndex(inputStageId);

        const auto newStageId = props.StageGraph.AddStage();
        aggregate->Props.StageId = newStageId;
        if (CanEliminateAggregateShuffle(*aggregate, ctx)) {
            props.StageGraph.Connect(inputStageId, newStageId, MakeIntrusive<TMapConnection>(outputIndex));
        } else if (!aggregate->KeyColumns.empty()) {
            props.StageGraph.Connect(inputStageId, newStageId, MakeIntrusive<TShuffleConnection>(aggregate->KeyColumns, outputIndex));
        } else {
            props.StageGraph.Connect(inputStageId, newStageId, MakeIntrusive<TUnionAllConnection>(outputIndex));
        }

        YQL_CLOG(TRACE, CoreDq) << "Assign stage to aggregation ";
    } else if (input->Kind == EOperator::TableLookup) {
        auto lookup = CastOperator<TOpTableLookup>(input);
        auto& exprCtx = ctx.ExprCtx;

        const auto inputStageId = *(lookup->GetInput()->Props.StageId);
        const auto outputIndex = props.StageGraph.GetOutputIndex(inputStageId);
        const auto newStageId = props.StageGraph.AddStage();
        input->Props.StageId = newStageId;

        TVector<NYql::NNodes::TCoAtom> columnAtoms;
        for (const auto& column : lookup->FetchColumns) {
            columnAtoms.push_back(NYql::NNodes::Build<NYql::NNodes::TCoAtom>(exprCtx, lookup->Pos).Value(column).Done());
        }
        auto columnsNode = NYql::NNodes::Build<NYql::NNodes::TCoAtomList>(exprCtx, lookup->Pos).Add(columnAtoms).Done().Ptr();

        TKqpStreamLookupSettings settings;
        NYql::TExprNode::TPtr inputTypeNode;
        if (lookup->IsJoin()) {
            settings.Strategy = lookup->JoinKind == "LeftSemi" ? EStreamLookupStrategyType::LookupSemiJoinRows : EStreamLookupStrategyType::LookupJoinRows;
            // For point prefix lookup we allow null keys with it size.
            settings.AllowNullKeysPrefixSize = lookup->Prefix ? lookup->Prefix->Columns.size() : 0;
        } else {
            settings.Strategy = EStreamLookupStrategyType::LookupRows;

            TVector<const NYql::TItemExprType*> keyItems;
            for (const auto& key : lookup->LookupKeys) {
                const auto* keyType = lookup->GetInput()->GetIUType(key);
                Y_ENSURE(keyType, "Lookup key type is not available");
                keyItems.push_back(exprCtx.MakeType<NYql::TItemExprType>(key.GetFullName(), keyType));
            }
            const auto* keyStructType = exprCtx.MakeType<NYql::TStructExprType>(keyItems);
            const auto* keyListType = exprCtx.MakeType<NYql::TListExprType>(keyStructType);
            inputTypeNode = NYql::ExpandType(lookup->Pos, *keyListType, exprCtx);
        }
        auto settingsNode = settings.BuildNode(exprCtx, lookup->Pos).Ptr();

        props.StageGraph.Connect(inputStageId, newStageId,
                                 MakeIntrusive<TStreamLookupConnection>(outputIndex, lookup->Table, columnsNode, inputTypeNode, settingsNode));
        YQL_CLOG(TRACE, CoreDq) << "Assign stages table lookup";
    } else if (input->Kind == EOperator::IndexLookupJoin) {
        // The lookup join shares the stage of its table lookup: the joined pairs only exist inside
        // the stage that the stream lookup connection feeds.
        auto lookupJoin = CastOperator<TOpIndexLookupJoin>(input);
        auto lookup = lookupJoin->GetTableLookup();
        Y_ENSURE(lookup->IsSingleConsumer(), "A table lookup in join mode must feed only its lookup join");
        input->Props.StageId = *lookup->Props.StageId;
        YQL_CLOG(TRACE, CoreDq) << "Assign stages index lookup join";
    } else {
        Y_ENSURE(false, "Unknown operator encountered");
    }

    return true;
}

} // namespace NKikimr::NKqp
