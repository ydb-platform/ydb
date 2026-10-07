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
        && (joinAlgo == EJoinAlgoType::GraceJoin || joinAlgo == EJoinAlgoType::ReverseBlockJoin || joinAlgo == EJoinAlgoType::MapJoin)
        && (joinKind == "Inner" || joinKind == "Left" || joinKind == "LeftSemi" || joinKind == "LeftOnly");
}

// For row storage read we create a separate stage.
// TODO: We can also push to row storage stage, but it requires an implementation on physical plan generation.
void ProcessSource(IOperator* op, TOpRead* read, TPlanProps& props) {
    const auto readStageId = *read->Props.StageId;
    if (read->GetTableStorageType() == NYql::EStorageType::RowStorage) {
        const auto newStageId = props.StageGraph.AddStage();
        op->Props.StageId = newStageId;
        props.StageGraph.Connect(readStageId, newStageId, MakeIntrusive<TUnionAllConnection>(props.StageGraph.GetOutputIndex(readStageId)));
    } else {
        op->Props.StageId = readStageId;
    }
}

// A shared Read feeds a consumer stage through a UnionAll connection, any other
// shared producer through a Map one.
TIntrusivePtr<TConnection> MakePortConnection(const IOperator& port, ui32 outputIndex) {
    const IOperator* producer = &port;
    while (producer->Kind == EOperator::Replicate) {
        producer = CastOperator<TOpReplicate>(*producer).GetReplicate().GetInput().Get();
    }
    if (producer->Kind == EOperator::Source) {
        return MakeIntrusive<TUnionAllConnection>(outputIndex);
    }
    return MakeIntrusive<TMapConnection>(outputIndex);
}

ui32 OutputIndex(const IOperator& input, TStageGraph& graph) {
    if (input.Props.StageOutputIndex) {
        return *input.Props.StageOutputIndex;
    }
    return graph.GetOutputIndex(*input.Props.StageId);
}

void AssignStage(IOperator* input, TRBOContext& ctx, TPlanProps& props) {
    const auto nodeName = input->ToString(ctx.ExprCtx, props.InfoUnitRegistry);
    YQL_CLOG(TRACE, CoreDq) << "Assign stages: " << nodeName;

    if (input->Props.StageId.has_value()) {
        YQL_CLOG(TRACE, CoreDq) << "Assign stages: " << nodeName << " stage assigned already";
        return;
    }

    for (const auto& child : input->GetChildren()) {
        Y_ENSURE(child->Props.StageId, "Stage assignment requires postorder traversal");
    }

    if (input->Kind == EOperator::EmptySource || input->Kind == EOperator::Source) {
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
    } else if (input->Kind == EOperator::Replicate) {
        auto& hub = CastOperator<TOpReplicate>(*input).GetReplicate();
        auto& producer = *hub.GetInput();
        // Match the compiler's AllowWithSpilling setting for multi-output stages.
        const auto& kqpCtx = ctx.KqpCtx;
        Y_ENSURE(kqpCtx.Config->GetEnableQueryServiceSpilling()
            && (kqpCtx.IsGenericQuery() || kqpCtx.IsScanQuery()) && kqpCtx.Config->SpillingEnabled(),
            "Cannot execute shared " << producer.GetExplainName() << " with channel spilling disabled");
        input->Props.StageId = producer.Props.StageId;
        // A Replicate over a port needs a row stream, not its producer's variant.
        if (producer.Kind == EOperator::Replicate) {
            const auto stage = props.StageGraph.AddStage();
            props.StageGraph.Connect(*producer.Props.StageId, stage,
                MakePortConnection(producer, OutputIndex(producer, props.StageGraph)));
            input->Props.StageId = stage;
        }
        auto ports = hub.GetOutputs();
        std::sort(ports.begin(), ports.end(), [](const auto* lhs, const auto* rhs) {
            return lhs->GetIndex() < rhs->GetIndex();
        });
        for (ui32 index = 0; index < ports.size(); ++index) {
            ports[index]->Props.StageId = input->Props.StageId;
            ports[index]->Props.StageOutputIndex = index;
        }
    } else if (input->Kind == EOperator::Join) {
        const auto join = CastOperator<TOpJoin>(input);
        const auto leftStage = *join->GetLeftInput()->Props.StageId;
        const auto rightStage = *join->GetRightInput()->Props.StageId;
        const auto leftOutputIndex = OutputIndex(*join->GetLeftInput(), props.StageGraph);
        const auto rightOutputIndex = OutputIndex(*join->GetRightInput(), props.StageGraph);

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
            TOrderedIUs<> leftShuffleKeys;
            TOrderedIUs<> rightShuffleKeys;
            for (const auto& [left, right, equalNulls] : join->JoinKeys.Items()) {
                leftShuffleKeys.Append(left);
                rightShuffleKeys.Append(right);
            }
            const auto& effectiveLeftShuffleKeys =
                join->Props.LeftShuffleBy ? *join->Props.LeftShuffleBy : leftShuffleKeys;
            const auto& effectiveRightShuffleKeys =
                join->Props.RightShuffleBy ? *join->Props.RightShuffleBy : rightShuffleKeys;
            const bool leftShuffleEliminated = join->Props.LeftShuffleBy && join->Props.LeftShuffleBy->Items().empty();
            const bool rightShuffleEliminated = join->Props.RightShuffleBy && join->Props.RightShuffleBy->Items().empty();

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
        auto childOp = CastOperator<IUnaryOperator>(input)->GetInput().Get();
        const auto prevStageId = *(childOp->Props.StageId);

        if (childOp->GetKind() == EOperator::Source) {
            ProcessSource(input, CastOperator<TOpRead>(childOp), props);
        } else if (childOp->Kind == EOperator::Replicate) {
            auto newStageId = props.StageGraph.AddStage();
            input->Props.StageId = newStageId;
            props.StageGraph.Connect(prevStageId, newStageId,
                MakePortConnection(*childOp, OutputIndex(*childOp, props.StageGraph)));
        } else {
            input->Props.StageId = prevStageId;
        }
        YQL_CLOG(TRACE, CoreDq) << "Assign stages map/filter";
    } else if (input->Kind == EOperator::Sort) {
        auto sort = CastOperator<TOpSort>(input);
        const auto newStageId = props.StageGraph.AddStage();
        input->Props.StageId = newStageId;
        const auto prevStageId = *(sort->GetInput()->Props.StageId);
        props.StageGraph.Connect(prevStageId, newStageId, MakeIntrusive<TUnionAllConnection>(OutputIndex(*sort->GetInput(), props.StageGraph)));
        YQL_CLOG(TRACE, CoreDq) << "Assign stages sort";
    } else if (input->Kind == EOperator::Limit) {
        const auto limit = CastOperator<TOpLimit>(input);
        const auto limitInput = limit->GetInput().Get();
        const auto prevStageId = *limitInput->Props.StageId;
        if (limitInput->GetKind() == EOperator::Sort) {
            // Put limit to sort stage.
            limit->Props.StageId = prevStageId;
        } else {
            const auto newStageId = props.StageGraph.AddStage();
            const auto outputIndex = OutputIndex(*limitInput, props.StageGraph);
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
        for (const auto& child : unionAll->GetChildren()) {
            const auto childStageId = *child->Props.StageId;
            props.StageGraph.Connect(childStageId, newStageId,
                                     MakeIntrusive<TUnionAllConnection>(OutputIndex(*child, props.StageGraph), parallelUnionAllConnections));
        }

        YQL_CLOG(TRACE, CoreDq) << "Assign stages union_all";
    } else if (input->Kind == EOperator::Aggregate) {
        auto aggregate = CastOperator<TOpAggregate>(input);
        const auto inputStageId = *(aggregate->GetInput()->Props.StageId);
        const auto outputIndex = OutputIndex(*aggregate->GetInput(), props.StageGraph);

        const auto newStageId = props.StageGraph.AddStage();
        aggregate->Props.StageId = newStageId;
        if (CanEliminateAggregateShuffle(*aggregate, ctx)) {
            props.StageGraph.Connect(inputStageId, newStageId, MakeIntrusive<TMapConnection>(outputIndex));
        } else if (!aggregate->GetKeyColumns().Items().empty()) {
            props.StageGraph.Connect(inputStageId, newStageId, MakeIntrusive<TShuffleConnection>(aggregate->GetKeyColumns(), outputIndex));
        } else {
            props.StageGraph.Connect(inputStageId, newStageId, MakeIntrusive<TUnionAllConnection>(outputIndex));
        }

        YQL_CLOG(TRACE, CoreDq) << "Assign stage to aggregation ";
    } else if (input->Kind == EOperator::Window) {
        auto window = CastOperator<TOpWindow>(input);
        const auto inputStageId = *(window->GetInput()->Props.StageId);
        const auto outputIndex = OutputIndex(*window->GetInput(), props.StageGraph);

        const auto newStageId = props.StageGraph.AddStage();
        window->Props.StageId = newStageId;
        if (!window->GetPartitionKeys().Items().empty()) {
            props.StageGraph.Connect(inputStageId, newStageId, MakeIntrusive<TShuffleConnection>(window->GetPartitionKeys(), outputIndex));
        } else {
            // Without partition by we assume the whole input is one partition, so do it in one task.
            props.StageGraph.Connect(inputStageId, newStageId, MakeIntrusive<TUnionAllConnection>(outputIndex));
        }

        YQL_CLOG(TRACE, CoreDq) << "Assign stage to window";
    } else if (input->Kind == EOperator::TableLookup) {
        auto lookup = CastOperator<TOpTableLookup>(input);
        auto& exprCtx = ctx.ExprCtx;

        const auto inputStageId = *(lookup->GetInput()->Props.StageId);
        const auto outputIndex = OutputIndex(*lookup->GetInput(), props.StageGraph);
        const auto newStageId = props.StageGraph.AddStage();
        input->Props.StageId = newStageId;

        TVector<NYql::NNodes::TCoAtom> columnAtoms;
        THashSet<TString> fetchedNames;
        for (const auto id : lookup->GetColumns()) {
            const auto column = props.InfoUnitRegistry.Get(id).GetColumnName();
            if (fetchedNames.insert(column).second) {
                columnAtoms.push_back(NYql::NNodes::Build<NYql::NNodes::TCoAtom>(exprCtx, lookup->Pos).Value(column).Done());
            }
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
            for (const auto& [key, column] : lookup->LookupKeys.Items()) {
                const auto* keyType = lookup->GetInput()->GetIUType(key, exprCtx);
                Y_ENSURE(keyType, "Lookup key type is not available");
                keyItems.push_back(exprCtx.MakeType<NYql::TItemExprType>(column, keyType));
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
        auto lookup = &lookupJoin->GetTableLookup();
        input->Props.StageId = *lookup->Props.StageId;
        YQL_CLOG(TRACE, CoreDq) << "Assign stages index lookup join";
    } else if (input->Kind == EOperator::TableEffect) {
        auto tableEffect = CastOperator<TOpTableEffect>(input);

        const auto newStageId = props.StageGraph.AddSinkStage(tableEffect->BuildSettings(ctx.ExprCtx));
        const auto inputStageId = *(tableEffect->GetInput()->Props.StageId);
        const auto outputIndex = OutputIndex(*tableEffect->GetInput(), props.StageGraph);

        input->Props.StageId = newStageId;
        props.StageGraph.Connect(inputStageId, newStageId,
                                 MakeIntrusive<TUnionAllConnection>(outputIndex));
        YQL_CLOG(TRACE, CoreDq) << "Assign stages table effects";
    }
    else {
        Y_ENSURE(false, TStringBuilder() << "Unknown operator encountered: " << input->GetExplainName());
    }

}

} // anonymous namespace

void TAssignStagesStage::RunStage(TOpRoot& root, TRBOContext& ctx) {
    for (const auto& item : root) {
        AssignStage(item.Current, ctx, root.PlanProps);
    }
}

} // namespace NKikimr::NKqp
