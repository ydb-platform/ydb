#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <yql/essentials/core/yql_expr_optimize.h>

#include <algorithm>

namespace NKikimr::NKqp {

namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;

void FinalizeJoinPhysicalProps(TOpJoin& join, const TRBOContext& rboCtx) {
    auto& props = join.Props;
    if (!props.JoinAlgo.has_value()) {
        const auto joinMode = rboCtx.KqpCtx.Config->GetHashJoinMode();
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
    const auto joinAlgo = *props.JoinAlgo;
    props.UseBlockHashJoin = rboCtx.KqpCtx.Config->GetUseBlockHashJoin()
        && (joinAlgo == EJoinAlgoType::GraceJoin || joinAlgo == EJoinAlgoType::ReverseBlockJoin)
        && (joinKind == "Inner" || joinKind == "Left" || joinKind == "LeftSemi" || joinKind == "LeftOnly");
}

// For row storage read we create a separate stage.
// TODO: We can also push to row storage stage, but it requires an implementation on physical plan generation.
void ProcessSource(TIntrusivePtr<IOperator> op, TIntrusivePtr<TOpRead> read, TPlanProps& props) {
    const auto readStageId = *read->Props.StageId;
    if (!op->IsSingleConsumer() || !read->IsSingleConsumer() || read->GetTableStorageType() == NYql::EStorageType::RowStorage) {
        const auto newStageId = props.StageGraph.AddStage();
        op->Props.StageId = newStageId;
        props.StageGraph.Connect(readStageId, newStageId, MakeIntrusive<TUnionAllConnection>(props.StageGraph.GetOutputIndex(readStageId)));
    } else {
        op->Props.StageId = readStageId;
    }
}

bool HasWindowSemantics(const TOpMap& map) {
    return std::any_of(
        map.MapElements.begin(),
        map.MapElements.end(),
        [](const TMapElement& element) {
            return element.GetExpression().HasWindowSemantics();
        });
}

enum class EWindowDistribution {
    Partitioned,
    Global,
};

const TExprNode* ExtractDirectWindowColumnName(const TExprNode& lambda) {
    if (!lambda.IsLambda() || lambda.ChildrenSize() != 2 ||
        !lambda.Child(0)->IsArguments() ||
        lambda.Child(0)->ChildrenSize() != 1 ||
        !lambda.Child(0)->Child(0)->IsArgument())
    {
        return nullptr;
    }

    const auto* argument = lambda.Child(0)->Child(0);
    const auto* body = lambda.Child(1);
    if (body->IsCallable("YqlGroupRef") &&
        body->ChildrenSize() == 4 && body->Child(0) == argument &&
        body->Child(3)->IsAtom() && !body->Child(3)->Content().empty())
    {
        return body->Child(3);
    }
    if (body->IsCallable("Member") && body->ChildrenSize() == 2 &&
        body->Child(0) == argument && body->Child(1)->IsAtom() &&
        !body->Child(1)->Content().empty())
    {
        return body->Child(1);
    }
    return nullptr;
}

std::optional<EWindowDistribution> GetMatchingWindowDistribution(
    const TExpression& expression)
{
    const auto& metadata = expression.GetWindowMetadata();
    if (!metadata ||
        !metadata->Definition ||
        !metadata->Definition->IsCallable("YqlWindow") ||
        metadata->Definition->ChildrenSize() != 5 ||
        !metadata->Definition->Child(0)->IsAtom() ||
        metadata->Definition->Child(0)->Content().empty() ||
        !metadata->Definition->Child(1)->IsAtom("") ||
        !metadata->Definition->Child(2)->IsList())
    {
        return std::nullopt;
    }

    TInfoUnitSet sourcePartitionKeys;
    for (const auto& partition : metadata->Definition->Child(2)->Children()) {
        if (!partition->IsCallable("YqlGroup") ||
            partition->ChildrenSize() != 2)
        {
            return std::nullopt;
        }
        const auto* name =
            ExtractDirectWindowColumnName(*partition->Child(1));
        if (!name) {
            return std::nullopt;
        }
        sourcePartitionKeys.insert(TInfoUnit(TString(name->Content())));
    }
    size_t count = 0;
    const TExprNode* window = nullptr;
    VisitExpr(*expression.GetExpressionBody(), [&](const TExprNode& node) {
        if (node.IsCallable({"YqlAggWin", "YqlWin"})) {
            ++count;
            window = &node;
        }
        return true;
    });
    if (count != 1 ||
        window->ChildrenSize() < 2 ||
        !window->Child(1)->IsAtom() ||
        window->Child(1)->Content() !=
            metadata->Definition->Child(0)->Content())
    {
        return std::nullopt;
    }

    if (window->IsCallable("YqlAggWin")) {
        if (window->ChildrenSize() != 5 ||
            sourcePartitionKeys.empty() ||
            expression.GetWindowPartitionBy().size() !=
                sourcePartitionKeys.size())
        {
            return std::nullopt;
        }
        return EWindowDistribution::Partitioned;
    }

    if (!window->IsCallable("YqlWin") ||
        window->ChildrenSize() != 4 ||
        !window->Child(0)->IsAtom("rank") ||
        !window->Child(2)->IsList() ||
        window->Child(2)->ChildrenSize() != 0)
    {
        return std::nullopt;
    }
    const auto& resultDescriptor = *window->Child(3);
    if (!resultDescriptor.IsCallable("DataType") ||
        resultDescriptor.ChildrenSize() != 1 ||
        !resultDescriptor.Child(0)->IsAtom("Uint64") ||
        !sourcePartitionKeys.empty() ||
        !expression.GetWindowPartitionBy().empty() ||
        !metadata->Definition->Child(3)->IsList() ||
        metadata->Definition->Child(3)->ChildrenSize() != 1 ||
        expression.GetWindowOrderBy().size() != 1)
    {
        return std::nullopt;
    }
    return EWindowDistribution::Global;
}

// Hashing on any non-empty common subset of the partition keys is sufficient:
// rows in the same partition agree on every key in that subset.  An untracked
// window, a global window, or windows without a common available key must run
// behind a serial connection instead.
std::optional<TVector<TInfoUnit>> GetWindowShuffleKeys(TOpMap& map) {
    TInfoUnitSet available;
    for (const auto& iu : map.GetInput()->GetOutputIUs()) {
        available.insert(iu);
    }

    std::optional<TVector<TInfoUnit>> commonKeys;
    for (const auto& element : map.MapElements) {
        const auto& expression = element.GetExpression();
        if (!expression.HasWindowSemantics()) {
            continue;
        }
        const auto distribution = GetMatchingWindowDistribution(expression);
        if (!distribution) {
            return std::nullopt;
        }
        if (*distribution == EWindowDistribution::Global) {
            return std::nullopt;
        }

        const auto resolvedPartitionKeys = expression.GetWindowPartitionBy();
        TInfoUnitSet partitionKeys;
        for (const auto& key : resolvedPartitionKeys) {
            if (!available.contains(key)) {
                return std::nullopt;
            }
            partitionKeys.insert(key);
        }
        if (partitionKeys.empty()) {
            return std::nullopt;
        }

        if (!commonKeys) {
            commonKeys.emplace();
            commonKeys->reserve(partitionKeys.size());
            for (const auto& key : resolvedPartitionKeys) {
                if (partitionKeys.contains(key) &&
                    std::find(commonKeys->begin(), commonKeys->end(), key) == commonKeys->end())
                {
                    commonKeys->push_back(key);
                }
            }
        } else {
            std::erase_if(*commonKeys, [&](const TInfoUnit& key) {
                return !partitionKeys.contains(key);
            });
            if (commonKeys->empty()) {
                return std::nullopt;
            }
        }
    }

    return commonKeys;
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

        if (input->Kind == EOperator::Map && HasWindowSemantics(*CastOperator<TOpMap>(input))) {
            const auto newStageId = props.StageGraph.AddStage();
            input->Props.StageId = newStageId;
            const auto outputIndex = props.StageGraph.GetOutputIndex(prevStageId);
            const auto shuffleKeys = GetWindowShuffleKeys(*CastOperator<TOpMap>(input));
            if (shuffleKeys) {
                props.StageGraph.Connect(
                    prevStageId,
                    newStageId,
                    MakeIntrusive<TShuffleConnection>(*shuffleKeys, outputIndex));
            } else {
                props.StageGraph.Connect(
                    prevStageId,
                    newStageId,
                    MakeIntrusive<TUnionAllConnection>(outputIndex));
            }
        } else if (childOp->GetKind() == EOperator::Source) {
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
        const auto outputIndex = props.StageGraph.GetOutputIndex(inputStageId);

        const auto newStageId = props.StageGraph.AddStage();
        aggregate->Props.StageId = newStageId;
        if (!aggregate->KeyColumns.empty()) {
            auto connection = MakeIntrusive<TShuffleConnection>(aggregate->KeyColumns, outputIndex);

            props.StageGraph.Connect(inputStageId, newStageId, std::move(connection));
        } else {
            props.StageGraph.Connect(inputStageId, newStageId, MakeIntrusive<TUnionAllConnection>(outputIndex));
        }

        YQL_CLOG(TRACE, CoreDq) << "Assign stage to aggregation ";
    } else {
        Y_ENSURE(false, "Unknown operator encountered");
    }

    return true;
}

} // namespace NKikimr::NKqp
