#include "dq_opt_phy_finalizing.h"
#include "ydb/library/yql/core/yql_opt_utils.h"

#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>

#include <algorithm>

namespace NYql::NDq {

using namespace NNodes;

namespace {

ui32 GetStageOutputsCount(const TDqStage& stage, const TCoAtom& indexAtom, TExprContext& ctx) {
    auto result = stage.Program().Body();
    auto resultType = result.Ref().GetTypeAnn();

    const TTypeAnnotationNode* resultItemType = nullptr;
    if (!EnsureNewSeqType<false, false>(result.Pos(), *resultType, ctx, &resultItemType)) {
        YQL_ENSURE(false, "got " << FormatType(resultType));
    }

    ui32 index = FromString<ui32>(indexAtom.Value());
    ui32 outputsCount = 0;
    if (index > 0 || resultItemType->GetKind() == ETypeAnnotationKind::Variant) {
        YQL_ENSURE(resultItemType->GetKind() == ETypeAnnotationKind::Variant);
        auto variantUnderlyingType = resultItemType->Cast<TVariantExprType>()->GetUnderlyingType();
        YQL_ENSURE(variantUnderlyingType->GetKind() == ETypeAnnotationKind::Tuple);
        YQL_ENSURE(variantUnderlyingType->Cast<TTupleExprType>()->GetSize() > index);
        outputsCount = variantUnderlyingType->Cast<TTupleExprType>()->GetSize();
    } else {
        outputsCount = 1;
    }
    return outputsCount;
}

// returns new DqStage and list of added output indexes
std::pair<TDqStage, TVector<TCoAtom>> ReplicateStageOutput(const TDqStage& stage, const TCoAtom& indexAtom,
    const TVector<TCoLambda>& lambdas, TExprContext& ctx)
{
    auto result = stage.Program().Body();
    auto resultType = result.Ref().GetTypeAnn();

    const TTypeAnnotationNode& resultItemType = GetSeqItemType(*resultType);

    ui32 index = FromString<ui32>(indexAtom.Value());
    ui32 outputsCount = GetStageOutputsCount(stage, indexAtom, ctx);

    YQL_CLOG(TRACE, CoreDq) << "replicate stage (#" << stage.Ref().UniqueId() << ", " << index << "), outputs: "
        << outputsCount << ", about to add " << lambdas.size() << " copies." << Endl << PrintDqStageOnly(stage, ctx);

    TExprNode::TPtr newResult;
    ui32 newOutputIndexStart;

    if (outputsCount > 1) {
        YQL_ENSURE(result.Maybe<TDqReplicate>(), "got: " << NCommon::ExprToPrettyString(ctx, result.Ref()));
        auto outputTypesTuple = resultItemType.Cast<TVariantExprType>()->GetUnderlyingType()->Cast<TTupleExprType>();
        newOutputIndexStart = outputTypesTuple->GetSize();
    } else {
        newOutputIndexStart = 1;
    }

    ui32 newOutputIndexEnd = newOutputIndexStart + lambdas.size(); // exclusive

    YQL_CLOG(TRACE, CoreDq) << "add output indexes: [" << newOutputIndexStart << ".." << newOutputIndexEnd << ")";

    TVector<TExprBase> variants;
    variants.reserve(newOutputIndexEnd);

    TExprNode::TPtr input;

    if (outputsCount > 1) {
        auto dqReplicate = result.Cast<TDqReplicate>();
        for (ui32 i = 0; i < newOutputIndexStart; ++i) {
            variants.emplace_back(dqReplicate.Args().Get(1 + i));
        }
        input = dqReplicate.Input().Ptr();
    } else {
        variants.emplace_back(BuildIdentityLambda(stage.Pos(), ctx));
        input = result.Ptr();
    }

    if (input->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Stream) {
        input = Build<TCoToFlow>(ctx, input->Pos())
            .Input(input)
            .Done().Ptr();
    } else {
        YQL_ENSURE(input->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Flow, "" << *input->GetTypeAnn());
    }

    for (ui32 i = newOutputIndexStart, j = 0; i < newOutputIndexEnd; ++i, ++j) {
        if (outputsCount > 1) {
            variants.emplace_back(ctx.FuseLambdas(lambdas[j].Ref(), variants[index].Ref()));
        } else {
            variants.emplace_back(ctx.DeepCopyLambda(lambdas[j].Ref()));
        }
    }

    newResult = Build<TDqReplicate>(ctx, stage.Pos())
        .Input(input)
        .FreeArgs()
            .Add(variants)
            .Build()
        .Done().Ptr();

    std::vector<TExprNode::TPtr> newStageArgs;
    newStageArgs.reserve(stage.Program().Args().Size());
    TNodeOnNodeOwnedMap stageArgsReplaces;
    for (size_t i = 0; i < stage.Program().Args().Size(); ++i) {
        auto oldArg = stage.Program().Args().Arg(i);
        newStageArgs.emplace_back(ctx.NewArgument(oldArg.Pos(), TStringBuilder() << "arg_" << i));
        stageArgsReplaces[oldArg.Raw()] = newStageArgs.back();
    }

    auto newStage = Build<TDqStage>(ctx, stage.Pos())
        .Inputs(stage.Inputs())
        .Program()
            .Args(newStageArgs)
            .Body(ctx.ReplaceNodes(std::move(newResult), stageArgsReplaces))
            .Build()
        .Settings(TDqStageSettings().BuildNode(ctx, stage.Pos()))
        .Done();

    YQL_CLOG(TRACE, CoreDq) << "new stage #" << newStage.Ref().UniqueId();

    TVector<TCoAtom> indexes;
    indexes.reserve(lambdas.size());
    for (ui32 i = newOutputIndexStart; i < newOutputIndexEnd; ++i) {
        indexes.emplace_back(BuildAtom(ToString(i), stage.Pos(), ctx));
    }
    return {newStage, std::move(indexes)};
}

struct TMultiUsedOutput {
    TDqOutput Output;
    const TNodeMultiSet& Consumers;

    TMultiUsedOutput(const TDqOutput& output, const TNodeMultiSet& consumers)
        : Output(output)
        , Consumers(consumers) {}
};

struct TMultiUsedConnection {
    TDqConnection Connection;
    const TNodeMultiSet& Consumers;

    // set if Connection has single consumer, but it's Output has many consumers
    // in that case, Connection - is on of connections, references to this Output
    TMaybe<TMultiUsedOutput> Output;

    TMultiUsedConnection(const TDqConnection& connection, const TNodeMultiSet& consumers)
        : Connection(connection)
        , Consumers(consumers) {}

    TString Print(TExprContext& ctx) const {
        TStringBuilder sb;
        if (!Output.Defined()) {
            auto output = Connection.Output();
            sb << "multiused connection: " << Connection.Ref().Content() << ", #" << Connection.Ref().UniqueId()
               << ", index: " << output.Index().Value() << ", stage: " << PrintDqStageOnly(output.Stage(), ctx)
               << ", consumers: " << Consumers.size() << Endl;
            for (const auto& consumer : Consumers) {
                sb << "consumer: " << PrintConsumer(consumer, output.Stage(), ctx) << Endl;
            }
        } else {
            sb << "multiused output, index: " << Output->Output.Index().Value()
               << ", #" << Output->Output.Ref().UniqueId()
               << ", stage: " << PrintDqStageOnly(Output->Output.Stage(), ctx)
               << ", consumers: " << Output->Consumers.size() << Endl;
            for (const auto& consumer : Output->Consumers) {
                sb << "consumer: " << PrintConsumer(consumer, Output->Output.Stage(), ctx) << Endl;
            }
        }
        return sb;
    }

    TString PrintConsumer(const TExprNode* consumer, const TDqStageBase& stage, TExprContext& ctx) const {
        auto stageReplacement = Build<TCoParameter>(ctx, consumer->Pos())
            .Name().Build("<__stage__>")
            .Type(ExpandType(stage.Pos(), *stage.Ref().GetTypeAnn(), ctx))
            .Done();

        TNodeOnNodeOwnedMap clones;
        auto x = ctx.DeepCopy(*consumer, ctx, clones, true, true, false,
            [&](const TExprNode& node, TExprNode::TListType& newChildren) -> bool {
                if (TDqOutput::Match(&node)) {
                    auto output = TDqOutput(&node);
                    if (output.Stage().Raw() == stage.Raw()) {
                        newChildren.emplace_back(stageReplacement.Ptr());
                        newChildren.emplace_back(output.Index().Ptr());
                        return true;
                    }
                }
                return false;
            });

        try {
            return NCommon::ExprToPrettyString(ctx, *x);
        } catch (...) {
            return x->Dump();
        }
    }
};

TString IndexesToString(const TCoAtom& head, const TVector<TCoAtom>& tail) {
    TStringBuilder sb;
    sb << head.Value();
    for (auto& idx : tail) {
        sb << "," << idx.Value();
    }
    return sb;
}

void BuildIdentityLambdas(TVector<TCoLambda>& lambdas, ui32 count, TExprContext& ctx, TPositionHandle pos) {
    lambdas.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        lambdas.emplace_back(BuildIdentityLambda(pos, ctx));
    }
}

TExprNode::TPtr ReplicateDqOutput(TExprNode::TPtr&& input, const TMultiUsedConnection& muConnection, TExprContext& ctx)
{
    YQL_ENSURE(muConnection.Output.Defined());
    auto dqOutput = muConnection.Output->Output;
    auto dqStage = dqOutput.Stage().Cast<TDqStage>();
    auto outputIndex = dqOutput.Index();
    auto consumersCount = muConnection.Output->Consumers.size();
    YQL_ENSURE(consumersCount > 1);

    TVector<TCoLambda> lambdas;
    BuildIdentityLambdas(lambdas, consumersCount - 1, ctx, dqOutput.Pos());

    auto ret = ReplicateStageOutput(dqStage, outputIndex, lambdas, ctx);
    auto newStage = ret.first;
    auto newAdditionalIndexes = ret.second;

    YQL_CLOG(TRACE, CoreDq) << "Replicate DQ output (stage #" << dqStage.Ref().UniqueId() << ", "
        << outputIndex.Value() << ") -> (#" << newStage.Ref().UniqueId() << ", ["
        << IndexesToString(outputIndex, newAdditionalIndexes) << "])." << Endl << PrintDqStageOnly(newStage, ctx);

    TNodeOnNodeOwnedMap replaces;
    replaces[dqStage.Raw()] = newStage.Ptr();

    ui32 consumerIdx = 0;
    TVector<const TExprNode*> consumers; consumers.reserve(muConnection.Output->Consumers.size());
    consumers.insert(consumers.end(), muConnection.Output->Consumers.begin(), muConnection.Output->Consumers.end());
    std::sort(consumers.begin(), consumers.end(), [](auto l, auto r) { return l->UniqueId() < r->UniqueId(); });

    for (auto& connection : consumers) {
        YQL_ENSURE(TExprBase(connection).Maybe<TDqConnection>(), "DqOutput "
            << NCommon::ExprToPrettyString(ctx, muConnection.Output->Output.Ref())
            << " used by not DqConnection callable: " << connection->Content());

        if (consumerIdx == 0) {
            // Keep first (any of) consumer as is.
            ++consumerIdx;
            continue;
        }

        auto newOutput = Build<TDqOutput>(ctx, connection->Pos())
            .Stage(newStage)
            .Index(newAdditionalIndexes[consumerIdx - 1])
            .Done();
        auto newConnection = ctx.ChangeChild(*connection, TDqConnection::idx_Output, newOutput.Ptr());
        replaces[connection] = newConnection;
        ++consumerIdx;
    }

    return ctx.ReplaceNodes(std::move(input), replaces);
}

TExprNode::TPtr ReplaceStageForConsumer(TDqStage newStage, const TExprNode* consumer, TExprNode::TPtr&& input,
    TExprContext& ctx, bool skipFirstUsage, const TExprNode* dqConnection, const TVector<TCoAtom>& outputlIndices = {}) {
    bool isStageConsumer = TMaybeNode<TDqStage>(consumer).IsValid();
    auto consumerNode = isStageConsumer
        ? TDqStage(consumer).Inputs().Raw()
        : consumer;

    ui32 usageIdx = 0;
    TExprNode::TPtr newConsumer = ctx.ShallowCopy(*consumerNode);
    for (size_t childIndex = 0; childIndex < newConsumer->ChildrenSize(); ++childIndex) {
        TExprBase child(newConsumer->Child(childIndex));

        if (child.Raw() == dqConnection) {
            if (skipFirstUsage && usageIdx == 0) {
                // Keep first (any of) usage as is.
                skipFirstUsage = false;
                continue;
            }

            const auto newIdx = outputlIndices.empty() ? BuildAtom("0", dqConnection->Pos(), ctx) : outputlIndices[usageIdx];
            auto newOutput = Build<TDqOutput>(ctx, child.Pos())
                .Stage(newStage)
                .Index(newIdx)
                .Done();

            auto newConnection = ctx.ChangeChild(child.Ref(), TDqConnection::idx_Output, newOutput.Ptr());

            newConsumer = ctx.ChangeChild(*newConsumer, childIndex, std::move(newConnection));
            ++usageIdx;
        }
    }

    if (isStageConsumer) {
        newConsumer = ctx.ChangeChild(*consumer, TDqStage::idx_Inputs, std::move(newConsumer));
    }

    return ctx.ReplaceNode(std::move(input), *consumer, newConsumer);
}

TExprNode::TPtr ReplicateDqConnection(TExprNode::TPtr&& input, const TMultiUsedConnection& muConnection,
    TExprContext& ctx)
{
    YQL_CLOG(TRACE, CoreDq) << "-- ReplicateDqConnection: " << NCommon::ExprToPrettyString(ctx, *input);

    YQL_ENSURE(!muConnection.Output.Defined());
    auto dqOutput = muConnection.Connection.Output();
    auto dqStage = dqOutput.Stage().Cast<TDqStage>();
    auto outputIndex = dqOutput.Index();

    auto& consumers = muConnection.Consumers;
    YQL_ENSURE(consumers.size() > 1);
    if (GetStageOutputsCount(dqStage, outputIndex, ctx) > 1 && !dqStage.Program().Body().Maybe<TDqReplicate>()) {
        // create a stage with single output, which is used by multiple consumers
        auto newStage = Build<TDqStage>(ctx, dqStage.Pos())
            .Inputs()
                .Add(muConnection.Connection)
                .Build()
            .Program()
                .Args({"arg"})
                .Body("arg")
                .Build()
            .Settings(TDqStageSettings().BuildNode(ctx, dqStage.Pos()))
            .Done();
        TNodeSet processed;
        for (const auto& consumer : consumers) {
            if (processed.contains(consumer)) {
                continue;
            }
            processed.insert(consumer);
            input = ReplaceStageForConsumer(newStage, consumer, std::move(input), ctx, /* skipFirstUsage = */ false, muConnection.Connection.Raw());
        }
        return input;
    }

    // NOTE: Only handle one consumer at a time, as there might be dependencies between them.
    // Ensure stable order by processing connection with minimal ID
    auto& consumer = *std::min_element(consumers.begin(), consumers.end(),
        [](auto l, auto r) { return l->UniqueId() < r->UniqueId(); });

    auto usagesCount = consumers.count(consumer);
    bool isLastConsumer = consumers.size() == usagesCount;

    YQL_CLOG(TRACE, CoreDq) << "-- usagesCount: " << usagesCount << ", isLastConsumer: " << isLastConsumer;

    if (isLastConsumer) {
        YQL_ENSURE(usagesCount > 1);
    }

    auto lambdasCount = isLastConsumer
        ? usagesCount - 1
        : usagesCount;

    YQL_CLOG(TRACE, CoreDq) << "-- lambdas count: " << lambdasCount;

    TVector<TCoLambda> lambdas;
    BuildIdentityLambdas(lambdas, lambdasCount, ctx, dqOutput.Pos());

    auto [newStage, newAdditionalIndexes] = ReplicateStageOutput(dqStage, outputIndex, lambdas, ctx);

    auto result = ReplaceStageForConsumer(newStage, consumer, std::move(input), ctx, /* skipFirstUsage = */ isLastConsumer, muConnection.Connection.Raw(), newAdditionalIndexes);
    return ctx.ReplaceNode(std::move(result), dqStage.Ref(), newStage.Ptr());
}

template <typename TExpr>
TVector<TExpr> CollectNodes(const TExprNode::TPtr& input) {
    TVector<TExpr> result;

    VisitExpr(input, [&result](const TExprNode::TPtr& node) {
        if (TExpr::Match(node.Get())) {
            result.emplace_back(TExpr(node));
        }
        return true;
    });

    return result;
}

bool GatherConsumersImpl(const TExprNode& node, TParentsMultiMap& consumers, TNodeSet& visited) {
    if (!visited.emplace(&node).second) {
        return true;
    }

    switch (node.Type()) {
        case TExprNode::Atom:
        case TExprNode::Argument:
        case TExprNode::Arguments:
        case TExprNode::World:
            return true;

        case TExprNode::List:
        case TExprNode::Callable:
        case TExprNode::Lambda:
            break;
    }

    if (auto stageBase = TMaybeNode<TDqStageBase>(&node)) {
        if (!stageBase.Maybe<TDqStage>()) {
            return false;
        }

        TDqStage stage(&node);
        for (const auto& input : stage.Inputs()) {
            if (auto connection = input.Maybe<TDqConnection>()) {
                consumers[connection.Cast().Raw()].insert(stage.Raw());
                consumers[connection.Cast().Output().Raw()].insert(connection.Raw());
            }

            if (!GatherConsumersImpl(input.Ref(), consumers, visited)) {
                return false;
            }
        }

        if (!GatherConsumersImpl(stage.Program().Ref(), consumers, visited)) {
            return false;
        }

        if (!GatherConsumersImpl(stage.Settings().Ref(), consumers, visited)) {
            return false;
        }

        if (stage.Outputs()) {
            if (!GatherConsumersImpl(stage.Outputs().Ref(), consumers, visited)) {
                return false;
            }
        }

        return true;
    }

    for (const auto& child : node.Children()) {
        if (auto connection = TMaybeNode<TDqConnection>(child)) {
            consumers[connection.Cast().Raw()].insert(&node);
            consumers[connection.Cast().Output().Raw()].insert(connection.Raw());
        }

        if (!GatherConsumersImpl(*child, consumers, visited)) {
            return false;
        }
    }

    return true;
}

bool GatherConsumers(const TExprNode& root, TParentsMultiMap& consumers) {
    TNodeSet visited;
    return GatherConsumersImpl(root, consumers, visited);
}

} // anonymous namespace

IGraphTransformer::TStatus DqReplicateStageMultiOutput(TExprNode::TPtr input, TExprNode::TPtr& output,
    TExprContext& ctx)
{
    output = input;

    // YQL_CLOG(TRACE, CoreDq) << "-- replicate query: " << NCommon::ExprToPrettyString(ctx, *input);
    // YQL_CLOG(TRACE, CoreDq) << "-- replicate query: " << input->Dump();

    TParentsMultiMap consumersMap;
    if (!GatherConsumers(*input, consumersMap)) {
        return IGraphTransformer::TStatus::Ok;
    }

    // rewrite only 1 (any of) multi-used connection at a time
    std::optional<TMultiUsedConnection> multiUsedConnection;
    TDeque<TExprNode::TPtr> precomputes;
    TNodeSet visitedNodes;

    precomputes.emplace_back(input);

    while (!precomputes.empty()) {
        auto head = precomputes.front();
        precomputes.pop_front();

        YQL_CLOG(TRACE, CoreDq) << "DqReplicateStageMultiOutput: start traverse node #" << head->UniqueId()
            << ", " << head->Content();

        visitedNodes.erase(head.Get());

        VisitExpr(
            head,
            [&](const TExprNode::TPtr& ptr) {
                if (ptr == head) {
                    return true;
                }
                if (multiUsedConnection.has_value()) {
                    YQL_CLOG(TRACE, CoreDq) << "DqReplicateStageMultiOutput: already have multiused connection";
                    return false;
                }

                TExprBase expr{ptr};

                if (auto precompute = expr.Maybe<TDqPhyPrecompute>()) {
                    YQL_CLOG(TRACE, CoreDq) << "DqReplicateStageMultiOutput: got precompute (#"
                        << ptr->UniqueId() << "),"
                        << " stop iteration. Child: " << precompute.Connection().Ref().Content()
                        << ", #" << precompute.Connection().Ref().UniqueId();
                    precomputes.emplace_back(ptr);
                    return false;
                }

                if (expr.Maybe<TDqConnection>() && consumersMap.find(expr.Raw()) != consumersMap.end()) {
                    auto connection = expr.Cast<TDqConnection>();
                    YQL_CLOG(TRACE, CoreDq) << "DqReplicateStageMultiOutput: test connection "
                        << connection.Ref().Content() << " (#" << ptr->UniqueId() << ")";
                    const auto& consumers = GetConsumers(connection, consumersMap);
                    if (consumers.size() > 1) {
                        // if connection has multiple consumers - stop traversing and return this connection
                        multiUsedConnection.emplace(connection, consumers);
                        return false;
                    }
                    auto output = connection.Output();
                    const auto& outputConsumers = GetConsumers(output, consumersMap);
                    if (outputConsumers.size() > 1) {
                        // connection has single consumer, but it's output has multiple ones
                        // in that case we check, that this output is used by single-client connections only
                        bool allConnectionsWithSingleConsumer = true;
                        for (auto conn : outputConsumers) {
                            bool singleConsumer = GetConsumers(TExprBase(conn), consumersMap).size() == 1;
                            allConnectionsWithSingleConsumer &= singleConsumer;
                        }
                        if (allConnectionsWithSingleConsumer) {
                            multiUsedConnection.emplace(connection, consumers);
                            multiUsedConnection->Output.ConstructInPlace(output, outputConsumers);
                        } else {
                            YQL_ENSURE(false);
                        }
                    }
                }

                return true;
            },
            visitedNodes);
    }

    if (!multiUsedConnection.has_value()) {
        return IGraphTransformer::TStatus::Ok;
    }

    YQL_CLOG(TRACE, CoreDq) << "DqReplicateStageMultiOutput: " << multiUsedConnection->Print(ctx);

    if (multiUsedConnection->Output.Defined()) {
        output = ReplicateDqOutput(std::move(input), *multiUsedConnection, ctx);
    } else {
        output = ReplicateDqConnection(std::move(input), *multiUsedConnection, ctx);
    }

    return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
}

IGraphTransformer::TStatus DqExtractPrecomputeToStageInput(const TExprNode::TPtr& input, TExprNode::TPtr& output,
    TExprContext& ctx)
{
    auto stages = CollectNodes<TDqStage>(input);

    TNodeOnNodeOwnedMap replaces;
    for (auto& stage : stages) {
        auto dqPrecomputes = CollectNodes<TDqPhyPrecompute>(stage.Program().Ptr());
        if (dqPrecomputes.empty()) {
            continue;
        }

        YQL_CLOG(TRACE, CoreDq) << "DqExtractPrecomputeToStageInput: stage: " << PrintDqStageOnly(stage, ctx)
                << ", DqPhyPrecompute: " << dqPrecomputes.size();

        TVector<TExprNode::TPtr> inputs;
        TVector<TExprNode::TPtr> args;
        inputs.reserve(stage.Inputs().Size() + dqPrecomputes.size());
        args.reserve(stage.Inputs().Size() + dqPrecomputes.size());

        auto exprApplier = Build<TExprApplier>(ctx, stage.Pos())
            .Apply(stage.Program());

        for (ui64 i = 0; i < stage.Inputs().Size(); ++i) {
            inputs.emplace_back(stage.Inputs().Item(i).Ptr());
            args.emplace_back(ctx.NewArgument(stage.Pos(), TStringBuilder() << "_kqp_arg_" << i));
            exprApplier.With(i, TCoArgument(args.back()));
        }

        for (ui64 i = 0; i < dqPrecomputes.size(); ++i) {
            inputs.emplace_back(dqPrecomputes[i].Ptr());
            args.emplace_back(ctx.NewArgument(stage.Pos(), TStringBuilder() << "_kqp_pc_arg_" << i));
            exprApplier.With(dqPrecomputes[i], TCoArgument(args.back()));
        }

        auto newStage = Build<TDqStage>(ctx, stage.Pos())
            .Inputs()
                .Add(inputs)
                .Build()
            .Program()
                .Args(args)
                .Body(exprApplier.Done())
                .Build()
            .Settings().Build()
            .Done();

        replaces.emplace(stage.Raw(), newStage.Ptr());
    }

    if (replaces.empty()) {
        return IGraphTransformer::TStatus::Ok;
    }

    return RemapExpr(input, output, replaces, ctx, TOptimizeExprSettings(nullptr));
}

} // NKikimr::NKqp
