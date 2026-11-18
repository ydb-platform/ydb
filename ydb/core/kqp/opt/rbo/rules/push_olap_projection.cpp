#include <ydb/core/kqp/opt/physical/kqp_opt_phy_olap_filter.h>
#include <ydb/core/kqp/opt/physical/predicate_collector.h>
#include <ydb/core/kqp/opt/rbo/kqp_expression.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

namespace NKikimr::NKqp {

namespace {

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

// The input of the map without the filter the column shard could not run, which stays between the map and the read.
IOperator* SkipRemainingFilter(IOperator* input) {
    return input->Kind == EOperator::Filter ? input->GetChildren().front() : input;
}

// The port the map reads when other operators share its read (the spilling plan keeps one read for them), or null.
TOpReplicate* GetSharedReadPort(IOperator* input) {
    if (input->Kind != EOperator::Replicate) {
        return nullptr;
    }
    auto& port = CastOperator<TOpReplicate>(*input);
    return port.GetInput()->Kind == EOperator::Source ? &port : nullptr;
}

// The read under the map: directly, under the remaining filter, or shared through a port.
IOperator* GetReadUnderMap(IOperator* input) {
    if (auto* port = GetSharedReadPort(input)) {
        return port->GetInput().Get();
    }
    return SkipRemainingFilter(input);
}

bool HasOlapProjections(const TExprNode::TPtr& lambda) {
    return lambda && FindNode(lambda, [](const TExprNode::TPtr& node) { return !!TMaybeNode<TKqpOlapProjections>(node); });
}

// Whether `body` reads `column` of the row `arg` other than through `projection`. Both uses can share one Member node, so
// the projection is cut out of the body before the search instead of counting the members.
bool ReadsColumnBesideProjection(const TExprNode::TPtr& body, const TExprNode& projection, const TExprNode& arg, const TString& column,
                                 TExprContext& ctx) {
    const auto rest = ctx.ReplaceNode(TExprNode::TPtr(body), projection, ctx.NewCallable(projection.Pos(), "Void", {}));
    return !!FindNode(rest, [&](const TExprNode::TPtr& node) {
        return node->IsCallable("Member") && node->HeadPtr().Get() == &arg && node->Tail().Content() == column;
    });
}

bool IsSuitableToPushProjectionToColumnTables(IOperator* input) {
    if (input->Kind != EOperator::Map) {
        return false;
    }

    const auto map = CastOperator<TOpMap>(input);
    const auto maybeRead = GetReadUnderMap(map->GetInput().Get());
    return ((maybeRead->Kind == EOperator::Source) && (CastOperator<TOpRead>(maybeRead)->GetTableStorageType() == NYql::EStorageType::ColumnStorage) &&
            map->GetTypeAnn());
}

TIntrusivePtr<TOpRead> BuildReadWithOlapProjections(const TOpRead& read,
                                                    const TVector<std::pair<TString, TExprNode::TPtr>>& operations,
                                                    TExprContext& ctx) {
    TVector<TExprBase> projections;
    for (const auto& [columnName, olapOperation] : operations) {
        // clang-format off
        projections.push_back(Build<TKqpOlapProjection>(ctx, olapOperation->Pos())
            .OlapOperation(olapOperation)
            .ColumnName().Build(columnName)
        .Done());
        // clang-format on
    }

    const auto olapProcess = read.OlapFilterLambda ? TCoLambda(read.OlapFilterLambda) :
                                                   // clang-format off
        Build<TCoLambda>(ctx, read.Pos)
            .Args({"arg_0"})
            .Body("arg_0")
        .Done();
    // clang-format on

    // clang-format off
    auto olapProjections = Build<TKqpOlapProjections>(ctx, olapProcess.Pos())
        .Input(olapProcess.Body())
        .Projections()
            .Add(projections)
        .Build()
    .Done();
    auto newLambda = Build<TCoLambda>(ctx, olapProcess.Pos())
        .Args({"arg"})
        .Body<TExprApplier>()
            .Apply(olapProjections)
            .With(olapProcess.Args().Arg(0), "arg")
        .Build()
    .Done().Ptr();
    // clang-format on

    YQL_CLOG(TRACE, ProviderKqp) << "Pushed OLAP projection: " << KqpExprToPrettyString(TExprBase(newLambda), ctx);
    return MakeIntrusive<TOpRead>(read.Alias, read.GetColumns(), read.StorageType, read.TableCallable, newLambda,
                                  read.Limit, read.RangeInfo, read.OriginalPredicate, read.SortDir, read.Props,
                                  read.Pos, read.RequireSortedRead);
}

struct TReplicatePortJsonValue {
    TOpMap* Map;
    TInfoUnitId Output;
    TInfoUnitId SourceColumn;
    TInfoUnitId PortColumn;
    TString Path;
    TExprNode::TPtr Lambda;
};

struct TReplicateJsonValueBranchRewrite {
    TOpMap* Map;
    TInfoUnitId Output;
    TExprNode::TPtr Lambda;
    TExprNode::TPtr OlapOperation;
};

struct TReplicateJsonValueRewrite {
    TInfoUnitId SourceColumn;
    TExprNode::TPtr OlapOperation;
    TVector<TReplicateJsonValueBranchRewrite> Branches;
};

std::optional<std::pair<TInfoUnitId, TInfoUnitId>> FindReplicateSourceAndPortColumns(TOpReplicate& port, TOpRead& read, TInfoUnitId memberId) {
    for (const auto source : read.GetOutputIUs()) {
        const auto portColumn = port.IsPrimary() ? source : port.GetRebindings().At(source);
        if (memberId == portColumn) {
            return std::pair{source, portColumn};
        }
    }
    return std::nullopt;
}

bool UsesSourceOutsideJsonValue(const TExprNode::TPtr& node, const TExprNode& jsonValue,
                                const TExprNode& rowArg, TStringBuf column) {
    return !!FindNode(node,
                      [&jsonValue, &rowArg](const TExprNode::TPtr& current) {
            if (current.Get() == &jsonValue) {
                return false;
            }
            const auto member = TMaybeNode<TCoMember>(current);
            return !(member && member.Cast().Struct().Raw() == &rowArg);
        },
                      [&rowArg, column](const TExprNode::TPtr& current) {
            if (const auto member = TMaybeNode<TCoMember>(current); member && member.Cast().Struct().Raw() == &rowArg) {
                return member.Cast().Name().StringValue() == column;
            }
            return current.Get() == &rowArg;
        });
}

std::optional<TReplicatePortJsonValue> FindReplicatePortJsonValue(TOpReplicate& port, TOpRead& read) {
    if (port.Parents.size() != 1 || port.Parents.front().first->Kind != EOperator::Map) {
        return std::nullopt;
    }
    auto* map = CastOperator<TOpMap>(port.Parents.front().first);
    if (map->GetInput().Get() != &port) {
        return std::nullopt;
    }

    std::optional<TReplicatePortJsonValue> result;
    for (const auto& [id, element] : map->GetMapElements().Items()) {
        const auto lambda = TCoLambda(element.GetExpression().Node);
        const auto jsonValues = FindNodes(lambda.Body().Ptr(), [](const TExprNode::TPtr& node) {
            return TCoJsonValue::Match(node.Get());
        });
        if (jsonValues.empty()) {
            continue;
        }
        if (result || jsonValues.size() != 1 || lambda.Args().Size() != 1) {
            return std::nullopt;
        }
        const auto jsonValue = TCoJsonValue(jsonValues.front());
        const auto member = jsonValue.Json().Maybe<TCoMember>();
        if (!member || !CanBePushedAsOlapJsonValue(jsonValue, lambda.Args().Arg(0).Raw()) ||
            member.Cast().Struct().Raw() != lambda.Args().Arg(0).Raw()) {
            return std::nullopt;
        }
        const auto columns = FindReplicateSourceAndPortColumns(port, read, GetMemberId(member.Cast().Ref()));
        if (!columns || GetLiveOut(map).Contains(columns->second) ||
            UsesSourceOutsideJsonValue(lambda.Body().Ptr(), jsonValue.Ref(), lambda.Args().Arg(0).Ref(),
                                       member.Cast().Name().StringValue())) {
            return std::nullopt;
        }
        result.emplace(TReplicatePortJsonValue{map, id, columns->first, columns->second,
            TString(jsonValue.JsonPath().Cast<TCoUtf8>().Literal().Value()), lambda.Ptr()});
    }
    return result;
}

std::optional<TReplicateJsonValueBranchRewrite> BuildReplicateJsonValueBranchRewrite(
    const TReplicatePortJsonValue& portJsonValue, TRBOContext& ctx) {
    const auto lambda = TCoLambda(portJsonValue.Lambda);
    THashSet<TString> projectionMembers;
    TVector<std::tuple<TString, TExprNode::TPtr, TExprNode::TPtr, TExprNode::TPtr>> candidates;
    ui32 nextMemberId = 0;
    if (!CollectOlapOperationForProjection(lambda.Body().Ptr(), lambda.Args().Arg(0).Ref(), {},
                                           projectionMembers, candidates, nextMemberId, ctx.ExprCtx,
                                           TPushdownOptions(false, false, false)) ||
        candidates.size() != 1) {
        return std::nullopt;
    }
    for (const auto& [otherId, otherElement] : portJsonValue.Map->GetMapElements().Items()) {
        if (otherId != portJsonValue.Output && otherElement.GetExpression().GetRawInputIUs().Contains(portJsonValue.PortColumn)) {
            return std::nullopt;
        }
    }
    const auto replacement = std::get<2>(candidates.front());
    const auto rewrittenBody = ctx.ExprCtx.ReplaceNode(lambda.Body().Ptr(), *std::get<1>(candidates.front()), replacement);
    auto newLambda = Build<TCoLambda>(ctx.ExprCtx, lambda.Pos())
                         .Args({"arg"})
                         .Body<TExprApplier>()
                         .Apply(TExprBase(rewrittenBody))
                         .With(lambda.Args().Arg(0), "arg")
                         .Build()
                         .Done()
                         .Ptr();
    return TReplicateJsonValueBranchRewrite{ portJsonValue.Map, portJsonValue.Output, std::move(newLambda), std::get<3>(candidates.front()) };
}

std::optional<TReplicateJsonValueRewrite> BuildReplicateJsonValueRewrite(TReplicate& hub, TOpRead& read, TRBOContext& ctx) {
    TVector<TReplicateJsonValueBranchRewrite> branches;
    std::optional<TInfoUnitId> sourceColumn;
    TString path;
    TExprNode::TPtr olapOperation;

    for (auto* port : hub.GetOutputs()) {
        const auto portJsonValue = FindReplicatePortJsonValue(*port, read);
        if (!portJsonValue || (sourceColumn && (*sourceColumn != portJsonValue->SourceColumn || path != portJsonValue->Path))) {
            return std::nullopt;
        }

        auto branch = BuildReplicateJsonValueBranchRewrite(*portJsonValue, ctx);
        if (!branch) {
            return std::nullopt;
        }
        sourceColumn = portJsonValue->SourceColumn;
        path = portJsonValue->Path;
        if (port->IsPrimary()) {
            olapOperation = branch->OlapOperation;
        }
        branches.push_back(std::move(*branch));
    }

    if (!sourceColumn || !olapOperation) {
        return std::nullopt;
    }
    return TReplicateJsonValueRewrite{ *sourceColumn, std::move(olapOperation), std::move(branches) };
}

} // anonymous namespace

bool TPushOlapProjectionRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Map &&
        GetReadUnderMap(input->GetChildren().front())->Kind == EOperator::Source;
}

TIntrusivePtr<IOperator> TPushOlapProjectionRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (!(ctx.KqpCtx.Config->HasOptEnableOlapPushdown() && ctx.KqpCtx.Config->GetEnableOlapPushdownProjections())) {
        return input;
    }

    if (!IsSuitableToPushProjectionToColumnTables(input.get())) {
        return input;
    }

    const auto map = CastOperator<TOpMap>(input);
    TIntrusivePtr<TOpFilter> filter;
    if (map->GetInput()->Kind == EOperator::Filter) {
        filter = CastOperator<TOpFilter>(map->GetInput());
    }
    auto* port = GetSharedReadPort(map->GetInput().Get());
    const auto read = CastOperator<TOpRead>(GetReadUnderMap(map->GetInput().Get()));
    // A shared read takes the projections of one consumer: those of another would nest in its program.
    if (port && HasOlapProjections(read->OlapFilterLambda)) {
        return input;
    }
    // A port other than the first names the columns of the read by its own IDs. The projections are found and built over
    // the IDs of the read, and the elements which read them are renamed back to the IDs of the port.
    TSubstitutions toRead;
    TSubstitutions fromRead;
    if (port && !port->IsPrimary()) {
        for (const auto& [source, output] : port->GetRebindings().Items()) {
            toRead.Add(output, source);
            fromRead.Add(source, output);
        }
    }
    TMapIUs mapElements;
    for (const auto& [output, element] : map->GetMapElements().Items()) {
        mapElements.Add(output, TMapElement(element.GetExpression().ApplyRenames(toRead)));
    }
    // Preserve the input's IU-ID atoms and row-schema fields during construction.
    const TPushdownOptions pushdownOptions(false, false, /*StripAliasPrefixForColumnName=*/false);

    TVector<std::pair<TString, TExprNode::TPtr>> olapOperationsForProjections;
    auto memberPred = [](const TExprNode::TPtr& node) -> bool { return !!TMaybeNode<TCoMember>(node); };
    THashSet<TString> projectionMembers;
    // As in the old optimizer, a projection cannot overwrite a column the remaining filter still reads.
    THashSet<TString> predicateMembers;
    if (filter) {
        for (const auto id : filter->GetFilterExpression().GetRawInputIUs()) {
            predicateMembers.insert(ToString(id));
        }
    }
    THashSet<TString> notSuitableToPushMembers;
    // OLAP projection overwrites the source field. An implicit pass-through
    // or an explicit copy still needs its original value and type.
    auto passthrough = GetLiveOut(map.Get());
    if (port) {
        passthrough.IntersectWith(port->GetOutputIUs());
        passthrough = port->MapToInput(passthrough);
    } else {
        passthrough.IntersectWith(read->GetOutputIUs());
    }
    for (const auto id : passthrough) {
        notSuitableToPushMembers.insert(ToString(id));
    }
    for (const auto& [id, element] : mapElements.Items()) {
        if (element.IsColumnAccess()) {
            notSuitableToPushMembers.insert(ToString(element.GetColumnAccess()));
        }
    }
    if (port) {
        // The projection overwrites its column in the read all the ports share, so no other port may still read the column.
        for (auto* other : port->GetReplicate().GetOutputs()) {
            if (other != port) {
                for (const auto id : other->MapToInput(GetLiveOut(other))) {
                    notSuitableToPushMembers.insert(ToString(id));
                }
            }
        }
    }
    ui32 nextMemberId = 0;

    TVector<std::tuple<TString, TExprNode::TPtr, TExprNode::TPtr, TExprNode::TPtr>> projectionCandidates;
    TVector<TInfoUnitId> inMapIds;
    // Iterate over map elements and try to find an expression to push down to column shard.
    for (const auto output : mapElements.Keys()) {
        const auto& mapElement = *mapElements.Find(output);
        if (!mapElement.IsColumnAccess()) {
            const auto lambda = TCoLambda(mapElement.GetExpression().Node);
            const auto& arg = lambda.Args().Arg(0).Ref();
            auto body = lambda.Body().Ptr();
            if (!CollectOlapOperationForProjection(body, arg, predicateMembers, projectionMembers, projectionCandidates, nextMemberId, ctx.ExprCtx,
                                                   pushdownOptions)) {
                auto members = FindNodes(body, memberPred);
                for (const auto& member : members) {
                    notSuitableToPushMembers.insert(TString(TExprBase(member).Cast<TCoMember>().Name()));
                }
            } else {
                inMapIds.push_back(output);
                // The projection overwrites its column, so the rest of the expression must not read the column too, as in
                // IF(c IS NULL, "none", JSON_VALUE(c, "$.a")).
                const auto& [colName, projection, replace, olapOperation] = projectionCandidates.back();
                if (ReadsColumnBesideProjection(body, *projection, arg, colName, ctx.ExprCtx)) {
                    notSuitableToPushMembers.insert(colName);
                }
            }
        }
    }

    if (projectionCandidates.empty()) {
        return input;
    }

    ui32 projectionIndex = 0;
    TMapIUs newMapElements;
    // The elements which read the projections, kept apart when they have to move below the remaining filter.
    TMapIUs projectedElements;
    for (const auto output : mapElements.Keys()) {
        TMapElement mapElement = *map->GetMapElements().Find(output);
        bool projected = false;
        if (projectionIndex < inMapIds.size() && inMapIds[projectionIndex] == output) {
            const auto& [colName, projection, replace, olapOperation] = projectionCandidates[projectionIndex++];
            Y_ENSURE(colName.find(NOpt::KqpOlapProjectionNamePrefix) == TString::npos, "Multiple projections for same column is not supported");
            if (!notSuitableToPushMembers.count(colName)) {
                olapOperationsForProjections.emplace_back(colName, olapOperation);
                // Replace old expression with new.
                auto oldLambda = TCoLambda(mapElements.Find(output)->GetExpression().Node);
                // clang-format off
                auto newLambda = Build<TCoLambda>(ctx.ExprCtx, projection->Pos())
                    .Args({"arg"})
                    .Body<TExprApplier>()
                        .Apply(TExprBase(ctx.ExprCtx.ReplaceNode(oldLambda.Body().Ptr(), *projection, replace)))
                        .With(oldLambda.Args().Arg(0), "arg")
                    .Build()
                .Done().Ptr();
                // clang-format on
                mapElement = TMapElement(TExpression(newLambda, &ctx.ExprCtx, &props).ApplyRenames(fromRead));
                projected = true;
            }
        }
        (projected && filter ? projectedElements : newMapElements).Add(output, std::move(mapElement));
    }

    if (olapOperationsForProjections.empty()) {
        return input;
    }

    TIntrusivePtr<IOperator> newInput = BuildReadWithOlapProjections(*read, olapOperationsForProjections, ctx.ExprCtx);
    if (port) {
        // Every port reads the new read. The others do not read the columns it overwrites.
        port->GetReplicate().SetInput(std::move(newInput));
        return MakeIntrusive<TOpMap>(map->GetInput(), map->Pos, newMapElements);
    }
    if (filter) {
        // A projection replaces its source column in the read, with the type of the projection. The filter is typed over the
        // stored column, so the elements which read the projections move below it, onto the read: as in the old optimizer,
        // the column shard computes them before the remaining filter.
        newInput = MakeIntrusive<TOpMap>(newInput, map->Pos, std::move(projectedElements));
        newInput = MakeIntrusive<TOpFilter>(newInput, filter->Pos, filter->Props, filter->GetFilterExpression(), filter->PartiallyPushedDown);
    }
    return MakeIntrusive<TOpMap>(newInput, map->Pos, newMapElements);
}

bool TPushOlapReplicateJsonValueRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Replicate && CastOperator<TOpReplicate>(input)->IsPrimary();
}

bool TPushOlapReplicateJsonValueRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx,
                                                    TPlanProps& props) {
    if (!QuickMatch(input) ||
        !(ctx.KqpCtx.Config->HasOptEnableOlapPushdown() && ctx.KqpCtx.Config->GetEnableOlapPushdownProjections())) {
        return false;
    }

    auto& hub = CastOperator<TOpReplicate>(input)->GetReplicate();
    if (hub.GetOutputs().size() < 2 || hub.GetInput()->Kind != EOperator::Source) {
        return false;
    }
    const auto read = CastOperator<TOpRead>(hub.GetInput());
    if (read->GetTableStorageType() != NYql::EStorageType::ColumnStorage) {
        return false;
    }

    auto rewrite = BuildReplicateJsonValueRewrite(hub, *read, ctx);
    if (!rewrite) {
        return false;
    }
    hub.SetInput(BuildReadWithOlapProjections(*read, { { ToString(rewrite->SourceColumn), rewrite->OlapOperation } }, ctx.ExprCtx));
    for (auto& branch : rewrite->Branches) {
        branch.Map->SetMapElementExpression(branch.Output, TExpression(std::move(branch.Lambda), &ctx.ExprCtx, &props));
    }
    return true;
}

} // namespace NKikimr::NKqp
