#include "yql_yt_helpers.h"
#include "yql_yt_provider_impl.h"
#include "yql_yt_op_settings.h"
#include "yql_yt_op_hash.h"

#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/yt/lib/mkql_helpers/mkql_helpers.h>
#include <ydb/library/yql/providers/yt/common/yql_configuration.h>
#include <ydb/library/yql/providers/yt/opt/yql_yt_key_selector.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/type_ann/type_ann_expr.h>
#include <ydb/library/yql/core/type_ann/type_ann_core.h>
#include <ydb/library/yql/core/issue/protos/issue_id.pb.h>
#include <ydb/library/yql/core/peephole_opt/yql_opt_peephole_physical.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_expr_constraint.h>
#include <ydb/library/yql/core/yql_expr_csee.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/utils/log/log.h>

#include <library/cpp/yson/node/node_io.h>

#include <util/string/cast.h>
#include <util/string/hex.h>
#include <util/generic/xrange.h>
#include <util/generic/utility.h>
#include <util/generic/algorithm.h>
#include <util/generic/bitmap.h>

namespace NYql {

using namespace NNodes;

namespace {

bool IsYtIsolatedLambdaImpl(const TExprNode& lambdaBody, TSyncMap& syncList, TString* usedCluster, bool supportsDq, TNodeSet& visited) {
    if (!visited.insert(&lambdaBody).second) {
        return true;
    }

    if (TMaybeNode<TCoTypeOf>(&lambdaBody)) {
        return true;
    }

    if (auto maybeLength = TMaybeNode<TYtLength>(&lambdaBody)) {
        if (auto maybeRead = maybeLength.Input().Maybe<TYtReadTable>()) {
            auto read = maybeRead.Cast();
            if (usedCluster && !UpdateUsedCluster(*usedCluster, TString{read.DataSource().Cluster().Value()})) {
                return false;
            }
            syncList.emplace(read.Ptr(), syncList.size());
        }
        if (auto maybeOutput = maybeLength.Input().Maybe<TYtOutput>()) {
            auto output = maybeOutput.Cast();
            if (usedCluster && !UpdateUsedCluster(*usedCluster, TString{GetOutputOp(output).DataSink().Cluster().Value()})) {
                return false;
            }
            syncList.emplace(output.Operation().Ptr(), syncList.size());
        }
        return true;
    }

    if (auto maybeContent = TMaybeNode<TYtTableContent>(&lambdaBody)) {
        if (auto maybeRead = maybeContent.Input().Maybe<TYtReadTable>()) {
            auto read = maybeRead.Cast();
            if (usedCluster && !UpdateUsedCluster(*usedCluster, TString{read.DataSource().Cluster().Value()})) {
               return false;
            }
            syncList.emplace(read.Ptr(), syncList.size());
        }
        if (auto maybeOutput = maybeContent.Input().Maybe<TYtOutput>()) {
            auto output = maybeOutput.Cast();
            if (usedCluster && !UpdateUsedCluster(*usedCluster, TString{GetOutputOp(output).DataSink().Cluster().Value()})) {
                return false;
            }
            syncList.emplace(output.Operation().Ptr(), syncList.size());
        }
        return true;
    }

    if (auto maybeContent = TMaybeNode<TDqReadWrapBase>(&lambdaBody)) {
        if (!supportsDq) {
            return false;
        }
        if (auto maybeRead = maybeContent.Input().Maybe<TYtReadTable>()) {
            auto read = maybeRead.Cast();
            if (usedCluster && !UpdateUsedCluster(*usedCluster, TString{read.DataSource().Cluster().Value()})) {
                return false;
            }
            syncList.emplace(read.Ptr(), syncList.size());
        }
        if (auto maybeOutput = maybeContent.Input().Maybe<TYtOutput>()) {
            auto output = maybeOutput.Cast();
            if (usedCluster && !UpdateUsedCluster(*usedCluster, TString{GetOutputOp(output).DataSink().Cluster().Value()})) {
                return false;
            }
            syncList.emplace(output.Operation().Ptr(), syncList.size());
        }
        return true;
    }

    if (!supportsDq && (TDqConnection::Match(&lambdaBody) || TDqPhyPrecompute::Match(&lambdaBody) || TDqStageBase::Match(&lambdaBody) || TDqSourceWrapBase::Match(&lambdaBody))) {
        return false;
    }

    if (auto maybeRead = TMaybeNode<TCoRight>(&lambdaBody).Input().Maybe<TYtReadTable>()) {
        auto read = maybeRead.Cast();
        if (usedCluster && !UpdateUsedCluster(*usedCluster, TString{read.DataSource().Cluster().Value()})) {
            return false;
        }
        syncList.emplace(read.Ptr(), syncList.size());
        return true;
    } else if (auto out = TMaybeNode<TYtOutput>(&lambdaBody)) {
        auto op = GetOutputOp(out.Cast());
        if (usedCluster && !UpdateUsedCluster(*usedCluster, TString{op.DataSink().Cluster().Value()})) {
            return false;
        }
        syncList.emplace(out.Cast().Operation().Ptr(), syncList.size());
        return true;
    }

    if (auto right = TMaybeNode<TCoRight>(&lambdaBody).Input()) {
        if (auto maybeCons = right.Maybe<TCoCons>()) {
            syncList.emplace(maybeCons.Cast().World().Ptr(), syncList.size());
            return IsYtIsolatedLambdaImpl(maybeCons.Cast().Input().Ref(), syncList, usedCluster, supportsDq, visited);
        }

        if (right.Cast().Raw()->IsCallable("PgReadTable!")) {
            syncList.emplace(right.Cast().Raw()->HeadPtr(), syncList.size());
            return true;
        }
    }

    if (lambdaBody.IsCallable("WithWorld")) {
        syncList.emplace(lambdaBody.ChildPtr(1), syncList.size());
        return true;
    }

    if (!lambdaBody.GetTypeAnn()->IsComposable()) {
        return false;
    }

    for (auto& child : lambdaBody.Children()) {
        if (!IsYtIsolatedLambdaImpl(*child, syncList, usedCluster, supportsDq, visited)) {
            return false;
        }
    }

    return true;
}

IGraphTransformer::TStatus EstimateDataSize(TVector<ui64>& result, TSet<TString>& requestedColumns,
    const TString& cluster, const TVector<TYtPathInfo::TPtr>& paths,
    const TMaybe<TVector<TString>>& columns, const TYtState& state, TExprContext& ctx, bool sync)
{
    result.clear();
    result.resize(paths.size(), 0);
    requestedColumns.clear();

    const bool useColumnarStat = GetJoinCollectColumnarStatisticsMode(*state.Configuration) != EJoinCollectColumnarStatisticsMode::Disable
        && !state.Types->UseTableMetaFromGraph;

    TVector<size_t> reqMap;
    TVector<IYtGateway::TPathStatReq> pathStatReqs;
    for (size_t i: xrange(paths.size())) {
        const TYtPathInfo::TPtr& pathInfo = paths[i];
        YQL_ENSURE(pathInfo->Table->Stat);
        result[i] = pathInfo->Table->Stat->DataSize;
        if (pathInfo->Ranges) {
            if (auto usedRows = pathInfo->Ranges->GetUsedRows(pathInfo->Table->Stat->RecordsCount)) {
                if (usedRows.GetRef() && pathInfo->Table->Stat->RecordsCount) {
                    result[i] *= double(usedRows.GetRef()) / double(pathInfo->Table->Stat->RecordsCount);
                } else {
                    result[i] = 0;
                }
            }
        }

        if (useColumnarStat) {
            TMaybe<TVector<TString>> overrideColumns;
            if (columns && pathInfo->Table->RowSpec && (pathInfo->Table->RowSpec->StrictSchema || nullptr == FindPtr(*columns, YqlOthersColumnName))) {
                overrideColumns = columns;
            }

            auto ytPath = BuildYtPathForStatRequest(cluster, *pathInfo, overrideColumns, state, ctx);
            if (!ytPath) {
                return IGraphTransformer::TStatus::Error;
            }

            if (ytPath->Columns_) {
                pathStatReqs.push_back(
                    IYtGateway::TPathStatReq()
                        .Path(*ytPath)
                        .IsTemp(pathInfo->Table->IsTemp)
                        .IsAnonymous(pathInfo->Table->IsAnonymous)
                        .Epoch(pathInfo->Table->Epoch.GetOrElse(0))
                );
                reqMap.push_back(i);
            }
        }
    }

    if (!pathStatReqs.empty()) {
        for (auto& req : pathStatReqs) {
            YQL_ENSURE(req.Path().Columns_);
            requestedColumns.insert(req.Path().Columns_->Parts_.begin(), req.Path().Columns_->Parts_.end());
        }

        IYtGateway::TPathStatResult pathStats;
        IYtGateway::TPathStatOptions pathStatOptions =
            IYtGateway::TPathStatOptions(state.SessionId)
                .Cluster(cluster)
                .Paths(pathStatReqs)
                .Config(state.Configuration->Snapshot());
        if (sync) {
            auto future = state.Gateway->PathStat(std::move(pathStatOptions));
            pathStats = future.GetValueSync();
            pathStats.ReportIssues(ctx.IssueManager);
            if (!pathStats.Success()) {
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            pathStats = state.Gateway->TryPathStat(std::move(pathStatOptions));
            if (!pathStats.Success()) {
                return IGraphTransformer::TStatus::Repeat;
            }
        }

        YQL_ENSURE(pathStats.DataSize.size() == reqMap.size());
        for (size_t i: xrange(pathStats.DataSize.size())) {
            result[reqMap[i]] = pathStats.DataSize[i];
        }
    }

    return IGraphTransformer::TStatus::Ok;
}

bool NeedCalc(NNodes::TExprBase node) {
    auto type = node.Ref().GetTypeAnn();
    if (type->IsSingleton()) {
        return false;
    }

    if (type->GetKind() == ETypeAnnotationKind::Optional) {
        if (node.Maybe<TCoNothing>()) {
            return false;
        }
        if (auto maybeJust = node.Maybe<TCoJust>()) {
            return NeedCalc(maybeJust.Cast().Input());
        }
        return true;
    }

    if (type->GetKind() == ETypeAnnotationKind::Tuple) {
        if (auto maybeTuple = node.Maybe<TExprList>()) {
            return AnyOf(maybeTuple.Cast(), [](const auto& item) { return NeedCalc(item); });
        }
        return true;
    }

    if (type->GetKind() == ETypeAnnotationKind::List) {
        if (node.Maybe<TCoList>()) {
            YQL_ENSURE(node.Ref().ChildrenSize() == 1, "Should be rewritten to AsList");
            return false;
        }
        if (auto maybeAsList = node.Maybe<TCoAsList>()) {
            return AnyOf(maybeAsList.Cast().Args(), [](const auto& item) { return NeedCalc(NNodes::TExprBase(item)); });
        }
        return true;
    }

    YQL_ENSURE(type->GetKind() == ETypeAnnotationKind::Data,
        "Object of type " << *type << " should not be considered for calculation");

    return !node.Maybe<TCoDataCtor>();
}

} // unnamed

bool UpdateUsedCluster(TString& usedCluster, const TString& newCluster) {
    if (!usedCluster) {
        usedCluster = newCluster;
    } else if (usedCluster != newCluster) {
        return false;
    }
    return true;
}

bool IsYtIsolatedLambda(const TExprNode& lambdaBody, TSyncMap& syncList, bool supportsDq) {
    TNodeSet visited;
    return IsYtIsolatedLambdaImpl(lambdaBody, syncList, nullptr, supportsDq, visited);
}

bool IsYtIsolatedLambda(const TExprNode& lambdaBody, TSyncMap& syncList, TString& usedCluster, bool supportsDq) {
    TNodeSet visited;
    return IsYtIsolatedLambdaImpl(lambdaBody, syncList, &usedCluster, supportsDq, visited);
}

bool IsYtCompleteIsolatedLambda(const TExprNode& lambda, TSyncMap& syncList, bool supportsDq) {
    return lambda.IsComplete() && IsYtIsolatedLambda(lambda, syncList, supportsDq);
}

bool IsYtCompleteIsolatedLambda(const TExprNode& lambda, TSyncMap& syncList, TString& usedCluster, bool supportsDq) {
    return lambda.IsComplete() && IsYtIsolatedLambda(lambda, syncList, usedCluster, supportsDq);
}

TExprNode::TPtr YtCleanupWorld(const TExprNode::TPtr& input, TExprContext& ctx, TYtState::TPtr state) {
    TExprNode::TPtr output = input;

    TNodeOnNodeOwnedMap remaps;
    VisitExpr(output, [&remaps, &ctx](const TExprNode::TPtr& node) {
        if (TYtLength::Match(node.Get())) {
            return false;
        }

        if (TYtTableContent::Match(node.Get())) {
            return false;
        }

        if (auto read = TMaybeNode<TCoRight>(node).Input().Maybe<TYtReadTable>()) {
            remaps[node.Get()] = Build<TYtTableContent>(ctx, node->Pos())
                .Input(read.Cast())
                .Settings().Build()
                .Done().Ptr();

            return false;
        }

        if (TYtReadTable::Match(node.Get())) {
            return false;
        }

        if (node->IsCallable("WithWorld")) {
            remaps[node.Get()] = node->HeadPtr();
            return false;
        }

        TDynBitMap outs;
        for (size_t i = 0; i < node->ChildrenSize(); ++i) {
            if (TYtOutput::Match(node->Child(i))) {
                outs.Set(i);
            }
        }
        if (!outs.Empty()) {
            auto res = node;
            Y_FOR_EACH_BIT(i, outs) {
                res = ctx.ChangeChild(*res, i,
                    Build<TYtTableContent>(ctx, node->Pos())
                        .Input(node->ChildPtr(i))
                        .Settings().Build()
                        .Done().Ptr()
                    );
            }
            remaps[node.Get()] = res;
        }

        if (TYtOutput::Match(node.Get())) {
            return false;
        }

        if (auto right = TMaybeNode<TCoRight>(node)) {
            auto cons = right.Cast().Input().Maybe<TCoCons>();
            if (cons) {
                remaps[node.Get()] = cons.Cast().Input().Ptr();
                return false;
            }

            if (right.Cast().Input().Ref().IsCallable("PgReadTable!")) {
                const auto& read = right.Cast().Input().Ref();
                remaps[node.Get()] = ctx.Builder(node->Pos())
                    .Callable("PgTableContent")
                        .Add(0, read.Child(1)->TailPtr())
                        .Add(1, read.ChildPtr(2))
                        .Add(2, read.ChildPtr(3))
                        .Add(3, read.ChildPtr(4))
                    .Seal()
                    .Build();
            }
        }

        return true;
    });

    if (output->IsLambda() && TYtOutput::Match(output->Child(1))) {
        remaps[output->Child(1)] = Build<TYtTableContent>(ctx, output->Child(1)->Pos())
            .Input(output->ChildPtr(1))
            .Settings().Build()
            .Done().Ptr();
    }

    IGraphTransformer::TStatus status = IGraphTransformer::TStatus::Ok;
    if (!remaps.empty()) {
        TOptimizeExprSettings settings(state->Types);
        settings.VisitChanges = true;
        settings.VisitTuples = true;
        status = RemapExpr(output, output, remaps, ctx, settings);
    }

    remaps.clear();
    TNodeSet visitedReadTables;
    ui64 sumSize = 0;
    TMaybe<TPositionHandle> bigPos;
    VisitExpr(output, [&remaps, &ctx, &visitedReadTables, &sumSize, &bigPos, state](const TExprNode::TPtr& node) {
        if (auto maybeRead = TMaybeNode<TYtReadTable>(node)) {
            if (state->Types->EvaluationInProgress &&
                state->Configuration->EvaluationTableSizeLimit.Get() &&
                visitedReadTables.emplace(maybeRead.Cast().Raw()).second) {
                for (auto section : TYtSectionList(maybeRead.Cast().Input())) {
                    for (auto path : section.Paths()) {
                        auto info = TYtTableBaseInfo::Parse(path.Table());
                        if (info && info->Stat) {
                            sumSize += info->Stat->DataSize;
                            if (info->Stat->DataSize > *state->Configuration->EvaluationTableSizeLimit.Get()) {
                                bigPos = path.Table().Pos();
                            }
                        }
                    }
                }
            }

            if (maybeRead.Cast().World().Ref().Type() != TExprNode::World) {
                remaps[node.Get()] = ctx.ChangeChild(*node, 0, ctx.NewWorld(node->Pos()));
            }
            return false;
        }
        if (TYtOutput::Match(node.Get())) {
            return false;
        }
        return true;
    });

    if (state->Types->EvaluationInProgress && state->Configuration->EvaluationTableSizeLimit.Get()) {
        if (sumSize > *state->Configuration->EvaluationTableSizeLimit.Get()) {
            ctx.AddError(TIssue(ctx.GetPosition(bigPos.GetOrElse(input->Pos())), TStringBuilder() << "Too large table(s) for evaluation pass: "
                << sumSize << " > " << *state->Configuration->EvaluationTableSizeLimit.Get()));
            return nullptr;
        }
    }

    if (!remaps.empty()) {
        TOptimizeExprSettings settings(state->Types);
        settings.VisitChanges = true;
        status = status.Combine(RemapExpr(output, output, remaps, ctx, settings));
    }

    YQL_ENSURE(status.Level != IGraphTransformer::TStatus::Error, "Bad input graph");

    if (state->Types->EvaluationInProgress) {
        status = status.Combine(SubstTables(output, state, false, ctx));
        YQL_ENSURE(status.Level != IGraphTransformer::TStatus::Error, "Subst tables failed");
    }

    return output;
}

TYtOutputOpBase GetOutputOp(TYtOutput output) {
    if (const auto tr = output.Operation().Maybe<TYtTryFirst>()) {
        return tr.Cast().Second();
    }
    return output.Operation().Cast<TYtOutputOpBase>();
}

TVector<TYtTableBaseInfo::TPtr> GetInputTableInfos(TExprBase input) {
    TVector<TYtTableBaseInfo::TPtr> res;
    if (auto out = input.Maybe<TYtOutput>()) {
        res.push_back(MakeIntrusive<TYtOutTableInfo>(GetOutTable(out.Cast())));
        res.back()->IsUnordered = IsUnorderedOutput(out.Cast());
    } else {
        auto read = input.Maybe<TCoRight>().Input().Maybe<TYtReadTable>();
        YQL_ENSURE(read, "Unknown operation input");
        for (auto section: read.Cast().Input()) {
            for (auto path: section.Paths()) {
                res.push_back(TYtTableBaseInfo::Parse(path.Table()));
            }
        }
    }
    return res;
}

TVector<TYtPathInfo::TPtr> GetInputPaths(TExprBase input) {
    TVector<TYtPathInfo::TPtr> res;
    if (auto out = input.Maybe<TYtOutput>()) {
        res.push_back(MakeIntrusive<TYtPathInfo>());
        res.back()->Table = MakeIntrusive<TYtOutTableInfo>(GetOutTable(out.Cast()));
        res.back()->Table->IsUnordered = IsUnorderedOutput(out.Cast());
    } else {
        auto read = input.Maybe<TCoRight>().Input().Maybe<TYtReadTable>();
        YQL_ENSURE(read, "Unknown operation input");
        for (auto section: read.Cast().Input()) {
            for (auto path: section.Paths()) {
                res.push_back(MakeIntrusive<TYtPathInfo>(path));
            }
        }
    }
    return res;
}

TStringBuf GetClusterName(NNodes::TExprBase input) {
    if (auto read = input.Maybe<TCoRight>().Input().Maybe<TYtReadTable>()) {
        return read.Cast().DataSource().Cluster().Value();
    } else if (auto output = input.Maybe<TYtOutput>()) {
        return GetOutputOp(output.Cast()).DataSink().Cluster().Value();
    } else if (auto op = input.Maybe<TCoRight>().Input().Maybe<TYtOutputOpBase>()) {
        return op.Cast().DataSink().Cluster().Value();
    } else {
        YQL_ENSURE(false, "Unknown operation input");
    }
}

bool IsYtProviderInput(NNodes::TExprBase input, bool withVariantList) {
    if (input.Maybe<TYtOutput>()) {
        return true;
    }
    if (auto maybeYtInput = input.Maybe<TCoRight>().Input()) {
        if (withVariantList && maybeYtInput.Maybe<TYtOutputOpBase>()) {
            return true;
        }
        if (auto maybeRead = maybeYtInput.Maybe<TYtReadTable>()) {
            return withVariantList || maybeRead.Cast().Input().Size() == 1;
        }
    }
    return false;
}

bool IsConstExpSortDirections(NNodes::TExprBase sortDirections) {
    if (sortDirections.Maybe<TCoBool>()) {
        return true;
    } else if (sortDirections.Maybe<TExprList>()) {
        for (auto child: sortDirections.Cast<TExprList>()) {
            if (!child.Maybe<TCoBool>()) {
                return false;
            }
        }
        return true;
    }
    return false;
}

TExprNode::TListType GetNodesToCalculate(const TExprNode::TPtr& input) {
    TExprNode::TListType needCalc;
    TNodeSet uniqNodes;
    VisitExpr(input, [&needCalc, &uniqNodes](const TExprNode::TPtr& node) {
        if (auto maybeOp = TMaybeNode<TYtTransientOpBase>(node)) {
            auto op = maybeOp.Cast();
            for (auto setting: op.Settings()) {
                switch (FromString<EYtSettingType>(setting.Name().Value())) {
                case EYtSettingType::Limit:
                    for (auto expr: setting.Value().Cast().Ref().Children()) {
                        for (auto item: expr->Children()) {
                            if (uniqNodes.insert(item->Child(1)).second) {
                                if (NeedCalc(TExprBase(item->Child(1)))) {
                                    needCalc.push_back(item->ChildPtr(1));
                                }
                            }
                        }
                    }
                    break;
                default:
                    break;
                }
            }
        }
        else if (auto maybeSection = TMaybeNode<TYtSection>(node)) {
            TYtSection section = maybeSection.Cast();
            for (auto setting: section.Settings()) {
                switch (FromString<EYtSettingType>(setting.Name().Value())) {
                case EYtSettingType::Take:
                case EYtSettingType::Skip:
                    if (uniqNodes.insert(setting.Value().Cast().Raw()).second) {
                        if (NeedCalc(setting.Value().Cast())) {
                            needCalc.push_back(setting.Value().Cast().Ptr());
                        }
                    }
                    break;
                case EYtSettingType::KeyFilter: {
                    auto value = setting.Value().Cast<TExprList>();
                    if (value.Size() > 0) {
                        for (auto member: value.Item(0).Cast<TCoNameValueTupleList>()) {
                            for (auto cmp: member.Value().Cast<TCoNameValueTupleList>()) {
                                if (cmp.Value() && uniqNodes.insert(cmp.Value().Cast().Raw()).second) {
                                    if (NeedCalc(cmp.Value().Cast())) {
                                        needCalc.push_back(cmp.Value().Cast().Ptr());
                                    }
                                }
                            }
                        }
                    }
                    break;
                }
                case EYtSettingType::KeyFilter2: {
                    auto value = setting.Value().Cast<TExprList>();
                    if (value.Size() > 0) {
                        if (uniqNodes.insert(value.Item(0).Raw()).second && NeedCalc(value.Item(0))) {
                            needCalc.push_back(value.Item(0).Ptr());
                        }
                    }
                    break;
                }
                default:
                    break;
                }
            }
        }
        else if (TMaybeNode<TYtOutput>(node)) {
            // Stop traversing dependent operations
            return false;
        }
        return true;
    });
    return needCalc;
}

bool HasNodesToCalculate(const TExprNode::TPtr& input) {
    bool needCalc = false;
    VisitExpr(input, [&needCalc](const TExprNode::TPtr& node) {
        if (auto maybeOp = TMaybeNode<TYtTransientOpBase>(node)) {
            auto op = maybeOp.Cast();
            for (auto setting: op.Settings()) {
                switch (FromString<EYtSettingType>(setting.Name().Value())) {
                case EYtSettingType::Limit:
                    for (auto expr: setting.Value().Cast().Ref().Children()) {
                        for (auto item: expr->Children()) {
                            if (NeedCalc(TExprBase(item->Child(1)))) {
                                needCalc = true;
                                return false;
                            }
                        }
                    }
                    break;
                default:
                    break;
                }
            }
        }
        else if (auto maybeSection = TMaybeNode<TYtSection>(node)) {
            TYtSection section = maybeSection.Cast();
            for (auto setting: section.Settings()) {
                switch (FromString<EYtSettingType>(setting.Name().Value())) {
                case EYtSettingType::Take:
                case EYtSettingType::Skip:
                    if (NeedCalc(setting.Value().Cast())) {
                        needCalc = true;
                        return false;
                    }
                    break;
                case EYtSettingType::KeyFilter: {
                    auto value = setting.Value().Cast<TExprList>();
                    if (value.Size() > 0) {
                        for (auto member: value.Item(0).Cast<TCoNameValueTupleList>()) {
                            for (auto cmp: member.Value().Cast<TCoNameValueTupleList>()) {
                                if (cmp.Value() && NeedCalc(cmp.Value().Cast())) {
                                    needCalc = true;
                                    return false;
                                }
                            }
                        }
                    }
                    break;
                }
                case EYtSettingType::KeyFilter2: {
                    auto value = setting.Value().Cast<TExprList>();
                    if (value.Size() > 0) {
                        if (value.Item(0).Raw() && NeedCalc(value.Item(0))) {
                            needCalc = true;
                            return false;
                        }
                    }
                    break;
                }
                default:
                    break;
                }
            }
        }
        else if (TMaybeNode<TYtOutput>(node)) {
            // Stop traversing dependent operations
            return false;
        }
        return !needCalc;
    });
    return needCalc;
}

std::pair<IGraphTransformer::TStatus, TAsyncTransformCallbackFuture> CalculateNodes(TYtState::TPtr state,
    const TExprNode::TPtr& input,
    const TString& cluster,
    const TExprNode::TListType& needCalc,
    TExprContext& ctx)
{
    YQL_ENSURE(!needCalc.empty());
    YQL_ENSURE(!input->HasResult(), "Infinitive calculation loop detected");
    TNodeMap<size_t> calcNodes;
    TUserDataTable files;

    TExprNode::TPtr list = ctx.NewList(input->Pos(), TExprNode::TListType(needCalc));
    TTypeAnnotationNode::TListType tupleTypes;
    std::transform(needCalc.cbegin(), needCalc.cend(), std::back_inserter(tupleTypes), [](const TExprNode::TPtr& n) { return n->GetTypeAnn(); });
    list->SetTypeAnn(ctx.MakeType<TTupleExprType>(tupleTypes));
    list->SetState(TExprNode::EState::ConstrComplete);

    auto status = SubstTables(list, state, /*anonOnly*/true, ctx);
    if (status.Level == IGraphTransformer::TStatus::Error) {
        return SyncStatus(status);
    }

    auto callableTransformer = CreateExtCallableTypeAnnotationTransformer(*state->Types);
    auto typeTransformer = CreateTypeAnnotationTransformer(callableTransformer, *state->Types);

    TExprNode::TPtr optimized;
    bool hasNonDeterministicFunctions = false;
    status = PeepHoleOptimizeNode(list, optimized, ctx, *state->Types, typeTransformer.Get(), hasNonDeterministicFunctions);
    if (status.Level == IGraphTransformer::TStatus::Error) {
        return SyncStatus(status);
    }

    auto filesRes = NCommon::FreezeUsedFiles(*optimized, files, *state->Types, ctx, MakeUserFilesDownloadFilter(*state->Gateway, cluster));
    if (filesRes.first.Level != IGraphTransformer::TStatus::Ok) {
        return filesRes;
    }

    TString calcHash;
    auto config = state->Configuration->GetSettingsForNode(*input);
    const auto queryCacheMode = config->QueryCacheMode.Get().GetOrElse(EQueryCacheMode::Disable);
    if (queryCacheMode != EQueryCacheMode::Disable) {
        if (!hasNonDeterministicFunctions && config->QueryCacheUseForCalc.Get().GetOrElse(true)) {
            calcHash = TYtNodeHashCalculator(state, cluster, config).GetHash(*list);
        }
        YQL_CLOG(DEBUG, ProviderYt) << "Calc hash: " << HexEncode(calcHash).Quote()
            << ", cache mode: " << queryCacheMode;
    }

    for (size_t i: xrange(needCalc.size())) {
        calcNodes.emplace(needCalc[i].Get(), i);
    }

    THashMap<TString, TString> secureParams;
    NCommon::FillSecureParams(input, *state->Types, secureParams);

    auto future = state->Gateway->Calc(optimized->ChildrenList(), ctx,
        IYtGateway::TCalcOptions(state->SessionId)
            .Cluster(cluster)
            .UserDataBlocks(files)
            .UdfModules(state->Types->UdfModules)
            .UdfResolver(state->Types->UdfResolver)
            .UdfValidateMode(state->Types->ValidateMode)
            .PublicId(state->Types->TranslateOperationId(input->UniqueId()))
            .Config(state->Configuration->GetSettingsForNode(*input))
            .OptLLVM(state->Types->OptLLVM.GetOrElse(TString()))
            .OperationHash(calcHash)
            .SecureParams(secureParams)
        );
    return WrapFutureCallback(future, [state, calcNodes](const IYtGateway::TCalcResult& res, const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        YQL_ENSURE(res.Data.size() == calcNodes.size());

        TProcessedNodesSet processedNodes;
        if (TYtOpBase::Match(input.Get())) {
            processedNodes.insert(input->Child(TYtOpBase::idx_World)->UniqueId());
        }
        VisitExpr(input, [&processedNodes](const TExprNode::TPtr& node) {
            if (TYtOutput::Match(node.Get())) {
                // Stop traversing dependent operations
                processedNodes.insert(node->UniqueId());
                return false;
            }
            return true;
        });

        TNodeOnNodeOwnedMap remaps;
        for (auto& it: calcNodes) {
            auto node = it.first;
            auto type = node->GetTypeAnn();
            YQL_ENSURE(type);
            NYT::TNode data = res.Data[it.second];
            remaps.emplace(node, NCommon::NodeToExprLiteral(node->Pos(), *type, data, ctx));
        }
        TOptimizeExprSettings settings(state->Types);
        settings.VisitChanges = true;
        settings.VisitStarted = true;
        settings.ProcessedNodes = &processedNodes;
        auto status = RemapExpr(input, output, remaps, ctx, settings);

        if (status.Level == IGraphTransformer::TStatus::Error) {
            return status;
        }
        input->SetState(TExprNode::EState::ExecutionComplete);
        output->SetResult(ctx.NewAtom(output->Pos(), "calc")); // Special marker to check infinitive loop
        return status.Combine(IGraphTransformer::TStatus::Repeat);
    });
}

TMaybe<ui64> GetLimit(const TExprNode& settings) {
    auto limitNode = NYql::GetSetting(settings, EYtSettingType::Limit);
    if (!limitNode) {
        return Nothing();
    }
    limitNode = limitNode->ChildPtr(1);

    TMaybe<ui64> limit;
    for (auto part: limitNode->Children()) {
        TRecordsRange partialRange;
        partialRange.Fill(*part);
        if (!partialRange.Limit.Defined()) {
            return Nothing();
        }

        // check overflow
        if (std::numeric_limits<ui64>::max() - partialRange.Limit.GetRef() < partialRange.Offset.GetOrElse(0)) {
            return Nothing();
        }

        if (!limit.Defined()) {
            limit = partialRange.Limit.GetRef() + partialRange.Offset.GetOrElse(0);
        } else {
            limit = Max(limit.GetRef(), partialRange.Limit.GetRef() + partialRange.Offset.GetOrElse(0));
        }
    }

    return limit == std::numeric_limits<ui64>::max() ? Nothing() : limit;
}

TExprNode::TPtr GetLimitExpr(const TExprNode::TPtr& limitSetting, TExprContext& ctx) {
    auto limitItems = limitSetting->ChildPtr(1);
    TExprNode::TListType limitValues;
    for (const auto& child : limitItems->Children()) {
        TExprNode::TPtr skip, take;
        for (auto& setting: child->Children()) {
            if (setting->ChildrenSize() == 0) {
                continue;
            }

            auto settingName = setting->Child(0)->Content();
            if (settingName == TStringBuf("take")) {
                take = setting->ChildPtr(1);
            } else if (settingName == TStringBuf("skip")) {
                skip = setting->ChildPtr(1);
            }
        }

        if (!take) {
            return nullptr;
        }

        if (skip) {
            limitValues.push_back(ctx.NewCallable(child->Pos(), "+", { take, skip }));
        } else {
            limitValues.push_back(take);
        }
    }

    if (limitValues.empty()) {
        return nullptr;
    }

    if (limitValues.size() == 1) {
        return limitValues.front();
    }

    return ctx.NewCallable(limitSetting->Pos(), "Max", std::move(limitValues));
}

IGraphTransformer::TStatus UpdateTableMeta(const TExprNode::TPtr& tableNode, TExprNode::TPtr& newTableNode,
    const TYtTablesData::TPtr& tablesData, bool checkSqlView, bool updateRowSpecType, TExprContext& ctx)
{
    newTableNode = tableNode;
    TYtTableInfo tableInfo = tableNode;
    const TYtTableDescription& tableDesc = tablesData->GetTable(tableInfo.Cluster, tableInfo.Name, tableInfo.Epoch);
    const bool withQB = NYql::HasSetting(tableInfo.Settings.Ref(), EYtSettingType::WithQB);
    const bool hasUserSchema = NYql::HasSetting(tableInfo.Settings.Ref(), EYtSettingType::UserSchema);
    const bool hasUserColumns = NYql::HasSetting(tableInfo.Settings.Ref(), EYtSettingType::UserColumns);
    bool update = false;

    auto rowSpec = withQB ? tableDesc.QB2RowSpec : tableDesc.RowSpec;
    if (updateRowSpecType) {
        if (rowSpec && tableInfo.RowSpec && !rowSpec->GetType()) {
            rowSpec->CopyType(*tableInfo.RowSpec);
            rowSpec->SortedByTypes = tableInfo.RowSpec->SortedByTypes;
        }
    }

    if (!tableInfo.Stat) {
        if (tableDesc.Stat) {
            tableInfo.Stat = tableDesc.Stat;
            update = true;
        }
        else if (tableDesc.Meta && tableDesc.Meta->DoesExist && tableInfo.Epoch.GetOrElse(0) == 0) {
            ctx.AddError(TIssue(ctx.GetPosition(tableNode->Pos()), TStringBuilder() <<
                "Table " << tableInfo.Name << " stat was not loaded"));
            return IGraphTransformer::TStatus::Error;
        }
    }
    if (!tableInfo.Meta) {
        if (!tableDesc.Meta) {
            if (tableInfo.Epoch.GetOrElse(0) != 0) {
                return IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true);
            }
            ctx.AddError(TIssue(ctx.GetPosition(tableNode->Pos()), TStringBuilder() <<
                "Table " << tableInfo.Name << " metadata was not loaded"));
            return IGraphTransformer::TStatus::Error;
        }

        tableInfo.Meta = tableDesc.Meta;
        tableInfo.RowSpec = rowSpec;
        update = true;
    }
    else if (rowSpec && !tableInfo.RowSpec) {
        tableInfo.RowSpec = rowSpec;
        update = true;
    }

    if (checkSqlView && tableInfo.Meta->SqlView) {
        ctx.AddError(TIssue(ctx.GetPosition(tableNode->Pos()), TStringBuilder()
            << "Reading from " << tableInfo.Name.Quote() << " view is not supported"));
        return IGraphTransformer::TStatus::Error;
    }

    if (hasUserSchema || hasUserColumns) {
        const auto setting = GetSetting(tableInfo.Settings.Ref(), hasUserSchema ? EYtSettingType::UserSchema : EYtSettingType::UserColumns);
        auto type = setting->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
        const auto prevRowSpec = tableInfo.RowSpec;
        if (!(prevRowSpec && prevRowSpec->StrictSchema) && type->Cast<TStructExprType>()->FindItem("_other")) {
            ctx.AddError(TIssue(ctx.GetPosition(setting->Tail().Pos()), "It is forbidden to specify the column '_other'."));
            return IGraphTransformer::TStatus::Error;
        }

        TVector<TString> explicitYson;
        if (prevRowSpec && hasUserColumns) {
            const bool hasNativeFlags = prevRowSpec->GetNativeYtTypeFlags() != 0;
            // patch original type
            auto items = prevRowSpec->GetType()->GetItems();
            for (const auto& newItem : type->GetItems()) {
                if (auto pos = prevRowSpec->GetType()->FindItem(newItem->GetName())) {
                    if (hasNativeFlags) {
                        bool isOptional = false;
                        const TDataExprType* dataType = nullptr;
                        if (IsDataOrOptionalOfData(items[*pos]->GetItemType(), isOptional, dataType)
                            && dataType->GetSlot() == EDataSlot::Yson
                            && !IsDataOrOptionalOfData(newItem->GetItemType()))
                        {
                            explicitYson.emplace_back(newItem->GetName());
                        }
                    }
                    items[*pos] = ctx.MakeType<TItemExprType>(newItem->GetName(), newItem->GetItemType());
                } else {
                    items.push_back(newItem);
                }
            }

            type = ctx.MakeType<TStructExprType>(items);
        }

        if ((prevRowSpec && !IsSameAnnotation(*prevRowSpec->GetType(), *type)) || (!prevRowSpec && hasUserSchema)) {
            update = true;

            auto strict = hasUserSchema;
            if (hasUserColumns) {
                if (prevRowSpec) {
                    strict = prevRowSpec->StrictSchema;
                }
            }

            tableInfo.RowSpec = MakeIntrusive<TYqlRowSpecInfo>();
            tableInfo.RowSpec->SetType(type, prevRowSpec ?  prevRowSpec->GetNativeYtTypeFlags() : 0ul);
            tableInfo.RowSpec->UniqueKeys = false;
            tableInfo.RowSpec->StrictSchema = strict;
            tableInfo.RowSpec->ExplicitYson = explicitYson;

            if (prevRowSpec) {
                if (auto nativeType = prevRowSpec->GetNativeYtType()) {
                    tableInfo.RowSpec->CopyTypeOrders(*nativeType);
                }
                if (prevRowSpec->IsSorted()) {
                    tableInfo.RowSpec->CopySortness(ctx, *prevRowSpec, TYqlRowSpecInfo::ECopySort::WithDesc);
                    tableInfo.RowSpec->MakeCommonSortness(ctx, *prevRowSpec); // Truncated keys with changed types
                }
            }
        }
    } else {
        if (!update && rowSpec && tableInfo.RowSpec && (!rowSpec->CompareSortness(*tableInfo.RowSpec) || rowSpec->GetNativeYtType() != tableInfo.RowSpec->GetNativeYtType())) {
            tableInfo.RowSpec = rowSpec;
            update = true;
        }
    }

    if (update) {
        newTableNode = tableInfo.ToExprNode(ctx, tableNode->Pos()).Ptr();
        return IGraphTransformer::TStatus::Repeat;
    }

    return IGraphTransformer::TStatus::Ok;
}

TExprNode::TPtr ValidateAndUpdateTablesMeta(const TExprNode::TPtr& input, TStringBuf cluster, const TYtTablesData::TPtr& tablesData, bool updateRowSpecType, TExprContext& ctx) {
    TNodeSet tables;
    VisitExpr(input, [&](const TExprNode::TPtr& node) {
        if (auto maybeTable = TMaybeNode<TYtTable>(node)) {
            tables.insert(maybeTable.Cast().Raw());
            return false;
        }
        else if (TMaybeNode<TYtOutput>(node)) {
            // Don't traverse deeper to inner operations
            return false;
        }
        return true;
    });

    if (!tables.empty()) {
        bool valid = true;
        for (auto table: tables) {
            if (cluster != table->Child(TYtTable::idx_Cluster)->Content()) {
                ctx.AddError(TIssue(ctx.GetPosition(table->Child(TYtTable::idx_Cluster)->Pos()), TStringBuilder()
                    << "Table " << TString{table->Child(TYtTable::idx_Name)->Content()}.Quote()
                    << " cluster doesn't match DataSource/DataSink cluster: "
                    << TString{table->Child(TYtTable::idx_Cluster)->Content()}.Quote() << " != " << TString{cluster}.Quote()));
                valid = false;
            }
        }
        if (!valid) {
            return {};
        }

        TOptimizeExprSettings settings(nullptr);
        settings.VisitChanges = true;
        TExprNode::TPtr output = input;
        auto status = OptimizeExpr(input, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
            if (tables.find(node.Get()) != tables.cend()) {
                if (!TYtTableInfo::HasSubstAnonymousLabel(TExprBase(node))) {
                    TExprNode::TPtr newNode;
                    auto status = UpdateTableMeta(node, newNode, tablesData, true, updateRowSpecType, ctx);
                    if (IGraphTransformer::TStatus::Error == status.Level) {
                        return {};
                    }
                    return newNode;
                }
            }
            return node;
        }, ctx, settings);

        if (IGraphTransformer::TStatus::Error == status.Level) {
            return {};
        }
        return output;
    }

    return input;
}

TExprNode::TPtr ResetTableMeta(const TExprNode::TPtr& tableNode, TExprContext& ctx) {
    TExprNode::TListType children;
    for (auto id: {TYtTable::idx_Meta, TYtTable::idx_Stat, TYtTable::idx_RowSpec}) {
        if (!TCoVoid::Match(tableNode->Child(id))) {
            if (children.empty()) {
                children = tableNode->ChildrenList();
            }
            children[id] = ctx.NewCallable(tableNode->Pos(), TCoVoid::CallableName(), {});
        }
    }
    if (children.empty()) {
        return tableNode;
    }
    return ctx.ChangeChildren(*tableNode, std::move(children));
}

TExprNode::TPtr ResetOutTableMeta(const TExprNode::TPtr& tableNode, TExprContext& ctx) {
    TExprNode::TListType children;
    if (!TCoVoid::Match(tableNode->Child(TYtOutTable::idx_Stat))) {
        if (children.empty()) {
            children = tableNode->ChildrenList();
        }
        children[TYtOutTable::idx_Stat] = ctx.NewCallable(tableNode->Pos(), TCoVoid::CallableName(), {});
    }

    if (tableNode->Child(TYtOutTable::idx_Name)->Content()) {
        if (children.empty()) {
            children = tableNode->ChildrenList();
        }
        children[TYtOutTable::idx_Name] = ctx.NewAtom(tableNode->Pos(), TStringBuf());
    }

    if (children.empty()) {
        return tableNode;
    }
    return ctx.ChangeChildren(*tableNode, std::move(children));
}

TExprNode::TPtr ResetTablesMeta(const TExprNode::TPtr& input, TExprContext& ctx, bool resetTmpOnly, bool isEvaluationInProgress) {
    TNodeSet tables;
    TNodeSet outTables;
    VisitExpr(input, [&](const TExprNode::TPtr& node) {
        if (auto maybeTable = TMaybeNode<TYtTable>(node)) {
            const bool isAnonymous = NYql::HasSetting(maybeTable.Cast().Settings().Ref(), EYtSettingType::Anonymous);
            if (!resetTmpOnly && !(isEvaluationInProgress && isAnonymous)) {
                if (!TCoVoid::Match(maybeTable.Stat().Raw()) || !TCoVoid::Match(maybeTable.Meta().Raw()) || !TCoVoid::Match(maybeTable.RowSpec().Raw())) {
                    tables.insert(maybeTable.Raw());
                }
            }
            return false;
        }
        else if (auto maybeTable = TMaybeNode<TYtOutTable>(node)) {
            if (!isEvaluationInProgress) {
                if (!TCoVoid::Match(maybeTable.Stat().Raw()) || maybeTable.Cast().Name().Value()) {
                    outTables.insert(maybeTable.Raw());
                }
            }
            return false;
        }
        else if (TMaybeNode<TYtOutput>(node)) {
            // Don't traverse deeper to inner operations
            return false;
        }
        return true;
    });

    if (!tables.empty() || !outTables.empty()) {
        TOptimizeExprSettings settings(nullptr);
        settings.VisitChanges = true;
        TExprNode::TPtr output = input;
        auto status = OptimizeExpr(input, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
            if (tables.find(node.Get()) != tables.cend()) {
                return ResetTableMeta(node, ctx);
            }
            else if (outTables.find(node.Get()) != outTables.cend()) {
                return ResetOutTableMeta(node, ctx);
            }
            return node;
        }, ctx, settings);

        if (IGraphTransformer::TStatus::Error == status.Level) {
            return {};
        }
        return output;
    }

    return input;
}

std::pair<TExprBase, TString> GetOutTableWithCluster(TExprBase ytOutput) {
    const auto output = ytOutput.Cast<TYtOutput>();
    const auto op = GetOutputOp(output);
    const auto cluster = TString{ op.DataSink().Cluster().Value() };
    size_t ndx = 0;
    YQL_ENSURE(TryFromString<size_t>(output.OutIndex().Value(), ndx), "Bad " << TYtOutput::CallableName() << " output index value");
    const auto opOut = op.Output();
    YQL_ENSURE(ndx < opOut.Size());
    return { opOut.Item(ndx), cluster };
}

TExprBase GetOutTable(TExprBase ytOutput) {
    return GetOutTableWithCluster(ytOutput).first;
}

TMaybeNode<TCoFlatMapBase> GetFlatMapOverInputStream(TCoLambda opLambda, const TParentsMap& parentsMap) {
    TMaybeNode<TCoFlatMapBase> map;
    if (const auto it = parentsMap.find(opLambda.Args().Arg(0).Raw()); parentsMap.cend() != it) {
        for (const auto& parent : it->second) {
            if (!map) {
                if (map = TMaybeNode<TCoFlatMapBase>(parent))
                    continue;
            }

            if (!TCoDependsOn::Match(parent)) {
                map = {};
                break;
            }
        }
    }

    return map;
}

TMaybeNode<TCoFlatMapBase> GetFlatMapOverInputStream(TCoLambda opLambda) {
    TParentsMap parentsMap;
    GatherParents(opLambda.Body().Ref(), parentsMap);
    return GetFlatMapOverInputStream(opLambda, parentsMap);
}

TExprNode::TPtr ToOutTableWithHash(TExprBase output, const TYtState::TPtr& state, TExprContext& ctx) {
    auto [outTableNode, cluster] = GetOutTableWithCluster(output);
    auto outTable = outTableNode.Ptr();
    auto hash = TYtNodeHashCalculator(state, cluster, state->Configuration->Snapshot()).GetHash(output.Ref());
    outTable = ctx.ChangeChild(*outTable, TYtOutTable::idx_Settings,
        NYql::AddSetting(*outTable->Child(TYtOutTable::idx_Settings), EYtSettingType::OpHash, ctx.NewAtom(output.Pos(), HexEncode(hash)), ctx)
    );
    return outTable;
}

IGraphTransformer::TStatus SubstTables(TExprNode::TPtr& input, const TYtState::TPtr& state, bool anonOnly, TExprContext& ctx)
{
    TProcessedNodesSet processedNodes;
    VisitExpr(input, [&processedNodes](const TExprNode::TPtr& node) {
        if (TYtOutput::Match(node.Get())) {
            // Stop traversing dependent operations
            processedNodes.insert(node->UniqueId());
            return false;
        }
        return true;
    });

    TOptimizeExprSettings settings(state->Types);
    settings.VisitChanges = true;
    settings.VisitStarted = true;
    settings.CustomInstantTypeTransformer = state->Types->CustomInstantTypeTransformer.Get();
    settings.ProcessedNodes = &processedNodes;

    TExprNode::TPtr optimizedInput = input;
    auto status = OptimizeExpr(optimizedInput, optimizedInput, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
        if (auto maybeTable = TMaybeNode<TYtTable>(node)) {
            auto table = maybeTable.Cast();
            if (auto anon = NYql::GetSetting(table.Settings().Ref(), EYtSettingType::Anonymous)) {
                if (anon->ChildrenSize() == 1) {
                    TString cluster = TString{table.Cluster().Value()};
                    TString anonTableName = TString{table.Name().Value()};
                    TString realTableName = state->AnonymousLabels.Value(std::make_pair(cluster, anonTableName), TString());
                    if (!realTableName) {
                        ctx.AddError(TIssue(ctx.GetPosition(table.Pos()), TStringBuilder() << "Unaccounted anonymous table: " << cluster << '.' << anonTableName));
                        return {};
                    }
                    auto children = node->ChildrenList();
                    children[TYtTable::idx_Name] = ctx.NewAtom(node->Pos(), realTableName);
                    children[TYtTable::idx_Settings] = NYql::AddSetting(
                        *NYql::RemoveSetting(table.Settings().Ref(), EYtSettingType::Anonymous, ctx),
                        EYtSettingType::Anonymous, ctx.NewAtom(node->Pos(), anonTableName), ctx);
                    return ctx.ChangeChildren(*node, std::move(children));
                }
            }
        }

        return node;
    }, ctx, settings);

    if (status.Level == IGraphTransformer::TStatus::Error) {
        return status;
    }

    if (!anonOnly) {
        const bool useQueryCache = state->Configuration->QueryCacheMode.Get().GetOrElse(EQueryCacheMode::Disable) != EQueryCacheMode::Disable
            && state->Configuration->QueryCacheUseForCalc.Get().GetOrElse(true);

        TNodeOnNodeOwnedMap toOpt;
        VisitExpr(optimizedInput, [&toOpt, &state, useQueryCache, &ctx](const TExprNode::TPtr& node) {
            if (auto maybePath = TMaybeNode<TYtPath>(node)) {
                if (maybePath.Table().Maybe<TYtOutput>()) {
                    auto path = maybePath.Cast();
                    toOpt[node.Get()] = Build<TYtPath>(ctx, node->Pos())
                        .InitFrom(path)
                        .Table(useQueryCache ? ToOutTableWithHash(path.Table(), state, ctx) : GetOutTable(path.Table()).Ptr())
                        .Done().Ptr();
                }
                return false;
            }
            if (TMaybeNode<TYtLength>(node).Input().Maybe<TYtOutput>()) {
                auto length = TYtLength(node);
                toOpt[node.Get()] = Build<TYtLength>(ctx, node->Pos())
                    .InitFrom(length)
                    .Input<TYtReadTable>()
                        .World<TCoWorld>().Build()
                        .DataSource(ctx.RenameNode(GetOutputOp(length.Input().Cast<TYtOutput>()).DataSink().Ref(), TYtDSource::CallableName()))
                        .Input()
                            .Add()
                                .Paths()
                                    .Add()
                                        .Table(useQueryCache ? ToOutTableWithHash(length.Input(), state, ctx) : GetOutTable(length.Input()).Ptr())
                                        .Columns<TCoVoid>().Build()
                                        .Ranges<TCoVoid>().Build()
                                        .Stat<TCoVoid>().Build()
                                    .Build()
                                .Build()
                                .Settings()
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                    .Done().Ptr();
                return false;
            }
            if (TMaybeNode<TYtTableContent>(node).Input().Maybe<TYtOutput>()) {
                auto content = TYtTableContent(node);
                toOpt[node.Get()] = Build<TYtTableContent>(ctx, node->Pos())
                    .InitFrom(content)
                    .Input<TYtReadTable>()
                        .World<TCoWorld>().Build()
                        .DataSource(ctx.RenameNode(GetOutputOp(content.Input().Cast<TYtOutput>()).DataSink().Ref(), TYtDSource::CallableName()))
                        .Input()
                            .Add()
                                .Paths()
                                    .Add()
                                        .Table(useQueryCache ? ToOutTableWithHash(content.Input(), state, ctx) : GetOutTable(content.Input()).Ptr())
                                        .Columns<TCoVoid>().Build()
                                        .Ranges<TCoVoid>().Build()
                                        .Stat<TCoVoid>().Build()
                                    .Build()
                                .Build()
                                .Settings()
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                    .Done().Ptr();
                return false;
            }
            if (auto maybeOut = TMaybeNode<TYtOutput>(node)) {
                auto out = maybeOut.Cast();
                toOpt[node.Get()] = Build<TCoRight>(ctx, node->Pos())
                    .Input<TYtReadTable>()
                        .World<TCoWorld>().Build()
                        .DataSource(ctx.RenameNode(GetOutputOp(out).DataSink().Ref(), TYtDSource::CallableName()))
                        .Input()
                            .Add()
                                .Paths()
                                    .Add()
                                        .Table(useQueryCache ? ToOutTableWithHash(out, state, ctx) : GetOutTable(out).Ptr())
                                        .Columns<TCoVoid>().Build()
                                        .Ranges<TCoVoid>().Build()
                                        .Stat<TCoVoid>().Build()
                                    .Build()
                                .Build()
                                .Settings()
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                    .Done().Ptr();
                return false;
            }
            return true;
        });

        if (!toOpt.empty()) {
            settings.ProcessedNodes = nullptr;
            status = RemapExpr(optimizedInput, optimizedInput, toOpt, ctx, settings);
            if (status.Level == IGraphTransformer::TStatus::Error) {
                return status;
            }
        }
    }

    if (optimizedInput != input) {
        auto typeTransformer = CreateTypeAnnotationTransformer(CreateExtCallableTypeAnnotationTransformer(*state->Types, true), *state->Types);
        auto constrTransformer = CreateConstraintTransformer(*state->Types, true, true);
        TVector<TTransformStage> transformers;
        const auto issueCode = TIssuesIds::CORE_TYPE_ANN;
        transformers.push_back(TTransformStage(typeTransformer, "TypeAnnotation", issueCode));
        transformers.push_back(TTransformStage(
            CreateFunctorTransformer([](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) { return UpdateCompletness(input, output, ctx); }),
            "UpdateCompletness", issueCode));
        transformers.push_back(TTransformStage(constrTransformer, "Constraints", issueCode));
        auto fullTransformer = CreateCompositeGraphTransformer(transformers, false);
        status = InstantTransform(*fullTransformer, optimizedInput, ctx);
        if (status.Level == IGraphTransformer::TStatus::Error) {
            return status;
        }

        input = optimizedInput;
    }

    return IGraphTransformer::TStatus::Ok;
}

TYtPath CopyOrTrivialMap(TPositionHandle pos, TExprBase world, TYtDSink dataSink, const TTypeAnnotationNode& scheme,
    TYtSection section, TYqlRowSpecInfo::TPtr outRowSpec, TExprContext& ctx, const TYtState::TPtr& state, const TCopyOrTrivialMapOpts& opts)
{
    bool tryKeepSortness = opts.TryKeepSortness;
    const bool singleInput = section.Paths().Size() == 1;
    bool needMap = false;
    const auto sysColumns = NYql::GetSetting(section.Settings().Ref(), EYtSettingType::SysColumns);
    bool useExplicitColumns = false;
    bool exactCopySort = false;
    bool hasAux = false;
    TVector<std::pair<TYqlRowSpecInfo::TPtr, bool>> rowSpecs;
    const ui64 outNativeYtTypeFlags = outRowSpec ? outRowSpec->GetNativeYtTypeFlags() : (state->Configuration->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);
    TYtOutTableInfo outTable(scheme.Cast<TStructExprType>(), outNativeYtTypeFlags);
    outTable.RowSpec->SetConstraints(opts.Constraints);
    TMaybe<NYT::TNode> outNativeType;
    if (outRowSpec) {
        outNativeType = outRowSpec->GetNativeYtType();
    }
    bool first = !outRowSpec;
    const bool useNativeDescSort = state->Configuration->UseNativeDescSort.Get().GetOrElse(DEFAULT_USE_NATIVE_DESC_SORT);
    for (auto path: section.Paths()) {
        TYtPathInfo pathInfo(path);
        const bool hasRowSpec = !!pathInfo.Table->RowSpec;
        const bool tableHasAux = hasRowSpec && pathInfo.Table->RowSpec->HasAuxColumns();
        TMaybe<NYT::TNode> currentNativeType;
        if (hasRowSpec) {
            currentNativeType = pathInfo.GetNativeYtType();
        }
        if (first) {
            outNativeType = currentNativeType;
            first = false;
        }
        const bool needTableMap = pathInfo.RequiresRemap() || bool(sysColumns)
            || outTable.RowSpec->GetNativeYtTypeFlags() != pathInfo.GetNativeYtTypeFlags()
            || currentNativeType != outNativeType;
        useExplicitColumns = useExplicitColumns || !pathInfo.Table->IsTemp || (tableHasAux && pathInfo.HasColumns());
        needMap = needMap || needTableMap;
        hasAux = hasAux || tableHasAux;
        if (tryKeepSortness) {
            if (pathInfo.Table->IsUnordered || (opts.RangesResetSort && pathInfo.Ranges && pathInfo.Ranges->GetRangesCount() > 1)) {
                tryKeepSortness = false;
            }
            rowSpecs.emplace_back(pathInfo.Table->RowSpec, needTableMap);

            exactCopySort = singleInput && pathInfo.Table->IsTemp && hasRowSpec
                && IsSameAnnotation(scheme, *pathInfo.Table->RowSpec->GetType());
        }
    }
    if (!needMap && outNativeType) {
        outTable.RowSpec->CopyTypeOrders(*outNativeType);
    }
    useExplicitColumns = useExplicitColumns || (!tryKeepSortness && hasAux);

    bool trimSort = false;
    const bool sortConstraintEnabled = ctx.IsConstraintEnabled<TSortedConstraintNode>();
    if (tryKeepSortness) {
        bool sortIsChanged = false;
        for (size_t i = 0; i < rowSpecs.size(); ++i) {
            if (!rowSpecs[i].first) {
                sortIsChanged = outTable.RowSpec->ClearSortness(ctx);
                continue;
            }
            if (0 == i) {
                TYqlRowSpecInfo::ECopySort mode = TYqlRowSpecInfo::ECopySort::Pure;
                if (rowSpecs[i].second) {
                    if (sortConstraintEnabled) {
                        mode = TYqlRowSpecInfo::ECopySort::WithDesc;
                    }
                } else {
                    mode = exactCopySort
                        ? TYqlRowSpecInfo::ECopySort::Exact
                        : TYqlRowSpecInfo::ECopySort::WithDesc;
                }
                sortIsChanged = outTable.RowSpec->CopySortness(ctx, *rowSpecs[i].first, mode);
            } else {
                sortIsChanged = outTable.RowSpec->MakeCommonSortness(ctx, *rowSpecs[i].first) || sortIsChanged;
                if (rowSpecs[i].second && !sortConstraintEnabled) {
                    sortIsChanged = outTable.RowSpec->KeepPureSortOnly(ctx) || sortIsChanged;
                }
            }
        }

        useExplicitColumns = useExplicitColumns || (sortIsChanged && hasAux);
        tryKeepSortness = outTable.RowSpec->IsSorted();
        trimSort = !tryKeepSortness;
    }
    outTable.SetUnique(opts.SectionUniq, pos, ctx);

    if (tryKeepSortness) {
        if (needMap && !singleInput) {
            auto settingsBuilder = Build<TCoNameValueTupleList>(ctx, pos);
            settingsBuilder
                .Add()
                    .Name()
                        .Value(ToString(EYtSettingType::Ordered))
                    .Build()
                .Build();
            if (!opts.LimitNodes.empty()) {
                settingsBuilder
                    .Add()
                        .Name()
                            .Value(ToString(EYtSettingType::Limit))
                        .Build()
                        .Value<TExprList>()
                            .Add(opts.LimitNodes)
                        .Build()
                    .Build();
            }
            if (state->Configuration->UseFlow.Get().GetOrElse(DEFAULT_USE_FLOW)) {
                settingsBuilder
                    .Add()
                        .Name()
                            .Value(ToString(EYtSettingType::Flow))
                        .Build()
                    .Build();
            }

            TExprNode::TPtr mapSectionSettings = ctx.NewList(section.Pos(), {});
            TExprNode::TPtr sectionSettings = section.Settings().Ptr();
            if (sysColumns) {
                mapSectionSettings = NYql::AddSetting(*mapSectionSettings, EYtSettingType::SysColumns, sysColumns->ChildPtr(1), ctx);
                sectionSettings = NYql::RemoveSetting(*sectionSettings, EYtSettingType::SysColumns, ctx);
            }

            auto getPathUniq = [] (const TYtPath& path) {
                if (path.Ref().GetState() != TExprNode::EState::Initial) {
                    return path.Ref().GetConstraint<TDistinctConstraintNode>();
                }
                // Dynamically constructed YtPath for YtOutput
                return path.Table().Ref().GetConstraint<TDistinctConstraintNode>();
            };
            TVector<TYtPath> updatedPaths;
            YQL_ENSURE(rowSpecs.size() == section.Paths().Size());
            for (size_t i = 0; i < section.Paths().Size(); ++i) {
                auto path = section.Paths().Item(i);
                if (rowSpecs[i].second) {
                    TYtOutTableInfo mapOutTable(scheme.Cast<TStructExprType>(), outNativeYtTypeFlags);
                    if (outNativeType) {
                        mapOutTable.RowSpec->CopyTypeOrders(*outNativeType);
                    }
                    YQL_ENSURE(rowSpecs[i].first);
                    mapOutTable.SetUnique(getPathUniq(path), path.Pos(), ctx);
                    auto mapper = Build<TCoLambda>(ctx, path.Pos())
                        .Args({"stream"})
                        .Body("stream")
                        .Done().Ptr();

                    mapOutTable.RowSpec->CopySortness(ctx, *rowSpecs[i].first, sortConstraintEnabled ? TYqlRowSpecInfo::ECopySort::WithDesc : TYqlRowSpecInfo::ECopySort::Pure);
                    if (sortConstraintEnabled) {
                        TKeySelectorBuilder builder(path.Pos(), ctx, useNativeDescSort, scheme.Cast<TStructExprType>());
                        builder.ProcessRowSpec(*mapOutTable.RowSpec);
                        if (builder.NeedMap()) {
                            mapper = builder.MakeRemapLambda(true);
                        }
                    }

                    path = Build<TYtPath>(ctx, path.Pos())
                        .Table<TYtOutput>()
                            .Operation<TYtMap>()
                                .World(world)
                                .DataSink(dataSink)
                                .Input()
                                    .Add()
                                        .Paths()
                                            .Add(path)
                                        .Build()
                                        .Settings(mapSectionSettings)
                                    .Build()
                                .Build()
                                .Output()
                                    .Add(mapOutTable.ToExprNode(ctx, path.Pos()).Cast<TYtOutTable>())
                                .Build()
                                .Settings(settingsBuilder.Done())
                                .Mapper(mapper)
                            .Build()
                            .OutIndex()
                                .Value("0")
                            .Build()
                        .Build()
                        .Columns<TCoVoid>().Build()
                        .Ranges<TCoVoid>().Build()
                        .Stat<TCoVoid>().Build()
                        .Done();
                }
                updatedPaths.push_back(path);
            }
            section = Build<TYtSection>(ctx, section.Pos())
                .InitFrom(section)
                .Paths()
                    .Add(updatedPaths)
                .Build()
                .Settings(sectionSettings)
                .Done();
            needMap = false;
        }
    } else if (!trimSort) {
        section = MakeUnorderedSection(section, ctx);
    }

    if (needMap) {
        auto settingsBuilder = Build<TCoNameValueTupleList>(ctx, pos);
        settingsBuilder
            .Add()
                .Name()
                    .Value(ToString(EYtSettingType::Ordered))
                .Build()
            .Build();
        if (!opts.LimitNodes.empty()) {
            settingsBuilder
                .Add()
                    .Name()
                        .Value(ToString(EYtSettingType::Limit))
                    .Build()
                    .Value<TExprList>()
                        .Add(opts.LimitNodes)
                    .Build()
                .Build();
        }
        if (state->Configuration->UseFlow.Get().GetOrElse(DEFAULT_USE_FLOW)) {
            settingsBuilder
                .Add()
                    .Name()
                        .Value(ToString(EYtSettingType::Flow))
                    .Build()
                .Build();
        }

        auto mapper = Build<TCoLambda>(ctx, pos)
            .Args({"stream"})
            .Body("stream")
            .Done().Ptr();

        if (sortConstraintEnabled && outTable.RowSpec->IsSorted()) {
            TKeySelectorBuilder builder(pos, ctx, useNativeDescSort, scheme.Cast<TStructExprType>());
            builder.ProcessRowSpec(*outTable.RowSpec);
            if (builder.NeedMap()) {
                mapper = builder.MakeRemapLambda(true);
            }
        }

        return Build<TYtPath>(ctx, pos)
            .Table<TYtOutput>()
                .Operation<TYtMap>()
                    .World(world)
                    .DataSink(dataSink)
                    .Input()
                        .Add(section)
                    .Build()
                    .Output()
                        .Add(outTable.ToExprNode(ctx, pos).Cast<TYtOutTable>())
                    .Build()
                    .Settings(settingsBuilder.Done())
                    .Mapper(mapper)
                .Build()
                .OutIndex()
                    .Value("0")
                .Build()
            .Build()
            .Columns<TCoVoid>().Build()
            .Ranges<TCoVoid>().Build()
            .Stat<TCoVoid>().Build()
            .Done();
    }

    auto settingsBuilder = Build<TCoNameValueTupleList>(ctx, pos);
    if (NYql::HasSetting(section.Settings().Ref(), EYtSettingType::Sample)) {
        settingsBuilder
            .Add()
                .Name()
                    .Value(ToString(EYtSettingType::ForceTransform))
                .Build()
            .Build();
    }
    if (opts.CombineChunks) {
        settingsBuilder
            .Add()
                .Name()
                    .Value(ToString(EYtSettingType::CombineChunks))
                .Build()
            .Build();
    }
    if (!opts.LimitNodes.empty()) {
        settingsBuilder
            .Add()
                .Name()
                    .Value(ToString(EYtSettingType::Limit))
                .Build()
                .Value<TExprList>()
                    .Add(opts.LimitNodes)
                .Build()
            .Build();
    }

    if (useExplicitColumns) {
        TSet<TStringBuf> columns;
        for (auto item: outTable.RowSpec->GetType()->GetItems()) {
            columns.insert(item->GetName());
        }
        for (auto item: outTable.RowSpec->GetAuxColumns()) {
            columns.insert(item.first);
        }

        section = UpdateInputFields(section, std::move(columns), ctx, false);
    }

    return Build<TYtPath>(ctx, pos)
        .Table<TYtOutput>()
            .Operation<TYtMerge>()
                .World(world)
                .DataSink(dataSink)
                .Input()
                    .Add(section)
                .Build()
                .Output()
                    .Add(outTable.ToExprNode(ctx, pos).Cast<TYtOutTable>())
                .Build()
                .Settings(settingsBuilder.Done())
            .Build()
            .OutIndex()
                .Value(TStringBuf("0"))
            .Build()
        .Build()
        .Columns<TCoVoid>().Build()
        .Ranges<TCoVoid>().Build()
        .Stat<TCoVoid>().Build()
        .Done();
}

namespace {

template <class T>
const TExprNode* GetSingleParent(const TExprNode* node, const TParentsMap& parentsMap) {
    if (T::Match(node)) {
        auto parentsIt = parentsMap.find(node);
        YQL_ENSURE(parentsIt != parentsMap.cend());
        if (parentsIt->second.size() != 1) {
            return nullptr;
        }
        return *parentsIt->second.begin();
    }
    return node;
}

}


bool IsOutputUsedMultipleTimes(const TExprNode& op, const TParentsMap& parentsMap) {
    const TExprNode* node = &op;
    node = GetSingleParent<TYtOutputOpBase>(node, parentsMap);
    if (nullptr == node) {
        return true;
    }
    node = GetSingleParent<TYtOutput>(node, parentsMap);
    if (nullptr == node) {
        return true;
    }
    node = GetSingleParent<TYtPath>(node, parentsMap);
    if (nullptr == node) {
        return true;
    }
    node = GetSingleParent<TYtPathList>(node, parentsMap);
    if (nullptr == node) {
        return true;
    }
    node = GetSingleParent<TYtSection>(node, parentsMap);
    if (nullptr == node) {
        return true;
    }
    node = GetSingleParent<TYtSectionList>(node, parentsMap);
    return node == nullptr;
}

TMaybe<NYT::TRichYPath> BuildYtPathForStatRequest(const TString& cluster, const TYtPathInfo& pathInfo,
    const TMaybe<TVector<TString>>& overrideColumns, const TYtState& state, TExprContext& ctx)
{
    auto ytPath = NYT::TRichYPath(pathInfo.Table->Name);
    pathInfo.FillRichYPath(ytPath);
    if (overrideColumns) {
        ytPath.Columns(*overrideColumns);
    }

    if (ytPath.Columns_ && dynamic_cast<TYtTableInfo*>(pathInfo.Table.Get()) && pathInfo.Table->IsAnonymous
        && !TYtTableInfo::HasSubstAnonymousLabel(pathInfo.Table->FromNode.Cast())) {
        TString realTableName = state.AnonymousLabels.Value(std::make_pair(cluster, pathInfo.Table->Name), TString());
        if (!realTableName) {
            TPositionHandle pos;
            if (pathInfo.FromNode) {
                pos = pathInfo.FromNode.Cast().Pos();
            }
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Unaccounted anonymous table: " << cluster << '.' << pathInfo.Table->Name));
            return {};
        }
        ytPath.Path_ = realTableName;
    }

    return ytPath;
}

TMaybe<TVector<ui64>> EstimateDataSize(const TString& cluster, const TVector<TYtPathInfo::TPtr>& paths,
    const TMaybe<TVector<TString>>& columns, const TYtState& state, TExprContext& ctx)
{
    TVector<ui64> result;
    TSet<TString> requestedColumns;

    bool sync = true;

    auto status = EstimateDataSize(result, requestedColumns, cluster, paths, columns, state, ctx, sync);
    if (status != IGraphTransformer::TStatus::Ok) {
        return {};
    }

    return result;
}

IGraphTransformer::TStatus TryEstimateDataSize(TVector<ui64>& result, TSet<TString>& requestedColumns,
    const TString& cluster, const TVector<TYtPathInfo::TPtr>& paths,
    const TMaybe<TVector<TString>>& columns, const TYtState& state, TExprContext& ctx)
{
    bool sync = false;
    return EstimateDataSize(result, requestedColumns, cluster, paths, columns, state, ctx, sync);
}

TYtSection UpdateInputFields(TYtSection section, TExprBase fields, TExprContext& ctx) {
    auto settings = section.Settings().Ptr();

    auto sysColumns = NYql::GetSettingAsColumnList(*settings, EYtSettingType::SysColumns);
    if (!sysColumns.empty()) {
        if (auto list = fields.Maybe<TExprList>()) {
            TMap<TStringBuf, TExprNode::TPtr> fieldMap;
            for (auto item: list.Cast()) {
                if (auto atom = item.Maybe<TCoAtom>()) {
                    fieldMap.emplace(atom.Cast().Value(), item.Ptr());
                } else {
                    fieldMap.emplace(item.Cast<TCoAtomList>().Item(0).Value(), item.Ptr());
                }
            }
            TVector<TString> updatedSysColumns;
            for (auto sys: sysColumns) {
                auto sysColName = TString(YqlSysColumnPrefix).append(sys);
                if (fieldMap.contains(sysColName)) {
                    updatedSysColumns.push_back(sys);
                    fieldMap.erase(sysColName);
                }
            }
            if (updatedSysColumns.size() != sysColumns.size()) {
                settings = NYql::RemoveSetting(*settings, EYtSettingType::SysColumns, ctx);
                if (!updatedSysColumns.empty()) {
                    settings = NYql::AddSettingAsColumnList(*settings, EYtSettingType::SysColumns, updatedSysColumns, ctx);
                }
            }
            if (fieldMap.size() != list.Cast().Size()) {
                TExprNode::TListType children;
                std::transform(fieldMap.begin(), fieldMap.end(), std::back_inserter(children), [](const auto& pair) { return pair.second; });
                fields = TExprBase(ctx.NewList(fields.Pos(), std::move(children)));
            }
        }
    }

    auto pathsBuilder = Build<TYtPathList>(ctx, section.Paths().Pos());
    for (const auto& path : section.Paths()) {
        pathsBuilder.Add<TYtPath>()
            .InitFrom(path)
            .Columns(fields)
            .Build();
    }
    return Build<TYtSection>(ctx, section.Pos())
        .InitFrom(section)
        .Paths(pathsBuilder.Done())
        .Settings(settings)
        .Done();
}

TYtSection UpdateInputFields(TYtSection section, TSet<TStringBuf>&& members, TExprContext& ctx, bool hasWeakFields) {
    auto settings = section.Settings().Ptr();

    auto sysColumns = NYql::GetSettingAsColumnList(*settings, EYtSettingType::SysColumns);
    if (!sysColumns.empty()) {
        TVector<TString> updatedSysColumns;
        for (auto sys: sysColumns) {
            auto sysColName = TString(YqlSysColumnPrefix).append(sys);
            if (members.contains(sysColName)) {
                updatedSysColumns.push_back(sys);
                members.erase(sysColName);
            }
        }
        if (updatedSysColumns.size() != sysColumns.size()) {
            settings = NYql::RemoveSetting(*settings, EYtSettingType::SysColumns, ctx);
            if (!updatedSysColumns.empty()) {
                settings = NYql::AddSettingAsColumnList(*settings, EYtSettingType::SysColumns, updatedSysColumns, ctx);
            }
        }
    }

    auto fields = ToAtomList(members, section.Pos(), ctx);
    auto pathsBuilder = Build<TYtPathList>(ctx, section.Paths().Pos());
    for (const auto& path : section.Paths()) {
        if (!hasWeakFields || path.Columns().Maybe<TCoVoid>()) {
            pathsBuilder.Add<TYtPath>()
                .InitFrom(path)
                .Columns(fields)
                .Build();
        } else {
            THashMap<TStringBuf, TExprNode::TPtr> weakFields;
            for (auto col: path.Columns().Cast<TExprList>()) {
                if (col.Ref().ChildrenSize() == 2) {
                    weakFields[col.Ref().Child(0)->Content()] = col.Ptr();
                }
            }
            TExprNode::TListType updatedColumns;
            for (auto member: fields->Children()) {
                if (auto p = weakFields.FindPtr(member->Content())) {
                    updatedColumns.push_back(*p);
                } else {
                    updatedColumns.push_back(member);
                }
            }
            pathsBuilder.Add<TYtPath>()
                .InitFrom(path)
                .Columns(ctx.NewList(path.Pos(), std::move(updatedColumns)))
                .Build();
        }
    }
    return Build<TYtSection>(ctx, section.Pos())
        .InitFrom(section)
        .Paths(pathsBuilder.Done())
        .Settings(settings)
        .Done();
}

TYtPath MakeUnorderedPath(TYtPath path, bool hasLimits, TExprContext& ctx) {
    bool makeUnordered = false;
    bool keepSort = false;
    if (auto maybeOut = path.Table().Maybe<TYtOutput>()) {
        const auto out = maybeOut.Cast();
        if (!IsUnorderedOutput(out)) {
            makeUnordered = true;
            if (!path.Ranges().Maybe<TCoVoid>()) {
                for (auto range: path.Ranges().Cast<TExprList>()) {
                    if (range.Maybe<TYtKeyExact>() || range.Maybe<TYtKeyRange>()) {
                        makeUnordered = false;
                    } else if (range.Maybe<TYtRow>() || range.Maybe<TYtRowRange>()) {
                        hasLimits = true;
                    }
                }
            }
        }
        if (auto settings = GetOutputOp(out).Maybe<TYtTransientOpBase>().Settings()) {
            hasLimits = hasLimits || NYql::HasSetting(settings.Ref(), EYtSettingType::Limit);
            keepSort = NYql::HasSetting(settings.Ref(), EYtSettingType::KeepSorted);
        } else if (auto settings = GetOutputOp(out).Maybe<TYtFill>().Settings()) {
            keepSort = NYql::HasSetting(settings.Ref(), EYtSettingType::KeepSorted);
        }
        keepSort = keepSort || GetOutputOp(out).Maybe<TYtSort>();
    }
    if (makeUnordered && hasLimits && keepSort) {
        makeUnordered = false;
    }
    if (makeUnordered) {
        return Build<TYtPath>(ctx, path.Pos())
            .InitFrom(path)
            .Table<TYtOutput>()
                .InitFrom(path.Table().Cast<TYtOutput>())
                .Mode()
                    .Value(ToString(EYtSettingType::Unordered))
                .Build()
            .Build()
            .Done();
    }
    return path;

}

template<bool WithUnorderedSetting>
TYtSection MakeUnorderedSection(TYtSection section, TExprContext& ctx) {
    if (HasNonEmptyKeyFilter(section)) {
        if constexpr (WithUnorderedSetting)
            return Build<TYtSection>(ctx, section.Pos())
                .Paths(section.Paths())
                .Settings(NYql::AddSetting(section.Settings().Ref(), EYtSettingType::Unordered, {}, ctx))
                .Done();
        else
            return section;
    }
    const bool hasLimits = NYql::HasAnySetting(section.Settings().Ref(), EYtSettingType::Take | EYtSettingType::Skip);
    bool hasUpdated = false;
    TVector<TYtPath> updatedPaths;
    for (auto path: section.Paths()) {
        updatedPaths.push_back(MakeUnorderedPath(path, hasLimits, ctx));
        hasUpdated = hasUpdated || updatedPaths.back().Raw() != path.Raw();
    }

    if constexpr (WithUnorderedSetting) {
        return Build<TYtSection>(ctx, section.Pos())
            .Paths()
                .Add(updatedPaths)
            .Build()
            .Settings(NYql::AddSetting(section.Settings().Ref(), EYtSettingType::Unordered, {}, ctx))
            .Done();
    } else {
        if (!hasUpdated)
            return section;

        return Build<TYtSection>(ctx, section.Pos())
            .Paths()
                .Add(updatedPaths)
            .Build()
            .Settings(section.Settings())
            .Done();
    }
}

template TYtSection MakeUnorderedSection<true>(TYtSection section, TExprContext& ctx);
template TYtSection MakeUnorderedSection<false>(TYtSection section, TExprContext& ctx);

TYtSection ClearUnorderedSection(TYtSection section, TExprContext& ctx) {
    const bool hasUnorderedOut = AnyOf(section.Paths(), [](const auto& path) { auto out = path.Table().template Maybe<TYtOutput>(); return out && IsUnorderedOutput(out.Cast()); });
    if (hasUnorderedOut) {
        TVector<TYtPath> updatedPaths;
        for (auto path: section.Paths()) {
            if (auto out = path.Table().Maybe<TYtOutput>()) {
                if (IsUnorderedOutput(out.Cast())) {
                    path = Build<TYtPath>(ctx, path.Pos())
                        .InitFrom(path)
                        .Table<TYtOutput>()
                            .InitFrom(out.Cast())
                            .Mode(TMaybeNode<TCoAtom>())
                        .Build()
                        .Done();
                }
            }
            updatedPaths.push_back(path);
        }
        section = Build<TYtSection>(ctx, section.Pos())
            .InitFrom(section)
            .Paths()
                .Add(updatedPaths)
            .Build()
            .Done();
    }
    if (NYql::HasSetting(section.Settings().Ref(), EYtSettingType::Unordered)) {
        section = Build<TYtSection>(ctx, section.Pos())
            .InitFrom(section)
            .Settings(NYql::RemoveSetting(section.Settings().Ref(), EYtSettingType::Unordered, ctx))
            .Done();
    }
    return section;
}

TYtDSource GetDataSource(TExprBase input, TExprContext& ctx) {
    TMaybeNode<TExprBase> n = input;
    if (auto right = input.Maybe<TCoRight>()) {
        n = right.Input();
    } else if (auto content = input.Maybe<TYtTableContent>()) {
        n = content.Input();
    }
    if (auto read = n.Maybe<TYtReadTable>())
        return read.Cast().DataSource();
    if (auto out = n.Maybe<TYtOutput>()) {
        return TYtDSource(ctx.RenameNode(GetOutputOp(out.Cast()).DataSink().Ref(), "DataSource"));
    } else {
        YQL_ENSURE(false, "Unknown operation input");
    }
}

TExprNode::TPtr BuildEmptyTablesRead(TPositionHandle pos, const TExprNode& userSchema, TExprContext& ctx) {
    if (!EnsureArgsCount(userSchema, 2, ctx)) {
        return {};
    }

    return ctx.Builder(pos)
        .Callable("Cons!")
            .World(0)
            .Callable(1, "List")
                .Callable(0, "ListType")
                    .Add(0, userSchema.ChildPtr(1))
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr GetFlowSettings(TPositionHandle pos, const TYtState& state, TExprContext& ctx, TExprNode::TPtr settings) {
    if (!settings) {
        settings = ctx.NewList(pos, {});
    }
    if (state.Configuration->UseFlow.Get().GetOrElse(DEFAULT_USE_FLOW)) {
        settings = NYql::AddSetting(*settings, EYtSettingType::Flow, {}, ctx);
    }
    return settings;
}

TVector<TStringBuf> GetKeyFilterColumns(const NNodes::TYtSection& section, EYtSettingTypes kind) {
    TVector<TStringBuf> result;
    if (kind.HasFlags(EYtSettingType::KeyFilter) && NYql::HasSetting(section.Settings().Ref(), EYtSettingType::KeyFilter)) {
        for (auto keyFilter: NYql::GetAllSettingValues(section.Settings().Ref(), EYtSettingType::KeyFilter)) {
            auto value = TExprList(keyFilter);
            if (value.Size() > 0) {
                for (auto member: value.Item(0).Cast<TCoNameValueTupleList>()) {
                    result.emplace_back(member.Name().Value());
                }
            }
        }
    }
    if (kind.HasFlags(EYtSettingType::KeyFilter2) && NYql::HasSetting(section.Settings().Ref(), EYtSettingType::KeyFilter2)) {
        for (auto keyFilter: NYql::GetAllSettingValues(section.Settings().Ref(), EYtSettingType::KeyFilter2)) {
            auto value = TExprList(keyFilter);
            if (value.Size() > 0) {
                for (auto member: value.Item(1).Cast<TCoNameValueTupleList>()) {
                    if (member.Name().Value() == "usedKeys") {
                        for (auto key : member.Value().Cast<TCoAtomList>()) {
                            result.emplace_back(key.Value());
                        }
                    }
                }
            }
        }
    }
    return result;
}

bool HasNonEmptyKeyFilter(const NNodes::TYtSection& section) {
    auto hasChildren = [](const auto& node) { return node->ChildrenSize() > 0; };
    return AnyOf(NYql::GetAllSettingValues(section.Settings().Ref(), EYtSettingType::KeyFilter), hasChildren) ||
           AnyOf(NYql::GetAllSettingValues(section.Settings().Ref(), EYtSettingType::KeyFilter2), hasChildren);
}

TYtReadTable ConvertContentInputToRead(TExprBase input, TMaybeNode<TCoNameValueTupleList> settings, TExprContext& ctx, TMaybeNode<TCoAtomList> customFields) {
    TExprNode::TPtr world;
    TVector<TYtSection> sections;
    TExprBase columns = customFields ? TExprBase(customFields.Cast()) : TExprBase(Build<TCoVoid>(ctx, input.Pos()).Done());
    if (auto out = input.Maybe<TYtOutput>()) {
        world = ctx.NewWorld(input.Pos());
        if (!settings) {
            settings = Build<TCoNameValueTupleList>(ctx, input.Pos()).Done();
        }
        sections.push_back(Build<TYtSection>(ctx, input.Pos())
            .Paths()
                .Add()
                    .Table(out.Cast())
                    .Columns(columns)
                    .Ranges<TCoVoid>().Build()
                    .Stat<TCoVoid>().Build()
                .Build()
            .Build()
            .Settings(settings.Cast())
            .Done());
    }
    else {
        auto read = input.Maybe<TYtReadTable>();
        YQL_ENSURE(read, "Unknown operation input");
        world = read.Cast().World().Ptr();
        for (auto section: read.Cast().Input()) {
            if (settings) {
                section = Build<TYtSection>(ctx, section.Pos())
                    .InitFrom(section)
                    .Settings(MergeSettings(section.Settings().Ref(), settings.Cast().Ref(), ctx))
                    .Done();
            }

            if (customFields) {
                section = UpdateInputFields(section, customFields.Cast(), ctx);
            }

            sections.push_back(section);
        }
    }

    return Build<TYtReadTable>(ctx, input.Pos())
        .World(world)
        .DataSource(GetDataSource(input, ctx))
        .Input()
            .Add(sections)
        .Build()
        .Done();
}

size_t GetMapDirectOutputsCount(const NNodes::TYtMapReduce& mapReduce) {
    if (mapReduce.Mapper().Maybe<TCoVoid>()) {
        return 0;
    }
    const auto& mapOutputType = GetSeqItemType(*mapReduce.Mapper().Ref().GetTypeAnn());
    if (mapOutputType.GetKind() != ETypeAnnotationKind::Variant) {
        return 0;
    }

    auto numVariants = mapOutputType.Cast<TVariantExprType>()->GetUnderlyingType()->Cast<TTupleExprType>()->GetSize();
    YQL_ENSURE(numVariants > 1);
    return numVariants - 1;
}

bool HasYtRowNumber(const TExprNode& node) {
    bool hasRowNumber = false;
    VisitExpr(node, [&hasRowNumber](const TExprNode& n) {
        if (TYtRowNumber::Match(&n)) {
            hasRowNumber = true;
        } else if (TYtOutput::Match(&n)) {
            return false;
        }
        return !hasRowNumber;
    });
    return hasRowNumber;
}


} // NYql
