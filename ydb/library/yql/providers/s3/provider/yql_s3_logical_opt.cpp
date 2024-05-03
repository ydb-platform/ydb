#include "yql_s3_provider_impl.h"

#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>
#include <ydb/library/yql/providers/s3/range_helpers/path_list_reader.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/utils/log/log.h>

#include <util/generic/size_literals.h>

namespace NYql {

using namespace NNodes;

namespace {

using namespace NYql::NS3Details;

void RebuildPredicateForPruning(const TExprNode::TPtr& pred, const TExprNode& arg, const TStructExprType& extraType,
                                TExprNode::TPtr& prunedPred, TExprNode::TPtr& extraPred, TExprContext& ctx)
{
    if (pred->IsCallable({"And", "Or"})) {
        TExprNodeList childPruned;
        TExprNodeList childExtra;
        for (auto& child : pred->ChildrenList()) {
            childPruned.emplace_back();
            TExprNode::TPtr extra;
            RebuildPredicateForPruning(child, arg, extraType, childPruned.back(), extra, ctx);
            if (extra) {
                childExtra.emplace_back(std::move(extra));
            }
        }
        YQL_ENSURE(pred->ChildrenSize() > 0);
        if (pred->IsCallable("Or") && childExtra.size() < pred->ChildrenSize() || childExtra.empty()) {
            prunedPred = pred;
            extraPred = nullptr;
            return;
        }

        prunedPred = ctx.ChangeChildren(*pred, std::move(childPruned));
        extraPred = ctx.ChangeChildren(*pred, std::move(childExtra));
        return;
    }

    // analyze remaining predicate part
    bool usedNonExtraMembers = false;
    VisitExpr(*pred, [&](const TExprNode& node) {
        if (node.IsCallable("Member") && &node.Head() == &arg) {
            auto col = node.Tail().Content();
            if (!extraType.FindItem(col)) {
                usedNonExtraMembers = true;
            }
            return false;
        }

        if (&node == &arg) {
            usedNonExtraMembers = false;
            return false;
        }

        return !usedNonExtraMembers;
    });

    if (usedNonExtraMembers) {
        prunedPred = pred;
        extraPred = nullptr;
    } else {
        prunedPred = MakeBool(pred->Pos(), true, ctx);
        extraPred = pred;
    }
}

TCoFlatMapBase CalculatePrunedPaths(TCoFlatMapBase flatMap, TExprContext& ctx, TTypeAnnotationContext* types) {
    auto dqSource = flatMap.Input().Cast<TDqSourceWrap>();
    auto extraColumns = GetSetting(dqSource.Settings().Ref(), "extraColumns");
    if (!extraColumns) {
        return flatMap;
    }

    YQL_ENSURE(extraColumns->ChildrenSize() == 2);
    const TStructExprType* extraType = extraColumns->Tail().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

    TExprNode::TPtr pred = flatMap.Lambda().Body().Cast<TCoConditionalValueBase>().Predicate().Ptr();

    TOptimizeExprSettings optimizeExprSettings(types);
    optimizeExprSettings.VisitLambdas = false;
    optimizeExprSettings.VisitChanges = true;
    OptimizeExpr(pred, pred, [&](const TExprNode::TPtr& node, TExprContext& ctx) {
        if (node->IsCallable("Not")) {
            if (node->Head().IsCallable({"And", "Or"})) {
                auto children = node->Head().ChildrenList();
                for (auto& child : children) {
                    child = ctx.NewCallable(child->Pos(), "Not", { child });
                }
                return ctx.NewCallable(node->Head().Pos(), node->Head().IsCallable("Or") ? "And" : "Or", std::move(children));
            }
            if (node->Head().IsCallable("Not")) {
                return node->Head().HeadPtr();
            }
        }
        return node;
    }, ctx, optimizeExprSettings);

    const TExprNode& arg = flatMap.Lambda().Args().Arg(0).Ref();
    TExprNode::TPtr prunedPred;
    TExprNode::TPtr extraPred;

    RebuildPredicateForPruning(pred, arg, *extraType, prunedPred, extraPred, ctx);
    YQL_ENSURE(prunedPred);
    TExprNode::TPtr filteredPathList;
    if (extraPred) {
        auto source = flatMap.Input().Cast<TDqSourceWrap>().Input().Cast<TS3SourceSettingsBase>();
        filteredPathList = ctx.Builder(pred->Pos())
            .Callable("EvaluateExpr")
                .Callable(0, "OrderedFilter")
                    .Add(0, ctx.NewCallable(pred->Pos(), "AsList", source.Paths().Ref().ChildrenList()))
                    .Lambda(1)
                        .Param("item")
                        .Apply(ctx.NewLambda(extraPred->Pos(), flatMap.Lambda().Args().Ptr(), std::move(extraPred)))
                            .With(0)
                                .Callable("Nth")
                                    .Arg(0, "item")
                                    .Atom(1, "2", TNodeFlags::Default)
                                .Seal()
                            .Done()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

    auto newLambda = flatMap.Lambda();
    if (filteredPathList) {
        auto cvBase = flatMap.Lambda().Body().Cast<TCoConditionalValueBase>();
        newLambda = Build<TCoLambda>(ctx, flatMap.Lambda().Pos())
            .Args(flatMap.Lambda().Args())
            .Body<TCoConditionalValueBase>()
                .CallableName(cvBase.CallableName())
                .Predicate(prunedPred)
                .Value(cvBase.Value())
            .Build()
            .Done();
    }

    return Build<TCoFlatMapBase>(ctx, flatMap.Pos())
        .CallableName(flatMap.CallableName())
        .Input<TDqSourceWrap>()
            .InitFrom(dqSource)
            .Settings(AddSetting(dqSource.Settings().Ref(), dqSource.Settings().Cast().Pos(), "prunedPaths", filteredPathList, ctx))
        .Build()
        .Lambda(newLambda)
        .Done();
}

class TS3LogicalOptProposalTransformer : public TOptimizeTransformerBase {
public:
    TS3LogicalOptProposalTransformer(TS3State::TPtr state)
        : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderS3, {})
        , State_(state)
    {
#define HNDL(name) "LogicalOptimizer-"#name, Hndl(&TS3LogicalOptProposalTransformer::name)
        AddHandler(0, &TCoFlatMapBase::Match, HNDL(TryPrunePaths));
        AddHandler(0, &TDqSourceWrap::Match, HNDL(ApplyPrunedPath));
        AddHandler(0, &TCoExtractMembers::Match, HNDL(ExtractMembersOverDqSource));
        AddHandler(0, &TDqSourceWrap::Match, HNDL(MergeS3Paths));
        AddHandler(0, &TDqSourceWrap::Match, HNDL(CleanupExtraColumns));
        AddHandler(0, &TCoTake::Match, HNDL(PushDownLimit));
#undef HNDL
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        auto status = TOptimizeTransformerBase::DoTransform(input, output, ctx);
        if (status != TStatus::Ok) {
            return status;
        }

        // check if we need to do listings
        bool needList = false;
        status = OptimizeExpr(input, output, [&](const TExprNode::TPtr& n, TExprContext& ctx) {
            if (needList) {
                return n;
            }
            TExprBase node(n);
            if (auto maybeParse = node.Maybe<TS3ParseSettings>()) {
                auto maybeSettings = maybeParse.Cast().Settings();
                if (maybeSettings && HasSetting(maybeSettings.Cast().Ref(), "directories")) {
                    needList = true;
                    // noop optimization just to update graph and allow restart
                    return ctx.ShallowCopy(*n);
                }
            }
            return n;
        }, ctx, TOptimizeExprSettings(nullptr));

        if (status != TStatus::Ok) {
            if (status != TStatus::Error) {
                YQL_ENSURE(needList);
                YQL_CLOG(INFO, ProviderS3) << "Restarting io discovery - need to list S3 directories";
                ctx.Step.Repeat(TExprStep::DiscoveryIO);
            }
            return status;
        }

        // check read limits after pruning
        bool hasErr = false;
        size_t count = 0;
        size_t totalSize = 0;
        VisitExpr(input, [&] (const TExprNode::TPtr& n) {
            TExprBase node(n);
            if (auto maybeSource = node.Maybe<TDqSourceWrap>()) {
                const TDqSourceWrap dqSource = node.Cast<TDqSourceWrap>();
                if (dqSource.DataSource().Category() == S3ProviderName) {
                    const auto& maybeS3SourceSettings = dqSource.Input().Maybe<TS3SourceSettingsBase>();
                    if (maybeS3SourceSettings && dqSource.Settings()) {
                        TString formatName;
                        {
                            auto format = GetSetting(dqSource.Settings().Ref(), "format");
                            if (format) {
                                formatName = format->Child(1)->Content();
                            }
                        }
                        auto fileSizeLimit = State_->Configuration->FileSizeLimit;
                        if (formatName) {
                            auto it = State_->Configuration->FormatSizeLimits.find(formatName);
                            if (it != State_->Configuration->FormatSizeLimits.end() && fileSizeLimit > it->second) {
                                fileSizeLimit = it->second;
                            }
                        }

                        ui64 userSizeLimit = std::numeric_limits<ui64>::max();
                        if (formatName == "parquet") {
                            fileSizeLimit = State_->Configuration->BlockFileSizeLimit;
                        } else if (formatName == "raw") {
                            const auto sizeLimitParam = dqSource.Input().Cast<TS3SourceSettings>().SizeLimit().Maybe<TCoAtom>();
                            if (sizeLimitParam.IsValid()) {
                                userSizeLimit = FromString<ui64>(sizeLimitParam.Cast().StringValue());
                            }
                        }

                        for (const TS3Path& batch : maybeS3SourceSettings.Cast().Paths()) {
                            TStringBuf packed = batch.Data().Literal().Value();
                            bool isTextEncoded = FromString<bool>(batch.IsText().Literal().Value());

                            TPathList paths;
                            UnpackPathsList(packed, isTextEncoded, paths);

                            for (auto& entry : paths) {
                                const ui64 bytesUsed = std::min(entry.Size, userSizeLimit);
                                if (bytesUsed > fileSizeLimit) {
                                    ctx.AddError(TIssue(ctx.GetPosition(batch.Pos()),
                                        TStringBuilder() << "Size of object " << entry.Path << " = " << entry.Size << " and exceeds limit = " << fileSizeLimit << " specified for format " << formatName));
                                    hasErr = true;
                                    return false;
                                }
                                totalSize += bytesUsed;
                                ++count;
                            }
                        }
                        return false;
                    }
                }
            }
            return !hasErr;
        });

        if (hasErr) {
            return TStatus::Error;
        }

        const auto maxFiles = std::max(
            State_->Configuration->MaxFilesPerQuery,
            State_->Configuration->MaxDirectoriesAndFilesPerQuery);
        if (count > maxFiles) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Too many objects to read: " << count << ", but limit is " << maxFiles));
            return TStatus::Error;
        }

        const auto maxSize = State_->Configuration->MaxReadSizePerQuery;
        if (totalSize > maxSize) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Too large objects to read: " << totalSize << ", but limit is " << maxSize));
            return TStatus::Error;
        }

        if (count > 0) {
            YQL_CLOG(INFO, ProviderS3) << "Will read from S3 " << count << " files with total size " << totalSize << " bytes";
        }
        return TStatus::Ok;
    }

    TMaybeNode<TExprBase> ApplyPrunedPath(TExprBase node, TExprContext& ctx) const {
        const TDqSourceWrap dqSource = node.Cast<TDqSourceWrap>();
        if (dqSource.DataSource().Category() != S3ProviderName) {
            return node;
        }

        const auto& maybeS3SourceSettings = dqSource.Input().Maybe<TS3SourceSettingsBase>();
        if (!maybeS3SourceSettings || dqSource.Ref().ChildrenSize() <= TDqSourceWrap::idx_Settings) {
            return node;
        }

        auto prunedPathSetting = GetSetting(dqSource.Settings().Ref(), "prunedPaths");
        if (!prunedPathSetting || prunedPathSetting->ChildrenSize() == 1) {
            return node;
        }

        const size_t beforeEntries = maybeS3SourceSettings.Cast().Paths().Size();
        auto prunedPaths = prunedPathSetting->ChildPtr(1);
        if (prunedPaths->IsCallable("List")) {
            YQL_CLOG(INFO, ProviderS3) << "S3 Paths completely pruned: " << beforeEntries << " entries";
            return ctx.NewCallable(node.Pos(), "List", { ExpandType(node.Pos(), *node.Ref().GetTypeAnn(), ctx) });
        }

        YQL_ENSURE(prunedPaths->IsCallable("AsList"), "prunedPaths should have literal value");
        YQL_ENSURE(prunedPaths->ChildrenSize() > 0);
        const size_t afterEntries = prunedPaths->ChildrenSize();

        auto newSettings = ReplaceSetting(dqSource.Settings().Ref(), prunedPathSetting->Pos(), "prunedPaths", nullptr, ctx);
        if (beforeEntries == afterEntries) {
            YQL_CLOG(INFO, ProviderS3) << "No S3 paths are pruned: " << afterEntries << " entries";
            return Build<TDqSourceWrap>(ctx, dqSource.Pos())
                .InitFrom(dqSource)
                .Settings(newSettings)
                .Done();
        }

        YQL_CLOG(INFO, ProviderS3) << "Pruning S3 Paths: " << beforeEntries << " -> " << afterEntries << " entries";
        TExprNodeList newExtraColumnsExtents;

        for (auto& entry : prunedPaths->ChildrenList()) {
            TS3Path batch(entry);
            TStringBuf packed = batch.Data().Literal().Value();
            bool isTextEncoded = FromString<bool>(batch.IsText().Literal().Value());

            TPathList paths;
            UnpackPathsList(packed, isTextEncoded, paths);

            newExtraColumnsExtents.push_back(
                ctx.Builder(batch.ExtraColumns().Pos())
                    .Callable("Replicate")
                        .Add(0, batch.ExtraColumns().Ptr())
                        .Callable(1, "Uint64")
                            .Atom(0, ToString(paths.size()), TNodeFlags::Default)
                        .Seal()
                    .Seal()
                    .Build()
            );
        }

        newSettings = ReplaceSetting(*newSettings, newSettings->Pos(), "extraColumns",
                                     ctx.NewCallable(newSettings->Pos(), "OrderedExtend", std::move(newExtraColumnsExtents)), ctx);
        auto oldSrc = dqSource.Input().Cast<TS3SourceSettingsBase>();
        auto newSrc = ctx.ChangeChild(dqSource.Input().Ref(), TS3SourceSettingsBase::idx_Paths,
                                      ctx.NewList(oldSrc.Paths().Pos(), prunedPaths->ChildrenList()));

        return Build<TDqSourceWrap>(ctx, dqSource.Pos())
            .InitFrom(dqSource)
            .Input(newSrc)
            .Settings(newSettings)
            .Done();
    }

    TMaybeNode<TExprBase> TryPrunePaths(TExprBase node, TExprContext& ctx) const {
        const TCoFlatMapBase flatMap = node.Cast<TCoFlatMapBase>();
        if (!flatMap.Lambda().Body().Maybe<TCoConditionalValueBase>()) {
            return node;
        }

        const auto& maybeDqSource = flatMap.Input().Maybe<TDqSourceWrap>();
        if (!maybeDqSource) {
            return node;
        }

        TDqSourceWrap dqSource = maybeDqSource.Cast();
        if (dqSource.DataSource().Category() != S3ProviderName) {
            return node;
        }

        const auto& maybeS3SourceSettings = dqSource.Input().Maybe<TS3SourceSettingsBase>();
        if (!maybeS3SourceSettings || dqSource.Ref().ChildrenSize() <= TDqSourceWrap::idx_Settings) {
            return node;
        }

        if (!HasSetting(dqSource.Settings().Ref(), "extraColumns")) {
            return node;
        }

        if (!HasSetting(dqSource.Settings().Ref(), "prunedPaths")) {
            return CalculatePrunedPaths(flatMap, ctx, State_->Types);
        }

        return node;
    }

    TMaybeNode<TExprBase> ExtractMembersOverDqSource(TExprBase node, TExprContext& ctx) const {
        const auto& extract = node.Cast<TCoExtractMembers>();
        const auto& maybeDqSource = extract.Input().Maybe<TDqSourceWrap>();
        if (!maybeDqSource) {
            return node;
        }

        const auto& dqSource = maybeDqSource.Cast();
        if (dqSource.DataSource().Category() != S3ProviderName) {
            return node;
        }

        const auto& maybeS3SourceSettings = dqSource.Input().Maybe<TS3SourceSettingsBase>();
        if (!maybeS3SourceSettings) {
            return node;
        }

        TSet<TStringBuf> extractMembers;
        for (auto member : extract.Members()) {
            extractMembers.insert(member.Value());
        }

        TMaybeNode<TExprBase> settings = dqSource.Settings();

        TMaybeNode<TExprBase> newSettings = settings;
        TExprNode::TPtr newPaths = maybeS3SourceSettings.Cast().Paths().Ptr();

        if (settings) {
            if (auto prunedPaths = GetSetting(settings.Cast().Ref(), "prunedPaths")) {
                if (prunedPaths->ChildrenSize() > 1) {
                    // pruning in progress
                    return node;
                }
            }

            if (auto extraColumnsSetting = GetSetting(settings.Cast().Ref(), "extraColumns")) {
                const TStructExprType* extraType = extraColumnsSetting->Tail().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
                auto extraTypeItems = extraType->GetItems();
                EraseIf(extraTypeItems, [&](const TItemExprType* item) { return !extractMembers.contains(item->GetName()); });
                if (extraTypeItems.size() < extraType->GetSize()) {
                    auto originalPaths = maybeS3SourceSettings.Cast().Paths().Ptr();
                    auto originalExtra = extraColumnsSetting->TailPtr();
                    YQL_ENSURE(originalPaths->IsList());

                    TExprNodeList newPathItems;
                    TExprNodeList newExtraColumnsExtents;

                    for (const auto& batch : maybeS3SourceSettings.Cast().Paths()) {
                        auto extra = batch.ExtraColumns();
                        YQL_ENSURE(TCoAsStruct::Match(extra.Raw()));
                        TExprNodeList children = extra.Ref().ChildrenList();
                        EraseIf(children, [&](const TExprNode::TPtr& child) { return !extractMembers.contains(child->Head().Content()); });
                        auto newStruct = ctx.ChangeChildren(extra.Ref(), std::move(children));

                        TStringBuf packed = batch.Data().Literal().Value();
                        bool isTextEncoded = FromString<bool>(batch.IsText().Literal().Value());

                        TPathList paths;
                        UnpackPathsList(packed, isTextEncoded, paths);

                        newExtraColumnsExtents.push_back(
                            ctx.Builder(batch.ExtraColumns().Pos())
                                .Callable("Replicate")
                                    .Add(0, newStruct)
                                    .Callable(1, "Uint64")
                                        .Atom(0, ToString(paths.size()), TNodeFlags::Default)
                                    .Seal()
                                .Seal()
                                .Build()
                        );
                        newPathItems.push_back(ctx.ChangeChild(batch.Ref(), TS3Path::idx_ExtraColumns, std::move(newStruct)));
                    }

                    newPaths = ctx.ChangeChildren(maybeS3SourceSettings.Cast().Paths().Ref(), std::move(newPathItems));

                    TExprNode::TPtr newExtra = ctx.NewCallable(extraColumnsSetting->Pos(), "OrderedExtend", std::move(newExtraColumnsExtents));
                    newSettings = TExprBase(extraTypeItems.empty() ? RemoveSetting(settings.Cast().Ref(), "extraColumns", ctx) :
                        ReplaceSetting(settings.Cast().Ref(), extraColumnsSetting->Pos(), "extraColumns", newExtra, ctx));
                }
            }
        }

        const TStructExprType* outputRowType = node.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        const TExprNode::TPtr outputRowTypeNode = ExpandType(dqSource.RowType().Pos(), *outputRowType, ctx);

        YQL_CLOG(INFO, ProviderS3) << "ExtractMembers over DqSource with " << maybeS3SourceSettings.Cast().CallableName();

        if (maybeS3SourceSettings.Cast().CallableName() == TS3SourceSettings::CallableName()) {
            return Build<TDqSourceWrap>(ctx, dqSource.Pos())
                .InitFrom(dqSource)
                .Input<TS3SourceSettings>()
                    .InitFrom(dqSource.Input().Maybe<TS3SourceSettings>().Cast())
                    .Paths(newPaths)
                .Build()
                .RowType(outputRowTypeNode)
                .Settings(newSettings)
                .Done();
        }

        const auto parseSettings = dqSource.Input().Maybe<TS3ParseSettings>().Cast();

        const TStructExprType* readRowType = parseSettings.RowType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();

        TVector<const TItemExprType*> readRowDataItems = readRowType->GetItems();
        TVector<const TItemExprType*> outputRowDataItems = outputRowType->GetItems();

        if (auto settings = parseSettings.Settings()) {
            if (auto ps = GetSetting(settings.Cast().Ref(), "partitionedby")) {
                THashSet<TStringBuf> cols;
                for (size_t i = 1; i < ps->ChildrenSize(); ++i) {
                    YQL_ENSURE(ps->Child(i)->IsAtom());
                    cols.insert(ps->Child(i)->Content());
                }
                auto isPartitionedBy = [&](const auto& item) { return cols.contains(item->GetName()); };
                EraseIf(readRowDataItems, isPartitionedBy);
                EraseIf(outputRowDataItems, isPartitionedBy);
            }
        }

        auto formatName = parseSettings.Format().StringValue();
        if (outputRowDataItems.size() == 0 && readRowDataItems.size() != 0 && formatName != "parquet") {
            const TStructExprType* readRowDataType = ctx.MakeType<TStructExprType>(readRowDataItems);
            auto item = GetLightColumn(*readRowDataType);
            YQL_ENSURE(item);
            readRowType = ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{item});
        } else {
            readRowType = outputRowType;
        }

        return Build<TDqSourceWrap>(ctx, dqSource.Pos())
            .InitFrom(dqSource)
            .Input<TS3ParseSettings>()
                .InitFrom(parseSettings)
                .Paths(newPaths)
                .RowType(ExpandType(dqSource.Input().Pos(), *readRowType, ctx))
            .Build()
            .RowType(outputRowTypeNode)
            .Settings(newSettings)
            .Done();
    }

    TMaybeNode<TExprBase> MergeS3Paths(TExprBase node, TExprContext& ctx) const {
        const TDqSourceWrap dqSource = node.Cast<TDqSourceWrap>();
        if (dqSource.DataSource().Category() != S3ProviderName) {
            return node;
        }

        const auto& maybeS3SourceSettings = dqSource.Input().Maybe<TS3SourceSettingsBase>();
        if (!maybeS3SourceSettings) {
            return node;
        }

        TMaybeNode<TExprBase> settings = dqSource.Settings();
        if (settings) {
            if (auto prunedPaths = GetSetting(settings.Cast().Ref(), "prunedPaths")) {
                if (prunedPaths->ChildrenSize() > 1) {
                    // pruning in progress
                    return node;
                }
            }
        }

        TNodeMap<TExprNodeList> s3PathsByExtra;
        bool needMerge = false;
        for (const auto& batch : maybeS3SourceSettings.Cast().Paths()) {
            auto extra = batch.ExtraColumns();
            auto& group = s3PathsByExtra[extra.Raw()];
            group.emplace_back(batch.Ptr());
            needMerge = needMerge || group.size() > 1;
        }

        if (!needMerge) {
            return node;
        }

        TExprNodeList newPathItems;
        TExprNodeList newExtraColumnsExtents;

        for (const auto& origBatch : maybeS3SourceSettings.Cast().Paths()) {
            auto extra = origBatch.ExtraColumns();
            auto& group = s3PathsByExtra[extra.Raw()];
            if (group.empty()) {
                continue;
            }

            TPathList paths;
            for (auto& item : group) {
                TS3Path batch(item);
                TStringBuf packed = batch.Data().Literal().Value();
                bool isTextEncoded = FromString<bool>(batch.IsText().Literal().Value());
                UnpackPathsList(packed, isTextEncoded, paths);
            }

            bool isTextEncoded;
            TString packedPaths;
            PackPathsList(paths, packedPaths, isTextEncoded);

            newPathItems.emplace_back(
                Build<TS3Path>(ctx, origBatch.Pos())
                    .Data<TCoString>()
                        .Literal()
                        .Build(packedPaths)
                    .Build()
                    .IsText<TCoBool>()
                        .Literal()
                        .Build(ToString(isTextEncoded))
                    .Build()
                    .ExtraColumns(extra)
                .Done().Ptr()
            );

            newExtraColumnsExtents.push_back(
                ctx.Builder(extra.Pos())
                    .Callable("Replicate")
                        .Add(0, extra.Ptr())
                        .Callable(1, "Uint64")
                            .Atom(0, ToString(paths.size()), TNodeFlags::Default)
                        .Seal()
                    .Seal()
                    .Build()
            );

            group.clear();
        }

        TMaybeNode<TExprBase> newSettings = settings;
        if (settings) {
            if (auto extraColumnsSetting = GetSetting(settings.Cast().Ref(), "extraColumns")) {
                TPositionHandle pos = extraColumnsSetting->Pos();
                auto newExtra = ctx.NewCallable(pos, "OrderedExtend", std::move(newExtraColumnsExtents));
                newSettings = TExprBase(ReplaceSetting(settings.Cast().Ref(), pos, "extraColumns", newExtra, ctx));
            }
        }

        YQL_CLOG(INFO, ProviderS3) << "Merge S3 paths with same extra columns in DqSource over " << maybeS3SourceSettings.Cast().CallableName();
        auto sourceSettings = ctx.ChangeChild(maybeS3SourceSettings.Cast().Ref(), TS3SourceSettingsBase::idx_Paths,
            ctx.NewList(maybeS3SourceSettings.Cast().Paths().Pos(), std::move(newPathItems)));

        return Build<TDqSourceWrap>(ctx, dqSource.Pos())
            .InitFrom(dqSource)
            .Input(sourceSettings)
            .Settings(newSettings)
            .Done();
    }

    TMaybeNode<TExprBase> CleanupExtraColumns(TExprBase node, TExprContext& ctx) const {
        const TDqSourceWrap dqSource = node.Cast<TDqSourceWrap>();
        if (dqSource.DataSource().Category() != S3ProviderName) {
            return node;
        }

        TMaybeNode<TExprBase> settings = dqSource.Settings();
        if (!settings) {
            return node;
        }

        if (auto extraColumnsSetting = GetSetting(settings.Cast().Ref(), "extraColumns")) {
            const TStructExprType* extraType = extraColumnsSetting->Tail().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
            auto extraTypeItems = extraType->GetItems();
            if (!extraTypeItems) {
                auto newSettings =  TExprBase(RemoveSetting(settings.Cast().Ref(), "extraColumns", ctx));
                return Build<TDqSourceWrap>(ctx, dqSource.Pos())
                    .InitFrom(dqSource)
                    .Settings(newSettings)
                    .Done();
            }
        }

        return node;

    }

    template <class TSettings>
    TMaybeNode<TExprBase> ConstructNodeForPushDownLimit(TCoTake take, TDqSourceWrap source, TSettings settings, TCoUint64 count, TExprContext& ctx) const {
        return Build<TCoTake>(ctx, take.Pos())
            .InitFrom(take)
            .Input<TDqSourceWrap>()
                .InitFrom(source)
                .Input<TSettings>()
                    .InitFrom(settings)
                    .RowsLimitHint(count.Literal())
                    .Build()
            .Build()
        .Done();
    }

    TMaybeNode<TExprBase> PushDownLimit(TExprBase node, TExprContext& ctx) const {
        auto take = node.Cast<TCoTake>();

        auto maybeSource = take.Input().Maybe<TDqSourceWrap>();
        auto maybeSettings = maybeSource.Input().Maybe<TS3SourceSettingsBase>();
        if (!maybeSettings) {
            return take;
        }
        YQL_CLOG(TRACE, ProviderS3) << "Trying to push down limit for S3";

        auto maybeCount = take.Count().Maybe<TCoUint64>();
        if (!maybeCount) {
            return take;
        }
        auto count = maybeCount.Cast();
        auto countNum = FromString<ui64>(count.Literal().Ref().Content());
        YQL_ENSURE(countNum > 0, "Got invalid limit " << countNum << " to push down");

        auto settings = maybeSettings.Cast();
        if (auto rowsLimitHintStr = settings.RowsLimitHint().Ref().Content(); !rowsLimitHintStr.empty()) {
            // LimitHint is already pushed down
            auto rowsLimitHint = FromString<ui64>(rowsLimitHintStr);
            if (countNum >= rowsLimitHint) {
                // Already propagated
                return node;
            }
        }

        if (auto sourceSettings = maybeSettings.Maybe<TS3SourceSettings>()) {
            return ConstructNodeForPushDownLimit(take, maybeSource.Cast(), sourceSettings.Cast(), count, ctx);
        }
        auto parseSettings = maybeSettings.Maybe<TS3ParseSettings>();
        YQL_ENSURE(parseSettings, "unsupported type derived from TS3SourceSettingsBase");
        return ConstructNodeForPushDownLimit(take, maybeSource.Cast(), parseSettings.Cast(), count, ctx);
    }

private:
    const TS3State::TPtr State_;
};

}

THolder<IGraphTransformer> CreateS3LogicalOptProposalTransformer(TS3State::TPtr state) {
    return MakeHolder<TS3LogicalOptProposalTransformer>(state);
}

} // namespace NYql
