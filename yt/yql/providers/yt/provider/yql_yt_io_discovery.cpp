#include "yql_yt_provider_impl.h"
#include "yql_yt_key.h"
#include "yql_yt_gateway.h"
#include "yql_yt_op_settings.h"
#include "yql_yt_helpers.h"
#include "yql_yt_io_discovery_partitions.h"
#include "yql_yt_io_discovery_walk_folders.h"

#include <yt/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <yt/yql/providers/yt/common/yql_names.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/core/services/yql_eval_expr.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_expr_constraint.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/type_ann/type_ann_core.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/peephole_opt/yql_opt_peephole_physical.h>
#include <yql/essentials/core/issue/protos/issue_id.pb.h>
#include <yql/essentials/core/issue/yql_issue.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_paths.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/string/cast.h>
#include <util/string/strip.h>


namespace NYql {

using namespace NNodes;

class TYtIODiscoveryTransformer : public TGraphTransformerBase {
public:
    TYtIODiscoveryTransformer(TYtState::TPtr state)
        : State_(state)
    {
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;
        if (ctx.Step.IsDone(TExprStep::DiscoveryIO)) {
            return TStatus::Ok;
        }

        TVector<IYtGateway::TCanonizeReq> paths;
        const bool discoveryMode = State_->Types->DiscoveryMode;
        const bool evaluationInProgress = State_->Types->EvaluationInProgress;
        TOptimizeExprSettings settings(nullptr);

        const bool prunePartitions = State_->Configuration->EarlyPartitionPruning.Get().GetOrElse(DEFAULT_EARLY_PARTITION_PRUNING);
        if (prunePartitions && (!discoveryMode || !evaluationInProgress)) {
            auto status = OptimizeExpr(input, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
                auto maybeFilter = TMaybeNode<TCoFilterBase>(node);
                if (!maybeFilter) {
                    return node;
                }
                if (maybeFilter.Cast().Lambda().Args().Size() != 1) {
                    return node;
                }
                auto maybeRead = maybeFilter.Input().Maybe<TCoRight>().Input().Maybe<TYtRead>();
                if (!maybeRead) {
                    return node;
                }
                auto read = maybeRead.Cast();
                if (read.Args().Count() != 5) {
                    return node;
                }
                if (!read.Arg(2).Ref().IsCallable({MrPartitionListName, MrPartitionListStrictName})) {
                    return node;
                }
                if (!read.Arg(4).Maybe<TCoNameValueTupleList>()) {
                    return node;
                }
                if (HasSetting(read.Arg(4).Ref(), EYtSettingType::Pruned)) {
                    return node;
                }
                return PrunePartitionList(maybeFilter.Cast(), ctx);
            }, ctx, settings);

            if (status.Level != TStatus::Ok) {
                if (status.Level == TStatus::Repeat) {
                    ctx.Step
                        .Repeat(TExprStep::ExpandApplyForLambdas)
                        .Repeat(TExprStep::ExprEval);
                    return TStatus(TStatus::Repeat, true);
                }
                return status;
            }
        }

        settings.VisitChanges = true;
        bool seenMrPartitions = false;
        auto status = OptimizeExpr(input, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
            if (auto maybeRead = TMaybeNode<TYtRead>(node)) {
                if (!maybeRead.DataSource()) { // Validates provider
                    return node;
                }
                auto read = maybeRead.Cast();
                auto ds = read.DataSource();
                if (!EnsureArgsCount(read.Ref(), 5, ctx)) {
                    return {};
                }

                if (discoveryMode && evaluationInProgress) {
                    ctx.AddError(YqlIssue(ctx.GetPosition(read.Pos()), TIssuesIds::YQL_NOT_ALLOWED_IN_DISCOVERY,
                        TStringBuilder() << node->Content() << " is not allowed in Discovery mode"));
                    return {};
                }

                if (read.Arg(2).Ref().IsCallable({MrPartitionsName, MrPartitionsStrictName})) {
                    seenMrPartitions = true;
                    return ExpandMrPartitions(read, ctx, *(State_->Types));
                }

                if (read.Arg(2).Ref().IsCallable({MrPartitionListName, MrPartitionListStrictName})) {
                    return ExpandPartitionList(read, ctx);
                }

                TYtInputKeys keys;
                if (!keys.Parse(read.Arg(2).Ref(), ctx)) {
                    return {};
                }

                if (keys.IsProcessed()) {
                    // Already processed
                    return node;
                }

                if (keys.GetKeys().empty()) {
                    auto userSchema = GetSetting(*read.Ref().Child(4), EYtSettingType::UserSchema);
                    if (userSchema) {
                        return BuildEmptyTablesRead(read.Pos(), *userSchema, ctx);
                    }

                    ctx.AddError(TIssue(ctx.GetPosition(read.Arg(2).Pos()), "The list of tables is empty"));
                    return {};
                }

                if (keys.GetType() == TYtKey::EType::TableScheme) {
                    return ConvertTableScheme(read, keys.GetKeys().front(), ctx);
                }

                if (discoveryMode) {
                    for (auto& key: keys.GetKeys()) {
                        auto keyPos = ctx.GetPosition(key.GetNode()->Pos());
                        if (key.GetRange()) {
                            ctx.AddError(YqlIssue(ctx.GetPosition(read.Arg(2).Pos()), TIssuesIds::YQL_NOT_ALLOWED_IN_DISCOVERY,
                                TStringBuilder() << MrTableRangeName << '/' << MrTableRangeStrictName << " is not allowed in Discovery mode"));
                            return {};
                        }
                        else if (key.GetFolder()) {
                            ctx.AddError(YqlIssue(ctx.GetPosition(read.Arg(2).Pos()), TIssuesIds::YQL_NOT_ALLOWED_IN_DISCOVERY,
                                TStringBuilder() << MrFolderName << " is not allowed in Discovery mode"));
                            return {};
                        }
                    }
                }
                if (AllOf(keys.GetKeys(), [] (const TYtKey& key) { return key.IsAnonymous(); })) {
                    return ConvertTableRead(read, keys, ctx);
                }
                return node;
            }
            else if (auto maybeWrite = TMaybeNode<TYtWrite>(node)) {
                if (!maybeWrite.DataSink()) { // Validates provider
                    return node;
                }
                auto write = maybeWrite.Cast();
                auto ds = write.DataSink();
                if (!EnsureArgsCount(write.Ref(), 5, ctx)) {
                    return {};
                }

                if (!EnsureTuple(write.Arg(4).MutableRef(), ctx)) {
                    return {};
                }

                TYtOutputKey key;
                if (!key.Parse(write.Arg(2).Ref(), ctx)) {
                    return {};
                }
                if (key.GetType() == TYtKey::EType::Undefined) {
                    // Already processed
                    return node;
                }

                const auto systemSettings = { EYtSettingType::Initial, EYtSettingType::MutationId };
                for (auto setting : systemSettings) {
                    if (auto sNode = NYql::GetSetting(*node->ChildPtr(4), setting)) {
                        ctx.AddError(TIssue(
                            ctx.GetPosition(sNode->Pos()),
                            TStringBuilder() << "Write setting " << ToString(setting).Quote() << " is internal and not allowed in user queries"));
                        return {};
                    }
                }

                auto mode = NYql::GetSetting(*node->ChildPtr(4), EYtSettingType::Mode);
                const bool flush = mode && FromString<EYtWriteMode>(mode->Child(1)->Content()) == EYtWriteMode::Flush;

                TYtTableInfo tableInfo(key, ds.Cluster().Value());
                if (key.IsAnonymous()) {
                    if (flush) {
                        ctx.AddError(TIssue(
                            ctx.GetPosition(write.Pos()),
                            TStringBuilder() << "Using anonymous tables as checkpoints is not allowed"));
                        return {};
                    }
                    tableInfo.Settings = Build<TCoNameValueTupleList>(ctx, write.Pos())
                        .Add()
                            .Name().Value(ToString(EYtSettingType::Anonymous)).Build()
                        .Build()
                        .Done();
                } else if (tableInfo.Name.StartsWith(NYT::TConfig::Get()->Prefix)) {
                    tableInfo.Name = tableInfo.Name.substr(NYT::TConfig::Get()->Prefix.size());
                }

                if (tableInfo.Name.empty()) {
                    ctx.AddError(TIssue(ctx.GetPosition(write.Pos()), "Table name must not be empty"));
                    return {};
                }

                if (flush) {
                    auto setKey = std::make_pair(ds.Cluster().Value(), tableInfo.Name);
                    if (State_->Checkpoints.contains(setKey)) {
                        ctx.AddError(TIssue(
                            ctx.GetPosition(write.Pos()),
                            TStringBuilder() << "Table " << tableInfo.Name.Quote() << " already used as checkpoint"));
                        return {};
                    }
                    State_->Checkpoints.emplace(std::move(setKey));
                }
                node->ChildRef(2) = tableInfo.ToExprNode(ctx, write.Pos()).Ptr();
                return node;
            }

            return node;
        }, ctx, settings);

        if (status.Level != TStatus::Ok) {
            if (seenMrPartitions && status.Level == TStatus::Repeat) {
                ctx.Step
                    .Repeat(TExprStep::ExpandApplyForLambdas)
                    .Repeat(TExprStep::ExprEval);
                return TStatus(TStatus::Repeat, true);
            }
            return status;
        }

        status = VisitInputKeys(output, ctx, [this, &ctx, &paths] (TYtRead readNode, TYtInputKeys&& keys) -> TExprNode::TPtr {
            if (keys.GetType() != TYtKey::EType::TableScheme) {
                const auto cluster = TString{readNode.DataSource().Cluster().Value()};
                for (auto&& key: keys.ExtractKeys()) {
                    auto keyPos = ctx.GetPosition(key.GetNode()->Pos());
                    if (key.GetRange()) {
                        PendingRanges_.emplace(std::make_pair(cluster, *key.GetRange()), std::make_pair(keyPos, NThreading::TFuture<IYtGateway::TTableRangeResult>()));
                    }
                    else if (key.GetFolder()) {
                        PendingFolders_.emplace(std::make_pair(cluster, *key.GetFolder()), std::make_pair(keyPos, NThreading::TFuture<IYtGateway::TFolderResult>()));
                    }
                    else if (key.GetWalkFolderArgs()) {
                        return ctx.ChangeChild(readNode.Ref(), 2, InitializeWalkFolders(std::move(key), cluster, keyPos, ctx));
                    }
                    else if (key.GetWalkFolderImplArgs()) {
                        PendingWalkFoldersKeys_.insert(key.GetWalkFolderImplArgs()->StateKey);
                    }
                    else if (!key.IsAnonymous()) {
                        if (PendingCanonizations_.insert(std::make_pair(std::make_pair(cluster, key.GetPath()), paths.size())).second) {
                            paths.push_back(IYtGateway::TCanonizeReq()
                                .Cluster(cluster)
                                .Path(key.GetPath())
                                .Pos(keyPos)
                            );
                        }
                    }
                }
            }
            return readNode.Ptr();
        }, /* visitChanges */ true);

        if (status.Level == TStatus::Error) {
            PendingCanonizations_.clear();
            PendingFolders_.clear();
            PendingRanges_.clear();

            for (const auto& key : PendingWalkFoldersKeys_) {
                State_->WalkFoldersState.erase(key);
            }
            PendingWalkFoldersKeys_.clear();

            YQL_CLOG(INFO, ProviderYt) << "YtIODiscovery - finish, status: " << (TStatus::ELevel)status.Level;
            return status;
        }

        if (PendingRanges_.empty() && PendingFolders_.empty()
            && PendingCanonizations_.empty() && PendingWalkFoldersKeys_.empty()) {
            YQL_CLOG(INFO, ProviderYt) << "YtIODiscovery - finish, status: " << (TStatus::ELevel)status.Level;
            return status;
        }

        TVector<NThreading::TFuture<void>> allFutures;
        if (!PendingCanonizations_.empty()) {
            CanonizeFuture_ = State_->Gateway->CanonizePaths(
                IYtGateway::TCanonizePathsOptions(State_->SessionId)
                    .Paths(std::move(paths))
                    .Config(State_->Configuration->Snapshot())
            );
            allFutures.push_back(CanonizeFuture_.IgnoreResult());
        }

        for (auto& x : PendingRanges_) {
            auto& cluster = x.first.first;
            auto& range = x.first.second;
            auto filterLambda = range.Filter;
            TUserDataTable files;
            if (filterLambda) {
                const auto transformer = CreateTypeAnnotationTransformer(CreateExtCallableTypeAnnotationTransformer(*State_->Types), *State_->Types);
                const auto constraints = CreateConstraintTransformer(*State_->Types, true);
                const auto peephole = MakePeepholeOptimization(State_->Types);
                while (const auto stringType = ctx.MakeType<TDataExprType>(EDataSlot::String)) {
                    if (!UpdateLambdaAllArgumentsTypes(filterLambda, {stringType}, ctx)) {
                        return TStatus::Error;
                    }

                    if (const auto status = transformer->Transform(filterLambda, filterLambda, ctx); status.Level == TStatus::Error) {
                        return status;
                    } else if (status.Level == TStatus::Repeat) {
                        continue;
                    }

                    bool isOptional;
                    if (const TDataExprType* dataType = nullptr;
                        !(EnsureDataOrOptionalOfData(*filterLambda, isOptional, dataType, ctx) && EnsureSpecificDataType(filterLambda->Pos(), *dataType, EDataSlot::Bool, ctx))) {
                        return TStatus::Error;
                    }

                    if (const auto status = UpdateLambdaConstraints(*filterLambda); status.Level == TStatus::Error) {
                        return status;
                    }

                    if (const auto status = constraints->Transform(filterLambda, filterLambda, ctx); status.Level == TStatus::Error) {
                        return status;
                    } else if (status.Level == TStatus::Repeat) {
                        continue;
                    }

                    if (const auto status = peephole->Transform(filterLambda, filterLambda, ctx); status.Level == TStatus::Error) {
                        return status;
                    } else if (status.Level == TStatus::Repeat) {
                        continue;
                    }

                    break;
                }

                if (!NCommon::FreezeUsedFilesSync(*filterLambda, files, *State_->Types, ctx, MakeUserFilesDownloadFilter(*State_->Gateway, cluster))) {
                    return TStatus::Error;
                }
            }

            auto result = State_->Gateway->GetTableRange(
                IYtGateway::TTableRangeOptions(State_->SessionId)
                    .Cluster(cluster)
                    .Prefix(StripStringRight(range.Prefix, EqualsStripAdapter('/')))
                    .Suffix(StripStringLeft(range.Suffix, EqualsStripAdapter('/')))
                    .Filter(filterLambda.Get())
                    .ExprCtx(filterLambda ? &ctx : nullptr)
                    .UserDataBlocks(files)
                    .UdfModules(State_->Types->UdfModules)
                    .UdfResolver(State_->Types->UdfResolver)
                    .UdfValidateMode(State_->Types->ValidateMode)
                    .Config(State_->Configuration->Snapshot())
                    .OptLLVM(State_->Types->OptLLVM.GetOrElse(TString()))
                    .RuntimeLogLevel(State_->Types->RuntimeLogLevel)
                    .LangVer(State_->Types->LangVer)
                    .Pos(x.second.first)
            );
            allFutures.push_back(result.IgnoreResult());
            x.second.second = result;
        }

        for (auto& x : PendingFolders_) {
            auto& cluster = x.first.first;
            auto& folder = x.first.second;
            auto result = State_->Gateway->GetFolder(
                IYtGateway::TFolderOptions(State_->SessionId)
                    .Cluster(cluster)
                    .Prefix(folder.Prefix)
                    .Attributes(TSet<TString>(folder.Attributes.begin(), folder.Attributes.end()))
                    .Config(State_->Configuration->Snapshot())
                    .Pos(x.second.first)
            );
            allFutures.push_back(result.IgnoreResult());
            x.second.second = result;
        }

        CanonizationRangesFoldersFuture_ = NThreading::WaitExceptionOrAll(allFutures);
        return TStatus::Async;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
        Y_UNUSED(input);
        if (auto walkFoldersFuture = MaybeGetWalkFoldersFuture()) {
            if (PendingCanonizations_.empty() && PendingRanges_.empty() && PendingFolders_.empty()) {
                return walkFoldersFuture.GetRef();
            }
            return NThreading::WaitExceptionOrAll(walkFoldersFuture.GetRef(), CanonizationRangesFoldersFuture_);
        }

        return CanonizationRangesFoldersFuture_;
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        YQL_CLOG(INFO, ProviderYt) << "YtIODiscovery - DoApplyAsyncChanges start";
        output = input;

        if (!PendingCanonizations_.empty()) {
            auto& res = CanonizeFuture_.GetValue();
            res.ReportIssues(ctx.IssueManager);

            if (!res.Success()) {
                PendingCanonizations_.clear();
                PendingRanges_.clear();
                CanonizeFuture_ = {};
                CanonizationRangesFoldersFuture_ = {};

                for (const auto& key : PendingWalkFoldersKeys_) {
                    State_->WalkFoldersState.erase(key);
                }
                PendingWalkFoldersKeys_.clear();

                return TStatus::Error;
            }
        }

        TOptimizeExprSettings settings(nullptr);
        settings.VisitChanges = true;
        auto status = OptimizeExpr(input, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
            if (!TMaybeNode<TYtRead>(node).DataSource()) {
                return node;
            }
            auto read = TYtRead(node);
            auto cluster = TString{read.DataSource().Cluster().Value()};

            TYtInputKeys keys;
            if (!keys.Parse(*node->Child(2), ctx)) {
                return {};
            }

            if (keys.GetType() == TYtKey::EType::Folder) {
                const auto res = FetchFolderResult(ctx, cluster, *keys.GetKeys().front().GetFolder());
                if (!res) {
                    return {};
                }
                if (auto file = std::get_if<TFileLinkPtr>(&res->ItemsOrFileLink)) {
                    TString alias;
                    if (auto p = FolderFileToAlias_.FindPtr(file->Get()->GetPath().GetPath())) {
                        alias = *p;
                    } else {
                        alias = TString("_yql_folder").append(ToString(FolderFileToAlias_.size()));
                        FolderFileToAlias_.emplace(file->Get()->GetPath().GetPath(), alias);

                        TUserDataBlock tmpBlock;
                        tmpBlock.Type = EUserDataType::PATH;
                        tmpBlock.Data = file->Get()->GetPath().GetPath();
                        tmpBlock.Usage.Set(EUserDataBlockUsage::Path);
                        tmpBlock.FrozenFile = file->Get();

                        State_->Types->UserDataStorage->AddUserDataBlock(alias, tmpBlock);
                    }

                    auto folderListFromFile = ctx.Builder(node->Pos())
                        .Callable("Collect")
                            .Callable(0, "Apply")
                                .Callable(0, "Udf")
                                    .Atom(0, "File.FolderListFromFile")
                                .Seal()
                                .Callable(1, "FilePath")
                                    .Atom(0, alias)
                                .Seal()
                            .Seal()
                        .Seal()
                        .Build();

                    return BuildFolderTableResExpr(ctx, node->Pos(), read.World(), folderListFromFile).Ptr();
                }

                auto items = std::get<TVector<IYtGateway::TFolderResult::TFolderItem>>(res->ItemsOrFileLink);
                YQL_CLOG(INFO, ProviderYt) << "Got " << items.size() << " items for " << " GetFolder";
                TVector<TExprBase> listItems;
                for (auto& item: items) {
                    listItems.push_back(BuildFolderListItemExpr(ctx, node->Pos(), item.Path, item.Type, item.Attributes));
                }

                return BuildFolderTableResExpr(ctx, node->Pos(), read.World(), BuildFolderListExpr(ctx, node->Pos(), listItems).Ptr()).Ptr();
            }

            if (keys.GetType() != TYtKey::EType::Table) {
                return node;
            }

            TVector<TExprBase> tableSettings;
            TVector<TExprBase> readSettings;
            SplitReadSettings(read, tableSettings, readSettings, ctx);

            bool hasErrors = false;
            bool isStrict = keys.GetStrictConcat();
            TVector<TExprBase> tables;

            for (auto& key : keys.GetKeys()) {
                if (key.GetRange()) {
                    auto p = PendingRanges_.FindPtr(std::make_pair(cluster, *key.GetRange()));
                    YQL_ENSURE(p);
                    auto& res = p->second.GetValue();
                    res.ReportIssues(ctx.IssueManager);

                    if (res.Success()) {
                        for (auto& oneTable: res.Tables) {
                            TYtTableInfo tableInfo;
                            tableInfo.Name = oneTable.Path;
                            tableInfo.Cluster = cluster;
                            auto settingsBuilder = Build<TCoNameValueTupleList>(ctx, key.GetNode()->Pos()).Add(tableSettings);
                            if (key.GetView()) {
                                settingsBuilder.Add()
                                    .Name().Value(ToString(EYtSettingType::View)).Build()
                                    .Value<TCoAtom>().Value(key.GetView()).Build()
                                .Build();
                            }
                            tableInfo.Settings = settingsBuilder.Done();

                            TYtPathInfo pathInfo;
                            if (oneTable.Columns) {
                                pathInfo.SetColumns(*oneTable.Columns);
                            }
                            if (oneTable.Ranges) {
                                pathInfo.Ranges = MakeIntrusive<TYtRangesInfo>();
                                pathInfo.Ranges->Parse(*oneTable.Ranges, ctx, key.GetNode()->Pos());
                            }
                            tables.push_back(pathInfo.ToExprNode(ctx, key.GetNode()->Pos(), tableInfo.ToExprNode(ctx, key.GetNode()->Pos())));
                        }
                        isStrict = isStrict && key.GetRange()->IsStrict;
                    } else {
                        hasErrors = true;
                    }
                }
                else if (key.IsAnonymous()) {
                    TYtTableInfo table(key, cluster);
                    table.Settings = Build<TCoNameValueTupleList>(ctx, read.Pos())
                        .Add(tableSettings)
                        .Add()
                            .Name().Value(ToString(EYtSettingType::Anonymous)).Build()
                        .Build()
                        .Done();
                    auto path = Build<TYtPath>(ctx, read.Pos())
                        .Table(table.ToExprNode(ctx, read.Pos()).Cast<TYtTable>())
                        .Columns<TCoVoid>().Build()
                        .Ranges<TCoVoid>().Build()
                        .Stat<TCoVoid>().Build()
                        .Done();
                    tables.push_back(path);
                }
                else {
                    auto p = PendingCanonizations_.FindPtr(std::make_pair(cluster, key.GetPath()));
                    YQL_ENSURE(p);
                    auto& oneTable = CanonizeFuture_.GetValue().Data.at(*p);
                    if (oneTable.Path.empty()) {
                        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Bad table name: " << key.GetPath()));
                        hasErrors = true;
                        continue;
                    }
                    TYtTableInfo tableInfo;
                    tableInfo.Name = oneTable.Path;
                    tableInfo.Cluster = cluster;
                    auto settingsBuilder = Build<TCoNameValueTupleList>(ctx, key.GetNode()->Pos()).Add(tableSettings);
                    if (key.GetView()) {
                        settingsBuilder.Add()
                            .Name().Value(ToString(EYtSettingType::View)).Build()
                            .Value<TCoAtom>().Value(key.GetView()).Build()
                        .Build();
                    }
                    if (auto extraColumns = key.GetExtraColumns()) {
                        settingsBuilder.Add()
                            .Name().Value(ToString(EYtSettingType::ExtraColumns)).Build()
                            .Value(key.GetExtraColumns()).Build()
                        .Build();
                    }

                    tableInfo.Settings = settingsBuilder.Done();

                    TYtPathInfo pathInfo;
                    if (oneTable.Columns) {
                        pathInfo.SetColumns(*oneTable.Columns);
                    }
                    if (oneTable.Ranges) {
                        pathInfo.Ranges = MakeIntrusive<TYtRangesInfo>();
                        pathInfo.Ranges->Parse(*oneTable.Ranges, ctx, key.GetNode()->Pos());
                    }
                    pathInfo.AdditionalAttributes = oneTable.AdditionalAttributes;
                    tables.push_back(pathInfo.ToExprNode(ctx, key.GetNode()->Pos(), tableInfo.ToExprNode(ctx, key.GetNode()->Pos())));
                }
            }
            if (hasErrors) {
                return {};
            }

            if (!tables.size()) {
                auto userSchema = GetSetting(read.Arg(4).Ref(), EYtSettingType::UserSchema);
                if (userSchema) {
                    return BuildEmptyTablesRead(node->Pos(), *userSchema, ctx);
                }

                ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "The list of tables is empty"));
                return {};
            }

            if (!isStrict && tables.size() > 1) {
                readSettings.push_back(Build<TCoNameValueTuple>(ctx, read.Pos()).Name().Value(ToString(EYtSettingType::WeakConcat)).Build().Done());
            }

            auto res = read.Ptr();
            res->ChildRef(2) = Build<TExprList>(ctx, read.Pos()).Add(tables).Done().Ptr();
            res->ChildRef(4) = Build<TCoNameValueTupleList>(ctx, read.Pos()).Add(readSettings).Done().Ptr();
            return res;

        }, ctx, settings);

        PendingCanonizations_.clear();
        PendingRanges_.clear();
        PendingFolders_.clear();
        CanonizeFuture_ = {};
        CanonizationRangesFoldersFuture_ = {};

        if (status == TStatus::Ok && !PendingWalkFoldersKeys_.empty()) {
            const auto walkFoldersStatus = RewriteWalkFoldersOnAsyncOrEvalChanges(output, ctx);
            return walkFoldersStatus;
        }

        YQL_CLOG(INFO, ProviderYt) << "YtIODiscovery DoApplyAsyncChanges - finish";
        return status;
    }

    void Rewind() final {
        YQL_CLOG(INFO, ProviderYt) << "Rewinding YtIODiscovery";
        PendingRanges_.clear();
        PendingFolders_.clear();
        PendingCanonizations_.clear();
        PendingWalkFoldersKeys_.clear();

        CanonizeFuture_ = {};
        CanonizationRangesFoldersFuture_ = {};
    }

private:
    static bool ValidatePartitionList(const TExprNode& partListNode, TExprContext& ctx) {
        YQL_ENSURE(partListNode.IsCallable({MrPartitionListName, MrPartitionListStrictName}));
        const size_t argCount = partListNode.ChildrenSize();
        const size_t minArgs = 1;
        const size_t maxArgs = 2;
        if (argCount < minArgs || argCount > maxArgs) {
            ctx.AddError(TIssue(ctx.GetPosition(partListNode.Pos()),
                TStringBuilder() << partListNode.Content() << " should have between " << minArgs << " and " << maxArgs << " arguments, but got: " << argCount));
            return false;
        }

        if (argCount == maxArgs && !partListNode.Tail().IsAtom()) {
            ctx.AddError(TIssue(ctx.GetPosition(partListNode.Pos()),
                TStringBuilder() << "Last argument of " << partListNode.Content() << " should be an atom"));
            return false;
        }

        const auto& partList = partListNode.Head();

        // TODO: support empty list with user schema via BuildEmptyTableRead()
        if (!partList.IsCallable("AsList") || partList.ChildrenSize() == 0) {
            ctx.AddError(TIssue(ctx.GetPosition(partListNode.Pos()),
                TStringBuilder() << "First argument of " << partListNode.Content() << " should be non-empty literal list"));
            return false;
        }

        for (size_t i = 0; i < partList.ChildrenSize(); ++i) {
            auto listEntry = partList.Child(i);
            if (!listEntry->IsCallable({"Struct", "AsStruct"})) {
                ctx.AddError(TIssue(ctx.GetPosition(listEntry->Pos()),
                    TStringBuilder() << "Expecting list of literal structs in " << partListNode.Content() << " first argument"));
                return false;
            }
            size_t startIdx = listEntry->IsCallable("AsStruct") ? 0 : 1;
            if (listEntry->ChildrenSize() <= startIdx) {
                ctx.AddError(TIssue(ctx.GetPosition(listEntry->Pos()),
                    TStringBuilder() << "Got empty struct in " << partListNode.Content() << " first argument"));
                return false;
            }
            THashSet<TStringBuf> members;
            for (size_t j = startIdx; j < listEntry->ChildrenSize(); ++j) {
                auto structItem = listEntry->Child(j);
                if (!structItem->IsList() || structItem->ChildrenSize() != 2) {
                    ctx.AddError(TIssue(ctx.GetPosition(structItem->Pos()),
                        TStringBuilder() << "Expecting literal 2-element tuple as argument of " << listEntry->Content()));
                    return false;
                }
                if (!structItem->Head().IsAtom()) {
                    ctx.AddError(TIssue(ctx.GetPosition(structItem->Head().Pos()),
                        TStringBuilder() << "First tuple element of " << listEntry->Content() << " should be an atom"));
                    return false;
                }

                const TStringBuf member = structItem->Head().Content();
                if (!members.insert(member).second) {
                    ctx.AddError(TIssue(ctx.GetPosition(structItem->Head().Pos()),
                        TStringBuilder() << "Duplicate struct member `" << member << "`"));
                    return false;
                }

                if (member == MrPartitionListTableMember || member == MrPartitionListViewMember) {
                    auto value = structItem->Child(1);
                    if (member == MrPartitionListViewMember) {
                        if (value->IsCallable("Nothing")) {
                            continue;
                        }
                        if (value->IsCallable("Just")) {
                            if (value->ChildrenSize() != 1) {
                                ctx.AddError(TIssue(ctx.GetPosition(value->Pos()),
                                    TStringBuilder() << "Callable Just should have single argument, got: " << value->ChildrenSize()));
                                return false;
                            }
                            value = value->Child(0);
                        }
                    }
                    if (!value->IsCallable({"String", "Utf8"})) {
                        ctx.AddError(TIssue(ctx.GetPosition(value->Pos()),
                            TStringBuilder() << "Member " << member << " has to be "
                                << (member == MrPartitionListTableMember ? "" : "(optional) ") << "String or Utf8 literal"));
                        return false;
                    }

                    if (value->ChildrenSize() != 1 || !value->Child(0)->IsAtom()) {
                        ctx.AddError(TIssue(ctx.GetPosition(value->Pos()),
                            TStringBuilder() << "Detected invalid string literal value for " << member));
                        return false;
                    }

                    if (value->Child(0)->Content().empty()) {
                        ctx.AddError(TIssue(ctx.GetPosition(value->Pos()),
                            TStringBuilder() << "Empty string is not allowed for " << member));
                        return {};
                    }
                }
            }

            if (!members.contains(MrPartitionListTableMember)) {
                ctx.AddError(TIssue(ctx.GetPosition(listEntry->Pos()),
                    TStringBuilder() << "List entry for " << partListNode.Content() << " missing required member " << MrPartitionListTableMember));
                return false;
            }
        }

        return true;
    }

    static TMaybe<bool> IsStdBooleanOperator(const TExprNode::TPtr& node) {
        if (node->IsCallable({"Not", "Or", "And", "Xor"})) {
            if (node->IsCallable("Not") && node->ChildrenSize() != 1) {
                return {};
            }
            if (node->ChildrenSize() == 0) {
                return {};
            }
            return true;
        }
        return false;
    }

    static TExprNode::TPtr PrunePartitionList(TCoFilterBase filter, TExprContext& ctx) {
        TYtRead read = filter.Input().Maybe<TCoRight>().Input().Maybe<TYtRead>().Cast();
        const auto& partListNode = read.Arg(2).Ref();
        if (!ValidatePartitionList(partListNode, ctx)) {
            return {};
        }
        const auto& partList = partListNode.Head();

        THashSet<TStringBuf> extraColumns;
        for (auto entry : partList.ChildrenList()) {
            YQL_ENSURE(entry->IsCallable({"AsStruct", "Struct"}));
            for (ui32 i = entry->IsCallable("Struct") ? 1 : 0; i < entry->ChildrenSize(); ++i) {
                auto structItem = entry->ChildPtr(i);
                auto member = structItem->Head().Content();
                if (member != MrPartitionListTableMember && member != MrPartitionListViewMember) {
                    extraColumns.insert(member);
                }
            }
        }
        if (extraColumns.empty()) {
            return filter.Ptr();
        }

        TExprNode::TPtr predicate = filter.Lambda().Body().Ptr();
        if (predicate->IsCallable("Coalesce")) {
            if (predicate->ChildrenSize() != 2 || !predicate->Child(1)->IsCallable("Bool")) {
                return filter.Ptr();
            }
            predicate = predicate->HeadPtr();
        }
        TExprNode::TPtr arg = filter.Lambda().Args().Arg(0).Ptr();

        TParentsMap parents;
        GatherParents(*predicate, parents);

        TNodeOnNodeOwnedMap booleanSubTree;
        bool validExpr = true;

        enum class EUsedColumnStatus : ui8 {
            // Keep order
            CONST_VAL,
            ONLY_EXTRA,
            ALL,
        };

        TNodeMap<EUsedColumnStatus> nodeColumnStatuses;
        VisitExpr(predicate, [&](const TExprNode::TPtr& node) {
            if (!validExpr) {
                return false;
            }

            bool hasBoolParent = false;
            if (node == predicate) {
                hasBoolParent = true;
            } else if (auto it = parents.find(node.Get()); it != parents.end()) {
                hasBoolParent = AllOf(it->second, [&](const TExprNode* parent) { return booleanSubTree.contains(parent); });
            }

            if (hasBoolParent) {
                auto status = IsStdBooleanOperator(node);
                if (!status) {
                    validExpr = false;
                    return false;
                }
                if (*status) {
                    booleanSubTree.insert(std::make_pair(node.Get(), TExprNode::TPtr()));
                }
            }
            return true;
        }, [&](const TExprNode::TPtr& node) {
            if (!validExpr) {
                return false;
            }

            EUsedColumnStatus status = EUsedColumnStatus::CONST_VAL;
            switch (node->Type()) {
                case TExprNode::EType::Atom:
                case TExprNode::EType::World:
                case TExprNode::EType::Arguments:
                    status = EUsedColumnStatus::CONST_VAL;
                    break;
                case TExprNode::EType::Argument:
                    status = node == arg ? EUsedColumnStatus::ALL : EUsedColumnStatus::CONST_VAL;
                    break;
                case TExprNode::EType::Lambda: {
                    if (node->ChildrenSize() != 2 || node->Head().Type() != TExprNode::EType::Arguments) {
                        validExpr = false;
                        return false;
                    }
                    auto it = nodeColumnStatuses.find(node->Child(1));
                    YQL_ENSURE(it != nodeColumnStatuses.end());
                    status = it->second;
                    break;
                }
                case TExprNode::EType::Callable:
                case TExprNode::EType::List: {
                    if (node->IsCallable({"Member", "SqlPlainColumnOrType"})) {
                        if (node->ChildrenSize() != 2 || !node->Child(1)->IsAtom()) {
                            validExpr = false;
                            return false;
                        }
                        if (node->ChildPtr(0) == arg) {
                            TStringBuf col = node->Child(1)->Content();
                            status = extraColumns.contains(col) ? EUsedColumnStatus::ONLY_EXTRA : EUsedColumnStatus::ALL;
                            break;
                        }
                    }
                    for (auto& child : node->ChildrenList()) {
                        auto it = nodeColumnStatuses.find(child.Get());
                        YQL_ENSURE(it != nodeColumnStatuses.end());
                        status = std::max(status, it->second);
                    }
                    if (node->IsCallable("DependsOn") && status == EUsedColumnStatus::ONLY_EXTRA) {
                        status = EUsedColumnStatus::ALL;
                    }
                    break;
                }
            }

            YQL_ENSURE(nodeColumnStatuses.insert({ node.Get(), status}).second);

            auto it = booleanSubTree.find(node.Get());
            if (it != booleanSubTree.end()) {
                auto boolStatus = IsStdBooleanOperator(node);
                YQL_ENSURE(boolStatus && *boolStatus);
                YQL_ENSURE(!it->second);
                TExprNodeList newChildren;
                for (auto& child : node->ChildrenList()) {
                    TExprNode::TPtr newChild;
                    auto childIt = booleanSubTree.find(child.Get());
                    if (childIt != booleanSubTree.end()) {
                        newChild = childIt->second;
                        YQL_ENSURE(newChild);
                    } else {
                        auto statusIt = nodeColumnStatuses.find(child.Get());
                        YQL_ENSURE(statusIt != nodeColumnStatuses.end());
                        newChild = statusIt->second == EUsedColumnStatus::ALL ? MakeBoolNothing(child->Pos(), ctx) : child;
                    }
                    newChildren.emplace_back(std::move(newChild));
                }
                it->second = ctx.ChangeChildren(*node, std::move(newChildren));
            }
            return true;
        });

        if (!validExpr) {
            return filter.Ptr();
        }

        TExprNode::TPtr pruningPredicate;
        if (auto rootIt = booleanSubTree.find(predicate.Get()); rootIt != booleanSubTree.end()) {
            pruningPredicate = rootIt->second;
        } else {
            auto statusIt = nodeColumnStatuses.find(predicate.Get());
            YQL_ENSURE(statusIt != nodeColumnStatuses.end());
            pruningPredicate = statusIt->second == EUsedColumnStatus::ALL ? MakeBoolNothing(predicate->Pos(), ctx) : predicate;
        }
        YQL_ENSURE(pruningPredicate);
        if (pruningPredicate->IsCallable("Nothing")) {
            return filter.Ptr();
        }
        pruningPredicate = ctx.NewCallable(pruningPredicate->Pos(), "Coalesce", { pruningPredicate, MakeBool(pruningPredicate->Pos(), true, ctx) });

        TExprNode::TPtr pruningLambda = ctx.ChangeChild(filter.Lambda().Ref(), TCoLambda::idx_Body, std::move(pruningPredicate));

        auto prunedPartList = ctx.Builder(partListNode.Pos())
            .Callable("Filter")
                .Add(0, partListNode.HeadPtr())
                .Lambda(1)
                    .Param("item")
                    .Apply(pruningLambda)
                        .With(0, "item")
                    .Seal()
                .Seal()
            .Seal()
            .Build();


        auto newPartList = ctx.Builder(partListNode.Pos())
            .Callable("EvaluateExpr")
                .Callable(0, "IfStrict")
                    .Callable(0, "==")
                        .Callable(0, "Length")
                            .Add(0, prunedPartList)
                        .Seal()
                        .Callable(1, "Uint64")
                            .Atom(0, 0)
                        .Seal()
                    .Seal()
                    .Callable(1, "Top")
                        .Add(0, partListNode.HeadPtr())
                        .Callable(1, "Uint64")
                            .Atom(0, 1)
                        .Seal()
                        .Callable(2, "Bool")
                            .Atom(0, "false")
                        .Seal()
                        .Lambda(3)
                            .Param("entry")
                            .Callable("Member")
                                .Arg(0, "entry")
                                .Atom(1, MrPartitionListTableMember)
                            .Seal()
                        .Seal()
                    .Seal()
                    .Add(2, prunedPartList)
                .Seal()
            .Seal()
            .Build();

        auto newPartitions = ctx.ChangeChild(partListNode, 0, std::move(newPartList));

        YQL_CLOG(INFO, ProviderYt) << "Pruning " << partListNode.Content() << " with " << partListNode.Head().ChildrenSize() << " tables";

        auto newRead = read.Ptr();
        newRead->ChildRef(2) = std::move(newPartitions);
        newRead->ChildRef(4) = NYql::AddSetting(read.Arg(4).Ref(), EYtSettingType::Pruned, {}, ctx);
        newRead = ctx.NewCallable(newRead->Pos(), "Right!", { newRead });

        return ctx.ChangeChild(filter.Ref(), TCoFilterBase::idx_Input, std::move(newRead));
    }

    static TExprNode::TPtr ExpandPartitionList(TYtRead read, TExprContext& ctx) {
        const auto& partListNode = read.Arg(2).Ref();
        if (!ValidatePartitionList(partListNode, ctx)) {
            return {};
        }

        const auto& partList = partListNode.Head();
        TMaybe<TStringBuf> tablePrefix;
        if (partListNode.ChildrenSize() == 2) {
            tablePrefix = partListNode.Tail().Content();
        }
        TExprNodeList keys;
        for (auto entry : partList.ChildrenList()) {
            YQL_ENSURE(entry->IsCallable({"AsStruct", "Struct"}));
            TExprNodeList newChildren;
            TMaybe<TStringBuf> tablePath;
            TMaybe<TStringBuf> tableView;
            for (ui32 i = entry->IsCallable("Struct") ? 1 : 0; i < entry->ChildrenSize(); ++i) {
                auto structItem = entry->ChildPtr(i);
                if (structItem->Head().Content() == MrPartitionListTableMember) {
                    YQL_ENSURE(structItem->Tail().IsCallable({"String", "Utf8"}));
                    tablePath = structItem->Tail().Head().Content();
                } else if (structItem->Head().Content() == MrPartitionListViewMember) {
                    auto value = structItem->Child(1);
                    if (value->IsCallable("Nothing")) {
                        continue;
                    }
                    if (value->IsCallable("Just")) {
                        value = value->Child(0);
                    }
                    YQL_ENSURE(value->IsCallable({"String", "Utf8"}));
                    tableView = value->Head().Content();
                } else {
                    newChildren.emplace_back(structItem);
                }
            }
            YQL_ENSURE(tablePath.Defined());

            TExprNodeList keyArgs;
            keyArgs.push_back(ctx.Builder(entry->Pos())
                .List()
                    .Atom(0, "table")
                    .Callable(1, "String")
                        .Atom(0, tablePrefix.Defined() ? BuildTablePath(*tablePrefix, *tablePath) : *tablePath)
                    .Seal()
                .Seal()
                .Build());
            if (tableView.Defined()) {
                keyArgs.push_back(ctx.Builder(entry->Pos())
                    .List()
                        .Atom(0, "view")
                        .Callable(1, "String")
                            .Atom(0, *tableView)
                        .Seal()
                    .Seal()
                    .Build());
            }
            if (!newChildren.empty()) {
                keyArgs.push_back(ctx.Builder(entry->Pos())
                    .List()
                        .Atom(0, ToString(EYtSettingType::ExtraColumns))
                        .Add(1, ctx.NewCallable(entry->Pos(), "AsStruct", std::move(newChildren)))
                    .Seal()
                    .Build());
            }
            keys.push_back(ctx.NewCallable(entry->Pos(), "Key", std::move(keyArgs)));
        }
        YQL_CLOG(INFO, ProviderYt) << "Expanding " << partListNode.Content() << " with " << keys.size() << " tables";

        auto settings = read.Arg(4).Ptr();
        settings = NYql::RemoveSetting(*settings, EYtSettingType::Pruned, ctx);

        auto res = read.Ptr();
        res->ChildRef(2) = partListNode.IsCallable(MrPartitionListName) ?
            ctx.NewCallable(partListNode.Pos(), MrTableConcatName, std::move(keys)) :
            ctx.NewList(partListNode.Pos(), std::move(keys));
        res->ChildRef(4) = settings;

        return res;
    }

    TExprNode::TPtr ConvertTableScheme(TYtRead read, const TYtKey& key, TExprContext& ctx) {
        if (!read.Arg(3).Ref().IsCallable(TCoVoid::CallableName())) {
            ctx.AddError(TIssue(ctx.GetPosition(read.Arg(3).Pos()), TStringBuilder()
                << "Expected Void, but got: " << read.Arg(3).Ref().Content()));
            return {};
        }

        if (!EnsureTuple(*read.Raw()->Child(4), ctx)) {
            return {};
        }

        TYtTableInfo tableInfo(key, read.DataSource().Cluster().Value());

        auto settings = NYql::RemoveSetting(read.Arg(4).Ref(), EYtSettingType::DoNotFailOnInvalidSchema, ctx);
        if (key.GetView()) {
            settings = NYql::AddSetting(*settings, EYtSettingType::View, ctx.NewAtom(key.GetNode()->Pos(), key.GetView()), ctx);
        }
        tableInfo.Settings = TExprBase(settings);

        auto res = read.Ptr();
        res->ChildRef(2) = Build<TExprList>(ctx, read.Pos())
            .Add<TYtPath>()
                .Table(tableInfo.ToExprNode(ctx, read.Pos()).Cast<TYtTable>())
                .Columns<TCoVoid>().Build()
                .Ranges<TCoVoid>().Build()
                .Stat<TCoVoid>().Build()
            .Build()
            .Done().Ptr();
        res->ChildRef(4) = Build<TCoNameValueTupleList>(ctx, read.Pos())
            .Add()
                .Name().Value(ToString(EYtSettingType::Scheme)).Build()
            .Build()
            .Done().Ptr();
        return res;
    }

    void SplitReadSettings(TYtRead read, TVector<TExprBase>& tableSettings, TVector<TExprBase>& readSettings, TExprContext& ctx) {
        if (auto list = read.Arg(4).Maybe<TCoNameValueTupleList>()) {
            for (auto setting: list.Cast()) {
                auto type = FromString<EYtSettingType>(setting.Name().Value());
                if (ToString(type) != setting.Name().Value()) {
                    // Normalize setting name
                    setting = Build<TCoNameValueTuple>(ctx, setting.Pos())
                        .InitFrom(setting)
                        .Name()
                            .Value(ToString(type))
                        .Build()
                        .Done();
                }
                if (type & (EYtSettingType::InferScheme | EYtSettingType::ForceInferScheme |
                    EYtSettingType::DoNotFailOnInvalidSchema | EYtSettingType::XLock |
                    EYtSettingType::UserSchema | EYtSettingType::UserColumns | EYtSettingType::IgnoreTypeV3 |
                    EYtSettingType::ExtraColumns))
                {
                    tableSettings.push_back(setting);
                } else {
                    readSettings.push_back(setting);
                }
            }
        }
    }

    TExprNode::TPtr ConvertTableRead(TYtRead read, const TYtInputKeys& keys, TExprContext& ctx) {
        TVector<TExprBase> tableSettings;
        TVector<TExprBase> readSettings;
        SplitReadSettings(read, tableSettings, readSettings, ctx);

        auto cluster = read.DataSource().Cluster().Value();
        TVector<TExprBase> tables;
        for (auto& key: keys.GetKeys()) {
            TYtTableInfo table(key, cluster);
            auto settingsBuilder = Build<TCoNameValueTupleList>(ctx, read.Pos()).Add(tableSettings);
            if (key.GetView()) {
                settingsBuilder
                    .Add()
                        .Name().Value(ToString(EYtSettingType::View)).Build()
                        .Value<TCoAtom>().Value(key.GetView()).Build()
                    .Build();
            }
            if (key.IsAnonymous()) {
                settingsBuilder
                    .Add()
                        .Name().Value(ToString(EYtSettingType::Anonymous)).Build()
                    .Build();
            }
            table.Settings = settingsBuilder.Done();
            auto path = Build<TYtPath>(ctx, read.Pos())
                .Table(table.ToExprNode(ctx, read.Pos()).Cast<TYtTable>())
                .Columns<TCoVoid>().Build()
                .Ranges<TCoVoid>().Build()
                .Stat<TCoVoid>().Build()
                .Done();
            tables.push_back(path);
        }
        if (!keys.GetStrictConcat() && keys.GetKeys().size() > 1) {
            readSettings.push_back(Build<TCoNameValueTuple>(ctx, read.Pos()).Name().Value(ToString(EYtSettingType::WeakConcat)).Build().Done());
        }

        auto res = read.Ptr();
        res->ChildRef(2) = Build<TExprList>(ctx, read.Pos()).Add(tables).Done().Ptr();
        res->ChildRef(4) = Build<TCoNameValueTupleList>(ctx, read.Pos()).Add(readSettings).Done().Ptr();
        return res;
    }

    [[nodiscard]]
    TExprNode::TPtr InitializeWalkFolders(TYtKey&& key, const TString& cluster, TPosition pos, TExprContext& ctx) {
        auto& args = key.GetWalkFolderArgs().GetRef();

        TWalkFoldersImpl walkFolders {State_->SessionId, cluster, State_->Configuration->Snapshot(),
                         pos, args, State_->Gateway};
        YQL_CLOG(INFO, ProviderYt) << "Initialized WalkFolders from " << cluster << ".`"
            << args.InitialFolder.Prefix << "`" << " with root attributes cnt: "
            << args.InitialFolder.Attributes.size();
        const auto instanceKey = ctx.NextUniqueId;
        State_->WalkFoldersState.emplace(instanceKey, std::move(walkFolders));
        PendingWalkFoldersKeys_.insert(instanceKey);

        auto walkFoldersImplNode = Build<TYtWalkFoldersImpl>(ctx, key.GetNode()->Pos())
            .ProcessStateKey()
                .Value(instanceKey)
            .Build()
            .PickledUserState(args.PickledUserState)
            .UserStateType(args.UserStateType)
        .Build()
        .Value()
        .Ptr();

        return walkFoldersImplNode;
    }

    TStatus RewriteWalkFoldersOnAsyncOrEvalChanges(TExprNode::TPtr& output, TExprContext& ctx) {
        TStatus walkFoldersStatus = IGraphTransformer::TStatus::Ok;

        auto status = VisitInputKeys(output, ctx, [this, &ctx, &walkFoldersStatus] (TYtRead readNode, TYtInputKeys&& keys) -> TExprNode::TPtr {
            if (keys.GetType() == TYtKey::EType::WalkFoldersImpl) {
                YQL_CLOG(INFO, ProviderYt) << "YtIODiscovery - DoApplyAsyncChanges WalkFoldersImpl handling start";

                auto parsedKey = keys.ExtractKeys().front();
                if (!parsedKey.GetWalkFolderImplArgs()) {
                    YQL_CLOG(ERROR, ProviderYt) << "Failed to parse WalkFolderImpl args";
                    return {};
                }
                const ui64 instanceKey = parsedKey.GetWalkFolderImplArgs()->StateKey;
                if (*PendingWalkFoldersKeys_.begin() != instanceKey) {
                    return readNode.Ptr();
                }

                auto walkFoldersInstanceIt = this->State_->WalkFoldersState.find(instanceKey);
                YQL_ENSURE(!walkFoldersInstanceIt.IsEnd());
                auto& walkFoldersImpl = walkFoldersInstanceIt->second;

                Y_ENSURE(walkFoldersImpl.GetAnyOpFuture().HasValue(),
                    "Called RewriteWalkFoldersOnAsyncChanges, but impl future is not ready");

                auto nextState = parsedKey.GetWalkFolderImplArgs()->UserStateExpr;
                walkFoldersStatus = walkFoldersImpl.GetNextStateExpr(ctx, std::move(parsedKey.GetWalkFolderImplArgs().GetRef()), nextState);

                if (walkFoldersStatus == TStatus::Error) {
                    return {};
                }

                if (walkFoldersImpl.IsFinished()) {
                    YQL_CLOG(INFO, ProviderYt) << "Building result expr for WalkFolders with key: " << instanceKey;
                    this->State_->WalkFoldersState.erase(instanceKey);
                    PendingWalkFoldersKeys_.erase(instanceKey);

                    auto type = Build<TCoStructType>(ctx, readNode.Pos())
                        .Add<TExprList>()
                            .Add<TCoAtom>()
                                .Value("State")
                            .Build()
                            .Add(parsedKey.GetWalkFolderImplArgs()->UserStateType)
                        .Build()
                    .DoBuild();

                    auto resList = Build<TCoList>(ctx, readNode.Pos())
                        .ListType<TCoListType>()
                            .ItemType<TCoStructType>()
                                .InitFrom(type)
                            .Build()
                        .Build()
                        .FreeArgs()
                            .Add<TCoAsStruct>()
                                .Add()
                                    .Add<TCoAtom>()
                                        .Value("State")
                                    .Build()
                                    .Add(nextState)
                                .Build()
                            .Build()
                        .Build()
                    .DoBuild();

                    return Build<TCoCons>(ctx, readNode.Pos())
                        .World(readNode.World())
                        .Input<TCoAssumeColumnOrder>()
                            .Input(resList)
                        .ColumnOrder<TCoAtomList>()
                                .Add()
                                    .Value("State")
                                .Build()
                            .Build()
                        .Build()
                    .Done()
                    .Ptr();
                }

                if (nextState == parsedKey.GetWalkFolderImplArgs()->UserStateExpr) {
                    return readNode.Ptr();
                }

                YQL_CLOG(TRACE, ProviderYt) << "State expr ast: " << ConvertToAst(*nextState, ctx, {}).Root->ToString();

                auto walkFoldersImplNode = ctx.ChangeChild(*parsedKey.GetNode(), 0, std::move(nextState));
                return ctx.ChangeChild(readNode.Ref(), 2, std::move(walkFoldersImplNode));
            }
            return readNode.Ptr();
        });

        if (status == TStatus::Error) {
            YQL_CLOG(ERROR, ProviderYt) << "WalkFolders error transforming";
            return status;
        }

        YQL_CLOG(INFO, ProviderYt) << "WalkFolders next status: " << walkFoldersStatus;
        return walkFoldersStatus;
    }

    IGraphTransformer::TStatus VisitInputKeys(TExprNode::TPtr& output,
        TExprContext& ctx, std::function<TExprNode::TPtr(TYtRead node, TYtInputKeys&&)> processKeys, bool visitChanges = false) {
        TOptimizeExprSettings settings(nullptr);
        settings.VisitChanges = visitChanges;

        const auto status = OptimizeExpr(output, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
            if (auto maybeRead = TMaybeNode<TYtRead>(node)) {
                if (!maybeRead.DataSource()) { // Validates provider
                    return node;
                }
                auto read = maybeRead.Cast();
                auto ds = read.DataSource();
                if (!EnsureArgsCount(read.Ref(), 5, ctx)) {
                    return {};
                }

                TYtInputKeys keys;
                auto& keysNode = read.Arg(2).Ref();
                if (!keys.Parse(keysNode, ctx)) {
                    return {};
                }

                if (keys.IsProcessed()) {
                    // Already processed
                    return node;
                }

                if (keys.GetKeys().empty()) {
                    ctx.AddError(TIssue(ctx.GetPosition(read.Arg(2).Pos()), "The list of tables is empty"));
                    return {};
                }
                return processKeys(read, std::move(keys));
            }
            return node;
        }, ctx, settings);
        return status;
    }

    TCoCons BuildFolderTableResExpr(TExprContext& ctx, NYql::TPositionHandle pos, const TExprBase& world, const TExprNodePtr& folderList) {
        return Build<TCoCons>(ctx, pos)
            .World(world)
            .Input<TCoAssumeColumnOrder>()
                .Input(folderList)
            .ColumnOrder<TCoAtomList>()
                    .Add()
                        .Value("Path")
                    .Build()
                    .Add()
                        .Value("Type")
                    .Build()
                    .Add()
                        .Value("Attributes")
                    .Build()
                .Build()
            .Build()
            .Done();
    }

    TMaybe<NYql::IYtGateway::TFolderResult> FetchFolderResult(TExprContext& ctx, const TString& cluster, const TYtKey::TFolderList& folder) {
        auto p = PendingFolders_.FindPtr(std::make_pair(cluster, folder));
        YQL_ENSURE(p);
        auto res = p->second.GetValue();
        res.ReportIssues(ctx.IssueManager);
        if (!res.Success()) {
            return {};
        }
        return res;
    }

    TWalkFoldersImpl& GetCurrentWalkFoldersInstance() const {
        Y_ENSURE(!PendingWalkFoldersKeys_.empty());
        const auto key = PendingWalkFoldersKeys_.begin();
        auto stateIt = State_->WalkFoldersState.find(*key);
        YQL_ENSURE(stateIt != State_->WalkFoldersState.end());
        return stateIt->second;
    }

    TMaybe<NThreading::TFuture<void>> MaybeGetWalkFoldersFuture() const {
        // inflight 1
        if (!PendingWalkFoldersKeys_.empty()) {
            return GetCurrentWalkFoldersInstance().GetAnyOpFuture();
        }
        return Nothing();
    }

private:
    TYtState::TPtr State_;

    THashMap<std::pair<TString, TYtKey::TRange>, std::pair<TPosition, NThreading::TFuture<IYtGateway::TTableRangeResult>>> PendingRanges_;
    THashMap<std::pair<TString, TYtKey::TFolderList>, std::pair<TPosition, NThreading::TFuture<IYtGateway::TFolderResult>>> PendingFolders_;
    THashMap<std::pair<TString, TString>, size_t> PendingCanonizations_; // cluster, original table path -> positions in canon result
    TSet<ui64> PendingWalkFoldersKeys_;
    NThreading::TFuture<IYtGateway::TCanonizePathsResult> CanonizeFuture_;
    NThreading::TFuture<void> CanonizationRangesFoldersFuture_;

    THashMap<TString, TString> FolderFileToAlias_;
};

THolder<IGraphTransformer> CreateYtIODiscoveryTransformer(TYtState::TPtr state) {
    return THolder(new TYtIODiscoveryTransformer(state));
}

}
