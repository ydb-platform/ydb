#include "yql_yt_provider_impl.h"
#include "yql_yt_key.h"
#include "yql_yt_gateway.h"
#include "yql_yt_op_settings.h"
#include "yql_yt_helpers.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_expr_constraint.h>
#include <ydb/library/yql/core/type_ann/type_ann_core.h>
#include <ydb/library/yql/core/type_ann/type_ann_expr.h>
#include <ydb/library/yql/core/peephole_opt/yql_opt_peephole_physical.h>
#include <ydb/library/yql/core/issue/protos/issue_id.pb.h>
#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/utils/log/log.h>

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
        settings.VisitChanges = true;
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
                } else if (tableInfo.Name.StartsWith("//")) {
                    tableInfo.Name = tableInfo.Name.substr(2);
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
            return status;
        }

        status = OptimizeExpr(output, output, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
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
                if (!keys.Parse(read.Arg(2).Ref(), ctx)) {
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

                if (keys.GetType() != TYtKey::EType::TableScheme) {
                    auto cluster = TString{ds.Cluster().Value()};
                    for (auto& key: keys.GetKeys()) {
                        auto keyPos = ctx.GetPosition(key.GetNode()->Pos());
                        if (key.GetRange()) {
                            PendingRanges_.emplace(std::make_pair(cluster, *key.GetRange()), std::make_pair(keyPos, NThreading::TFuture<IYtGateway::TTableRangeResult>()));
                        }
                        else if (key.GetFolder()) {
                            PendingFolders_.emplace(std::make_pair(cluster, *key.GetFolder()), std::make_pair(keyPos, NThreading::TFuture<IYtGateway::TFolderResult>()));
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
                return node;
            }

            return node;
        }, ctx, TOptimizeExprSettings(nullptr));

        if (status.Level != TStatus::Ok) {
            PendingCanonizations_.clear();
            PendingFolders_.clear();
            PendingRanges_.clear();
            return status;
        }

        if (PendingRanges_.empty() && PendingFolders_.empty() && PendingCanonizations_.empty()) {
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

        AllFuture_ = NThreading::WaitExceptionOrAll(allFutures);
        return TStatus::Async;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
        Y_UNUSED(input);
        return AllFuture_;
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;

        if (!PendingCanonizations_.empty()) {
            auto& res = CanonizeFuture_.GetValue();
            res.ReportIssues(ctx.IssueManager);

            if (!res.Success()) {
                PendingCanonizations_.clear();
                PendingRanges_.clear();
                CanonizeFuture_ = {};
                AllFuture_ = {};

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
                    return node;
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
                    listItems.push_back(BuildFolderListItemExpr(ctx, node->Pos(), item));
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
        AllFuture_ = {};

        return status;
    }

    void Rewind() final {
        PendingRanges_.clear();
        PendingFolders_.clear();
        PendingCanonizations_.clear();
        CanonizeFuture_ = {};
        AllFuture_ = {};
    }

private:
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
                    EYtSettingType::UserSchema | EYtSettingType::UserColumns | EYtSettingType::IgnoreTypeV3)) {
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

    TExprBase BuildFolderListItemExpr(TExprContext& ctx, NYql::TPositionHandle pos, const IYtGateway::TFolderResult::TFolderItem& folderItem)  {
        return Build<TCoAsStruct>(ctx, pos)
            .Add()
                .Add<TCoAtom>()
                    .Value("Path")
                .Build()
                .Add<TCoString>()
                    .Literal()
                        .Value(folderItem.Path)
                    .Build()
                .Build()
            .Build()
            .Add()
                .Add<TCoAtom>()
                    .Value("Type")
                .Build()
                .Add<TCoString>()
                    .Literal()
                        .Value(folderItem.Type)
                    .Build()
                .Build()
            .Build()
            .Add()
                .Add<TCoAtom>()
                    .Value("Attributes")
                .Build()
                .Add<TCoYson>()
                    .Literal()
                        .Value(folderItem.Attributes)
                    .Build()
                .Build()
            .Build()
            .Done();
    }

    TCoList BuildFolderListExpr(TExprContext& ctx, NYql::TPositionHandle pos, const TVector<TExprBase>& folderItems) {
        return Build<TCoList>(ctx, pos)
            .ListType<TCoListType>()
                .ItemType<TCoStructType>()
                    .Add<TExprList>()
                        .Add<TCoAtom>()
                            .Value("Path")
                        .Build()
                        .Add<TCoDataType>()
                            .Type()
                                .Value("String")
                            .Build()
                        .Build()
                    .Build()
                    .Add<TExprList>()
                        .Add<TCoAtom>()
                            .Value("Type")
                        .Build()
                        .Add<TCoDataType>()
                            .Type()
                                .Value("String")
                            .Build()
                        .Build()
                    .Build()
                    .Add<TExprList>()
                        .Add<TCoAtom>()
                            .Value("Attributes")
                        .Build()
                        .Add<TCoDataType>()
                            .Type()
                                .Value("Yson")
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
            .FreeArgs()
                .Add(folderItems)
            .Build()
        .Build()
        .Value();
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

private:
    TYtState::TPtr State_;

    THashMap<std::pair<TString, TYtKey::TRange>, std::pair<TPosition, NThreading::TFuture<IYtGateway::TTableRangeResult>>> PendingRanges_;
    THashMap<std::pair<TString, TYtKey::TFolderList>, std::pair<TPosition, NThreading::TFuture<IYtGateway::TFolderResult>>> PendingFolders_;
    THashMap<std::pair<TString, TString>, size_t> PendingCanonizations_; // cluster, original table path -> positions in canon result
    NThreading::TFuture<IYtGateway::TCanonizePathsResult> CanonizeFuture_;
    NThreading::TFuture<void> AllFuture_;

    THashMap<TString, TString> FolderFileToAlias_;
};

THolder<IGraphTransformer> CreateYtIODiscoveryTransformer(TYtState::TPtr state) {
    return THolder(new TYtIODiscoveryTransformer(state));
}

}