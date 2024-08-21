#include "yql_s3_provider_impl.h"
#include "yql_s3_listing_strategy.h"

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>
#include <ydb/library/yql/providers/s3/object_listers/yql_s3_list.h>
#include <ydb/library/yql/providers/s3/object_listers/yql_s3_path.h>
#include <ydb/library/yql/providers/s3/path_generator/yql_s3_path_generator.h>
#include <ydb/library/yql/providers/s3/range_helpers/path_list_reader.h>
#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/url_builder.h>

#include <util/generic/size_literals.h>
#include <util/string/join.h>

namespace NYql {

namespace {

using namespace NNodes;

std::array<TExprNode::TPtr, 2U> ExtractSchema(TExprNode::TListType& settings) {
    for (auto it = settings.cbegin(); settings.cend() != it; ++it) {
        if (const auto item = *it; item->Head().IsAtom("userschema")) {
            settings.erase(it);
            return {item->ChildPtr(1), item->ChildrenSize() > 2 ? item->TailPtr() : TExprNode::TPtr()};
        }
    }

    return {};
}

bool FindFilePattern(const TExprNode& settings, TExprContext& ctx, TString& filePattern) {
    auto filePatternSetting = GetSetting(settings, "filepattern");
    if (!filePatternSetting) {
        // it is ok if settings is not set
        return true;
    }

    if (!EnsureTupleSize(*filePatternSetting, 2, ctx)) {
        return false;
    }

    if (!EnsureAtom(filePatternSetting->Tail(), ctx)) {
        return false;
    }

    filePattern = filePatternSetting->Tail().Content();
    return true;
}

using namespace NPathGenerator;

struct TListRequest {
    NS3Lister::TListingRequest S3Request;
    TString FilePattern;
    TS3ListingOptions Options;
    TVector<IPathGenerator::TColumnWithValue> ColumnValues;
};

bool operator<(const TListRequest& a, const TListRequest& b) {
    return std::tie(a.S3Request.Credentials, a.S3Request.Url, a.S3Request.Pattern) <
           std::tie(b.S3Request.Credentials, b.S3Request.Url, b.S3Request.Pattern);
}

using TPendingRequests = TMap<TListRequest, NThreading::TFuture<NS3Lister::TListResult>>;

struct TGeneratedColumnsConfig {
    TVector<TString> Columns;
    TPathGeneratorPtr Generator;
    TExprNode::TPtr SchemaTypeNode;
};

class TS3IODiscoveryTransformer : public TGraphTransformerBase {
public:
    TS3IODiscoveryTransformer(TS3State::TPtr state)
        : State_(std::move(state))
        , ListerFactory_(NS3Lister::MakeS3ListerFactory(
              State_->Configuration->MaxInflightListsPerQuery,
              State_->Configuration->ListingCallbackThreadCount,
              State_->Configuration->ListingCallbackPerThreadQueueSize,
              State_->Configuration->RegexpCacheSize,
              State_->ActorSystem))
        , ListingStrategy_(MakeS3ListingStrategy(
              State_->Gateway,
              State_->GatewayRetryPolicy,
              ListerFactory_,
              State_->Configuration->MinDesiredDirectoriesOfFilesPerQuery,
              State_->Configuration->MaxInflightListsPerQuery,
              State_->Configuration->AllowLocalFiles)) {
    }

    void Rewind() final {
        PendingRequests_.clear();
        RequestsByNode_.clear();
        GenColumnsByNode_.clear();
        AllFuture_ = {};
    }

private:
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;
        if (ctx.Step.IsDone(TExprStep::DiscoveryIO)) {
            return TStatus::Ok;
        }

        auto nodes = FindNodes(input, [&](const TExprNode::TPtr& node) {
            if (const auto maybeRead = TMaybeNode<TS3Read>(node)) {
                if (maybeRead.DataSource()) {
                    return maybeRead.Cast().Arg(2).Ref().IsCallable("MrTableConcat");
                }
            } else if (const auto maybeDqSource = TMaybeNode<TDqSourceWrap>(node)) {
                auto dqSource = maybeDqSource.Cast();
                if (dqSource.DataSource().Category() != S3ProviderName) {
                    return false;
                }
                auto maybeS3ParseSettings = dqSource.Input().Maybe<TS3ParseSettings>();
                if (!maybeS3ParseSettings) {
                    return false;
                }

                auto inner = maybeS3ParseSettings.Cast().Settings();
                return inner && HasSetting(inner.Cast().Ref(), "directories");
            }
            return false;
        });

        TVector<NThreading::TFuture<NS3Lister::TListResult>> futures;
        for (const auto& n : nodes) {
            try {
                if (auto maybeDqSource = TMaybeNode<TDqSourceWrap>(n)) {
                    const TDqSourceWrap source(n);
                    if (!LaunchListsForNode(source, futures, ctx)) {
                        return TStatus::Error;
                    }
                } else {
                    const TS3Read read(n);
                    if (!LaunchListsForNode(read, futures, ctx)) {
                        return TStatus::Error;
                    }
                }
            } catch (const std::exception& ex) {
                ctx.AddError(TIssue(ctx.GetPosition(n->Pos()), TStringBuilder() << "Error while doing S3 discovery: " << ex.what()));
                return TStatus::Error;
            }
        }

        if (futures.empty()) {
            return TStatus::Ok;
        }

        AllFuture_ = NThreading::WaitExceptionOrAll(futures);
        return TStatus::Async;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode&) final {
        return AllFuture_;
    }

    TStatus ApplyDirectoryListing(const TDqSourceWrap& source, const TPendingRequests& pendingRequests,
        const TVector<TListRequest>& requests, TNodeOnNodeOwnedMap& replaces, TExprContext& ctx)
    {
        TS3ParseSettings parse = source.Input().Maybe<TS3ParseSettings>().Cast();
        TExprNodeList newPaths;
        TExprNodeList extraValuesItems;
        size_t dirIndex = 0;
        for (auto generatedPathsPack : parse.Paths()) {
            NS3Details::TPathList directories;
            NS3Details::UnpackPathsList(
                generatedPathsPack.Data().Literal().Value(),
                FromString<bool>(generatedPathsPack.IsText().Literal().Value()),
                directories);

            YQL_ENSURE(dirIndex + directories.size() <= requests.size());
            NS3Details::TPathList listedPaths;
            for (size_t i = 0; i < directories.size(); ++i) {
                const auto& req = requests[dirIndex + i];

                auto it = pendingRequests.find(req);
                YQL_ENSURE(it != pendingRequests.end());
                YQL_ENSURE(it->second.HasValue());

                const auto& listResult = it->second.GetValue();
                if (listResult.index() == 1) {
                    const auto& error = std::get<NS3Lister::TListError>(listResult);
                    YQL_CLOG(INFO, ProviderS3)
                        << "Discovery " << req.S3Request.Url << req.S3Request.Pattern
                        << " error " << error.Issues.ToString();
                    std::for_each(
                        error.Issues.begin(),
                        error.Issues.end(),
                        std::bind(
                            &TExprContext::AddError, std::ref(ctx), std::placeholders::_1));
                    return TStatus::Error;
                }

                const auto& listEntries = std::get<NS3Lister::TListEntries>(listResult);

                for (auto& entry : listEntries.Objects) {
                    listedPaths.emplace_back(entry.Path, entry.Size, false, dirIndex + i);
                }
                for (auto& path : listEntries.Directories) {
                    listedPaths.emplace_back(path.Path, 0, true, dirIndex + i);
                }
            }

            dirIndex += directories.size();
            if (listedPaths.empty()) {
                continue;
            }

            TString packedPaths;
            bool isTextEncoded;
            NS3Details::PackPathsList(listedPaths, packedPaths, isTextEncoded);

            newPaths.emplace_back(
                Build<TS3Path>(ctx, generatedPathsPack.Pos())
                    .Data<TCoString>()
                        .Literal()
                        .Build(packedPaths)
                    .Build()
                    .IsText<TCoBool>()
                        .Literal()
                        .Build(ToString(isTextEncoded))
                    .Build()
                    .ExtraColumns(generatedPathsPack.ExtraColumns())
                    .Done().Ptr()
            );

            extraValuesItems.emplace_back(
                ctx.Builder(generatedPathsPack.ExtraColumns().Pos())
                    .Callable("Replicate")
                        .Add(0, generatedPathsPack.ExtraColumns().Ptr())
                        .Callable(1, "Uint64")
                            .Atom(0, ToString(listedPaths.size()), TNodeFlags::Default)
                        .Seal()
                    .Seal()
                    .Build()
            );
        }

        YQL_ENSURE(dirIndex == requests.size());

        if (newPaths.empty()) {
            replaces.emplace(source.Raw(),
                ctx.Builder(source.Pos())
                    .Callable("List")
                        .Callable(0, "ListType")
                            .Add(0, source.RowType().Ptr())
                        .Seal()
                    .Seal()
                    .Build()
            );
            return TStatus::Ok;
        }

        auto newExtraValues = ctx.NewCallable(source.Pos(), "OrderedExtend", std::move(extraValuesItems));
        auto newInput = Build<TS3ParseSettings>(ctx, parse.Pos())
            .InitFrom(parse)
            .Paths(ctx.NewList(parse.Paths().Pos(), std::move(newPaths)))
            .Settings(RemoveSetting(parse.Settings().Cast().Ref(), "directories", ctx))
            .Done();

        YQL_ENSURE(source.Settings());
        replaces.emplace(source.Raw(),
            Build<TDqSourceWrap>(ctx, source.Pos())
                .InitFrom(source)
                .Input(newInput)
                .Settings(ReplaceSetting(source.Settings().Cast().Ref(), source.Pos(), "extraColumns", newExtraValues, ctx))
                .Done().Ptr()
        );
        return TStatus::Ok;
    }

    struct TExtraColumnValue {
        TString Name;
        TMaybe<NUdf::EDataSlot> Type;
        TString Value;
        bool operator<(const TExtraColumnValue& other) const {
            return std::tie(Name, Type, Value) < std::tie(other.Name, other.Type, other.Value);
        }
    };

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        // Raise errors if any
        AllFuture_.GetValue();

        TPendingRequests pendingRequests;
        TNodeMap<TVector<TListRequest>> requestsByNode;
        TNodeMap<TGeneratedColumnsConfig> genColumnsByNode;

        pendingRequests.swap(PendingRequests_);
        requestsByNode.swap(RequestsByNode_);
        genColumnsByNode.swap(GenColumnsByNode_);

        TNodeOnNodeOwnedMap replaces;
        for (auto& [node, requests] : requestsByNode) {
            if (auto maybeSource = TMaybeNode<TDqSourceWrap>(node)) {
                auto status = ApplyDirectoryListing(maybeSource.Cast(), pendingRequests, requests, replaces, ctx);
                if (status != TStatus::Ok) {
                    return status;
                }
                continue;
            }
            const TS3Read read(node);
            const auto& object = read.Arg(2).Ref();
            YQL_ENSURE(object.IsCallable("MrTableConcat"));
            size_t readSize = 0;
            TExprNode::TListType pathNodes;

            TStringBuilder path;
            object.ForEachChild([&path](const TExprNode& child){ path << child.Head().Tail().Head().Content() << " "; });

            TMap<TMaybe<TVector<TExtraColumnValue>>, NS3Details::TPathList> pathsByExtraValues;
            const TGeneratedColumnsConfig* generatedColumnsConfig = nullptr;
            if (auto it = genColumnsByNode.find(node); it != genColumnsByNode.end()) {
                generatedColumnsConfig = &it->second;
            }

            const bool assumeDirectories = generatedColumnsConfig && generatedColumnsConfig->Generator;
            bool needsListingOnActors = false;
            for (auto& req : requests) {
                auto it = pendingRequests.find(req);
                YQL_ENSURE(it != pendingRequests.end());
                YQL_ENSURE(it->second.HasValue());

                const NS3Lister::TListResult& listResult = it->second.GetValue();
                if (listResult.index() == 1) {
                    const auto& error = std::get<NS3Lister::TListError>(listResult);
                    YQL_CLOG(INFO, ProviderS3)
                        << "Discovery " << req.S3Request.Url << req.S3Request.Pattern
                        << " error " << error.Issues.ToString();
                    std::for_each(
                        error.Issues.begin(),
                        error.Issues.end(),
                        std::bind(
                            &TExprContext::AddError, std::ref(ctx), std::placeholders::_1));
                    return TStatus::Error;
                }

                const auto& listEntries = std::get<NS3Lister::TListEntries>(listResult);
                if (listEntries.Size() == 0 && !NS3::HasWildcards(req.S3Request.Pattern)) {
                    // request to list particular files that are missing
                    ctx.AddError(TIssue(ctx.GetPosition(object.Pos()),
                        TStringBuilder() << "Object " << req.S3Request.Pattern << " doesn't exist."));
                    return TStatus::Error;
                }

                if (!listEntries.Directories.empty()) {
                    needsListingOnActors = true;
                }

                for (auto& entry: listEntries.Objects) {
                    TMaybe<TVector<TExtraColumnValue>> extraValues;
                    if (generatedColumnsConfig) {
                        extraValues = ExtractExtraColumnValues(
                            req, generatedColumnsConfig, entry.MatchedGlobs);
                    }

                    auto& pathList = pathsByExtraValues[extraValues];
                    pathList.emplace_back(NS3Details::TPath{entry.Path, entry.Size, false, pathList.size()});
                    readSize += entry.Size;
                }
                for (auto& entry: listEntries.Directories) {
                    TMaybe<TVector<TExtraColumnValue>> extraValues;
                    if (generatedColumnsConfig) {
                        extraValues = ExtractExtraColumnValues(
                            req, generatedColumnsConfig, entry.MatchedGlobs);
                    }

                    auto& pathList = pathsByExtraValues[extraValues];
                    pathList.emplace_back(NS3Details::TPath{entry.Path, 0, true, pathList.size()});
                }

                YQL_CLOG(INFO, ProviderS3) << "Pattern " << req.S3Request.Pattern << " has " << listEntries.Size() << " items with total size " << readSize;
            }

            for (const auto& [extraValues, pathList] : pathsByExtraValues) {
                TExprNodeList extraColumnsAsStructArgs;
                if (extraValues) {
                    YQL_ENSURE(generatedColumnsConfig);
                    YQL_ENSURE(generatedColumnsConfig->SchemaTypeNode);

                    for (auto& ev : *extraValues) {
                        auto resultType = ctx.Builder(object.Pos())
                            .Callable("StructMemberType")
                                .Add(0, generatedColumnsConfig->SchemaTypeNode)
                                .Atom(1, ev.Name)
                            .Seal()
                            .Build();
                        TExprNode::TPtr value;
                        if (ev.Type.Defined()) {
                            value = ctx.Builder(object.Pos())
                                .Callable("StrictCast")
                                    .Callable(0, "Data")
                                        .Add(0, ExpandType(object.Pos(), *ctx.MakeType<TDataExprType>(*ev.Type), ctx))
                                        .Atom(1, ev.Value)
                                    .Seal()
                                    .Add(1, resultType)
                                .Seal()
                                .Build();
                        } else {
                            value = ctx.Builder(object.Pos())
                                .Callable("DataOrOptionalData")
                                    .Add(0, resultType)
                                    .Atom(1, ev.Value)
                                .Seal()
                                .Build();
                        }
                        extraColumnsAsStructArgs.push_back(
                            ctx.Builder(object.Pos())
                                .List()
                                    .Atom(0, ev.Name)
                                    .Add(1, value)
                                .Seal()
                                .Build()
                        );
                    }
                }

                auto extraColumns = ctx.NewCallable(object.Pos(), "AsStruct", std::move(extraColumnsAsStructArgs));

                TString packedPaths;
                bool isTextFormat;
                NS3Details::PackPathsList(pathList, packedPaths, isTextFormat);

                pathNodes.emplace_back(
                    Build<TS3Path>(ctx, object.Pos())
                        .Data<TCoString>()
                            .Literal()
                            .Build(packedPaths)
                        .Build()
                        .IsText<TCoBool>()
                            .Literal()
                            .Build(ToString(isTextFormat))
                        .Build()
                        .ExtraColumns(extraColumns)
                    .Done().Ptr()
                );
            }

            auto settings = read.Ref().Child(4)->ChildrenList();
            const auto settingsPos = read.Ref().Child(4)->Pos();
            auto userSchema = ExtractSchema(settings);
            if (pathNodes.empty()) {
                auto data = ctx.Builder(read.Pos())
                    .Callable("List")
                        .Callable(0, "ListType")
                            .Add(0, userSchema.front())
                        .Seal()
                    .Seal()
                    .Build();
                if (userSchema.back()) {
                    data = ctx.NewCallable(read.Pos(), "AssumeColumnOrder", { data, userSchema.back() });
                }
                replaces.emplace(node, ctx.NewCallable(read.Pos(), "Cons!", { read.World().Ptr(), data }));
                continue;
            }

            auto format = ExtractFormat(settings);
            if (!format) {
                ctx.AddError(TIssue(ctx.GetPosition(settingsPos), "No read format specified."));
                return TStatus::Error;
            }

            if (assumeDirectories) {
                settings.push_back(ctx.NewList(settingsPos, { ctx.NewAtom(settingsPos, "directories", TNodeFlags::Default) }));
            }
            if (needsListingOnActors) {
                TString pathPattern;
                NS3Lister::ES3PatternVariant pathPatternVariant;
                if (requests[0].Options.IsPartitionedDataset) {
                    pathPattern = requests[0].FilePattern;
                    pathPatternVariant = NS3Lister::ES3PatternVariant::FilePattern;
                } else {
                    pathPattern = requests[0].S3Request.Pattern;
                    pathPatternVariant = NS3Lister::ES3PatternVariant::PathPattern;
                }

                settings.push_back(ctx.NewList(
                    settingsPos,
                    {
                        ctx.NewAtom(settingsPos, "pathpattern"),
                        ctx.NewAtom(settingsPos, pathPattern),
                    }));
                settings.push_back(ctx.NewList(
                    settingsPos,
                    {
                        ctx.NewAtom(settingsPos, "pathpatternvariant"),
                        ctx.NewAtom(settingsPos, ToString(pathPatternVariant)),
                    }));
            }

            TExprNode::TPtr s3Object;
            s3Object = Build<TS3Object>(ctx, object.Pos())
                    .Paths(ctx.NewList(object.Pos(), std::move(pathNodes)))
                    .Format(std::move(format))
                    .Settings(ctx.NewList(object.Pos(), std::move(settings)))
                .Done().Ptr();

            auto row = Build<TCoArgument>(ctx, read.Pos())
                .Name("row")
                .Done();
            auto emptyPredicate = Build<TCoLambda>(ctx, read.Pos())
                .Args({row})
                .Body<TCoBool>()
                    .Literal().Build("true")
                    .Build()
                .Done().Ptr();
            
            replaces.emplace(node, userSchema.back() ?
                Build<TS3ReadObject>(ctx, read.Pos())
                    .World(read.World())
                    .DataSource(read.DataSource())
                    .Object(std::move(s3Object))
                    .RowType(std::move(userSchema.front()))
                    .Path(ctx.NewAtom(object.Pos(), path))
                    .FilterPredicate(emptyPredicate)
                    .ColumnOrder(std::move(userSchema.back()))
                .Done().Ptr():
                Build<TS3ReadObject>(ctx, read.Pos())
                    .World(read.World())
                    .DataSource(read.DataSource())
                    .Object(std::move(s3Object))
                    .RowType(std::move(userSchema.front()))
                    .Path(ctx.NewAtom(object.Pos(), path))
                    .FilterPredicate(emptyPredicate)
                .Done().Ptr());
        }

        return RemapExpr(input, output, replaces, ctx, TOptimizeExprSettings(nullptr));
    }

    static TVector<TExtraColumnValue> ExtractExtraColumnValues(
        const TListRequest& req,
        const TGeneratedColumnsConfig* generatedColumnsConfig,
        const std::vector<TString>& matchedGlobs) {
        auto extraValues = TVector<TExtraColumnValue>{};
        if (!req.ColumnValues.empty()) {
            // explicit partitioning
            YQL_ENSURE(req.ColumnValues.size() == generatedColumnsConfig->Columns.size());
            for (auto& cv: req.ColumnValues) {
                TExtraColumnValue value;
                value.Name = cv.Name;
                value.Type = cv.Type;
                value.Value = cv.Value;
                extraValues.push_back(std::move(value));
            }
        } else {
            YQL_ENSURE(matchedGlobs.size() == generatedColumnsConfig->Columns.size());
            for (size_t i = 0; i < generatedColumnsConfig->Columns.size(); ++i) {
                TExtraColumnValue value;
                value.Name = generatedColumnsConfig->Columns[i];
                value.Value = matchedGlobs[i];
                extraValues.push_back(std::move(value));
            }
        }
        return extraValues;
    }

    static bool ValidateProjection(TPositionHandle pos, const TPathGeneratorPtr& generator, const TVector<TString>& partitionedBy, TExprContext& ctx) {
        const TSet<TString> partitionedBySet(partitionedBy.begin(), partitionedBy.end());
        TSet<TString> projectionSet;
        const auto& config = generator->GetConfig();
        if (!config.Enabled) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), "Projection is configured but not enabled"));
            return false;
        }
        for (auto& rule : config.Rules) {
            projectionSet.insert(rule.Name);
        }

        if (projectionSet != partitionedBySet) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Column set in partitioned_by doesn't match column set in projection: {"
                << JoinSeq(",", partitionedBySet) << "} != {" << JoinSeq(",", projectionSet) << "}"));
            return false;
        }
        return true;
    }

    bool LaunchListsForNode(const TDqSourceWrap& source, TVector<NThreading::TFuture<NS3Lister::TListResult>>& futures, TExprContext& ctx) {
        TS3DataSource dataSource = source.DataSource().Maybe<TS3DataSource>().Cast();
        const auto& connect = State_->Configuration->Clusters.at(dataSource.Cluster().StringValue());
        const auto& token = State_->Configuration->Tokens.at(dataSource.Cluster().StringValue());

        const auto& credentials = GetOrCreateCredentials(token);
        const TString url = connect.Url;
        auto s3ParseSettings = source.Input().Maybe<TS3ParseSettings>().Cast();
        TString filePattern;
        if (s3ParseSettings.Ref().ChildrenSize() > TS3ParseSettings::idx_Settings) {
            const auto& settings = *s3ParseSettings.Ref().Child(TS3ParseSettings::idx_Settings);
            if (!FindFilePattern(settings, ctx, filePattern)) {
                return false;
            }
        }
        const TString effectiveFilePattern = filePattern ? filePattern : "*";

        auto resultSetLimitPerPath = std::max(State_->Configuration->MaxDiscoveryFilesPerQuery, State_->Configuration->MaxDirectoriesAndFilesPerQuery);
        if (!s3ParseSettings.Paths().Empty()) {
            resultSetLimitPerPath /= s3ParseSettings.Paths().Size();
        }
        resultSetLimitPerPath =
            std::min(resultSetLimitPerPath,
                     State_->Configuration->MaxDiscoveryFilesPerDirectory.Get().GetOrElse(
                         State_->Configuration->MaxListingResultSizePerPhysicalPartition));

        for (auto path : s3ParseSettings.Paths()) {
            NS3Details::TPathList directories;
            NS3Details::UnpackPathsList(path.Data().Literal().Value(), FromString<bool>(path.IsText().Literal().Value()), directories);

            YQL_CLOG(DEBUG, ProviderS3) << "directories size: " << directories.size();
            for (auto & dir: directories) {
                YQL_CLOG(DEBUG, ProviderS3)
                    << "directory: path{" << dir.Path << "} size {" << dir.Size << "}";

                auto req = TListRequest{.S3Request{
                    .Url = url,
                    .Credentials = credentials,
                    .Pattern = NS3::NormalizePath(
                        TStringBuilder() << dir.Path << "/" << effectiveFilePattern),
                    .PatternType = NS3Lister::ES3PatternType::Wildcard,
                    .Prefix = dir.Path}};

                auto future = ListingStrategy_->List(
                    req.S3Request,
                    TS3ListingOptions{
                        .IsPartitionedDataset = false,
                        .IsConcurrentListing =
                            State_->Configuration->UseConcurrentDirectoryLister.Get().GetOrElse(
                                State_->Configuration->AllowConcurrentListings),
                        .MaxResultSet = resultSetLimitPerPath});


                RequestsByNode_[source.Raw()].push_back(req);
                PendingRequests_[req] = future;
                futures.push_back(std::move(future));
            }
        }

        return true;
    }

    TMap<TString, NUdf::EDataSlot> GetDataSlotColumns(const TExprNode& schema, TExprContext& ctx) {
        TMap<TString, NUdf::EDataSlot> columns;
        auto types = schema.Child(1);
        if (!types) {
            return columns;
        }

        TExprNode::TPtr holder;
        if (types->Content() == "SqlTypeFromYson") {
            auto type = NCommon::ParseTypeFromYson(types->Head().Content(), ctx, ctx.GetPosition(schema.Pos()));
            holder = ExpandType(schema.Pos(), *type, ctx);
            types = holder.Get();
        }

        for (size_t i = 0; i < types->ChildrenSize(); i++) {
            const auto& column = types->Child(i);
            const auto& name = column->Child(0);
            const auto& type = column->Child(1)->Child(0);
            auto slot = NKikimr::NUdf::FindDataSlot(type->Content());
            if (!slot) {
                continue;
            }
            columns[TString{name->Content()}] = *slot;
        }
        return columns;
    }

    bool LaunchListsForNode(const TS3Read& read, TVector<NThreading::TFuture<NS3Lister::TListResult>>& futures, TExprContext& ctx) {
        const auto& settings = *read.Ref().Child(4);

        // schema is required
        auto schema = GetSetting(settings, "userschema");
        if (!schema) {
            ctx.AddError(TIssue(ctx.GetPosition(read.Pos()), "Missing schema - please use WITH SCHEMA when reading from S3"));
            return false;
        }
        if (!EnsureTupleMinSize(*schema, 2, ctx)) {
            return false;
        }

        TVector<TString> partitionedBy;
        if (auto partitionedBySetting = GetSetting(settings, "partitionedby")) {
            if (!EnsureTupleMinSize(*partitionedBySetting, 2, ctx)) {
                return false;
            }

            THashSet<TStringBuf> uniqs;
            for (size_t i = 1; i < partitionedBySetting->ChildrenSize(); ++i) {
                const auto& column = partitionedBySetting->Child(i);
                if (!EnsureAtom(*column, ctx)) {
                    return false;
                }
                if (!uniqs.emplace(column->Content()).second) {
                    ctx.AddError(TIssue(ctx.GetPosition(column->Pos()), TStringBuilder() << "Duplicate partitioned_by column '" << column->Content() << "'"));
                    return false;
                }
                partitionedBy.push_back(ToString(column->Content()));

            }
        }

        TString projection;
        TPositionHandle projectionPos;
        if (auto projectionSetting = GetSetting(settings, "projection")) {
            if (!EnsureTupleSize(*projectionSetting, 2, ctx)) {
                return false;
            }

            if (!EnsureAtom(projectionSetting->Tail(), ctx)) {
                return false;
            }

            if (projectionSetting->Tail().Content().empty()) {
                ctx.AddError(TIssue(ctx.GetPosition(projectionSetting->Pos()), "Expecting non-empty projection setting"));
                return false;
            }

            if (partitionedBy.empty()) {
                ctx.AddError(TIssue(ctx.GetPosition(projectionSetting->Pos()), "Missing partitioned_by setting for projection"));
                return false;
            }
            projection = projectionSetting->Tail().Content();
            projectionPos = projectionSetting->Tail().Pos();
        }

        TString filePattern;
        if (!FindFilePattern(settings, ctx, filePattern)) {
            return false;
        }
        const TString effectiveFilePattern = filePattern ? filePattern : "*";

        TVector<TString> paths;
        const auto& object = read.Arg(2).Ref();
        YQL_ENSURE(object.IsCallable("MrTableConcat"));
        object.ForEachChild([&paths](const TExprNode& child){ paths.push_back(ToString(child.Head().Tail().Head().Content())); });

        const auto& connect = State_->Configuration->Clusters.at(read.DataSource().Cluster().StringValue());
        const auto& token = State_->Configuration->Tokens.at(read.DataSource().Cluster().StringValue());

        const auto& credentials = GetOrCreateCredentials(token);
        const TString url = connect.Url;

        TGeneratedColumnsConfig config;
        if (!partitionedBy.empty()) {
            config.Columns = partitionedBy;
            config.SchemaTypeNode = schema->ChildPtr(1);
            if (!projection.empty()) {
                config.Generator = CreatePathGenerator(
                    projection,
                    partitionedBy,
                    GetDataSlotColumns(*schema, ctx),
                    State_->Configuration->GeneratorPathsLimit);
                if (!ValidateProjection(projectionPos, config.Generator, partitionedBy, ctx)) {
                    return false;
                }
            }
            GenColumnsByNode_[read.Raw()] = config;
        }

        for (const auto& path : paths) {
            // each path in CONCAT() can generate multiple list requests for explicit partitioning
            TVector<TListRequest> reqs;

            auto isConcurrentListingEnabled =
                State_->Configuration->UseConcurrentDirectoryLister.Get().GetOrElse(
                    State_->Configuration->AllowConcurrentListings);
            auto req = TListRequest{
                .S3Request{.Url = url, .Credentials = credentials},
                .FilePattern = effectiveFilePattern,
                .Options{
                    .IsConcurrentListing = isConcurrentListingEnabled,
                    .MaxResultSet = std::max(State_->Configuration->MaxDiscoveryFilesPerQuery, State_->Configuration->MaxDirectoriesAndFilesPerQuery)
                }};

            if (partitionedBy.empty()) {
                if (path.empty()) {
                    ctx.AddError(TIssue(ctx.GetPosition(read.Pos()), "Can not read from empty path"));
                    return false;
                }
                if (path.EndsWith("/")) {
                    req.S3Request.Pattern = path + effectiveFilePattern;
                } else {
                    // treat paths as regular wildcard patterns
                    if (filePattern) {
                        ctx.AddError(TIssue(ctx.GetPosition(read.Pos()), TStringBuilder() << "Path pattern cannot be used with file_pattern"));
                        return false;
                    }

                    req.S3Request.Pattern = path;
                }
                req.S3Request.Pattern = NS3::NormalizePath(req.S3Request.Pattern);
                req.S3Request.PatternType = NS3Lister::ES3PatternType::Wildcard;
                req.S3Request.Prefix = req.S3Request.Pattern.substr(
                    0, NS3::GetFirstWildcardPos(req.S3Request.Pattern));
                req.Options.IsPartitionedDataset = false;
                reqs.push_back(req);
            } else {
                if (NS3::HasWildcards(path)) {
                    ctx.AddError(TIssue(ctx.GetPosition(read.Pos()), TStringBuilder() << "Path prefix: '" << path << "' contains wildcards"));
                    return false;
                }
                if (!config.Generator) {
                    // Hive-style partitioning
                    req.S3Request.Prefix = path;
                    if (!path.empty()) {
                        req.S3Request.Prefix = NS3::NormalizePath(TStringBuilder() << path << "/");
                        if (req.S3Request.Prefix == "/") {
                            req.S3Request.Prefix = "";
                        }
                    }
                    TString pp = req.S3Request.Prefix;
                    if (!pp.empty() && pp.back() == '/') {
                        pp.pop_back();
                    }

                    TStringBuilder generated;
                    generated << NS3::EscapeRegex(pp);
                    for (auto& col : config.Columns) {
                        if (!generated.empty()) {
                            generated << "/";
                        }
                        generated << NS3::EscapeRegex(col) << "=(.*?)";
                    }
                    generated << '/' << NS3::RegexFromWildcards(effectiveFilePattern);
                    req.S3Request.Pattern = generated;
                    req.S3Request.PatternType = NS3Lister::ES3PatternType::Regexp;
                    req.Options.IsPartitionedDataset = true;
                    reqs.push_back(req);
                } else {
                    for (auto& rule : config.Generator->GetRules()) {
                        YQL_ENSURE(rule.ColumnValues.size() == config.Columns.size());
                        req.ColumnValues.assign(rule.ColumnValues.begin(), rule.ColumnValues.end());
                        // Pattern will be directory path
                        req.S3Request.Pattern = NS3::NormalizePath(TStringBuilder() << path << "/" << rule.Path);
                        req.S3Request.PatternType = NS3Lister::ES3PatternType::Wildcard;
                        req.S3Request.Prefix = req.S3Request.Pattern.substr(
                            0, NS3::GetFirstWildcardPos(req.S3Request.Pattern));
                        req.Options.IsPartitionedDataset = true;
                        reqs.push_back(req);
                    }
                }
            }

            if (!reqs) {
                ctx.AddError(TIssue(ctx.GetPosition(read.Pos()), TStringBuilder() << "Path prefix: '" << path << "' empty list for discovery"));
                return false;
            }

            for (auto& req : reqs) {
                RequestsByNode_[read.Raw()].push_back(req);
                if (PendingRequests_.find(req) == PendingRequests_.end()) {
                    NThreading::TFuture<NS3Lister::TListResult> future;
                    if (config.Generator) {
                        // postpone actual directory listing (will do it after path pruning)
                        NS3Lister::TListEntries entries{
                            std::vector<NS3Lister::TObjectListEntry>{0},
                            std::vector<NS3Lister::TDirectoryListEntry>{1}};
                        entries.Directories.back().Path = req.S3Request.Pattern;
                        future = NThreading::MakeFuture<NS3Lister::TListResult>(std::move(entries));
                    } else {
                        auto useRuntimeListing = State_->Configuration->UseRuntimeListing.Get().GetOrElse(false);
                        if (useRuntimeListing && !req.Options.IsPartitionedDataset) {
                            req.Options.MaxResultSet = 1;
                        }
                        future = ListingStrategy_->List(req.S3Request, req.Options);
                    }
                    PendingRequests_[req] = future;
                    futures.push_back(std::move(future));
                }
            }
        }

        return true;
    }

    TS3Credentials GetOrCreateCredentials(const TString& token) {
        auto it = S3Credentials_.find(token);
        if (it != S3Credentials_.end()) {
            return it->second;
        }
        return S3Credentials_.insert({token, TS3Credentials(State_->CredentialsFactory, token)}).first->second;
    }

    const TS3State::TPtr State_;
    const NS3Lister::IS3ListerFactory::TPtr ListerFactory_;
    const IS3ListingStrategy::TPtr ListingStrategy_;

    TPendingRequests PendingRequests_;
    TNodeMap<TVector<TListRequest>> RequestsByNode_;
    TNodeMap<TGeneratedColumnsConfig> GenColumnsByNode_;
    std::unordered_map<TString, TS3Credentials> S3Credentials_;
    NThreading::TFuture<void> AllFuture_;
};

}

THolder<IGraphTransformer> CreateS3IODiscoveryTransformer(TS3State::TPtr state) {
    return THolder(new TS3IODiscoveryTransformer(std::move(state)));
}

}
