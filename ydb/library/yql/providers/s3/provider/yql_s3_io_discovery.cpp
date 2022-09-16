#include "yql_s3_list.h"
#include "yql_s3_path.h"
#include "yql_s3_provider_impl.h"

#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>
#include <ydb/library/yql/providers/s3/path_generator/yql_s3_path_generator.h>
#include <ydb/library/yql/providers/s3/range_helpers/path_list_reader.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
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

using namespace NPathGenerator;

struct TListRequest {
    TString Token;
    TString Url;
    TString Pattern;
    TMaybe<TString> PathPrefix; // set iff Pattern is regex (not glob pattern)
    TVector<IPathGenerator::TColumnWithValue> ColumnValues;
};

bool operator<(const TListRequest& a, const TListRequest& b) {
    return std::tie(a.Token, a.Url, a.Pattern) < std::tie(b.Token, b.Url, b.Pattern);
}

using TPendingRequests = TMap<TListRequest, NThreading::TFuture<IS3Lister::TListResult>>;

struct TGeneratedColumnsConfig {
    TVector<TString> Columns;
    TPathGeneratorPtr Generator;
    TExprNode::TPtr SchemaTypeNode;
};

class TS3IODiscoveryTransformer : public TGraphTransformerBase {
public:
    TS3IODiscoveryTransformer(TS3State::TPtr state, IHTTPGateway::TPtr gateway)
        : State_(std::move(state))
        , Lister_(IS3Lister::Make(gateway, State_->Configuration->MaxDiscoveryFilesPerQuery, State_->Configuration->MaxInflightListsPerQuery))
    {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;
        if (ctx.Step.IsDone(TExprStep::DiscoveryIO)) {
            return TStatus::Ok;
        }

        auto reads = FindNodes(input, [&](const TExprNode::TPtr& node) {
            if (const auto maybeRead = TMaybeNode<TS3Read>(node)) {
                if (maybeRead.DataSource()) {
                    return maybeRead.Cast().Arg(2).Ref().IsCallable("MrTableConcat");
                }
            }
            return false;
        });

        TVector<NThreading::TFuture<IS3Lister::TListResult>> futures;
        for (auto& r : reads) {
            const TS3Read read(std::move(r));
            try {
                if (!LaunchListsForNode(read, futures, ctx)) {
                    return TStatus::Error;
                }
            } catch (const std::exception& ex) {
                ctx.AddError(TIssue(ctx.GetPosition(read.Pos()), TStringBuilder() << "Error while doing S3 discovery: " << ex.what()));
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
            const TS3Read read(node);
            const auto& object = read.Arg(2).Ref();
            YQL_ENSURE(object.IsCallable("MrTableConcat"));
            size_t readSize = 0;
            TExprNode::TListType pathNodes;

            struct TExtraColumnValue {
                TString Name;
                TMaybe<NUdf::EDataSlot> Type;
                TString Value;
                bool operator<(const TExtraColumnValue& other) const {
                    return std::tie(Name, Type, Value) < std::tie(other.Name, other.Type, other.Value);
                }
            };

            TMap<TMaybe<TVector<TExtraColumnValue>>, NS3Details::TPathList> pathsByExtraValues;
            const TGeneratedColumnsConfig* generatedColumnsConfig = nullptr;
            if (auto it = genColumnsByNode.find(node); it != genColumnsByNode.end()) {
                generatedColumnsConfig = &it->second;
            }

            for (auto& req : requests) {
                auto it = pendingRequests.find(req);
                YQL_ENSURE(it != pendingRequests.end());
                YQL_ENSURE(it->second.HasValue());

                const IS3Lister::TListResult& listResult = it->second.GetValue();
                if (listResult.index() == 1) {
                    const auto& issues = std::get<TIssues>(listResult);
                    YQL_CLOG(INFO, ProviderS3) << "Discovery " << req.Url << req.Pattern << " error " << issues.ToString();
                    std::for_each(issues.begin(), issues.end(), std::bind(&TExprContext::AddError, std::ref(ctx), std::placeholders::_1));
                    return TStatus::Error;
                }

                const auto& listEntries = std::get<IS3Lister::TListEntries>(listResult);
                if (listEntries.empty() && !NS3::HasWildcards(req.Pattern)) {
                    // request to list particular files that are missing
                    ctx.AddError(TIssue(ctx.GetPosition(object.Pos()),
                        TStringBuilder() << "Object " << req.Pattern << " doesn't exist."));
                    return TStatus::Error;
                }

                for (auto& entry : listEntries) {
                    TMaybe<TVector<TExtraColumnValue>> extraValues;
                    if (generatedColumnsConfig) {
                        extraValues = TVector<TExtraColumnValue>{};
                        if (!req.ColumnValues.empty()) {
                            // explicit partitioning
                            YQL_ENSURE(req.ColumnValues.size() == generatedColumnsConfig->Columns.size());
                            for (auto& cv : req.ColumnValues) {
                                TExtraColumnValue value;
                                value.Name = cv.Name;
                                value.Type = cv.Type;
                                value.Value = cv.Value;
                                extraValues->push_back(std::move(value));
                            }
                        } else {
                            // last entry matches file name
                            YQL_ENSURE(entry.MatchedGlobs.size() == generatedColumnsConfig->Columns.size() + 1);
                            for (size_t i = 0; i < generatedColumnsConfig->Columns.size(); ++i) {
                                TExtraColumnValue value;
                                value.Name = generatedColumnsConfig->Columns[i];
                                value.Value = entry.MatchedGlobs[i];
                                extraValues->push_back(std::move(value));
                            }
                        }
                    }

                    auto& pathList = pathsByExtraValues[extraValues];
                    pathList.emplace_back(entry.Path, entry.Size);
                    readSize += entry.Size;
                }

                YQL_CLOG(INFO, ProviderS3) << "Object " << req.Pattern << " has " << listEntries.size() << " items with total size " << readSize;
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
                ctx.AddError(TIssue(ctx.GetPosition(read.Ref().Child(4)->Pos()), "No read format specified."));
                return TStatus::Error;
            }

            TExprNode::TPtr s3Object;
            s3Object = Build<TS3Object>(ctx, object.Pos())
                    .Paths(ctx.NewList(object.Pos(), std::move(pathNodes)))
                    .Format(std::move(format))
                    .Settings(ctx.NewList(object.Pos(), std::move(settings)))
                .Done().Ptr();

            replaces.emplace(node, userSchema.back() ?
                Build<TS3ReadObject>(ctx, read.Pos())
                    .World(read.World())
                    .DataSource(read.DataSource())
                    .Object(std::move(s3Object))
                    .RowType(std::move(userSchema.front()))
                    .ColumnOrder(std::move(userSchema.back()))
                .Done().Ptr():
                Build<TS3ReadObject>(ctx, read.Pos())
                    .World(read.World())
                    .DataSource(read.DataSource())
                    .Object(std::move(s3Object))
                    .RowType(std::move(userSchema.front()))
                .Done().Ptr());
        }

        return RemapExpr(input, output, replaces, ctx, TOptimizeExprSettings(nullptr));
    }
private:
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

    bool LaunchListsForNode(const TS3Read& read, TVector<NThreading::TFuture<IS3Lister::TListResult>>& futures, TExprContext& ctx) {
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

        TVector<TString> paths;
        const auto& object = read.Arg(2).Ref();
        YQL_ENSURE(object.IsCallable("MrTableConcat"));
        object.ForEachChild([&paths](const TExprNode& child){ paths.push_back(ToString(child.Head().Tail().Head().Content())); });

        const auto& connect = State_->Configuration->Clusters.at(read.DataSource().Cluster().StringValue());
        const auto& token = State_->Configuration->Tokens.at(read.DataSource().Cluster().StringValue());
        const auto credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(State_->CredentialsFactory, token);

        const TString url = connect.Url;
        const TString tokenStr = credentialsProviderFactory->CreateProvider()->GetAuthInfo();

        TGeneratedColumnsConfig config;
        if (!partitionedBy.empty()) {
            config.Columns = partitionedBy;
            config.SchemaTypeNode = schema->ChildPtr(1);
            if (!projection.empty()) {
                config.Generator = CreatePathGenerator(projection, partitionedBy);
                if (!ValidateProjection(projectionPos, config.Generator, partitionedBy, ctx)) {
                    return false;
                }
            }
            GenColumnsByNode_[read.Raw()] = config;
        }

        for (const auto& path : paths) {
            // each path in CONCAT() can generate multiple list requests for explicit partitioning
            TVector<TListRequest> reqs;

            TListRequest req;
            req.Token = tokenStr;
            req.Url = url;

            if (partitionedBy.empty()) {
                if (path.empty()) {
                    ctx.AddError(TIssue(ctx.GetPosition(read.Pos()), "Can not read from empty path"));
                    return false;
                }
                if (path.EndsWith("/")) {
                    req.Pattern = path + "*";
                } else {
                    // treat paths as regular wildcard patterns
                    req.Pattern = path;
                }
                req.Pattern = NS3::NormalizePath(req.Pattern);
                reqs.push_back(req);
            } else {
                if (NS3::HasWildcards(path)) {
                    ctx.AddError(TIssue(ctx.GetPosition(read.Pos()), TStringBuilder() << "Path prefix: '" << path << "' contains wildcards"));
                    return false;
                }
                if (!config.Generator) {
                    // Hive-style partitioning
                    req.PathPrefix = path;
                    if (!path.empty()) {
                        req.PathPrefix = NS3::NormalizePath(TStringBuilder() << path << "/");
                        if (req.PathPrefix == "/") {
                            req.PathPrefix = "";
                        }
                    }
                    TString pp = *req.PathPrefix;
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
                    generated << "/(.*)";
                    req.Pattern = generated;
                    reqs.push_back(req);
                } else {
                    for (auto& rule : config.Generator->GetRules()) {
                        YQL_ENSURE(rule.ColumnValues.size() == config.Columns.size());
                        req.ColumnValues.assign(rule.ColumnValues.begin(), rule.ColumnValues.end());
                        req.Pattern = NS3::NormalizePath(TStringBuilder() << path << "/" << rule.Path << "/*");
                        reqs.push_back(req);
                    }
                }
            }

            for (auto& req : reqs) {
                RequestsByNode_[read.Raw()].push_back(req);
                if (PendingRequests_.find(req) == PendingRequests_.end()) {
                    auto future = req.PathPrefix.Defined() ?
                        Lister_->ListRegex(req.Token, req.Url, req.Pattern, *req.PathPrefix) :
                        Lister_->List(req.Token, req.Url, req.Pattern);
                    PendingRequests_[req] = future;
                    futures.push_back(std::move(future));
                }
            }
        }

        return true;
    }

    const TS3State::TPtr State_;
    const IS3Lister::TPtr Lister_;

    TPendingRequests PendingRequests_;
    TNodeMap<TVector<TListRequest>> RequestsByNode_;
    TNodeMap<TGeneratedColumnsConfig> GenColumnsByNode_;
    NThreading::TFuture<void> AllFuture_;
};

}

THolder<IGraphTransformer> CreateS3IODiscoveryTransformer(TS3State::TPtr state, IHTTPGateway::TPtr gateway) {
    return THolder(new TS3IODiscoveryTransformer(std::move(state), std::move(gateway)));
}

}
