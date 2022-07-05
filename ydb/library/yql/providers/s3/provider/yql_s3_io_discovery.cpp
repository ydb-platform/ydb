#include "yql_s3_provider_impl.h"
#include "yql_s3_list.h"

#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/url_builder.h>

#include <util/generic/size_literals.h>

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

struct TListRequest {
    TString Token;
    TString Url;
    TString Pattern;
};

bool operator<(const TListRequest& a, const TListRequest& b) {
    return std::tie(a.Token, a.Url, a.Pattern) < std::tie(b.Token, b.Url, b.Pattern);
}

using TPendingRequests = TMap<TListRequest, NThreading::TFuture<IS3Lister::TListResult>>;

class TS3IODiscoveryTransformer : public TGraphTransformerBase {
public:
    TS3IODiscoveryTransformer(TS3State::TPtr state, IHTTPGateway::TPtr gateway)
        : State_(std::move(state))
        , Lister_(IS3Lister::Make(gateway, State_->Configuration->MaxDiscoveryFilesPerQuery))
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
            std::unordered_set<std::string_view> paths;
            const auto& object = read.Arg(2).Ref();
            YQL_ENSURE(object.IsCallable("MrTableConcat"));
            object.ForEachChild([&paths](const TExprNode& child){ paths.emplace(child.Head().Tail().Head().Content()); });
            const auto& connect = State_->Configuration->Clusters.at(read.DataSource().Cluster().StringValue());
            const auto& token = State_->Configuration->Tokens.at(read.DataSource().Cluster().StringValue());
            const auto credentialsProviderFactory = CreateCredentialsProviderFactoryForStructuredToken(State_->CredentialsFactory, token);

            TListRequest req;
            req.Token = credentialsProviderFactory->CreateProvider()->GetAuthInfo();
            req.Url = connect.Url;
            for (const auto& path : paths) {
                req.Pattern = path;
                RequestsByNode_[read.Raw()].push_back(req);

                if (PendingRequests_.find(req) == PendingRequests_.end()) {
                    auto future = Lister_->List(req.Token, req.Url, req.Pattern);
                    PendingRequests_[req] = future;
                    futures.push_back(std::move(future));
                }
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

        pendingRequests.swap(PendingRequests_);
        requestsByNode.swap(RequestsByNode_);

        TNodeOnNodeOwnedMap replaces;
        size_t count = 0;
        size_t totalSize = 0;
        for (auto& [node, requests] : requestsByNode) {
            const TS3Read read(node);
            const auto& object = read.Arg(2).Ref();
            YQL_ENSURE(object.IsCallable("MrTableConcat"));
            size_t readSize = 0;
            TExprNode::TListType pathNodes;
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
                if (listEntries.empty()) {
                    if (IS3Lister::HasWildcards(req.Pattern)) {
                        ctx.AddError(TIssue(ctx.GetPosition(object.Pos()), TStringBuilder() << "Object " << req.Pattern << " has no items."));
                    } else {
                        ctx.AddError(TIssue(ctx.GetPosition(object.Pos()), TStringBuilder() << "Object " << req.Pattern << " doesn't exist."));
                    }
                    return TStatus::Error;
                }
                for (auto& entry : listEntries) {
                    pathNodes.emplace_back(
                        ctx.Builder(object.Pos())
                            .List()
                                .Atom(0, entry.Path)
                                .Atom(1, ToString(entry.Size), TNodeFlags::Default)
                            .Seal()
                            .Build()
                    );
                    ++count;
                    readSize += entry.Size;
                }

                YQL_CLOG(INFO, ProviderS3) << "Object " << req.Pattern << " has " << listEntries.size() << " items with total size " << readSize;
                totalSize += readSize;
            }

            auto settings = read.Ref().Child(4)->ChildrenList();
            auto userSchema = ExtractSchema(settings);
            TExprNode::TPtr s3Object;
            s3Object = Build<TS3Object>(ctx, object.Pos())
                .Paths(ctx.NewList(object.Pos(), std::move(pathNodes)))
                .Format(ExtractFormat(settings))
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

        const auto maxFiles = State_->Configuration->MaxFilesPerQuery;
        if (count > maxFiles) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Too many objects to read: " << count << ", but limit is " << maxFiles));
            return TStatus::Error;
        }

        const auto maxSize = State_->Configuration->MaxReadSizePerQuery;
        if (totalSize > maxSize) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Too large objects to read: " << totalSize << ", but limit is " << maxSize));
            return TStatus::Error;
        }

        return RemapExpr(input, output, replaces, ctx, TOptimizeExprSettings(nullptr));
    }
private:
    const TS3State::TPtr State_;
    const IS3Lister::TPtr Lister_;

    TPendingRequests PendingRequests_;
    TNodeMap<TVector<TListRequest>> RequestsByNode_;
    NThreading::TFuture<void> AllFuture_;
};

}

THolder<IGraphTransformer> CreateS3IODiscoveryTransformer(TS3State::TPtr state, IHTTPGateway::TPtr gateway) {
    return THolder(new TS3IODiscoveryTransformer(std::move(state), std::move(gateway)));
}

}
