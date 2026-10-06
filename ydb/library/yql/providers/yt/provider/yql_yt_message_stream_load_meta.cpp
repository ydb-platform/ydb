#include "yql_yt_message_stream_impl.h"
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_message_stream_expr_nodes.h>
#include <ydb/library/yql/providers/yt/gateway/clients/message_stream/yql_qyt_message_stream_client.h>
#include <ydb/library/yql/providers/yt/gateway/clients/message_stream/yql_yt_client.h>
#include <ydb/library/yql/providers/common/message_stream/provider.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <thread>

namespace NYql {
namespace {
class TYtMessageStreamLoadMetadata final : public TGraphTransformerBase {
    using TKey = std::pair<TString, TString>;
    using TResult = NFq::TMessageStreamResult<NFq::TMessageStreamDescription>;
    struct TPending {
        TPositionHandle Pos;
        NThreading::TFuture<TResult> Future;
    };
public:
    explicit TYtMessageStreamLoadMetadata(std::shared_ptr<TYtMessageStreamState> state) : State_(std::move(state)) {}
private:
    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        output = input;
        if (ctx.Step.IsDone(TExprStep::LoadTablesMetadata)) {
            return TStatus::Ok;
        }
        bool failed = false;
        TVector<NThreading::TFuture<void>> completed;
        VisitExpr(input, [&](const TExprNode::TPtr& node) {
            const bool rawRead = node->IsCallable("Read!") && node->ChildrenSize() > 1
                && NNodes::TYtMessageStreamDataSource::Match(node->Child(1));
            if (!rawRead && !NNodes::TYtMessageStreamReadTable::Match(node.Get())) {
                return true;
            }
            try {
                // KQP can resolve an EDS after IO discovery. Load metadata for
                // the generic Read! too, before RewriteIO lowers that read.
                const auto path = rawRead ? ParseYtMessageStreamReadSettings(*node).Path
                    : NNodes::TYtMessageStreamReadTable(node).Table().StringValue();
                const auto cluster = NNodes::TYtMessageStreamDataSource(node->Child(1)).Cluster().StringValue();
                const TKey key{cluster, path};
                if (State_->Partitions.contains(key) || Pending_.contains(key)) {
                    return true;
                }
                const auto config = State_->Clusters.at(cluster);
                auto promise = NThreading::NewPromise<TResult>();
                // Native YT description currently performs blocking RPCs. Keep those
                // off the compiler thread; the worker owns no expression/context data.
                std::thread([promise, config, path, credentials = State_->Credentials]() mutable {
                    try {
                        const auto auth = credentials->Create(config.Token)->CreateProvider()->GetAuthInfo();
                        auto client = CreateQytMessageStreamClient(path, {.Client = CreateYtClient(config.Endpoint, TString(auth))});
                        promise.SetValue(client->DescribeStream().GetValueSync());
                    } catch (...) {
                        promise.SetException(std::current_exception());
                    }
                }).detach();
                Pending_.emplace(key, TPending{node->Pos(), promise.GetFuture()});
                completed.push_back(NFq::NMessageStream::CompletionFuture(promise.GetFuture()));
            } catch (const std::exception& error) {
                ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), error.what()));
                failed = true;
            }
            return true;
        });
        if (failed) {
            Pending_.clear();
            return TStatus::Error;
        }
        if (completed.empty()) {
            return TStatus::Ok;
        }
        Ready_ = NThreading::WaitAll(completed);
        return TStatus::Async;
    }
    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode&) override { return Ready_; }
    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        output = input;
        bool failed = false;
        for (const auto& [key, pending] : Pending_) {
            try {
                const auto& description = pending.Future.GetValue();
                Y_ENSURE(description.IsSuccess(), description.Issues.ToString());
                Y_ENSURE(!description.Value.Partitions.empty(), "YT queue has no partitions");
                State_->Partitions[key] = description.Value.Partitions.size();
            } catch (const std::exception& error) {
                ctx.AddError(TIssue(ctx.GetPosition(pending.Pos), error.what()));
                failed = true;
            }
        }
        Pending_.clear();
        Ready_ = {};
        return failed ? TStatus::Error : TStatus::Ok;
    }
    void Rewind() override { Pending_.clear(); Ready_ = {}; }
    const std::shared_ptr<TYtMessageStreamState> State_;
    THashMap<TKey, TPending> Pending_;
    NThreading::TFuture<void> Ready_;
};
}
THolder<IGraphTransformer> CreateYtMessageStreamLoadMetadata(std::shared_ptr<TYtMessageStreamState> state) {
    return MakeHolder<TYtMessageStreamLoadMetadata>(std::move(state));
}
}
