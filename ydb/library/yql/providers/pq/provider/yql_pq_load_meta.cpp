#include "yql_pq_provider_impl.h"
#include "yql_pq_settings.h"
#include "yql_pq_topic_key_parser.h"

#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/public/udf/udf_types.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

class TPqLoadTopicMetadataTransformer : public TGraphTransformerBase {
private:
    struct TPendingTopic {
        TPqState::TTopicMeta Meta;
        IPqGateway::TAsyncDescribeFederatedTopicResult Future;
    };
    using TTopicKey = std::pair<TString, TString>;
    using TTopics = THashMap<TTopicKey, TPendingTopic>;

public:
    explicit TPqLoadTopicMetadataTransformer(TPqState::TPtr state)
        : State_(std::move(state))
    {}

    void AddToPendingTopics(const TString& cluster, const TString& topicPath, TPositionHandle pos, TExprNode::TPtr rowSpec, TExprNode::TPtr columnOrder, TTopics& pendingTopics) {
        const auto topicKey = std::make_pair(cluster, topicPath);
        if (State_->Topics.contains(topicKey) || pendingTopics.FindPtr(topicKey)) {
            return;
        }

        YQL_CLOG(INFO, ProviderPq) << "Load topic meta for: `" << cluster << "`.`" << topicPath << "`";
        TPendingTopic pending;
        pending.Meta.Pos = pos;
        pending.Meta.RowSpec = rowSpec;
        pending.Meta.ColumnOrder = columnOrder;
        pendingTopics.emplace(topicKey, std::move(pending));
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;

        if (ctx.Step.IsDone(TExprStep::LoadTablesMetadata)) {
            return TStatus::Ok;
        }

        VisitExpr(input, [&](const TExprNode::TPtr& node) {
            if (auto maybePqRead = TMaybeNode<TPqRead>(node)) {
                TPqRead read = maybePqRead.Cast();
                if (read.DataSource().Category().Value() != PqProviderName) {
                    return true;
                }

                TTopicKeyParser topicParser(read.Arg(2).Ref(), read.Ref().Child(4), ctx);
                AddToPendingTopics(read.DataSource().Cluster().StringValue(), topicParser.GetTopicPath(), node->Pos(), topicParser.GetUserSchema(), topicParser.GetColumnOrder(), PendingReadTopics_);
            } else if (auto maybePqWrite = TMaybeNode<TPqWrite>(node)) {
                TPqWrite write = maybePqWrite.Cast();
                if (write.DataSink().Category().Value() == PqProviderName) {
                    TTopicKeyParser topicParser(write.Arg(2).Ref(), nullptr, ctx);
                    AddToPendingTopics(write.DataSink().Cluster().StringValue(), topicParser.GetTopicPath(), node->Pos(), {}, {}, PendingWriteTopics_);
                }
            }
            return true;
        });

        if (PendingReadTopics_.empty() && PendingWriteTopics_.empty()) {
            return TStatus::Ok;
        }

        TVector<NThreading::TFuture<void>> handles;
        handles.reserve(PendingReadTopics_.size() + PendingWriteTopics_.size());

        auto launchFetch = [&](TTopics& pendingTopics) {
            for (auto& [key, pending] : pendingTopics) {
                auto& [cluster, topic] = key;
                pending.Future = State_->Gateway->DescribeFederatedTopic(
                    State_->SessionId,
                    cluster,
                    State_->Configuration->GetDatabaseForTopic(cluster),
                    topic,
                    State_->Configuration->Tokens.at(cluster));

                // Use a completion promise that always resolves with a value (never exceptional),
                // so WaitAll does not see exceptions and DoApplyAsyncChanges is always invoked.
                // Per-topic errors are handled in DoApplyAsyncChanges via pending.Future.GetValue().
                auto completionPromise = NThreading::NewPromise();
                pending.Future.NoexceptSubscribe([p = completionPromise](const auto&) mutable {
                    p.TrySetValue();
                });
                handles.push_back(completionPromise.GetFuture());
            }
        };

        launchFetch(PendingReadTopics_);
        launchFetch(PendingWriteTopics_);

        AsyncFuture_ = NThreading::WaitAll(handles);
        return TStatus::Async;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
        Y_UNUSED(input);
        return AsyncFuture_;
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;

        if (auto status = FillState(PendingReadTopics_, ctx, false); status != TStatus::Ok) {
            return status;
        }
        if (auto status = FillState(PendingWriteTopics_, ctx, true); status != TStatus::Ok) {
            return status;
        }
        return TStatus::Ok;
    }

private:
    static const TStructExprType* CreateDefaultItemType(TExprContext& ctx) {
        // Schema for topic:
        // {
        //     Data:String
        // }
        TVector<const TItemExprType*> items;
        items.reserve(1);

        // Data column.
        {
            const TTypeAnnotationNode* typeNode = ctx.MakeType<TDataExprType>(NYql::NUdf::EDataSlot::String);
            items.push_back(ctx.MakeType<TItemExprType>(ctx.AppendString("Data"), typeNode));
        }

        return ctx.MakeType<TStructExprType>(items);
    }

    TStatus FillState(TTopics& pendingTopics, TExprContext& ctx, bool isWrite) {
        for (auto& [key, pending] : pendingTopics) {
            const TStructExprType* itemType = nullptr;
            try {
                pending.Meta.FederatedTopic = pending.Future.GetValue();
                itemType = CreateDefaultItemType(ctx);
            } catch (const std::exception& ex) {
                if (!State_->UseYtflowEngine || !isWrite) {
                    TIssues issues;
                    issues.AddIssue(ex.what());
                    ctx.IssueManager.AddIssues(issues);
                    return TStatus::Error;
                }
            }

            if (!pending.Meta.RowSpec) {
                pending.Meta.RowSpec = ExpandType(pending.Meta.Pos, *itemType, ctx);
            }
            if (!isWrite) {
                if (const auto consumer = State_->Configuration->Consumer.Get(); consumer && !consumer->empty() && meta.FederatedTopic) {
                    for (const auto& topic : *meta.FederatedTopic) {
                        // A zero partition count means that topic description failed or the physical cluster is unavailable for read.
                        if (topic.PartitionsCount && !topic.Consumers.contains(*consumer)) {
                            TStringBuilder message;
                            message << "Consumer `" << *consumer << "` does not exist in topic `" << x.second << '`';
                            if (!topic.Info.Name.empty()) {
                                message << " on cluster `" << topic.Info.Name << '`';
                            }
                            ctx.AddError(TIssue(ctx.GetPosition(meta.Pos), message));
                            return TStatus::Error;
                        }
                    }
                }
            }

            State_->Topics.emplace(key, pending.Meta);
        }
        pendingTopics.clear();
        return TStatus::Ok;
    }



    void Rewind() final {
        PendingWriteTopics_.clear();
        PendingReadTopics_.clear();
        AsyncFuture_ = {};
    }

private:
    TPqState::TPtr State_;
    TTopics PendingWriteTopics_;
    TTopics PendingReadTopics_;
    NThreading::TFuture<void> AsyncFuture_;
};

} // anonymous namespace

THolder<IGraphTransformer> CreatePqLoadTopicMetadataTransformer(TPqState::TPtr state) {
    return MakeHolder<TPqLoadTopicMetadataTransformer>(state);
}

} // namespace NYql
