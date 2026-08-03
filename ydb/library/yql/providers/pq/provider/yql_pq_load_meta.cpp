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
        bool IsWrite = false;
    };
    using TTopics = THashMap<std::pair<TString, TString>, TPendingTopic>;

public:
    explicit TPqLoadTopicMetadataTransformer(TPqState::TPtr state)
        : State_(std::move(state))
    {}

    void AddToPendingTopics(const TString& cluster, const TString& topicPath, TPositionHandle pos, TExprNode::TPtr rowSpec, TExprNode::TPtr columnOrder, bool isWrite) {
        const auto topicKey = std::make_pair(cluster, topicPath);
        if (State_->Topics.FindPtr(topicKey) || PendingTopics_.FindPtr(topicKey)) {
            return;
        }

        YQL_CLOG(INFO, ProviderPq) << "Load topic meta for: `" << cluster << "`.`" << topicPath << "`";
        TPendingTopic pending;
        pending.Meta.Pos = pos;
        pending.Meta.RowSpec = rowSpec;
        pending.Meta.ColumnOrder = columnOrder;
        pending.IsWrite = isWrite;
        PendingTopics_.emplace(topicKey, std::move(pending));
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
                AddToPendingTopics(read.DataSource().Cluster().StringValue(), topicParser.GetTopicPath(), node->Pos(), topicParser.GetUserSchema(), topicParser.GetColumnOrder(), /*isWrite*/ false);
            } else if (auto maybePqWrite = TMaybeNode<TPqWrite>(node)) {
                TPqWrite write = maybePqWrite.Cast();
                if (write.DataSink().Category().Value() == PqProviderName) {
                    TTopicKeyParser topicParser(write.Arg(2).Ref(), nullptr, ctx);
                    AddToPendingTopics(write.DataSink().Cluster().StringValue(), topicParser.GetTopicPath(), node->Pos(), {}, {}, /*isWrite*/ true);
                }
            }
            return true;
        });

        if (PendingTopics_.empty()) {
            return TStatus::Ok;
        }

        TVector<NThreading::TFuture<void>> handles;
        handles.reserve(PendingTopics_.size());
        for (auto& [key, pending] : PendingTopics_) {
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

        AsyncFuture_ = NThreading::WaitAll(handles);
        return TStatus::Async;
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
        Y_UNUSED(input);
        return AsyncFuture_;
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;

        for (auto& [key, pending] : PendingTopics_) {
            const TStructExprType* itemType = nullptr;
            try {
                pending.Meta.FederatedTopic = pending.Future.GetValue();
                itemType = CreateDefaultItemType(ctx);
            } catch (const std::exception& ex) {
                if (!State_->UseYtflowEngine || !pending.IsWrite) {
                    TIssues issues;
                    issues.AddIssue(ex.what());
                    ctx.IssueManager.AddIssues(issues);
                    return TStatus::Error;
                }
            }

            if (!itemType) {
                itemType = CreateDefaultItemType(ctx);
            }

            if (!pending.Meta.RowSpec) {
                pending.Meta.RowSpec = ExpandType(pending.Meta.Pos, *itemType, ctx);
            }
            State_->Topics.emplace(key, pending.Meta);
        }
        PendingTopics_.clear();
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

    void Rewind() final {
        PendingTopics_.clear();
        AsyncFuture_ = {};
    }

private:
    TPqState::TPtr State_;
    TTopics PendingTopics_;
    NThreading::TFuture<void> AsyncFuture_;
};

} // anonymous namespace

THolder<IGraphTransformer> CreatePqLoadTopicMetadataTransformer(TPqState::TPtr state) {
    return MakeHolder<TPqLoadTopicMetadataTransformer>(state);
}

} // namespace NYql
