#include "yql_pq_provider_impl.h"
#include "yql_pq_topic_key_parser.h"

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/public/udf/udf_types.h>

namespace NYql {

using namespace NNodes;

namespace {

class TPqLoadTopicMetadataTransformer : public TGraphTransformerBase {
private:
    using TTopics = THashMap<std::pair<TString, TString>, TPqState::TTopicMeta>;
public:
    explicit TPqLoadTopicMetadataTransformer(TPqState::TPtr state)
        : State_(std::move(state))
    {}

    void AddToPendingTopics(const TString& cluster, const TString& topicPath, TPositionHandle pos, TExprNode::TPtr rowSpec, TExprNode::TPtr columnOrder, TTopics& pendingTopics) {
        const auto topicKey = std::make_pair(cluster, topicPath);
        const auto found = State_->Topics.FindPtr(topicKey);
        if (found) {
            return;
        }

        YQL_CLOG(INFO, ProviderPq) << "Load topic meta for: `" << cluster << "`.`" << topicPath << "`";
        TPqState::TTopicMeta m;
        m.Pos = pos;
        m.RowSpec = rowSpec;
        m.ColumnOrder = columnOrder;
        pendingTopics.emplace(topicKey, m);
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

        TStatus status = FillState(PendingReadTopics_, ctx);
        if (status != TStatus::Ok) {
            return status;
        }

        return FillState(PendingWriteTopics_, ctx);
    }

    NThreading::TFuture<void> DoGetAsyncFuture(const TExprNode& input) final {
        Y_UNUSED(input);
        return AsyncFuture_;
    }

    TStatus DoApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        Y_UNUSED(ctx);
        YQL_ENSURE(AsyncFuture_.HasValue());
        output = input;
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

    TStatus FillState(TTopics& pendingTopics, TExprContext& ctx) {
        for (auto& [x, meta] : pendingTopics) {
            auto itemType = LoadTopicMeta(x.first, x.second, ctx, meta);
            if (!itemType) {
                return TStatus::Error;
            }

            if (!meta.RowSpec) {
                meta.RowSpec = ExpandType(meta.Pos, *itemType, ctx);
            }
            State_->Topics.emplace(x, meta);
        }
        pendingTopics.clear();
        return TStatus::Ok;
    }

    const TStructExprType* LoadTopicMeta(const TString& cluster, const TString& topic, TExprContext& ctx, TPqState::TTopicMeta& meta) {
        // todo: return TFuture
        try {
            auto future = State_->Gateway->DescribePath(State_->SessionId, cluster, State_->Configuration->GetDatabaseForTopic(cluster), topic, State_->Configuration->Tokens.at(cluster));
            NPq::NConfigurationManager::TDescribePathResult description = future.GetValueSync();
            if (!description.IsTopic()) {
                ctx.IssueManager.RaiseIssue(TIssue{TStringBuilder() << "Path '" << topic << "' is not a topic"});
                return {};
            }
            meta.Description = description.GetTopicDescription();
            return CreateDefaultItemType(ctx);
        } catch (const std::exception& ex) {
            TIssues issues;
            issues.AddIssue(ex.what());
            ctx.IssueManager.AddIssues(issues);
            return nullptr;
        }
    }

    void Rewind() final {
        PendingWriteTopics_.clear();
        PendingReadTopics_.clear();
        AsyncFuture_ = {};
    }

private:
    TPqState::TPtr State_;
    // (cluster, topic) -> meta
    TTopics PendingWriteTopics_;
    TTopics PendingReadTopics_;
    NThreading::TFuture<void> AsyncFuture_;
};

}

THolder<IGraphTransformer> CreatePqLoadTopicMetadataTransformer(TPqState::TPtr state) {
    return MakeHolder<TPqLoadTopicMetadataTransformer>(state);
}

} // namespace NYql
