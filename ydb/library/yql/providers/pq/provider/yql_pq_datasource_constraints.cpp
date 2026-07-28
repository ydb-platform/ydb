#include "yql_pq_datasource_constraints.h"
#include "yql_pq_helpers.h"

#include <ydb/library/yql/providers/pq/common/yql_names.h>
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>

#include <yql/essentials/core/yql_expr_constraint.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>

namespace NYql {

using namespace NNodes;

namespace {

class TPqDataSourceConstraintTransformer : public TVisitorTransformerBase {
    using TBase = TVisitorTransformerBase;

public:
    explicit TPqDataSourceConstraintTransformer(TPqState::TPtr state)
        : TBase(/* failOnUnknown */ true)
        , State_(std::move(state))
    {
        AddHandler({TPqReadTopic::CallableName()}, Hndl(&TPqDataSourceConstraintTransformer::HandlePqReadTopic));
        AddHandler({TDqPqTopicSource::CallableName()}, Hndl(&TPqDataSourceConstraintTransformer::HandleDqPqTopicSource));
        AddHandler({TPqParsingWrap::CallableName()}, Hndl(&TPqDataSourceConstraintTransformer::HandleParsingWrap));
        AddHandler({TDqPqPhyParsingWrap::CallableName()}, Hndl(&TPqDataSourceConstraintTransformer::HandleParsingWrap));
        AddHandler({
            TCoConfigure::CallableName(),
            TPqTopic::CallableName(),
            TCoSystemMetadata::CallableName(),
            TDqPqFederatedCluster::CallableName(),
        }, Hndl(&TPqDataSourceConstraintTransformer::HandleDefault));
    }

    TStatus HandleDefault(TExprBase node, TExprContext&) {
        return UpdateAllChildLambdasConstraints(node.Ref());
    }

    TStatus HandlePqReadTopic(TExprBase node, TExprContext& ctx) {
        const auto pqReadTopic = node.Cast<TPqReadTopic>();
        const auto maybeWatermark = pqReadTopic.Watermark().Maybe<TCoLambda>();

        if (ReadInStreamingMode(pqReadTopic.Settings().Ptr(), "streaming"sv)) {
            if (maybeWatermark) {
                auto& watermark = node.MutableRaw()->ChildRef(TPqReadTopic::idx_Watermark);

                TPartOfConstraintBase::TPathType path;
                if (const auto status = TryExtractEventTime(watermark, path, ctx); status != TStatus::Ok) {
                    return status;
                }

                node.MutableRaw()->AddConstraint(ctx.MakeConstraint<TStreamingConstraintNode>(std::move(path)));
            } else {
                node.MutableRaw()->AddConstraint(ctx.MakeConstraint<TStreamingConstraintNode>());
            }
        } else if (maybeWatermark) {
            const auto watermark = maybeWatermark.Cast();
            if (const auto status = UpdateLambdaConstraints(watermark.Ref()); status != TStatus::Ok) {
                return status;
            }
        }
        return TStatus::Ok;
    }

    TStatus HandleDqPqTopicSource(TExprBase node, TExprContext& ctx) {
        const auto pqTopicSource = node.Cast<TDqPqTopicSource>();
        const auto maybeWatermark = pqTopicSource.WatermarkExpr();

        if (ReadInStreamingMode(pqTopicSource.Settings().Ptr(), StreamingTopicRead)) {
            if (maybeWatermark) {
                auto& watermark = node.MutableRaw()->ChildRef(TDqPqTopicSource::idx_WatermarkExpr);

                TPartOfConstraintBase::TPathType path;
                if (const auto status = TryExtractEventTime(watermark, path, ctx); status != TStatus::Ok) {
                    return status;
                }

                node.MutableRaw()->AddConstraint(ctx.MakeConstraint<TStreamingConstraintNode>(std::move(path)));
            } else {
                node.MutableRaw()->AddConstraint(ctx.MakeConstraint<TStreamingConstraintNode>());
            }
        } else if (maybeWatermark) {
            const auto watermark = maybeWatermark.Cast();
            if (const auto status = UpdateLambdaConstraints(watermark.Ref()); status != TStatus::Ok) {
                return status;
            }
        }
        return TStatus::Ok;
    }

    TStatus HandleParsingWrap(TExprBase node, TExprContext& /* ctx */) {
        if (const auto status = UpdateAllChildLambdasConstraints(node.Ref()); status != TStatus::Ok) {
            return status;
        }
        node.MutableRaw()->CopyConstraints(node.Raw()->Head());
        return TStatus::Ok;
    }

private:
    bool ReadInStreamingMode(TExprNode::TPtr settings, TStringBuf settingName) const {
        bool streamingTopicReadEnabled = State_->StreamingTopicsReadByDefault;
        if (const auto& setting = FindSetting(settings, settingName)) {
            streamingTopicReadEnabled = FromString<bool>(setting.Cast().Ref().Content());
        }
        return streamingTopicReadEnabled;
    }

    [[nodiscard]] TStatus TryExtractEventTime(TExprNode::TPtr& watermark, TPartOfConstraintBase::TPathType& path, TExprContext& ctx) {
        const auto lambda = TExprBase(watermark).Cast<TCoLambda>();
        const auto eventTimeAndDelay = SplitWatermarkExpr(lambda, *State_, ctx);
        if (!eventTimeAndDelay) {
            return TStatus::Error;
        }
        const auto [eventTimeExpr, _] = *eventTimeAndDelay;

        TMaybe<TPartOfConstraintBase::TPathType> eventTime;
        if (const auto status = NYql::TryExtractEventTime(
                watermark,
                eventTimeExpr.Ref(),
                ctx,
                eventTime
            );
            status != TStatus::Ok)
        {
            return status;
        }
        if (!eventTime) {
            ctx.AddError(TIssue(ctx.GetPosition(lambda.Pos()), "Event time expression must be materialized into a Timestamp column before assigning a watermark"));
            return TStatus::Error;
        }
        path = std::move(*eventTime);

        return TStatus::Ok;
    }

private:
    TPqState::TPtr State_;
};

} // anonymous namespace

TAutoPtr<IGraphTransformer> CreatePqDataSourceConstraintTransformer(TPqState::TPtr state) {
    if (!state->EnablePqConstraintsTransformer) {
        return CreateDefCallableConstraintTransformer();
    }
    return new TPqDataSourceConstraintTransformer(std::move(state));
}

} // namespace NYql
