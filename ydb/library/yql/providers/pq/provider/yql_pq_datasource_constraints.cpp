#include "yql_pq_datasource_constraints.h"
#include "yql_pq_helpers.h"

#include <ydb/library/yql/providers/pq/common/yql_names.h>
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>

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
        AddHandler({
            TCoConfigure::CallableName(),
            TPqTopic::CallableName(),
            TCoSystemMetadata::CallableName(),
            TDqPqFederatedCluster::CallableName(),
        }, Hndl(&TPqDataSourceConstraintTransformer::HandleDefault));
    }

    TStatus HandleDefault(TExprBase, TExprContext&) {
        return TStatus::Ok;
    }

    TStatus HandlePqReadTopic(TExprBase node, TExprContext& ctx) {
        if (ReadInStreamingMode(node.Cast<TPqReadTopic>().Settings().Ptr(), "streaming"sv)) {
            node.MutableRaw()->AddConstraint(ctx.MakeConstraint<TStreamingConstraintNode>());
        }
        return TStatus::Ok;
    }

    TStatus HandleDqPqTopicSource(TExprBase node, TExprContext& ctx) {
        if (ReadInStreamingMode(node.Cast<TDqPqTopicSource>().Settings().Ptr(), StreamingTopicRead)) {
            node.MutableRaw()->AddConstraint(ctx.MakeConstraint<TStreamingConstraintNode>());
        }
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

    TPqState::TPtr State_;
};

} // anonymous namespace

std::unique_ptr<IGraphTransformer> CreatePqDataSourceConstraintTransformer(TPqState::TPtr state) {
    return std::make_unique<TPqDataSourceConstraintTransformer>(std::move(state));
}

} // namespace NYql
