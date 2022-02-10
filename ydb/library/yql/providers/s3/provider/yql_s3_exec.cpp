#include "yql_s3_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

class TS3DataSourceExecutionTransformer : public TVisitorTransformerBase {
public:
    TS3DataSourceExecutionTransformer(TS3State::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(state)
    {
        using TSelf = TS3DataSourceExecutionTransformer;
        AddHandler({TPull::CallableName()}, Hndl(&TSelf::HandlePull));

    }

    TStatus HandlePull(TExprBase node, TExprContext&) {
        node.Ptr()->SetResult(TExprNode::GetResult(node.Ref().Head().HeadPtr()));
        node.Ptr()->SetState(TExprNode::EState::ExecutionComplete);
        return IGraphTransformer::TStatus::Ok;
    }
private:
    const TS3State::TPtr State_;
};

}

THolder<IGraphTransformer> CreateS3SourceCallableExecutionTransformer(TS3State::TPtr state) {
    return MakeHolder<TS3DataSourceExecutionTransformer>(state);
}

} // namespace NYql

