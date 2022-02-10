#include "yql_ydb_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/ydb/expr_nodes/yql_ydb_expr_nodes.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

class TYdbDataSourceExecutionTransformer : public TVisitorTransformerBase {
public:
    TYdbDataSourceExecutionTransformer(TYdbState::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(state)
    {
        using TSelf = TYdbDataSourceExecutionTransformer;
        AddHandler({TPull::CallableName()}, Hndl(&TSelf::HandlePull));
        AddHandler({TYdbReadTableScheme::CallableName()}, Hndl(&TSelf::HandleReadTableScheme));

    }

    TStatus HandleReadTableScheme(TExprBase node, TExprContext& ctx) {
        const TYdbReadTableScheme read(node.Ptr());
        const auto& table = TString(read.Table().Value());
        const auto& cluster = TString(read.DataSource().Cluster().Value());

        TStringStream out;
        NYson::TYsonWriter writer(&out, NYson::EYsonFormat::Binary);
        MetaToYson(cluster, table, State_, writer);

        node.Ptr()->SetResult(ctx.NewAtom(node.Pos(), out.Str()));
        node.Ptr()->SetState(TExprNode::EState::ExecutionComplete);
        return IGraphTransformer::TStatus::Ok;
    }

    TStatus HandlePull(TExprBase node, TExprContext&) {
        node.Ptr()->SetResult(TExprNode::GetResult(node.Ref().Head().HeadPtr()));
        node.Ptr()->SetState(TExprNode::EState::ExecutionComplete);
        return IGraphTransformer::TStatus::Ok;
    }
private:
    const TYdbState::TPtr State_;
};

}

THolder<IGraphTransformer> CreateYdbSourceCallableExecutionTransformer(TYdbState::TPtr state) {
    return MakeHolder<TYdbDataSourceExecutionTransformer>(state);
}

} // namespace NYql

