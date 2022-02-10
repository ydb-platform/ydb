#include "yql_clickhouse_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/clickhouse/expr_nodes/yql_clickhouse_expr_nodes.h>

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

class TClickHouseDataSinkTypeAnnotationTransformer : public TVisitorTransformerBase {
public:
    TClickHouseDataSinkTypeAnnotationTransformer(TClickHouseState::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(state)
    {
        using TSelf = TClickHouseDataSinkTypeAnnotationTransformer;
        AddHandler({TCoCommit::CallableName()}, Hndl(&TSelf::HandleCommit));
    }

    TStatus HandleCommit(TExprBase input, TExprContext& ctx) {
        Y_UNUSED(ctx);
        auto commit = input.Cast<TCoCommit>();
        input.Ptr()->SetTypeAnn(commit.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

private:
    TClickHouseState::TPtr State_;
};

THolder<TVisitorTransformerBase> CreateClickHouseDataSinkTypeAnnotationTransformer(TClickHouseState::TPtr state) {
    return MakeHolder<TClickHouseDataSinkTypeAnnotationTransformer>(state);
}

} // namespace NYql
