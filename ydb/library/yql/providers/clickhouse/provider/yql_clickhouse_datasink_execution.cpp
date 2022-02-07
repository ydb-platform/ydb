#include "yql_clickhouse_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/clickhouse/expr_nodes/yql_clickhouse_expr_nodes.h>

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

class TClickHouseDataSinkExecTransformer : public TExecTransformerBase {
public:
    TClickHouseDataSinkExecTransformer(TClickHouseState::TPtr state)
        : State_(state)
    {
        AddHandler({TCoCommit::CallableName()}, RequireFirst(), Pass());
    }

private:
    TClickHouseState::TPtr State_;
};

THolder<TExecTransformerBase> CreateClickHouseDataSinkExecTransformer(TClickHouseState::TPtr state) {
    return THolder(new TClickHouseDataSinkExecTransformer(state));
}

} // namespace NYql
