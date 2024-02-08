#include "yql_pg_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/pg/expr_nodes/yql_pg_expr_nodes.h>

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

class TPgDataSinkExecTransformer : public TExecTransformerBase {
public:
    TPgDataSinkExecTransformer(TPgState::TPtr state)
        : State_(state)
    {
        AddHandler({TCoCommit::CallableName()}, RequireFirst(), Pass());
    }

private:
    TPgState::TPtr State_;
};

THolder<TExecTransformerBase> CreatePgDataSinkExecTransformer(TPgState::TPtr state) {
    return THolder(new TPgDataSinkExecTransformer(state));
}

} // namespace NYql
