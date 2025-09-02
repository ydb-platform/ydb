#include "yql_ydb_provider_impl.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/ydb/expr_nodes/yql_ydb_expr_nodes.h>

#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>

#include <yql/essentials/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

class TYdbDataSinkExecTransformer : public TExecTransformerBase {
public:
    TYdbDataSinkExecTransformer(TYdbState::TPtr state)
        : State_(state)
    {
        AddHandler({TCoCommit::CallableName()}, RequireFirst(), Pass());
    }

private:
    TYdbState::TPtr State_;
};

}

THolder<TExecTransformerBase> CreateYdbDataSinkExecTransformer(TYdbState::TPtr state) {
    return THolder(new TYdbDataSinkExecTransformer(state));
}

} // namespace NYql
