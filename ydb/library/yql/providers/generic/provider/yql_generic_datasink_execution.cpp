#include "yql_generic_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

    using namespace NNodes;

    class TGenericDataSinkExecTransformer: public TExecTransformerBase {
    public:
        TGenericDataSinkExecTransformer(TGenericState::TPtr state)
            : State_(state)
        {
            AddHandler({TCoCommit::CallableName()}, RequireFirst(), Pass());
        }

    private:
        TGenericState::TPtr State_;
    };

    THolder<TExecTransformerBase> CreateGenericDataSinkExecTransformer(TGenericState::TPtr state) {
        return THolder(new TGenericDataSinkExecTransformer(state));
    }

} // namespace NYql
