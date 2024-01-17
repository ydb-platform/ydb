#include "yql_pg_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/pg/expr_nodes/yql_pg_expr_nodes.h>

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

class TPgDataSinkTypeAnnotationTransformer : public TVisitorTransformerBase {
public:
    TPgDataSinkTypeAnnotationTransformer(TPgState::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(state)
    {
        using TSelf = TPgDataSinkTypeAnnotationTransformer;
        AddHandler({TCoCommit::CallableName()}, Hndl(&TSelf::HandleCommit));
    }

    TStatus HandleCommit(TExprBase input, TExprContext& ctx) {
        Y_UNUSED(ctx);
        auto commit = input.Cast<TCoCommit>();
        input.Ptr()->SetTypeAnn(commit.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

private:
    TPgState::TPtr State_;
};

THolder<TVisitorTransformerBase> CreatePgDataSinkTypeAnnotationTransformer(TPgState::TPtr state) {
    return MakeHolder<TPgDataSinkTypeAnnotationTransformer>(state);
}

} // namespace NYql
