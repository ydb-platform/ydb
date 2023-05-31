#include "yql_generic_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

    using namespace NNodes;

    class TGenericDataSinkTypeAnnotationTransformer: public TVisitorTransformerBase {
    public:
        TGenericDataSinkTypeAnnotationTransformer(TGenericState::TPtr state)
            : TVisitorTransformerBase(true)
            , State_(state)
        {
            using TSelf = TGenericDataSinkTypeAnnotationTransformer;
            AddHandler({TCoCommit::CallableName()}, Hndl(&TSelf::HandleCommit));
        }

        TStatus HandleCommit(TExprBase input, TExprContext& ctx) {
            Y_UNUSED(ctx);
            auto commit = input.Cast<TCoCommit>();
            input.Ptr()->SetTypeAnn(commit.World().Ref().GetTypeAnn());
            return TStatus::Ok;
        }

    private:
        TGenericState::TPtr State_;
    };

    THolder<TVisitorTransformerBase> CreateGenericDataSinkTypeAnnotationTransformer(TGenericState::TPtr state) {
        return MakeHolder<TGenericDataSinkTypeAnnotationTransformer>(state);
    }

} // namespace NYql
