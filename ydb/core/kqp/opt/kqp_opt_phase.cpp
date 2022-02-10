#include "kqp_opt_impl.h"

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

using TStatus = IGraphTransformer::TStatus;

TAutoPtr<IGraphTransformer> CreateKqpQueryPhasesTransformer() {
    return CreateFunctorTransformer([](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        Y_UNUSED(ctx);
        output = input;

        return TStatus::Ok;
    });
}

} // namespace NKikimr::NKqp::NOpt
