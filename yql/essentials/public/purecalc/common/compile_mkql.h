#pragma once

#include <yql/essentials/public/purecalc/common/interface.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_user_data.h>

namespace NYql {
    namespace NPureCalc {
        /**
         * Compile expr to mkql byte-code
         */

        NKikimr::NMiniKQL::TRuntimeNode CompileMkql(const TExprNode::TPtr& exprRoot, TExprContext& exprCtx,
            const NKikimr::NMiniKQL::IFunctionRegistry& funcRegistry, const NKikimr::NMiniKQL::TTypeEnvironment& env, const TUserDataTable& userData);
    }
}
