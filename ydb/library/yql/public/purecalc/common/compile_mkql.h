#pragma once

#include <ydb/library/yql/public/purecalc/common/interface.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_user_data.h>

namespace NYql {
    namespace NPureCalc {
        /**
         * Compile expr to mkql byte-code
         */

        NKikimr::NMiniKQL::TRuntimeNode CompileMkql(const TExprNode::TPtr& exprRoot, TExprContext& exprCtx,
            const NKikimr::NMiniKQL::IFunctionRegistry& funcRegistry, const NKikimr::NMiniKQL::TTypeEnvironment& env, const TUserDataTable& userData);
    }
}
