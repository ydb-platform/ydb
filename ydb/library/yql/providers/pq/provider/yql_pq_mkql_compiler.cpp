#include "yql_pq_mkql_compiler.h"

#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/common/mkql/parser.h>

namespace NYql {

using namespace NKikimr::NMiniKQL;
using namespace NNodes;

void RegisterDqPqMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler) {
    compiler.ChainCallable(TDqSourceWideWrap::CallableName(),
        [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
            if (const auto wrapper = TDqSourceWideWrap(&node); wrapper.DataSource().Category().Value() == PqProviderName) {
                const auto wrapped = TryWrapWithParser(wrapper, ctx);
                if (wrapped) {
                    return *wrapped;
                }

                const auto input = MkqlBuildExpr(wrapper.Input().Ref(), ctx);
                auto flow = ctx.ProgramBuilder.ToFlow(input);
                return ctx.ProgramBuilder.ExpandMap(flow,
                    [&](TRuntimeNode item) -> TRuntimeNode::TList {
                        return {item};
                    });
            }

            return TRuntimeNode();
        });
}

}
