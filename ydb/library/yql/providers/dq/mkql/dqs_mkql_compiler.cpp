#include "dqs_mkql_compiler.h"

#include <ydb/library/yql/dq/integration/yql_dq_integration.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/common/mkql/yql_provider_mkql.h>

namespace NYql::NDqs {

using namespace NKikimr::NMiniKQL;
using namespace NNodes;

void RegisterDqsMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler, const TTypeAnnotationContext& ctx) {
    compiler.AddCallable({TDqSourceWideWrap::CallableName(), TDqSourceWideBlockWrap::CallableName(), TDqReadWideWrap::CallableName(), TDqReadBlockWideWrap::CallableName()},
        [](const TExprNode& node, NCommon::TMkqlBuildContext&) {
            YQL_ENSURE(false, "Unsupported reader: " << node.Head().Content());
            return TRuntimeNode();
        });

    auto integrations = GetUniqueIntegrations(ctx);
    std::for_each(integrations.cbegin(), integrations.cend(), std::bind(&IDqIntegration::RegisterMkqlCompiler, std::placeholders::_1, std::ref(compiler)));
}

}
