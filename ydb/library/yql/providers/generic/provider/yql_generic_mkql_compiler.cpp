#include "yql_generic_mkql_compiler.h"

#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/dq/mkql/parser.h>

#include <library/cpp/json/json_writer.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/providers/common/mkql/yql_type_mkql.h>

#include <algorithm>

namespace NYql {
using namespace NKikimr::NMiniKQL;
using namespace NNodes;

void RegisterDqGenericMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler, const TGenericState::TPtr&) {
    compiler.ChainCallable(TDqSourceWideBlockWrap::CallableName(), [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
        if (const auto wrapper = TDqSourceWideBlockWrap(&node); wrapper.DataSource().Category().Value() == GenericProviderName) {
            const auto wrapped = TryWrapWithParserForArrowIPCStreaming(wrapper, ctx);
            if (wrapped) {
                return *wrapped;
            }
        }

        return TRuntimeNode();
    });
}
}   // namespace NYql
