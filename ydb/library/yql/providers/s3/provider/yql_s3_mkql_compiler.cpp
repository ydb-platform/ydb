#include "yql_s3_mkql_compiler.h"

#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>
#include <ydb/library/yql/providers/common/mkql/parser.h>

#include <util/stream/str.h>

namespace NYql {

using namespace NKikimr::NMiniKQL;
using namespace NNodes;

void RegisterDqS3MkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler, const TS3State::TPtr&) {
    compiler.ChainCallable(TDqSourceWideWrap::CallableName(),
        [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
            if (const auto wrapper = TDqSourceWideWrap(&node); wrapper.DataSource().Category().Value() == S3ProviderName) {
                const auto wrapped = TryWrapWithParser(wrapper, ctx);
                if (wrapped) {
                    return *wrapped;
                }
            }

            return TRuntimeNode();
        });
}

}
