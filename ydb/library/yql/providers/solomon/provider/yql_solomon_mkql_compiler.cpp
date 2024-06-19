#include "yql_solomon_mkql_compiler.h"

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/mkql/parser.h>

namespace NYql {

using namespace NKikimr::NMiniKQL;
using namespace NNodes;

namespace {
template <typename T>
TRuntimeNode Wrap(T wrapper, NCommon::TMkqlBuildContext& ctx) {
    const auto input = MkqlBuildExpr(wrapper.Input().Ref(), ctx);
                
    return ctx.ProgramBuilder.ExpandMap(ctx.ProgramBuilder.ToFlow(input), [&](TRuntimeNode item) {
        TRuntimeNode::TList fields;
        bool isOptional = false;
        auto unpacked = UnpackOptional(item, isOptional);
        auto tupleType = AS_TYPE(TTupleType, unpacked);
        fields.reserve(tupleType->GetElementsCount());
        for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
            fields.push_back(ctx.ProgramBuilder.Nth(item, i));
        }

        return fields;
    });
}
}

void RegisterDqSolomonMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler) {
    compiler.ChainCallable(TDqSourceWideWrap::CallableName(),
        [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
            if (const auto wrapper = TDqSourceWideWrap(&node); wrapper.DataSource().Category().Value() == SolomonProviderName) {
                return Wrap(wrapper, ctx);
            }
            return TRuntimeNode();
        });

    // compiler.ChainCallable(TDqSourceWideBlockWrap::CallableName(),
    //     [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
    //         if (const auto wrapper = TDqSourceWideBlockWrap(&node); wrapper.DataSource().Category().Value() == SolomonProviderName) {
    //             return Wrap(wrapper, ctx);
    //         }

    //     return TRuntimeNode();
    // });
}

}
