#include "yql_simple_arrow_resolver.h"

#include <ydb/library/yql/minikql/arrow/mkql_functions.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>

#include <util/stream/null.h>

namespace NYql {

using namespace NKikimr::NMiniKQL;

class TSimpleArrowResolver: public IArrowResolver {
public:
    TSimpleArrowResolver(const IFunctionRegistry& functionRegistry)
        : FunctionRegistry_(functionRegistry)
    {}

private:
    bool LoadFunctionMetadata(const TPosition& pos, TStringBuf name, const TVector<const TTypeAnnotationNode*>& argTypes,
        const TTypeAnnotationNode*& returnType, TExprContext& ctx) const override {
        try {
            returnType = nullptr;
            TScopedAlloc alloc;
            TTypeEnvironment env(alloc);
            TProgramBuilder pgmBuilder(env, FunctionRegistry_);
            TType* mkqlOutputType;
            TVector<TType*> mkqlInputTypes;
            for (const auto& type : argTypes) {
                TNullOutput null;
                auto mkqlType = NCommon::BuildType(*type, pgmBuilder, null);
                mkqlInputTypes.emplace_back(mkqlType);
            }

            if (!FindArrowFunction(name, mkqlInputTypes, mkqlOutputType, env)) {
                return true;
            }

            returnType = NCommon::ConvertMiniKQLType(pos, mkqlOutputType, ctx);
            return true;
        } catch (const std::exception& e) {
            ctx.AddError(TIssue(pos, e.what()));
            return false;
        }
    }

    bool HasCast(const TPosition& pos, const TTypeAnnotationNode* from, const TTypeAnnotationNode* to, bool& has, TExprContext& ctx) const override {
        try {
            has = false;
            TScopedAlloc alloc;
            TTypeEnvironment env(alloc);
            TProgramBuilder pgmBuilder(env, FunctionRegistry_);
            TNullOutput null;
            auto mkqlFromType = NCommon::BuildType(*from, pgmBuilder, null);
            auto mkqlToType = NCommon::BuildType(*to, pgmBuilder, null);
            if (!HasArrowCast(mkqlFromType, mkqlToType)) {
                return true;
            }

            has = true;
            return true;
        } catch (const std::exception& e) {
            ctx.AddError(TIssue(pos, e.what()));
            return false;
        }
    }

private:
    const IFunctionRegistry& FunctionRegistry_;
};

IArrowResolver::TPtr MakeSimpleArrowResolver(const IFunctionRegistry& functionRegistry) {
    return new TSimpleArrowResolver(functionRegistry);
}

} // namespace NYql
