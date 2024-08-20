#include "yql_simple_arrow_resolver.h"

#include <ydb/library/yql/minikql/arrow/mkql_functions.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
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
    EStatus LoadFunctionMetadata(const TPosition& pos, TStringBuf name, const TVector<const TTypeAnnotationNode*>& argTypes,
        const TTypeAnnotationNode* returnType, TExprContext& ctx) const override
    {
        try {
            TScopedAlloc alloc(__LOCATION__);
            TTypeEnvironment env(alloc);
            TTypeBuilder typeBuilder(env);
            TNullOutput null;
            TVector<TType*> mkqlInputTypes;
            for (const auto& type : argTypes) {
                auto mkqlType = NCommon::BuildType(*type, typeBuilder, null);
                YQL_ENSURE(mkqlType, "Failed to convert type " << *type << " to MKQL type");
                mkqlInputTypes.emplace_back(mkqlType);
            }
            TType* mkqlOutputType = NCommon::BuildType(*returnType, typeBuilder, null);
            bool found = FindArrowFunction(name, mkqlInputTypes, mkqlOutputType, *FunctionRegistry_.GetBuiltins());
            return found ? EStatus::OK : EStatus::NOT_FOUND;
        } catch (const std::exception& e) {
            ctx.AddError(TIssue(pos, e.what()));
            return EStatus::ERROR;
        }
    }

    EStatus HasCast(const TPosition& pos, const TTypeAnnotationNode* from, const TTypeAnnotationNode* to, TExprContext& ctx) const override {
        try {
            TScopedAlloc alloc(__LOCATION__);
            TTypeEnvironment env(alloc);
            TTypeBuilder typeBuilder(env);
            TNullOutput null;
            auto mkqlFromType = NCommon::BuildType(*from, typeBuilder, null);
            auto mkqlToType = NCommon::BuildType(*to, typeBuilder, null);
            return HasArrowCast(mkqlFromType, mkqlToType) ? EStatus::OK : EStatus::NOT_FOUND;
        } catch (const std::exception& e) {
            ctx.AddError(TIssue(pos, e.what()));
            return EStatus::ERROR;
        }
    }

    EStatus AreTypesSupported(const TPosition& pos, const TVector<const TTypeAnnotationNode*>& types, TExprContext& ctx,
        const TUnsupportedTypeCallback& onUnsupported = {}) const override
    {
        try {
            TScopedAlloc alloc(__LOCATION__);
            TTypeEnvironment env(alloc);
            TTypeBuilder typeBuilder(env);

	    bool allOk = true;
            TArrowConvertFailedCallback cb;
            if (onUnsupported) {
                cb = [&](TType* failed) {
                    if (failed->IsData()) {
                        auto slot = static_cast<TDataType*>(failed)->GetDataSlot();
                        YQL_ENSURE(slot);
                        onUnsupported(*slot);
                    } else {
                        onUnsupported(NYql::NCommon::ConvertMiniKQLTypeKind(failed));
                    }
                };
            }

            for (const auto& type : types) {
                YQL_ENSURE(type);
                TNullOutput null;
                auto mkqlType = NCommon::BuildType(*type, typeBuilder, null);
                std::shared_ptr<arrow::DataType> arrowType;
                if (!ConvertArrowType(mkqlType, arrowType, cb)) {
                    allOk = false;
                    if (!cb) {
                        break;
                    }
                }
            }
            return allOk ? EStatus::OK : EStatus::NOT_FOUND;
        } catch (const std::exception& e) {
            ctx.AddError(TIssue(pos, e.what()));
            return EStatus::ERROR;
        }
    }

private:
    const IFunctionRegistry& FunctionRegistry_;
};

IArrowResolver::TPtr MakeSimpleArrowResolver(const IFunctionRegistry& functionRegistry) {
    return new TSimpleArrowResolver(functionRegistry);
}

} // namespace NYql
