#pragma once
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/providers/common/mkql/yql_type_mkql.h>
#include <yql/essentials/public/langver/yql_langver.h>

#include <unordered_map>

namespace NYql {

class TKernelRequestBuilder {
public:
    enum class EUnaryOp {
        Not,
        Size,
        Minus,
        Abs,
        Just
    };

    enum class EBinaryOp {
        And = 0,
        Or,
        Xor,

        Add,
        Sub,
        Mul,
        Div,
        Mod,

        StartsWith,
        EndsWith,
        StringContains,

        Equals,
        NotEquals,
        Less,
        LessOrEqual,
        Greater,
        GreaterOrEqual,

        Coalesce
    };

    TKernelRequestBuilder(const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry, TLangVersion langver = MinLangVersion);
    ~TKernelRequestBuilder();

    ui32 AddUnaryOp(EUnaryOp op, const TTypeAnnotationNode* arg1Type, const TTypeAnnotationNode* retType);
    ui32 AddBinaryOp(EBinaryOp op, const TTypeAnnotationNode* arg1Type, const TTypeAnnotationNode* arg2Type, const TTypeAnnotationNode* retType);
    ui32 AddIf(const TTypeAnnotationNode* conditionType, const TTypeAnnotationNode* thenType, const TTypeAnnotationNode* elseType);
    ui32 Udf(const TString& name, bool isPolymorphic, const TTypeAnnotationNode::TListType& argTypes, const TTypeAnnotationNode* retType);
    ui32 AddScalarApply(const TExprNode& lambda, const TTypeAnnotationNode::TListType& argTypes, TExprContext& ctx);
    // (json/json?,utf8)->bool/bool?
    ui32 JsonExists(const TTypeAnnotationNode* arg1Type, const TTypeAnnotationNode* arg2Type, const TTypeAnnotationNode* retType);
    ui32 JsonValue(const TTypeAnnotationNode* arg1Type, const TTypeAnnotationNode* arg2Type, const TTypeAnnotationNode* retType);
    TString Serialize();

private:
    NKikimr::NMiniKQL::TRuntimeNode MakeArg(const TTypeAnnotationNode* type);
    NKikimr::NMiniKQL::TBlockType* MakeType(const TTypeAnnotationNode* type);
private:
    const TLangVersion Langver_;
    NKikimr::NMiniKQL::TScopedAlloc Alloc_;
    const NKikimr::NMiniKQL::TTypeEnvironment Env_;
    NKikimr::NMiniKQL::TProgramBuilder Pb_;
    std::vector<NKikimr::NMiniKQL::TRuntimeNode> Items_;
    std::vector<NKikimr::NMiniKQL::TRuntimeNode> ArgsItems_;
    NCommon::TMemoizedTypesMap TypesMemoization_;
    std::unordered_map<const TTypeAnnotationNode*, NKikimr::NMiniKQL::TRuntimeNode> CachedArgs_;
};

}
