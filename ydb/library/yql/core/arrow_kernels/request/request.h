#pragma once
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>

#include <unordered_map>

namespace NYql {

class TKernelRequestBuilder {
public:
    enum EUnaryOp {
        Not
    };

    enum EBinaryOp {
        Add,
        Sub,
        Mul,
        Div,
        StartsWith,
        EndsWith,
        StringContains
    };

    TKernelRequestBuilder(const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry);
    ~TKernelRequestBuilder();

    ui32 AddUnaryOp(EUnaryOp op, const TTypeAnnotationNode* arg1Type, const TTypeAnnotationNode* retType);
    ui32 AddBinaryOp(EBinaryOp op, const TTypeAnnotationNode* arg1Type, const TTypeAnnotationNode* arg2Type, const TTypeAnnotationNode* retType);
    ui32 Udf(const TString& name, bool isPolymorphic, const std::vector<const TTypeAnnotationNode*>& argTypes, const TTypeAnnotationNode* retType);
    // (json/json?,utf8)->bool/bool?
    ui32 JsonExists(const TTypeAnnotationNode* arg1Type, const TTypeAnnotationNode* arg2Type, const TTypeAnnotationNode* retType);
    TString Serialize();

private:
    NKikimr::NMiniKQL::TRuntimeNode MakeArg(const TTypeAnnotationNode* type);
    NKikimr::NMiniKQL::TBlockType* MakeType(const TTypeAnnotationNode* type);
private:
    NKikimr::NMiniKQL::TScopedAlloc Alloc_;
    NKikimr::NMiniKQL::TTypeEnvironment Env_;
    NKikimr::NMiniKQL::TProgramBuilder Pb_;
    TVector<NKikimr::NMiniKQL::TRuntimeNode> Items_;
    TVector<NKikimr::NMiniKQL::TRuntimeNode> ArgsItems_;
    std::unordered_map<const TTypeAnnotationNode*, NKikimr::NMiniKQL::TBlockType*> CachedTypes_;
    std::unordered_map<const TTypeAnnotationNode*, NKikimr::NMiniKQL::TRuntimeNode> CachedArgs_;
};


}
