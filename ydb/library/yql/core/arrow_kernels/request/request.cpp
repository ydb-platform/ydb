#include "request.h"
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>

namespace NYql {

TKernelRequestBuilder::TKernelRequestBuilder(const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry)
    : Alloc_(__LOCATION__)
    , Env_(Alloc_)
    , Pb_(Env_, functionRegistry)
{
    Alloc_.Release();
}

TKernelRequestBuilder::~TKernelRequestBuilder() {
    Alloc_.Acquire();
}

ui32 TKernelRequestBuilder::AddUnaryOp(EUnaryOp op, const TTypeAnnotationNode* arg1Type, const TTypeAnnotationNode* retType) {
    TGuard<NKikimr::NMiniKQL::TScopedAlloc> allocGuard(Alloc_);
    auto returnType = MakeType(retType);
    Y_UNUSED(returnType);
    auto arg1 = MakeArg(arg1Type);
    switch (op) {
    case EUnaryOp::Not:
        Items_.emplace_back(Pb_.BlockNot(arg1));
        break;
    }

    return Items_.size() - 1;
}

ui32 TKernelRequestBuilder::AddBinaryOp(EBinaryOp op, const TTypeAnnotationNode* arg1Type, const TTypeAnnotationNode* arg2Type, const TTypeAnnotationNode* retType) {
    TGuard<NKikimr::NMiniKQL::TScopedAlloc> allocGuard(Alloc_);
    auto returnType = MakeType(retType);
    auto arg1 = MakeArg(arg1Type);
    auto arg2 = MakeArg(arg2Type);
    switch (op) {
    case EBinaryOp::Add:
        Items_.emplace_back(Pb_.BlockFunc("Add", returnType, { arg1, arg2 }));
        break;
    case EBinaryOp::Sub:
        Items_.emplace_back(Pb_.BlockFunc("Sub", returnType, { arg1, arg2 }));
        break;        
    case EBinaryOp::Mul:
        Items_.emplace_back(Pb_.BlockFunc("Mul", returnType, { arg1, arg2 }));
        break;                
    case EBinaryOp::Div:
        Items_.emplace_back(Pb_.BlockFunc("Div", returnType, { arg1, arg2 }));
        break;
    case EBinaryOp::StartsWith:
        Items_.emplace_back(Pb_.BlockFunc("StartsWith", returnType, { arg1, arg2 }));
        break;
    case EBinaryOp::EndsWith:
        Items_.emplace_back(Pb_.BlockFunc("EndsWith", returnType, { arg1, arg2 }));
        break;
    case EBinaryOp::StringContains:
        Items_.emplace_back(Pb_.BlockFunc("StringContains", returnType, { arg1, arg2 }));
        break;    
    }

    return Items_.size() - 1;
}

ui32 TKernelRequestBuilder::Udf(const TString& name, bool isPolymorphic, const std::vector<const TTypeAnnotationNode*>& argTypes,
    const TTypeAnnotationNode* retType) {
    TGuard<NKikimr::NMiniKQL::TScopedAlloc> allocGuard(Alloc_);
    std::vector<NKikimr::NMiniKQL::TType*> inputTypes;
    for (const auto& type : argTypes) {
        inputTypes.emplace_back(MakeType(type));
    }

    const auto userType = Pb_.NewTupleType({
        Pb_.NewTupleType(inputTypes),
        Pb_.NewEmptyStructType(),
        Pb_.NewEmptyTupleType()});

    auto udf = Pb_.Udf(isPolymorphic ? name : (name + "_BlocksImpl"), Pb_.NewVoid(), userType);
    std::vector<NKikimr::NMiniKQL::TRuntimeNode> args;
    for (const auto& type : argTypes) {
        args.emplace_back(MakeArg(type));
    }

    auto apply = Pb_.Apply(udf, args);
    auto outType = MakeType(retType);
    Y_ENSURE(outType->IsSameType(*apply.GetStaticType()));
    Items_.emplace_back(apply);
    return Items_.size() - 1;
}

ui32 TKernelRequestBuilder::JsonExists(const TTypeAnnotationNode* arg1Type, const TTypeAnnotationNode* arg2Type, const TTypeAnnotationNode* retType) {
    TGuard<NKikimr::NMiniKQL::TScopedAlloc> allocGuard(Alloc_);
    auto exists = Pb_.Udf("Json2.SqlExists");
    auto parse = Pb_.Udf("Json2.Parse");
    auto compilePath = Pb_.Udf("Json2.CompilePath");
    auto outType = MakeType(retType);
    auto arg1 = MakeArg(arg1Type);
    auto arg2 = MakeArg(arg2Type);
    auto scalarApply = Pb_.ScalarApply({arg1, arg2}, [&](const auto& args) {
        auto json = args[0];
        auto processJson = [&](auto unpacked) {
            auto input = Pb_.NewOptional(Pb_.Apply(parse, { unpacked }));
            auto path = Pb_.Apply(compilePath, { args[1] });
            auto dictType = Pb_.NewDictType(Pb_.NewDataType(NUdf::EDataSlot::Utf8), Pb_.NewResourceType("JsonNode"), false);
            return Pb_.Apply(exists, { input, path, Pb_.NewDict(dictType, {}), Pb_.NewOptional(Pb_.NewDataLiteral(false)) });
        };

        if (json.GetStaticType()->IsOptional()) {
            return Pb_.FlatMap(json, processJson);
        } else {
            return processJson(json);
        }
    });

    Y_ENSURE(outType->IsSameType(*scalarApply.GetStaticType()));
    Items_.emplace_back(scalarApply);
    return Items_.size() - 1;
}

TString TKernelRequestBuilder::Serialize() {
    TGuard<NKikimr::NMiniKQL::TScopedAlloc> allocGuard(Alloc_);
    auto kernelTuple = Items_.empty() ? Pb_.AsScalar(Pb_.NewEmptyTuple()) : Pb_.BlockAsTuple(Items_);
    auto argsTuple = ArgsItems_.empty() ? Pb_.AsScalar(Pb_.NewEmptyTuple()) : Pb_.BlockAsTuple(ArgsItems_);
    auto tuple = Pb_.BlockAsTuple( { argsTuple, kernelTuple });
    return NKikimr::NMiniKQL::SerializeRuntimeNode(tuple, Env_);
}

NKikimr::NMiniKQL::TRuntimeNode TKernelRequestBuilder::MakeArg(const TTypeAnnotationNode* type) {
    auto [it, inserted] = CachedArgs_.emplace(type, NKikimr::NMiniKQL::TRuntimeNode());
    if (!inserted) {
        return it->second;
    }

    auto arg = Pb_.Arg(MakeType(type));
    it->second = arg;
    ArgsItems_.emplace_back(arg);
    return arg;
}

NKikimr::NMiniKQL::TBlockType* TKernelRequestBuilder::MakeType(const TTypeAnnotationNode* type) {
    auto [it, inserted] = CachedTypes_.emplace(type, nullptr);
    if (!inserted) {
        return it->second;
    }

    TStringStream err;
    auto ret = NCommon::BuildType(*type, Pb_, err);
    if (!ret) {
        ythrow yexception() << err.Str();
    }

    return it->second = AS_TYPE(NKikimr::NMiniKQL::TBlockType, ret);
}

}
