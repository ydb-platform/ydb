#include "request.h"
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>
#include <ydb/library/yql/providers/common/mkql/yql_provider_mkql.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>

namespace NYql {

using namespace NKikimr::NMiniKQL;

TKernelRequestBuilder::TKernelRequestBuilder(const IFunctionRegistry& functionRegistry)
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
    const TGuard<TScopedAlloc> allocGuard(Alloc_);
    const auto returnType = MakeType(retType);
    Y_UNUSED(returnType);
    const auto arg = MakeArg(arg1Type);
    switch (op) {
    case EUnaryOp::Not:
        Items_.emplace_back(Pb_.BlockNot(arg));
        break;
    case EUnaryOp::Just:
        Items_.emplace_back(Pb_.BlockJust(arg));
        break;
    case EUnaryOp::Size:
    case EUnaryOp::Minus:
    case EUnaryOp::Abs:
        Items_.emplace_back(Pb_.BlockFunc(ToString(op), returnType, { arg }));
        break;
    }

    return Items_.size() - 1;
}

ui32 TKernelRequestBuilder::AddBinaryOp(EBinaryOp op, const TTypeAnnotationNode* arg1Type, const TTypeAnnotationNode* arg2Type, const TTypeAnnotationNode* retType) {
    const TGuard<TScopedAlloc> allocGuard(Alloc_);
    const auto returnType = MakeType(retType);
    const auto arg1 = MakeArg(arg1Type);
    const auto arg2 = MakeArg(arg2Type);
    switch (op) {
    case EBinaryOp::And:
        Items_.emplace_back(Pb_.BlockAnd(arg1, arg2));
        break;
    case EBinaryOp::Or:
        Items_.emplace_back(Pb_.BlockOr(arg1, arg2));
        break;
    case EBinaryOp::Xor:
        Items_.emplace_back(Pb_.BlockXor(arg1, arg2));
        break;
    case EBinaryOp::Coalesce:
        Items_.emplace_back(Pb_.BlockCoalesce(arg1, arg2));
        break;
    case EBinaryOp::Add:
    case EBinaryOp::Sub:
    case EBinaryOp::Mul:
    case EBinaryOp::Div:
    case EBinaryOp::Mod:
    case EBinaryOp::StartsWith:
    case EBinaryOp::EndsWith:
    case EBinaryOp::StringContains:
    case EBinaryOp::Equals:
    case EBinaryOp::NotEquals:
    case EBinaryOp::Less:
    case EBinaryOp::LessOrEqual:
    case EBinaryOp::Greater:
    case EBinaryOp::GreaterOrEqual:
        Items_.emplace_back(Pb_.BlockFunc(ToString(op), returnType, { arg1, arg2 }));
        break;
    }

    return Items_.size() - 1;
}

ui32 TKernelRequestBuilder::AddIf(const TTypeAnnotationNode* conditionType, const TTypeAnnotationNode* thenType, const TTypeAnnotationNode* elseType) {
    const TGuard<TScopedAlloc> allocGuard(Alloc_);
    const auto arg1 = MakeArg(conditionType);
    const auto arg2 = MakeArg(thenType);
    const auto arg3 = MakeArg(elseType);

    Items_.emplace_back(Pb_.BlockIf(arg1, arg2, arg3));
    return Items_.size() - 1;
}

ui32 TKernelRequestBuilder::Udf(const TString& name, bool isPolymorphic, const TTypeAnnotationNode::TListType& argTypes,
    const TTypeAnnotationNode* retType) {
    const TGuard<TScopedAlloc> allocGuard(Alloc_);
    std::vector<TType*> inputTypes;
    for (const auto& type : argTypes) {
        inputTypes.emplace_back(MakeType(type));
    }

    const auto userType = Pb_.NewTupleType({
        Pb_.NewTupleType(inputTypes),
        Pb_.NewEmptyStructType(),
        Pb_.NewEmptyTupleType()});

    auto udf = Pb_.Udf(isPolymorphic ? name : (name + "_BlocksImpl"), Pb_.NewVoid(), userType);
    TRuntimeNode::TList args;
    for (const auto& type : argTypes) {
        args.emplace_back(MakeArg(type));
    }

    auto apply = Pb_.Apply(udf, args);
    auto outType = MakeType(retType);
    Y_ENSURE(outType->IsSameType(*apply.GetStaticType()));
    Items_.emplace_back(apply);
    return Items_.size() - 1;
}

ui32 TKernelRequestBuilder::AddScalarApply(const TExprNode& lambda, const TTypeAnnotationNode::TListType& argTypes, TExprContext& ctx) {
    const TGuard<TScopedAlloc> allocGuard(Alloc_);
    TRuntimeNode::TList args(argTypes.size());
    std::transform(argTypes.cbegin(), argTypes.cend(), args.begin(), std::bind(&TKernelRequestBuilder::MakeArg, this, std::placeholders::_1));

    NCommon::TMkqlCommonCallableCompiler compiler;
    NCommon::TMkqlBuildContext compileCtx(compiler, Pb_, ctx);
    const auto apply = Pb_.ScalarApply(args, [&lambda, &compileCtx] (const TArrayRef<const TRuntimeNode>& args) {
        return MkqlBuildLambda(lambda, compileCtx, TRuntimeNode::TList(args.cbegin(), args.cend()));
    });

    Items_.emplace_back(apply);
    return Items_.size() - 1U;
}

ui32 TKernelRequestBuilder::JsonExists(const TTypeAnnotationNode* arg1Type, const TTypeAnnotationNode* arg2Type, const TTypeAnnotationNode* retType) {
    const TGuard<TScopedAlloc> allocGuard(Alloc_);

    bool isScalar = false;
    bool isBinaryJson = (RemoveOptionalType(NYql::GetBlockItemType(*arg1Type, isScalar))->Cast<TDataExprType>()->GetSlot() == EDataSlot::JsonDocument);

    auto udfName = TStringBuilder() << "Json2." << (isBinaryJson ? "JsonDocument" : "" ) << "SqlExists";

    auto exists = Pb_.Udf(udfName);
    auto parse = Pb_.Udf("Json2.Parse");
    auto compilePath = Pb_.Udf("Json2.CompilePath");
    auto outType = MakeType(retType);
    auto arg1 = MakeArg(arg1Type);
    auto arg2 = MakeArg(arg2Type);
    auto scalarApply = Pb_.ScalarApply({arg1, arg2}, [&](const auto& args) {
        auto json = args[0];
        auto processJson = [&](auto unpacked) {
            auto input = Pb_.NewOptional(isBinaryJson ? unpacked : Pb_.Apply(parse, { unpacked }));
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

ui32 TKernelRequestBuilder::JsonValue(const TTypeAnnotationNode* arg1Type, const TTypeAnnotationNode* arg2Type, const TTypeAnnotationNode* retType) {
    const TGuard<TScopedAlloc> allocGuard(Alloc_);

    bool isScalar = false;
    bool isBinaryJson = (RemoveOptionalType(NYql::GetBlockItemType(*arg1Type, isScalar))->Cast<TDataExprType>()->GetSlot() == EDataSlot::JsonDocument);
    auto resultSlot = RemoveOptionalType(NYql::GetBlockItemType(*retType, isScalar))->Cast<TDataExprType>()->GetSlot();

    auto udfName = TStringBuilder() << "Json2." << (isBinaryJson ? "JsonDocument" : "" );
    if (NYql::IsDataTypeFloat(resultSlot)) {
        udfName << "SqlValueNumber";
    } else if (NYql::IsDataTypeNumeric(resultSlot)) {
        udfName << "SqlValueInt64";
    } else if (NYql::IsDataTypeString(resultSlot)) {
        udfName << "SqlValueConvertToUtf8";
    } else if (resultSlot == EDataSlot::Bool) {
        udfName << "SqlValueBool";
    } else {
        Y_ENSURE(false, "Invalid return type");
    }

    auto jsonValue = Pb_.Udf(udfName);

    auto parse = Pb_.Udf("Json2.Parse");
    auto compilePath = Pb_.Udf("Json2.CompilePath");
    auto outType = MakeType(retType);
    auto arg1 = MakeArg(arg1Type);
    auto arg2 = MakeArg(arg2Type);

    auto scalarApply = Pb_.ScalarApply({arg1, arg2}, [&](const auto& args) {
        auto json = args[0];
        auto processJson = [&](auto unpacked) {
            auto input = Pb_.NewOptional( isBinaryJson ? unpacked : Pb_.Apply(parse, { unpacked }));
            auto path = Pb_.Apply(compilePath, { args[1] });
            auto dictType = Pb_.NewDictType(Pb_.NewDataType(NUdf::EDataSlot::Utf8), Pb_.NewResourceType("JsonNode"), false);
            auto resultTuple = Pb_.Apply(jsonValue, { input, path, Pb_.NewDict(dictType, {})});
            return Pb_.VisitAll(resultTuple, [&](ui32 index, TRuntimeNode item) {
                if (index == 0) {
                    return Pb_.NewEmptyOptional(outType->GetItemType());
                }
                Y_ENSURE(index == 1);
                return Pb_.Cast(item, outType->GetItemType());
            });
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
    const TGuard<TScopedAlloc> allocGuard(Alloc_);
    const auto kernelTuple = Items_.empty() ? Pb_.AsScalar(Pb_.NewEmptyTuple()) : Pb_.BlockAsTuple(Items_);
    const auto argsTuple = ArgsItems_.empty() ? Pb_.AsScalar(Pb_.NewEmptyTuple()) : Pb_.BlockAsTuple(ArgsItems_);
    const auto tuple = Pb_.BlockAsTuple( { argsTuple, kernelTuple });
    return SerializeRuntimeNode(tuple, Env_);
}

TRuntimeNode TKernelRequestBuilder::MakeArg(const TTypeAnnotationNode* type) {
    auto [it, inserted] = CachedArgs_.emplace(type, TRuntimeNode());
    if (!inserted) {
        return it->second;
    }

    auto arg = Pb_.Arg(MakeType(type));
    it->second = arg;
    ArgsItems_.emplace_back(arg);
    return arg;
}

TBlockType* TKernelRequestBuilder::MakeType(const TTypeAnnotationNode* type) {
    auto [it, inserted] = CachedTypes_.emplace(type, nullptr);
    if (!inserted) {
        return it->second;
    }

    TStringStream err;
    const auto ret = NCommon::BuildType(*type, Pb_, err);
    if (!ret) {
        ythrow yexception() << err.Str();
    }

    return it->second = AS_TYPE(TBlockType, ret);
}

}
