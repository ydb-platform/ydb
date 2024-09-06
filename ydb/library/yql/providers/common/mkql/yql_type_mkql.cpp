#include "yql_type_mkql.h"

#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/mkql_runtime_version.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/public/udf/udf_type_inspection.h>

#include <ydb/library/yql/utils/yql_panic.h>

#include <util/generic/vector.h>
#include <util/string/cast.h>

namespace NYql {
namespace NCommon {

NKikimr::NMiniKQL::TType* BuildType(const TTypeAnnotationNode& annotation, const NKikimr::NMiniKQL::TTypeBuilder& typeBuilder, IOutputStream& err) {
    switch (annotation.GetKind()) {
    case ETypeAnnotationKind::Data: {
        auto data = annotation.Cast<TDataExprType>();

        auto slot = data->GetSlot();
        const auto schemeType = NUdf::GetDataTypeInfo(slot).TypeId;
        if (NUdf::TDataType<NUdf::TDecimal>::Id == schemeType) {
            const auto params = static_cast<const TDataExprParamsType&>(annotation);
            return typeBuilder.NewDecimalType(FromString<ui8>(params.GetParamOne()), FromString<ui8>(params.GetParamTwo()));
        } else {
            return typeBuilder.NewDataType(schemeType);
        }
    }

    case ETypeAnnotationKind::Pg: {
        auto pg = annotation.Cast<TPgExprType>();
        return typeBuilder.NewPgType(pg->GetId());
    }

    case ETypeAnnotationKind::Struct: {
        auto structObj = annotation.Cast<TStructExprType>();
        std::vector<std::pair<std::string_view, NKikimr::NMiniKQL::TType*>> members;
        members.reserve(structObj->GetItems().size());

        for (auto& item : structObj->GetItems()) {
            auto itemType = BuildType(*item->GetItemType(), typeBuilder, err);
            if (!itemType) {
                return nullptr;
            }
            members.emplace_back(item->GetName(), itemType);
        }
        return typeBuilder.NewStructType(members);
    }

    case ETypeAnnotationKind::List: {
        auto list = annotation.Cast<TListExprType>();
        auto itemType = BuildType(*list->GetItemType(), typeBuilder, err);
        if (!itemType) {
            return nullptr;
        }
        return typeBuilder.NewListType(itemType);
    }

    case ETypeAnnotationKind::Optional: {
        auto optional = annotation.Cast<TOptionalExprType>();
        auto itemType = BuildType(*optional->GetItemType(), typeBuilder, err);
        if (!itemType) {
            return nullptr;
        }
        return typeBuilder.NewOptionalType(itemType);
    }

    case ETypeAnnotationKind::Tuple: {
        auto tuple = annotation.Cast<TTupleExprType>();
        TVector<NKikimr::NMiniKQL::TType*> elements;
        elements.reserve(tuple->GetItems().size());
        for (auto& child : tuple->GetItems()) {
            elements.push_back(BuildType(*child, typeBuilder, err));
            if (!elements.back()) {
                return nullptr;
            }
        }
        return typeBuilder.NewTupleType(elements);
    }

    case ETypeAnnotationKind::Multi: {
        auto multi = annotation.Cast<TMultiExprType>();
        TVector<NKikimr::NMiniKQL::TType*> elements;
        elements.reserve(multi->GetItems().size());
        for (auto& child : multi->GetItems()) {
            elements.push_back(BuildType(*child, typeBuilder, err));
            if (!elements.back()) {
                return nullptr;
            }
        }
        return typeBuilder.NewMultiType(elements);
    }

    case ETypeAnnotationKind::Dict: {
        auto dictType = annotation.Cast<TDictExprType>();
        auto keyType = BuildType(*dictType->GetKeyType(), typeBuilder, err);
        auto payloadType = BuildType(*dictType->GetPayloadType(), typeBuilder, err);
        if (!keyType || !payloadType) {
            return nullptr;
        }
        return typeBuilder.NewDictType(keyType, payloadType, false);
    }

    case ETypeAnnotationKind::Type: {
        auto type = annotation.Cast<TTypeExprType>()->GetType();
        return BuildType(*type, typeBuilder, err);
    }

    case ETypeAnnotationKind::Void: {
        return typeBuilder.NewVoidType();
    }

    case ETypeAnnotationKind::Null: {
        return typeBuilder.NewNullType();
    }

    case ETypeAnnotationKind::Callable: {
        auto callable = annotation.Cast<TCallableExprType>();
        auto returnType = BuildType(*callable->GetReturnType(), typeBuilder, err);
        NKikimr::NMiniKQL::TCallableTypeBuilder callableTypeBuilder(typeBuilder.GetTypeEnvironment(), "", returnType);
        for (auto& child : callable->GetArguments()) {
            callableTypeBuilder.Add(BuildType(*child.Type, typeBuilder, err));
            if (!child.Name.empty()) {
                callableTypeBuilder.SetArgumentName(child.Name);
            }

            if (child.Flags != 0) {
                callableTypeBuilder.SetArgumentFlags(child.Flags);
            }
        }

        callableTypeBuilder.SetOptionalArgs(callable->GetOptionalArgumentsCount());
        if (!callable->GetPayload().empty()) {
            callableTypeBuilder.SetPayload(callable->GetPayload());
        }

        return callableTypeBuilder.Build();
    }

    case ETypeAnnotationKind::Generic:
    case ETypeAnnotationKind::Unit:
        return typeBuilder.NewVoidType();

    case ETypeAnnotationKind::Resource:
        return typeBuilder.NewResourceType(annotation.Cast<TResourceExprType>()->GetTag());

    case ETypeAnnotationKind::Tagged: {
        auto tagged = annotation.Cast<TTaggedExprType>();
        auto base = BuildType(*tagged->GetBaseType(), typeBuilder, err);
        return typeBuilder.NewTaggedType(base, tagged->GetTag());
    }

    case ETypeAnnotationKind::Variant: {
        auto var = annotation.Cast<TVariantExprType>();
        auto underlyingType = BuildType(*var->GetUnderlyingType(), typeBuilder, err);
        if (!underlyingType) {
            return nullptr;
        }
        return typeBuilder.NewVariantType(underlyingType);
    }

    case ETypeAnnotationKind::Stream: {
        auto stream = annotation.Cast<TStreamExprType>();
        auto itemType = BuildType(*stream->GetItemType(), typeBuilder, err);
        if (!itemType) {
            return nullptr;
        }
        return typeBuilder.NewStreamType(itemType);
    }

    case ETypeAnnotationKind::Flow: {
        auto flow = annotation.Cast<TFlowExprType>();
        auto itemType = BuildType(*flow->GetItemType(), typeBuilder, err);
        if (!itemType) {
            return nullptr;
        }
        return typeBuilder.NewFlowType(itemType);
    }

    case ETypeAnnotationKind::EmptyList: {
        if (NKikimr::NMiniKQL::RuntimeVersion < 11) {
            auto voidType = typeBuilder.NewVoidType();
            return typeBuilder.NewListType(voidType);
        }

        return typeBuilder.GetTypeEnvironment().GetTypeOfEmptyListLazy();
    }

    case ETypeAnnotationKind::EmptyDict: {
        if constexpr(NKikimr::NMiniKQL::RuntimeVersion < 11) {
            auto voidType = typeBuilder.NewVoidType();
            return typeBuilder.NewDictType(voidType, voidType, false);
        }

        return typeBuilder.GetTypeEnvironment().GetTypeOfEmptyDictLazy();
    }

    case ETypeAnnotationKind::Block: {
        auto block = annotation.Cast<TBlockExprType>();
        auto itemType = BuildType(*block->GetItemType(), typeBuilder, err);
        if (!itemType) {
            return nullptr;
        }
        return typeBuilder.NewBlockType(itemType, NKikimr::NMiniKQL::TBlockType::EShape::Many);
    }

    case ETypeAnnotationKind::Scalar: {
        auto scalar = annotation.Cast<TScalarExprType>();
        auto itemType = BuildType(*scalar->GetItemType(), typeBuilder, err);
        if (!itemType) {
            return nullptr;
        }
        return typeBuilder.NewBlockType(itemType, NKikimr::NMiniKQL::TBlockType::EShape::Scalar);
    }

    case ETypeAnnotationKind::Item:
    case ETypeAnnotationKind::World:
    case ETypeAnnotationKind::Error:
    case ETypeAnnotationKind::LastType:
        return nullptr;
    }
}

NKikimr::NMiniKQL::TType* BuildType(TPositionHandle pos, const TTypeAnnotationNode& annotation, NKikimr::NMiniKQL::TTypeBuilder& typeBuilder) {
    TStringStream err;
    auto type = BuildType(annotation, typeBuilder, err);
    if (!type) {
        ythrow TNodeException(pos) << err.Str();
    }
    return type;
}

NKikimr::NMiniKQL::TType* BuildType(const TExprNode& owner, const TTypeAnnotationNode& annotation, NKikimr::NMiniKQL::TTypeBuilder& typeBuilder) {
    return BuildType(owner.Pos(), annotation, typeBuilder);
}

const TTypeAnnotationNode* ConvertMiniKQLType(TPosition position, NKikimr::NMiniKQL::TType* type, TExprContext& ctx) {
    using namespace NKikimr::NMiniKQL;
    switch (type->GetKind()) {
    case TType::EKind::Type:
        return ctx.MakeType<TGenericExprType>();

    case TType::EKind::Void:
        return ctx.MakeType<TVoidExprType>();

    case TType::EKind::Data:
    {
        auto dataType = static_cast<TDataType*>(type);
        auto slot = NUdf::FindDataSlot(dataType->GetSchemeType());
        YQL_ENSURE(slot, "Unknown datatype: " << dataType->GetSchemeType());
        if (*slot == EDataSlot::Decimal) {
            const auto params = static_cast<TDataDecimalType*>(dataType)->GetParams();
            auto ret = ctx.MakeType<TDataExprParamsType>(*slot, ToString(params.first), ToString(params.second));
            YQL_ENSURE(ret->Validate(position, ctx));
            return ret;
        } else {
            return ctx.MakeType<TDataExprType>(*slot);
        }
    }

    case TType::EKind::Struct:
    {
        TVector<const TItemExprType*> items;
        auto structType = static_cast<TStructType*>(type);
        for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
            auto memberType = ConvertMiniKQLType(position, structType->GetMemberType(index), ctx);
            auto item = ctx.MakeType<TItemExprType>(structType->GetMemberName(index), memberType);
            YQL_ENSURE(item->Validate(position, ctx));
            items.push_back(item);
        }

        auto res = ctx.MakeType<TStructExprType>(items);
        YQL_ENSURE(res->Validate(position, ctx));
        return res;
    }

    case TType::EKind::List:
    {
        auto listType = static_cast<TListType*>(type);
        auto itemType = ConvertMiniKQLType(position, listType->GetItemType(), ctx);
        return ctx.MakeType<TListExprType>(itemType);
    }

    case TType::EKind::Optional:
    {
        auto optionalType = static_cast<TOptionalType*>(type);
        auto itemType = ConvertMiniKQLType(position, optionalType->GetItemType(), ctx);
        return ctx.MakeType<TOptionalExprType>(itemType);
    }

    case TType::EKind::Dict:
    {
        auto dictType = static_cast<TDictType*>(type);
        auto keyType = ConvertMiniKQLType(position, dictType->GetKeyType(), ctx);
        auto payloadType = ConvertMiniKQLType(position, dictType->GetPayloadType(), ctx);
        auto res = ctx.MakeType<TDictExprType>(keyType, payloadType);
        YQL_ENSURE(res->Validate(position, ctx));
        return res;
    }

    case TType::EKind::Callable:
    {
        TVector<TCallableExprType::TArgumentInfo> arguments;
        auto callableType = static_cast<TCallableType*>(type);
        TString payload;
        for (ui32 index = 0; index < callableType->GetArgumentsCount(); ++index) {
            auto argType = ConvertMiniKQLType(position, callableType->GetArgumentType(index), ctx);
            TCallableExprType::TArgumentInfo arg;
            arg.Type = argType;
            arguments.push_back(arg);
        }

        if (callableType->GetPayload()) {
            NKikimr::NMiniKQL::TTypeInfoHelper typeHelper;
            NKikimr::NUdf::TCallableTypeInspector callableInspector(typeHelper, callableType);
            payload = callableInspector.GetPayload();
            for (ui32 index = 0; index < callableType->GetArgumentsCount(); ++index) {
                auto& arg = arguments[index];
                arg.Name = callableInspector.GetArgumentName(index);
                arg.Flags = callableInspector.GetArgumentFlags(index);
            }
        }

        auto retType = ConvertMiniKQLType(position, callableType->GetReturnType(), ctx);
        return ctx.MakeType<TCallableExprType>(retType, arguments, callableType->GetOptionalArgumentsCount(), payload);
    }

    case TType::EKind::Any:
    case TType::EKind::ReservedKind:
        YQL_ENSURE(false, "Not supported");
        break;

    case TType::EKind::Tuple:
    {
        TTypeAnnotationNode::TListType elements;
        auto tupleType = static_cast<TTupleType*>(type);
        for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
            auto elementType = ConvertMiniKQLType(position, tupleType->GetElementType(index), ctx);
            elements.push_back(elementType);
        }

        auto res = ctx.MakeType<TTupleExprType>(elements);
        YQL_ENSURE(res->Validate(position, ctx));
        return res;
    }
    case TType::EKind::Resource:
    {
        auto resType = static_cast<TResourceType*>(type);
        return ctx.MakeType<TResourceExprType>(resType->GetTag());
    }

    case TType::EKind::Stream:
    {
        auto streamType = static_cast<TStreamType*>(type);
        auto itemType = ConvertMiniKQLType(position, streamType->GetItemType(), ctx);
        return ctx.MakeType<TStreamExprType>(itemType);
    }

    case TType::EKind::Variant:
    {
        auto variantType = static_cast<TVariantType*>(type);
        auto underlyingType = ConvertMiniKQLType(position, variantType->GetUnderlyingType(), ctx);
        return ctx.MakeType<TVariantExprType>(underlyingType);
    }

    case TType::EKind::Null:
        return ctx.MakeType<TNullExprType>();

    case TType::EKind::EmptyList:
        return ctx.MakeType<TEmptyListExprType>();

    case TType::EKind::EmptyDict:
        return ctx.MakeType<TEmptyDictExprType>();

    case TType::EKind::Tagged:
    {
        auto taggedType = static_cast<TTaggedType*>(type);
        auto baseType = ConvertMiniKQLType(position, taggedType->GetBaseType(), ctx);
        return ctx.MakeType<TTaggedExprType>(baseType, taggedType->GetTag());
    }

    case TType::EKind::Block:
    {
        auto blockType = static_cast<TBlockType*>(type);
        auto itemType = ConvertMiniKQLType(position, blockType->GetItemType(), ctx);
        if (blockType->GetShape() == NKikimr::NMiniKQL::TBlockType::EShape::Many) {
            return ctx.MakeType<TBlockExprType>(itemType);
        } else {
            return ctx.MakeType<TScalarExprType>(itemType);
        }
    }

    case TType::EKind::Flow:
    {
        auto flowType = static_cast<TFlowType*>(type);
        auto itemType = ConvertMiniKQLType(position, flowType->GetItemType(), ctx);
        return ctx.MakeType<TFlowExprType>(itemType);
    }

    case TType::EKind::Pg:
    {
        auto pgType = static_cast<TPgType*>(type);
        return ctx.MakeType<TPgExprType>(pgType->GetTypeId());
    }

    case TType::EKind::Multi:
    {
        TTypeAnnotationNode::TListType elements;
        auto multiType = static_cast<TMultiType*>(type);
        for (ui32 index = 0; index < multiType->GetElementsCount(); ++index) {
            auto elementType = ConvertMiniKQLType(position, multiType->GetElementType(index), ctx);
            elements.push_back(elementType);
        }

        auto res = ctx.MakeType<TMultiExprType>(elements);
        YQL_ENSURE(res->Validate(position, ctx));
        return res;
    }

    }

    YQL_ENSURE(false, "Unknown kind");
}

ETypeAnnotationKind ConvertMiniKQLTypeKind(NKikimr::NMiniKQL::TType* type) {
    using namespace NKikimr::NMiniKQL;
    switch (type->GetKind()) {
    case TType::EKind::Type:
        return ETypeAnnotationKind::Generic;
    case TType::EKind::Void:
        return ETypeAnnotationKind::Void;
    case TType::EKind::Data:
        return ETypeAnnotationKind::Data;
    case TType::EKind::Struct:
        return ETypeAnnotationKind::Struct;
    case TType::EKind::List:
        return ETypeAnnotationKind::List;
    case TType::EKind::Optional:
        return ETypeAnnotationKind::Optional;
    case TType::EKind::Dict:
        return ETypeAnnotationKind::Dict;
    case TType::EKind::Callable:
        return ETypeAnnotationKind::Callable;
    case TType::EKind::Any:
    case TType::EKind::ReservedKind:
        YQL_ENSURE(false, "Not supported");
        break;
    case TType::EKind::Tuple:
        return ETypeAnnotationKind::Tuple;
    case TType::EKind::Resource:
        return ETypeAnnotationKind::Resource;
    case TType::EKind::Stream:
        return ETypeAnnotationKind::Stream;
    case TType::EKind::Variant:
        return ETypeAnnotationKind::Variant;
    case TType::EKind::Null:
        return ETypeAnnotationKind::Null;
    case TType::EKind::EmptyList:
        return ETypeAnnotationKind::EmptyList;
    case TType::EKind::EmptyDict:
        return ETypeAnnotationKind::EmptyDict;
    case TType::EKind::Tagged:
        return ETypeAnnotationKind::Tagged;
    case TType::EKind::Block:
    {
        auto blockType = static_cast<TBlockType*>(type);
        if (blockType->GetShape() == NKikimr::NMiniKQL::TBlockType::EShape::Many) {
            return ETypeAnnotationKind::Block;
        } else {
            return ETypeAnnotationKind::Scalar;
        }
    }
    case TType::EKind::Flow:
        return ETypeAnnotationKind::Flow;
    case TType::EKind::Pg:
        return ETypeAnnotationKind::Pg;
    case TType::EKind::Multi:
        return ETypeAnnotationKind::Multi;
    }

    YQL_ENSURE(false, "Unknown kind");
}


} // namespace NCommon
} // namespace NYql
