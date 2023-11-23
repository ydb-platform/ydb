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

NKikimr::NMiniKQL::TType* BuildType(const TTypeAnnotationNode& annotation, NKikimr::NMiniKQL::TProgramBuilder& pgmBuilder, IOutputStream& err) {
    switch (annotation.GetKind()) {
    case ETypeAnnotationKind::Data: {
        auto data = annotation.Cast<TDataExprType>();

        auto slot = data->GetSlot();
        const auto schemeType = NUdf::GetDataTypeInfo(slot).TypeId;
        if (NUdf::TDataType<NUdf::TDecimal>::Id == schemeType) {
            const auto params = static_cast<const TDataExprParamsType&>(annotation);
            return pgmBuilder.NewDecimalType(FromString<ui8>(params.GetParamOne()), FromString<ui8>(params.GetParamTwo()));
        } else {
            return pgmBuilder.NewDataType(schemeType);
        }
    }

    case ETypeAnnotationKind::Pg: {
        auto pg = annotation.Cast<TPgExprType>();
        return pgmBuilder.NewPgType(pg->GetId());
    }

    case ETypeAnnotationKind::Struct: {
        auto structObj = annotation.Cast<TStructExprType>();
        std::vector<std::pair<std::string_view, NKikimr::NMiniKQL::TType*>> members;
        members.reserve(structObj->GetItems().size());

        for (auto& item : structObj->GetItems()) {
            auto itemType = BuildType(*item->GetItemType(), pgmBuilder, err);
            if (!itemType) {
                return nullptr;
            }
            members.emplace_back(item->GetName(), itemType);
        }
        return pgmBuilder.NewStructType(members);
    }

    case ETypeAnnotationKind::List: {
        auto list = annotation.Cast<TListExprType>();
        auto itemType = BuildType(*list->GetItemType(), pgmBuilder, err);
        if (!itemType) {
            return nullptr;
        }
        return pgmBuilder.NewListType(itemType);
    }

    case ETypeAnnotationKind::Optional: {
        auto optional = annotation.Cast<TOptionalExprType>();
        auto itemType = BuildType(*optional->GetItemType(), pgmBuilder, err);
        if (!itemType) {
            return nullptr;
        }
        return pgmBuilder.NewOptionalType(itemType);
    }

    case ETypeAnnotationKind::Tuple: {
        auto tuple = annotation.Cast<TTupleExprType>();
        TVector<NKikimr::NMiniKQL::TType*> elements;
        elements.reserve(tuple->GetItems().size());
        for (auto& child : tuple->GetItems()) {
            elements.push_back(BuildType(*child, pgmBuilder, err));
            if (!elements.back()) {
                return nullptr;
            }
        }
        return pgmBuilder.NewTupleType(elements);
    }

    case ETypeAnnotationKind::Multi: {
        auto multi = annotation.Cast<TMultiExprType>();
        TVector<NKikimr::NMiniKQL::TType*> elements;
        elements.reserve(multi->GetItems().size());
        for (auto& child : multi->GetItems()) {
            elements.push_back(BuildType(*child, pgmBuilder, err));
            if (!elements.back()) {
                return nullptr;
            }
        }
        return pgmBuilder.NewMultiType(elements);
    }

    case ETypeAnnotationKind::Dict: {
        auto dictType = annotation.Cast<TDictExprType>();
        auto keyType = BuildType(*dictType->GetKeyType(), pgmBuilder, err);
        auto payloadType = BuildType(*dictType->GetPayloadType(), pgmBuilder, err);
        if (!keyType || !payloadType) {
            return nullptr;
        }
        return pgmBuilder.NewDictType(keyType, payloadType, false);
    }

    case ETypeAnnotationKind::Type: {
        auto type = annotation.Cast<TTypeExprType>()->GetType();
        return BuildType(*type, pgmBuilder, err);
    }

    case ETypeAnnotationKind::Void: {
        return pgmBuilder.NewVoid().GetStaticType();
    }

    case ETypeAnnotationKind::Null: {
        return pgmBuilder.NewNull().GetStaticType();
    }

    case ETypeAnnotationKind::Callable: {
        auto callable = annotation.Cast<TCallableExprType>();
        auto returnType = BuildType(*callable->GetReturnType(), pgmBuilder, err);
        NKikimr::NMiniKQL::TCallableTypeBuilder callableTypeBuilder(pgmBuilder.GetTypeEnvironment(), "", returnType);
        for (auto& child : callable->GetArguments()) {
            callableTypeBuilder.Add(BuildType(*child.Type, pgmBuilder, err));
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
        return pgmBuilder.NewVoid().GetStaticType()->GetType();

    case ETypeAnnotationKind::Resource:
        return pgmBuilder.NewResourceType(annotation.Cast<TResourceExprType>()->GetTag());

    case ETypeAnnotationKind::Tagged: {
        auto tagged = annotation.Cast<TTaggedExprType>();
        auto base = BuildType(*tagged->GetBaseType(), pgmBuilder, err);
        return pgmBuilder.NewTaggedType(base, tagged->GetTag());
    }

    case ETypeAnnotationKind::Variant: {
        auto var = annotation.Cast<TVariantExprType>();
        auto underlyingType = BuildType(*var->GetUnderlyingType(), pgmBuilder, err);
        if (!underlyingType) {
            return nullptr;
        }
        return pgmBuilder.NewVariantType(underlyingType);
    }

    case ETypeAnnotationKind::Stream: {
        auto stream = annotation.Cast<TStreamExprType>();
        auto itemType = BuildType(*stream->GetItemType(), pgmBuilder, err);
        if (!itemType) {
            return nullptr;
        }
        return pgmBuilder.NewStreamType(itemType);
    }

    case ETypeAnnotationKind::Flow: {
        auto flow = annotation.Cast<TFlowExprType>();
        auto itemType = BuildType(*flow->GetItemType(), pgmBuilder, err);
        if (!itemType) {
            return nullptr;
        }
        return pgmBuilder.NewFlowType(itemType);
    }

    case ETypeAnnotationKind::EmptyList: {
        if (NKikimr::NMiniKQL::RuntimeVersion < 11) {
            auto voidType = pgmBuilder.NewVoid().GetStaticType();
            return pgmBuilder.NewListType(voidType);
        }

        return pgmBuilder.GetTypeEnvironment().GetTypeOfEmptyList();
    }

    case ETypeAnnotationKind::EmptyDict: {
        if (NKikimr::NMiniKQL::RuntimeVersion < 11) {
            auto voidType = pgmBuilder.NewVoid().GetStaticType();
            return pgmBuilder.NewDictType(voidType, voidType, false);
        }

        return pgmBuilder.GetTypeEnvironment().GetTypeOfEmptyDict();
    }

    case ETypeAnnotationKind::Block: {
        auto block = annotation.Cast<TBlockExprType>();
        auto itemType = BuildType(*block->GetItemType(), pgmBuilder, err);
        if (!itemType) {
            return nullptr;
        }
        return pgmBuilder.NewBlockType(itemType, NKikimr::NMiniKQL::TBlockType::EShape::Many);
    }

    case ETypeAnnotationKind::Scalar: {
        auto scalar = annotation.Cast<TScalarExprType>();
        auto itemType = BuildType(*scalar->GetItemType(), pgmBuilder, err);
        if (!itemType) {
            return nullptr;
        }
        return pgmBuilder.NewBlockType(itemType, NKikimr::NMiniKQL::TBlockType::EShape::Scalar);
    }

    case ETypeAnnotationKind::Item:
    case ETypeAnnotationKind::World:
    case ETypeAnnotationKind::Error:
    case ETypeAnnotationKind::LastType:
        return nullptr;
    }
}

NKikimr::NMiniKQL::TType* BuildType(TPositionHandle pos, const TTypeAnnotationNode& annotation, NKikimr::NMiniKQL::TProgramBuilder& pgmBuilder) {
    TStringStream err;
    auto type = BuildType(annotation, pgmBuilder, err);
    if (!type) {
        ythrow TNodeException(pos) << err.Str();
    }
    return type;
}

NKikimr::NMiniKQL::TType* BuildType(const TExprNode& owner, const TTypeAnnotationNode& annotation, NKikimr::NMiniKQL::TProgramBuilder& pgmBuilder) {
    return BuildType(owner.Pos(), annotation, pgmBuilder);
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

} // namespace NCommon
} // namespace NYql
