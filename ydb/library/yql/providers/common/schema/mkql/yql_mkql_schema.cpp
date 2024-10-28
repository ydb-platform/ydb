#include "yql_mkql_schema.h"

#include <ydb/library/yql/providers/common/schema/parser/yql_type_parser.h>
#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/mkql_runtime_version.h>
#include <ydb/library/yql/public/udf/udf_type_inspection.h>
#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/node/node_builder.h>
#include <library/cpp/yson/writer.h>

#include <util/stream/str.h>

namespace NYql {
namespace NCommon {

template <template<typename> class TSaver>
class TRuntimeTypeSaver: public TSaver<TRuntimeTypeSaver<TSaver>> {
    typedef TSaver<TRuntimeTypeSaver> TBase;

    struct TCallableAdaptor {
        const NKikimr::NMiniKQL::TCallableType* Type;
        NKikimr::NMiniKQL::TTypeInfoHelper TypeHelper;
        NKikimr::NUdf::TCallableTypeInspector CallableInspector;

        TCallableAdaptor(const NKikimr::NMiniKQL::TCallableType* type)
            : Type(type)
            , TypeHelper()
            , CallableInspector(TypeHelper, Type)
        {
        }

        size_t GetOptionalArgsCount() const {
            return CallableInspector.GetOptionalArgsCount();
        }

        TStringBuf GetPayload() const {
            return CallableInspector.GetPayload();
        }

        NKikimr::NMiniKQL::TType* GetReturnType() const {
            return Type->GetReturnType();
        }

        size_t GetArgumentsCount() const {
            return Type->GetArgumentsCount();
        }

        TStringBuf GetArgumentName(size_t i) const {
            return CallableInspector.GetArgumentName(i);
        }

        ui64 GetArgumentFlags(size_t i) const {
            return CallableInspector.GetArgumentFlags(i);
        }

        NKikimr::NMiniKQL::TType* GetArgumentType(size_t i) const {
            return Type->GetArgumentType(i);
        }
    };

public:
    TRuntimeTypeSaver(typename TBase::TConsumer& consumer)
        : TBase(consumer, false)
    {
    }

    void Save(const NKikimr::NMiniKQL::TType* type) {
        using namespace NKikimr;
        using namespace NKikimr::NMiniKQL;

        switch (type->GetKind()) {
            case TType::EKind::Type:
                TBase::SaveType();
                break;
            case TType::EKind::Void:
                TBase::SaveVoidType();
                break;
            case TType::EKind::Null:
                TBase::SaveNullType();
                break;
            case TType::EKind::EmptyList:
                TBase::SaveEmptyListType();
                break;
            case TType::EKind::EmptyDict:
                TBase::SaveEmptyDictType();
                break;
            case TType::EKind::Data: {
                const auto schemeType = static_cast<const TDataType*>(type)->GetSchemeType();
                auto slot = NUdf::FindDataSlot(schemeType);
                if (!slot) {
                    ythrow yexception() << "Unsupported data type: " << schemeType;
                }

                auto dataType = NUdf::GetDataTypeInfo(*slot).Name;
                if (NKikimr::NUdf::TDataType<NUdf::TDecimal>::Id == schemeType) {
                    const auto params = static_cast<const TDataDecimalType*>(type)->GetParams();
                    TBase::SaveDataTypeParams(dataType, ToString(params.first), ToString(params.second));
                } else {
                    TBase::SaveDataType(dataType);
                }
                break;
            }
            case TType::EKind::Pg: {
                const auto name = static_cast<const TPgType*>(type)->GetName();
                TBase::SavePgType(name);
                break;
            }
            case TType::EKind::Struct:
                TBase::SaveStructType(*static_cast<const TStructType*>(type));
                break;
            case TType::EKind::List:
                TBase::SaveListType(*static_cast<const TListType*>(type));
                break;
            case TType::EKind::Optional:
                TBase::SaveOptionalType(*static_cast<const TOptionalType*>(type));
                break;
            case TType::EKind::Dict:
                TBase::SaveDictType(*static_cast<const TDictType*>(type));
                break;
            case TType::EKind::Callable:
                TBase::SaveCallableType(TCallableAdaptor(static_cast<const TCallableType*>(type)));
                break;
            case TType::EKind::Tuple:
                TBase::SaveTupleType(*static_cast<const TTupleType*>(type));
                break;
            case TType::EKind::Resource:
                TBase::SaveResourceType(static_cast<const TResourceType*>(type)->GetTag());
                break;
            case TType::EKind::Variant:
                TBase::SaveVariantType(*static_cast<const TVariantType*>(type));
                break;
            case TType::EKind::Stream:
                TBase::SaveStreamType(*static_cast<const TStreamType*>(type));
                break;
            case TType::EKind::Tagged:
                TBase::SaveTaggedType(*static_cast<const TTaggedType*>(type));
                break;
            default:
                YQL_ENSURE(false, "Unsupported type kind:" << (ui32)type->GetKind());
        }
    }
};

void WriteTypeToYson(NYson::TYsonConsumerBase& writer, const NKikimr::NMiniKQL::TType* type) {
    TRuntimeTypeSaver<TYqlTypeYsonSaverImpl> saver(writer);
    saver.Save(type);
}

NYT::TNode TypeToYsonNode(const NKikimr::NMiniKQL::TType* type) {
    NYT::TNode res;
    NYT::TNodeBuilder builder(&res);
    WriteTypeToYson(builder, type);
    return res;
}

TString WriteTypeToYson(const NKikimr::NMiniKQL::TType* type, NYson::EYsonFormat format) {
    TStringStream stream;
    NYson::TYsonWriter writer(&stream, format);
    WriteTypeToYson(writer, type);
    return stream.Str();
}

struct TRuntimeTypeLoader {
    typedef NKikimr::NMiniKQL::TType* TType;

    NKikimr::NMiniKQL::TProgramBuilder& Builder;
    IOutputStream& Err;

    TRuntimeTypeLoader(NKikimr::NMiniKQL::TProgramBuilder& builder, IOutputStream& err)
        : Builder(builder)
        , Err(err)
    {
    }
    TMaybe<TType> LoadVoidType(ui32 /*level*/) {
        return Builder.NewVoid().GetStaticType();
    }
    TMaybe<TType> LoadNullType(ui32 /*level*/) {
        return Builder.NewNull().GetStaticType();
    }
    TMaybe<TType> LoadUnitType(ui32 /*level*/) {
        return Builder.NewVoid().GetStaticType();
    }
    TMaybe<TType> LoadGenericType(ui32 /*level*/) {
        return Builder.GetTypeEnvironment().GetTypeOfTypeLazy();
    }
    TMaybe<TType> LoadEmptyListType(ui32 /*level*/) {
        if (NKikimr::NMiniKQL::RuntimeVersion < 11) {
            return Builder.NewListType(Builder.NewVoid().GetStaticType());
        }

        return Builder.GetTypeEnvironment().GetTypeOfEmptyListLazy();
    }
    TMaybe<TType> LoadEmptyDictType(ui32 /*level*/) {
        if (NKikimr::NMiniKQL::RuntimeVersion < 11) {
            return Builder.NewDictType(Builder.NewVoid().GetStaticType(), Builder.NewVoid().GetStaticType(), false);
        }

        return Builder.GetTypeEnvironment().GetTypeOfEmptyDictLazy();
    }
    TMaybe<TType> LoadDataType(const TString& dataType, ui32 /*level*/) {
        const auto slot = NUdf::FindDataSlot(dataType);
        if (!slot) {
            Err << "Unsupported data type: " << dataType;
            return Nothing();
        }

        if (NKikimr::NUdf::EDataSlot::Decimal == slot) {
            Err << "Decimal type without parameters.";
            return Nothing();
        }

        return Builder.NewDataType(NUdf::GetDataTypeInfo(*slot).TypeId);
    }

    TMaybe<TType> LoadPgType(const TString& pgType, ui32 /*level*/) {
        auto typeId = NYql::NPg::HasType(pgType) ? NYql::NPg::LookupType(pgType).TypeId : Max<ui32>();
        return Builder.NewPgType(typeId);
    }

    TMaybe<TType> LoadDataTypeParams(const TString& dataType, const TString& paramOne, const TString& paramTwo, ui32 /*level*/) {
        const auto slot = NUdf::FindDataSlot(dataType);
        if (!slot) {
            Err << "Unsupported data type: " << dataType;
            return Nothing();
        }

        if (NKikimr::NUdf::EDataSlot::Decimal != slot) {
            Err << "Unexpected parameters for type: " << dataType;
            return Nothing();
        }

        return Builder.NewDecimalType(FromString<ui8>(paramOne), FromString<ui8>(paramTwo));
    }

    TMaybe<TType> LoadResourceType(const TString& tag, ui32 /*level*/) {
        return Builder.NewResourceType(tag);
    }
    TMaybe<TType> LoadTaggedType(TType baseType, const TString& /*tag*/, ui32 /*level*/) {
        return baseType;
    }
    TMaybe<TType> LoadErrorType(ui32 /*row*/, ui32 /*column*/, const TString& /*file*/, const TString& msg, ui32 /*level*/) {
        Err << msg;
        return Nothing();
    }
    TMaybe<TType> LoadStructType(const TVector<std::pair<TString, TType>>& members, ui32 /*level*/) {
        auto structType = Builder.NewEmptyStructType();
        for (auto& member : members) {
            structType = Builder.NewStructType(structType, member.first, member.second);
        }
        return structType;
    }
    TMaybe<TType> LoadListType(TType itemType, ui32 /*level*/) {
        return Builder.NewListType(itemType);
    }
    TMaybe<TType> LoadStreamType(TType itemType, ui32 /*level*/) {
        return Builder.NewStreamType(itemType);
    }
    TMaybe<TType> LoadOptionalType(TType itemType, ui32 /*level*/) {
        return Builder.NewOptionalType(itemType);
    }
    TMaybe<TType> LoadTupleType(const TVector<TType>& elements, ui32 /*level*/) {
        return Builder.NewTupleType(elements);
    }
    TMaybe<TType> LoadDictType(TType keyType, TType valType, ui32 /*level*/) {
        return Builder.NewDictType(keyType, valType, false);
    }
    TMaybe<TType> LoadCallableType(TType returnType, const TVector<TType>& argTypes, const TVector<TString>& argNames,
        const TVector<ui64>& argFlags, size_t optionalCount, const TString& payload, ui32 /*level*/) {

        YQL_ENSURE(argTypes.size() == argNames.size() && argTypes.size() == argFlags.size());

        NKikimr::NMiniKQL::TCallableTypeBuilder callableTypeBuilder(Builder.GetTypeEnvironment(), "", returnType);
        for (size_t i = 0; i < argTypes.size(); ++i) {
            callableTypeBuilder.Add(argTypes[i]);
            if (!argNames[i].empty()) {
                callableTypeBuilder.SetArgumentName(argNames[i]);
            }

            if (argFlags[i] != 0) {
                callableTypeBuilder.SetArgumentFlags(argFlags[i]);
            }
        }

        callableTypeBuilder.SetOptionalArgs(optionalCount);
        if (!payload.empty()) {
            callableTypeBuilder.SetPayload(payload);
        }
        return callableTypeBuilder.Build();
    }
    TMaybe<TType> LoadVariantType(TType underlyingType, ui32 /*level*/) {
        return Builder.NewVariantType(underlyingType);
    }
    void Error(const TString& info) {
        Err << info;
    }
};

NKikimr::NMiniKQL::TType* ParseTypeFromYson(const TStringBuf yson, NKikimr::NMiniKQL::TProgramBuilder& builder, IOutputStream& err) {
    NYT::TNode node;
    if (!ParseYson(node, yson, err)) {
        return nullptr;
    }

    TRuntimeTypeLoader loader(builder, err);
    return DoLoadTypeFromYson(loader, node, 0).GetOrElse(nullptr);
}

NKikimr::NMiniKQL::TType* ParseTypeFromYson(const NYT::TNode& node, NKikimr::NMiniKQL::TProgramBuilder& builder, IOutputStream& err) {
    TRuntimeTypeLoader loader(builder, err);
    return DoLoadTypeFromYson(loader, node, 0).GetOrElse(nullptr);
}

struct TOrderAwareRuntimeTypeLoader: public TRuntimeTypeLoader {
    typedef NKikimr::NMiniKQL::TType* TType;

    NCommon::TCodecContext& Ctx;

    TOrderAwareRuntimeTypeLoader(NCommon::TCodecContext& ctx, IOutputStream& err)
        : TRuntimeTypeLoader(ctx.Builder, err)
        , Ctx(ctx)
    {
    }
    TMaybe<TType> LoadStructType(const TVector<std::pair<TString, TType>>& members, ui32 level) {
        auto type = Builder.NewEmptyStructType();
        bool sorted = true;
        TString prev;
        for (auto& member : members) {
            if (member.first < prev) {
                sorted = false;
            }
            prev = member.first;
            type = Builder.NewStructType(type, member.first, member.second);
        }
        if (level > 0 && !sorted) {
            std::vector<size_t> reorder(members.size(), 0);
            using namespace NKikimr::NMiniKQL;
            auto structType = AS_TYPE(TStructType, type);
            size_t i = 0;
            for (auto& member : members) {
                reorder[i++] = structType->GetMemberIndex(member.first);
            }
            Ctx.StructReorders.push_back(std::move(reorder));
            type->SetCookie((ui64)&Ctx.StructReorders.back());
        }
        return type;
    }
};

NKikimr::NMiniKQL::TType* ParseOrderAwareTypeFromYson(const NYT::TNode& node, TCodecContext& ctx, IOutputStream& err) {
    TOrderAwareRuntimeTypeLoader loader(ctx, err);
    return DoLoadTypeFromYson(loader, node, 0).GetOrElse(nullptr);
}

} // namespace NCommon
} // namespace NYql
