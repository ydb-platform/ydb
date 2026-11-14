#include "kqp_types.h"

#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr::NScheme {

void ProtoMiniKQLTypeFromTypeInfo(NKikimrMiniKQL::TType* type, const TTypeInfo typeInfo) {
    switch (typeInfo.GetTypeId()) {
    case NTypeIds::Decimal: {
        const TDecimalType& decimal = typeInfo.GetDecimalType();
        type->SetKind(NKikimrMiniKQL::Data);
        type->MutableData()->SetScheme(NTypeIds::Decimal);
        type->MutableData()->MutableDecimalParams()->SetPrecision(decimal.GetPrecision());
        type->MutableData()->MutableDecimalParams()->SetScale(decimal.GetScale());
        break;
    }
    default: {
        type->SetKind(NKikimrMiniKQL::Data);
        type->MutableData()->SetScheme(typeInfo.GetTypeId());
        break;
    }
    }
}

TTypeInfo TypeInfoFromProtoMiniKQLType(const NKikimrMiniKQL::TType& type) {
    switch (type.GetKind()) {
    case NKikimrMiniKQL::Data: {
        NScheme::TTypeId typeId = type.GetData().GetScheme();
        if (typeId == NTypeIds::Decimal) {
            TDecimalType decimal(type.GetData().GetDecimalParams().GetPrecision(), type.GetData().GetDecimalParams().GetScale());
            return TTypeInfo(decimal);
        }
        else {
            return TTypeInfo(typeId);
        }
    }
    default:
        Y_ENSURE(false, "not a data type");
    }
}

TTypeInfo TypeInfoFromMiniKQLType(const NMiniKQL::TType* type) {
    const NMiniKQL::TDataType* dataType = type->GetKind() == NKikimr::NMiniKQL::TType::EKind::Optional ?
        AS_TYPE(NMiniKQL::TDataType, static_cast<const NKikimr::NMiniKQL::TOptionalType*>(type)->GetItemType()) :
        AS_TYPE(NMiniKQL::TDataType, type);
    Y_ENSURE(dataType->GetKind() == NMiniKQL::TType::EKind::Data, "data type is expected");
    
    NScheme::TTypeId typeId = dataType->GetSchemeType();
    const NUdf::EDataSlot dataSlot = *dataType->GetDataSlot();
    if (dataSlot == NUdf::EDataSlot::Decimal) {
        Y_ENSURE(typeId == NScheme::NTypeIds::Decimal, "decimal typeid is expected");
        const auto memberDataDecimalType = static_cast<const NKikimr::NMiniKQL::TDataDecimalType*>(dataType);
        auto [precision, scale] = memberDataDecimalType->GetParams();
        return NScheme::TDecimalType(precision, scale);
    }

    return TTypeInfo(typeId);
}

} // namespace NKikimr::NScheme

