#include "kqp_types.h"

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/type_desc.h>

namespace NKikimr::NScheme {

void ProtoMiniKQLTypeFromTypeInfo(NKikimrMiniKQL::TType* type, const TTypeInfo typeInfo) {
    switch (typeInfo.GetTypeId()) {
    case NTypeIds::Pg: {
        type->SetKind(NKikimrMiniKQL::Pg);
        type->MutablePg()->Setoid(NPg::PgTypeIdFromTypeDesc(typeInfo.GetPgTypeDesc()));
        break;
    }
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
    case NKikimrMiniKQL::Pg:
        return TTypeInfo(NPg::TypeDescFromPgTypeId(type.GetPg().Getoid()));
    default:
        Y_ENSURE(false, "not a data or pg type");
    }
}

TTypeInfo TypeInfoFromMiniKQLType(const NMiniKQL::TType* type) {
    switch (type->GetKind()) {
    case NMiniKQL::TType::EKind::Data: {
        NScheme::TTypeId typeId = AS_TYPE(NMiniKQL::TDataType, type)->GetSchemeType();
        Y_ENSURE(typeId != NScheme::NTypeIds::Decimal, "Decimal is no supported");
        return TTypeInfo(typeId);
    }
    case NMiniKQL::TType::EKind::Pg:
        return TTypeInfo(NPg::TypeDescFromPgTypeId(AS_TYPE(NMiniKQL::TPgType, type)->GetTypeId()));
    default:
        Y_ENSURE(false, "not a data or pg type");
    }
}

} // namespace NKikimr::NScheme

