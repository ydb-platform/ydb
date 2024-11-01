#include "kqp_types.h"

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/type_desc.h>

namespace NKikimr::NScheme {

void ProtoMiniKQLTypeFromTypeInfo(NKikimrMiniKQL::TType* type, const TTypeInfo typeInfo) {
    if (typeInfo.GetTypeId() == NTypeIds::Pg) {
        type->SetKind(NKikimrMiniKQL::Pg);
        type->MutablePg()->Setoid(NPg::PgTypeIdFromTypeDesc(typeInfo.GetPgTypeDesc()));
    } else {
        type->SetKind(NKikimrMiniKQL::Data);
        type->MutableData()->SetScheme(typeInfo.GetTypeId());
    }
}

TTypeInfo TypeInfoFromProtoMiniKQLType(const NKikimrMiniKQL::TType& type) {
    switch (type.GetKind()) {
    case NKikimrMiniKQL::Data:
        return TTypeInfo((NScheme::TTypeId)type.GetData().GetScheme());
    case NKikimrMiniKQL::Pg:
        return TTypeInfo(NPg::TypeDescFromPgTypeId(type.GetPg().Getoid()));
    default:
        Y_ENSURE(false, "not a data or pg type");
    }
}

const NMiniKQL::TType* MiniKQLTypeFromTypeInfo(const TTypeInfo typeInfo, const NMiniKQL::TTypeEnvironment& env) {
    if (typeInfo.GetTypeId() == NTypeIds::Pg) {
        return NMiniKQL::TPgType::Create(NPg::PgTypeIdFromTypeDesc(typeInfo.GetPgTypeDesc()), env);
    } else {
        return NMiniKQL::TDataType::Create((NUdf::TDataTypeId)typeInfo.GetTypeId(), env);
    }
}

TTypeInfo TypeInfoFromMiniKQLType(const NMiniKQL::TType* type) {
    switch (type->GetKind()) {
    case NMiniKQL::TType::EKind::Data:
        return TTypeInfo((NScheme::TTypeId)AS_TYPE(NMiniKQL::TDataType, type)->GetSchemeType());
    case NMiniKQL::TType::EKind::Pg:
        return TTypeInfo(NPg::TypeDescFromPgTypeId(AS_TYPE(NMiniKQL::TPgType, type)->GetTypeId()));
    default:
        Y_ENSURE(false, "not a data or pg type");
    }
}

} // namespace NKikimr::NScheme

