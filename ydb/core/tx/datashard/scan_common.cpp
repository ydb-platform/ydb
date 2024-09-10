#include "scan_common.h"
#include <ydb/core/tx/datashard/datashard_user_table.h>

namespace NKikimr::NDataShard {

TColumnsTags GetAllTags(const TUserTable& tableInfo) {
    TColumnsTags result;

    for (const auto& it : tableInfo.Columns) {
        result[it.second.Name] = it.first;
    }

    return result;
}

void AddTags(TTags& tags, const TColumnsTags& allTags, TProtoColumnsCRef columns) {
    for (const auto& colName : columns) {
        tags.push_back(allTags.at(colName));
    }
}

TColumnsTypes GetAllTypes(const TUserTable& tableInfo) {
    TColumnsTypes result;

    for (const auto& it : tableInfo.Columns) {
        result[it.second.Name] = it.second.Type;
    }

    return result;
}

void ProtoYdbTypeFromTypeInfo(Ydb::Type* type, const NScheme::TTypeInfo typeInfo) {
    if (typeInfo.GetTypeId() == NScheme::NTypeIds::Pg) {
        auto* typeDesc = typeInfo.GetTypeDesc();
        auto* pg = type->mutable_pg_type();
        pg->set_type_name(NPg::PgTypeNameFromTypeDesc(typeDesc));
        pg->set_oid(NPg::PgTypeIdFromTypeDesc(typeDesc));
    } else {
        type->set_type_id((Ydb::Type::PrimitiveTypeId)typeInfo.GetTypeId());
    }
}

}
