#include "scheme_type_info.h"

#include <ydb/public/lib/scheme_types/scheme_type_id.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/type_desc.h>

namespace NKikimr::NScheme {

const char* TypeName(const TTypeInfo typeInfo) {
    if (typeInfo.GetTypeId() == NScheme::NTypeIds::Pg) {
        return NPg::PgTypeNameFromTypeDesc(typeInfo.GetTypeDesc());
    } else {
        return TypeName(typeInfo.GetTypeId());
    }
}

} // namespace NKikimr::NScheme
