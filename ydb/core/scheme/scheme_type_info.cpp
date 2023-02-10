#include "scheme_type_info.h"

#include <ydb/public/lib/scheme_types/scheme_type_id.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/type_desc.h>

namespace NKikimr::NScheme {

::TString TypeName(const TTypeInfo typeInfo, const ::TString& typeMod) {
    if (typeInfo.GetTypeId() == NScheme::NTypeIds::Pg) {
        return NPg::PgTypeNameFromTypeDesc(typeInfo.GetTypeDesc(), typeMod);
    } else {
        return TypeName(typeInfo.GetTypeId());
    }
}

} // namespace NKikimr::NScheme
