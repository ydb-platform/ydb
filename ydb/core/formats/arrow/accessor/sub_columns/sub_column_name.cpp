#include "json_value_path.h"
#include "sub_column_name.h"

namespace NKikimr::NArrow::NAccessor::NSubColumns {

TCanonicalSubColumnName TCanonicalSubColumnName::Parse(TStringBuf path) {
    return TCanonicalSubColumnName(ToSubcolumnName(path));
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
