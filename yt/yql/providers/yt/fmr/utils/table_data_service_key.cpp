#include "table_data_service_key.h"
#include <util/string/builder.h>

namespace NYql::NFmr {

TString GetTableDataServiceKey(const TString& tableId, const TString& partId, const ui64 chunk) {
    return TStringBuilder() << tableId << "_" << partId << "_" << chunk;
}

} // namespace NYql::NFmr
