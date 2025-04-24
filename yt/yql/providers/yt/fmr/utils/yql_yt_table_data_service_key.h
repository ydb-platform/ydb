#include <util/generic/fwd.h>

namespace NYql::NFmr {

TString GetTableDataServiceKey(const TString& tableId, const TString& partId, const ui64 chunk);

} // namespace NYql::NFmr
