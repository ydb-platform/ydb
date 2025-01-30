#include "common.h"

#include <util/string/builder.h>

namespace NFq::NRowDispatcher {

//// TSchemaColumn

TString TSchemaColumn::ToString() const {
    return TStringBuilder() << "'" << Name << "' : " << TypeYson;
}

//// TCountersDesc

TCountersDesc TCountersDesc::CopyWithNewMkqlCountersName(const TString& mkqlCountersName) const {
    TCountersDesc result(*this);
    result.MkqlCountersName = mkqlCountersName;
    return result;
}

}  // namespace NFq::NRowDispatcher
