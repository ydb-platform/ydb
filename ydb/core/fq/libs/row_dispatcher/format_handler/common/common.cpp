#include "common.h"

#include <util/string/builder.h>

namespace NFq::NRowDispatcher {

//// TSchemaColumn

TString TSchemaColumn::ToString() const {
    return TStringBuilder() << "'" << Name << "' : " << TypeYson;
}

//// TCountersDesc

TCountersDesc TCountersDesc::SetPath(const TString& countersPath) const {
    TCountersDesc result(*this);
    result.CountersPath = countersPath;
    return result;
}

}  // namespace NFq::NRowDispatcher
