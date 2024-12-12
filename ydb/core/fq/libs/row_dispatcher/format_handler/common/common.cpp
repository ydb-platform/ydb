#include "common.h"

#include <util/string/builder.h>

namespace NFq::NRowDispatcher {

//// TSchemaColumn

TString TSchemaColumn::ToString() const {
    return TStringBuilder() << "'" << Name << "' : " << TypeYson;
}

}  // namespace NFq::NRowDispatcher
