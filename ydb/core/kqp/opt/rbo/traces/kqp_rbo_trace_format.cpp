#include "kqp_rbo_trace_format.h"

#include <algorithm>
#include <sstream>
#include <utility>
#include <util/string/builder.h>

namespace NKikimr {
namespace NKqp {

std::string ToStdString(const TString& value) {
    return std::string(value.c_str());
}

std::string FormatBool(bool value) {
    return value ? "true" : "false";
}

TString FormatInfoUnit(TInfoUnitId unit, const TInfoUnitRegistry& registry) {
    return registry.GetDebugName(unit);
}

std::string FormatCountedSummary(const std::vector<std::string>& items, size_t maxItems) {
    std::ostringstream out;
    out << "(" << items.size() << ")";
    if (items.empty()) {
        return out.str();
    }

    out << " ";
    const size_t limit = std::min(maxItems, items.size());
    for (size_t i = 0; i < limit; ++i) {
        if (i) {
            out << ", ";
        }
        out << items[i];
    }
    if (items.size() > limit) {
        out << ", ...";
    }
    return out.str();
}

std::string JoinStrings(const std::vector<std::string>& items, const char* delimiter) {
    std::ostringstream out;
    for (size_t i = 0; i < items.size(); ++i) {
        if (i) {
            out << delimiter;
        }
        out << items[i];
    }
    return out.str();
}

} // namespace NKqp
} // namespace NKikimr
