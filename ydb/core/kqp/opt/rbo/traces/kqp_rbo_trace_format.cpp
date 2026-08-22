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

TString FormatInfoUnit(const TInfoUnit& unit) {
    if (unit.GetAlias().empty()) {
        return unit.GetColumnName();
    }
    return TStringBuilder() << unit.GetAlias() << "." << unit.GetColumnName();
}

TString FormatInfoUnits(const TVector<TInfoUnit>& units) {
    TStringBuilder result;
    for (size_t i = 0; i < units.size(); ++i) {
        if (i) {
            result << ", ";
        }
        result << FormatInfoUnit(units[i]);
    }
    return result;
}

std::vector<std::string> MakeInfoUnitItems(const TVector<TInfoUnit>& units) {
    std::vector<std::string> items;
    items.reserve(units.size());
    for (const auto& unit : units) {
        items.push_back(ToStdString(FormatInfoUnit(unit)));
    }
    return items;
}

TVector<TInfoUnit> SortInfoUnits(TVector<TInfoUnit> units) {
    std::sort(units.begin(), units.end(), [](const TInfoUnit& lhs, const TInfoUnit& rhs) {
        return lhs.GetFullName() < rhs.GetFullName();
    });
    return units;
}

TVector<TInfoUnit> SortInfoUnitSet(const TInfoUnitSet& units) {
    TVector<TInfoUnit> result;
    result.reserve(units.size());
    for (const auto& unit : units) {
        result.push_back(unit);
    }
    return SortInfoUnits(std::move(result));
}

TVector<TInfoUnit> UniqueInfoUnits(const TVector<TInfoUnit>& units) {
    TVector<TInfoUnit> result;
    TInfoUnitSet seen;
    result.reserve(units.size());
    for (const auto& unit : units) {
        if (seen.insert(unit).second) {
            result.push_back(unit);
        }
    }
    return result;
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
