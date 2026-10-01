#pragma once

#include "../kqp_info_unit.h"

#include <cstddef>
#include <string>
#include <vector>
#include <util/string/builder.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr {
namespace NKqp {

std::string ToStdString(const TString& value);
std::string FormatBool(bool value);
TString FormatInfoUnit(TInfoUnitId unit, const TInfoUnitRegistry& registry);

template <class TRange>
TString FormatInfoUnits(const TRange& units, const TInfoUnitRegistry& registry) {
    TStringBuilder result;
    TStringBuf separator;
    for (const auto unit : units) {
        result << separator << FormatInfoUnit(unit, registry);
        separator = ", ";
    }
    return result;
}

template <class TRange>
std::vector<std::string> MakeInfoUnitItems(const TRange& units, const TInfoUnitRegistry& registry) {
    std::vector<std::string> items;
    for (const auto unit : units) {
        items.push_back(ToStdString(FormatInfoUnit(unit, registry)));
    }
    return items;
}
std::string FormatCountedSummary(const std::vector<std::string>& items, size_t maxItems = 6);
std::string JoinStrings(const std::vector<std::string>& items, const char* delimiter = ", ");

} // namespace NKqp
} // namespace NKikimr
