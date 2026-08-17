#pragma once

#include "../kqp_info_unit.h"

#include <cstddef>
#include <string>
#include <vector>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr {
namespace NKqp {

std::string ToStdString(const TString& value);
std::string FormatBool(bool value);
TString FormatInfoUnit(const TInfoUnit& unit);
TString FormatInfoUnits(const TVector<TInfoUnit>& units);
std::vector<std::string> MakeInfoUnitItems(const TVector<TInfoUnit>& units);
TVector<TInfoUnit> SortInfoUnits(TVector<TInfoUnit> units);
TVector<TInfoUnit> SortInfoUnitSet(const TInfoUnitSet& units);
TVector<TInfoUnit> UniqueInfoUnits(const TVector<TInfoUnit>& units);
std::string FormatCountedSummary(const std::vector<std::string>& items, size_t maxItems = 6);
std::string JoinStrings(const std::vector<std::string>& items, const char* delimiter = ", ");

} // namespace NKqp
} // namespace NKikimr
