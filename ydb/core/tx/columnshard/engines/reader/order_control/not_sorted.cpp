#include "not_sorted.h"

namespace NKikimr::NOlap::NIndexedReader {

std::vector<TGranule::TPtr> TNonSorting::DoDetachReadyGranules(THashMap<ui64, NIndexedReader::TGranule::TPtr>& granulesToOut) {
    std::vector<TGranule::TPtr> result;
    result.reserve(granulesToOut.size());
    for (auto&& i : granulesToOut) {
        result.emplace_back(i.second);
    }
    granulesToOut.clear();
    return result;
}

}
