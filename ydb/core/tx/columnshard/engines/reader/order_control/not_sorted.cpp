#include "not_sorted.h"

namespace NKikimr::NOlap::NIndexedReader {

std::vector<TGranule*> TNonSorting::DoDetachReadyGranules(THashMap<ui64, NIndexedReader::TGranule*>& granulesToOut) {
    std::vector<TGranule*> result;
    result.reserve(granulesToOut.size());
    for (auto&& i : granulesToOut) {
        result.emplace_back(i.second);
    }
    granulesToOut.clear();
    return result;
}

}
