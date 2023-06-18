#include "not_sorted.h"

namespace NKikimr::NOlap::NIndexedReader {

std::vector<TGranule::TPtr> TNonSorting::DoDetachReadyGranules(TResultController& granulesToOut) {
    std::vector<TGranule::TPtr> result;
    result.reserve(granulesToOut.GetCount());
    while (granulesToOut.GetCount()) {
        result.emplace_back(granulesToOut.ExtractFirst());
    }
    return result;
}

}
