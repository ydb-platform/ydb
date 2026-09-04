#include "constructors.h"
#include "source.h"

namespace NKikimr::NOlap::NReader::NPlain {

std::shared_ptr<NCommon::IDataSource> TPortionSources::DoTryExtractNext(
    const std::shared_ptr<NCommon::TSpecialReadContext>& context, const ui32 /*inFlightCurrentLimit*/) {
    auto result = std::make_shared<TPortionDataSource>(
        SourceIdx++, Sources.front().GetPortion(), std::static_pointer_cast<TSpecialReadContext>(context), Sources.front().GetIsConflicting());
    Sources.pop_front();
    return result;
}

std::vector<TPortionInfo::TConstPtr> TPortionSources::GetConflictingPortions() const {
    std::vector<TPortionInfo::TConstPtr> result;
    for (auto&& i : Sources) {
        if (i.GetIsConflicting()) {
            result.emplace_back(i.GetPortion());
        }
    }
    return result;
}

}   // namespace NKikimr::NOlap::NReader::NPlain
