#include "events.h"

#include <ydb/core/tx/columnshard/engines/reader/trivial_reader/iterator/source.h>

namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering {

TEvRequestFilter::TEvRequestFilter(const TPortionDataSource& source, const std::shared_ptr<IFilterSubscriber>& subscriber)
    : MinPK(source.GetPortionInfo().IndexKeyStart())
    , MaxPK(source.GetPortionInfo().IndexKeyEnd())
    , PortionId(source.GetPortionInfo().GetPortionId())
    , RecordsCount(source.GetRecordsCount())
    , MaxVersion(source.GetContext()->GetCommonContext()->GetReadMetadata()->GetRequestSnapshot())
    , Subscriber(subscriber)
    , AbortionFlag(source.GetContext()->GetCommonContext()->GetAbortionFlag())
{
}

TEvRequestFilter::TEvRequestFilter(const NArrow::TSimpleRow& minPK, const NArrow::TSimpleRow& maxPK, const ui64 portionId,
    const ui64 recordsCount, const TSnapshot& maxVersion, const std::shared_ptr<IFilterSubscriber>& subscriber,
    const std::shared_ptr<const TAtomicCounter>& abortionFlag)
    : MinPK(minPK)
    , MaxPK(maxPK)
    , PortionId(portionId)
    , RecordsCount(recordsCount)
    , MaxVersion(maxVersion)
    , Subscriber(subscriber)
    , AbortionFlag(abortionFlag)
{
}

TSnapshot TEvRequestFilter::GetMaxVersion() const {
    return MaxVersion;
}

}   // namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering
