#include "events.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

TEvRequestFilter::TEvRequestFilter(const TPortionDataSource& source, const std::shared_ptr<IFilterSubscriber>& subscriber)
    : ExternalTaskId(source.GetContext()->GetCommonContext()->GetReadMetadata()->GetScanIdentifier())
    , MinPK(source.GetPortionInfo().IndexKeyStart())
    , MaxPK(source.GetPortionInfo().IndexKeyEnd())
    , PortionId(source.GetPortionId())
    , RecordsCount(source.GetRecordsCount())
    , MaxVersion(source.GetContext()->GetCommonContext()->GetReadMetadata()->GetRequestSnapshot())
    , Subscriber(subscriber)
    , AbortionFlag(source.GetContext()->GetCommonContext()->GetAbortionFlag())
{
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
