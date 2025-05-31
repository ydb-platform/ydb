#include "events.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

TEvRequestFilter::TEvRequestFilter(const IDataSource& source, const std::shared_ptr<IFilterSubscriber>& subscriber)
    : SourceId(source.GetSourceId())
    , MaxVersion(source.GetContext()->GetCommonContext()->GetReadMetadata()->GetRequestSnapshot())
    , MemoryGroup(source.GetGroupGuard())
    , Subscriber(subscriber) {
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
