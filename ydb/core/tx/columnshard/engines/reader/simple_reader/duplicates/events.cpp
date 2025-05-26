#include "events.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>

namespace NKikimr::NOlap::NReader::NSimple {

TEvRequestFilter::TEvRequestFilter(const IDataSource& source, const std::shared_ptr<IFilterSubscriber>& subscriber)
    : RecordsCount(source.GetRecordsCount())
    , Subscriber(subscriber) {
}

}   // namespace NKikimr::NOlap::NReader::NSimple
