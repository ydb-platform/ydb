#pragma once

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/engines/reader/duplicates/subscriber.h>

#include <ydb/library/actors/core/event_local.h>

namespace NKikimr::NOlap::NReader {

namespace NSimple {
class IDataSource;
};

class TEvRequestFilter: public NActors::TEventLocal<TEvRequestFilter, NColumnShard::TEvPrivate::EvRequestFilter> {
private:
    YDB_READONLY_DEF(std::shared_ptr<NSimple::IDataSource>, Source);
    YDB_READONLY_DEF(std::shared_ptr<IFilterSubscriber>, Subscriber);

public:
    TEvRequestFilter(const std::shared_ptr<NSimple::IDataSource>& source, const std::shared_ptr<IFilterSubscriber>& subscriber)
        : Source(source)
        , Subscriber(subscriber) {
    }
};

}   // namespace NKikimr::NOlap::NReader
