#pragma once

#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/source.h>

#include <ydb/library/actors/core/event_local.h>

namespace NKikimr::NOlap::NReader::NSimple {

class IDataSource;

class IFilterSubscriber {
public:
    virtual void OnFilterReady(NArrow::TColumnFilter&&) = 0;
    virtual void OnFailure(const TString& reason) = 0;
    virtual ~IFilterSubscriber() = default;
};

class TEvRequestFilter: public NActors::TEventLocal<TEvRequestFilter, NColumnShard::TEvPrivate::EvRequestFilter> {
private:
    YDB_READONLY_DEF(std::shared_ptr<IDataSource>, Source);
    YDB_READONLY_DEF(std::shared_ptr<IFilterSubscriber>, Subscriber);

public:
    TEvRequestFilter(const std::shared_ptr<IDataSource>& source, const std::shared_ptr<IFilterSubscriber>& subscriber)
        : Source(source)
        , Subscriber(subscriber) {
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
