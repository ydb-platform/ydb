#pragma once

#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/source.h>

#include <ydb/library/actors/core/event_local.h>

namespace NKikimr::NOlap::NReader::NSimple {

class IDataSource;

using TIntervalId = ui64;   // SourceId

class IFilterSubscriber {
public:
    virtual void OnFilterReady(const NArrow::TColumnFilter&) = 0;
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

class TEvDuplicateFilterDataFetched
    : public NActors::TEventLocal<TEvDuplicateFilterDataFetched, NColumnShard::TEvPrivate::EvDuplicateFilterDataFetched> {
private:
    YDB_READONLY_DEF(ui64, SourceId);
    YDB_READONLY(TConclusion<std::shared_ptr<NArrow::TGeneralContainer>>, Result, TConclusionStatus::Success());
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> MemoryGuard;

public:
    TEvDuplicateFilterDataFetched(const ui64 sourceId, TConclusion<std::shared_ptr<NArrow::TGeneralContainer>>&& result,
        std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& memoryGuard)
        : SourceId(sourceId)
        , Result(std::move(result))
        , MemoryGuard(std::move(memoryGuard)) {
        if (result.IsSuccess()) {
            AFL_VERIFY(MemoryGuard)("source", sourceId);
        }
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
