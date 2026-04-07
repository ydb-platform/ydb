#pragma once

#include "common.h"

#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>

#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/conclusion/result.h>

namespace NKikimr::NOlap::NReader::NSimple {
class TPortionDataSource;
}

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class IFilterSubscriber {
public:
    virtual void OnFilterReady(NArrow::TColumnFilter&&) = 0;
    virtual void OnFailure(const TString& reason) = 0;
    virtual ~IFilterSubscriber() = default;
};

class TEvRequestFilter: public NActors::TEventLocal<TEvRequestFilter, NColumnShard::TEvPrivate::EvRequestFilter> {
private:
    NArrow::TSimpleRow MinPK;
    NArrow::TSimpleRow MaxPK;
    YDB_READONLY_DEF(ui64, PortionId);
    YDB_READONLY_DEF(ui64, RecordsCount);
    TSnapshot MaxVersion;
    YDB_READONLY_DEF(std::shared_ptr<IFilterSubscriber>, Subscriber);
    YDB_READONLY_DEF(std::shared_ptr<const TAtomicCounter>, AbortionFlag);

public:
    TEvRequestFilter(const TPortionDataSource& source, const std::shared_ptr<IFilterSubscriber>& subscriber);

    // Test-only constructor that doesn't require TPortionDataSource
    TEvRequestFilter(const NArrow::TSimpleRow& minPK, const NArrow::TSimpleRow& maxPK, const ui64 portionId,
        const ui64 recordsCount, const TSnapshot& maxVersion, const std::shared_ptr<IFilterSubscriber>& subscriber,
        const std::shared_ptr<const TAtomicCounter>& abortionFlag);

    TSnapshot GetMaxVersion() const;
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
