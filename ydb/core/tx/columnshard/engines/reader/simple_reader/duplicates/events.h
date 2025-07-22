#pragma once

#include "common.h"

#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/source.h>

#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/conclusion/result.h>

namespace NKikimr::NOlap::NReader::NSimple {
class IDataSource;
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
    YDB_READONLY_DEF(TString, ExternalTaskId);
    NArrow::TSimpleRow MinPK;
    NArrow::TSimpleRow MaxPK;
    YDB_READONLY_DEF(ui64, SourceId);
    YDB_READONLY_DEF(ui64, RecordsCount);
    TSnapshot MaxVersion;
    YDB_READONLY_DEF(std::shared_ptr<IFilterSubscriber>, Subscriber);
    YDB_READONLY_DEF(std::shared_ptr<const TAtomicCounter>, AbortionFlag);

public:
    TEvRequestFilter(const IDataSource& source, const std::shared_ptr<IFilterSubscriber>& subscriber);

    TSnapshot GetMaxVersion() const {
        return MaxVersion;
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
