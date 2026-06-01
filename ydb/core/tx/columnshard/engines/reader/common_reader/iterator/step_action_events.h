#pragma once

#include "source.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/common/script_cursor.h>

#include <ydb/library/actors/core/event_local.h>

namespace NKikimr::NColumnShard {

class TEvContinueStepAction: public NActors::TEventLocal<TEvContinueStepAction, TEvPrivate::EvContinueStepAction> {
private:
    std::shared_ptr<NOlap::NReader::NCommon::IDataSource> Source;
    NOlap::NReader::NCommon::TFetchingScriptCursor Cursor;
    ui64 ConveyorProcessId = 0;
    ui64 SourceId = 0;

public:
    TEvContinueStepAction(std::shared_ptr<NOlap::NReader::NCommon::IDataSource>&& source,
        NOlap::NReader::NCommon::TFetchingScriptCursor&& cursor, const ui64 conveyorProcessId, const ui64 sourceId)
        : Source(std::move(source))
        , Cursor(std::move(cursor))
        , ConveyorProcessId(conveyorProcessId)
        , SourceId(sourceId)
    {
    }

    ui64 GetSourceId() const {
        return SourceId;
    }

    ui64 GetConveyorProcessId() const {
        return ConveyorProcessId;
    }

    std::shared_ptr<NOlap::NReader::NCommon::IDataSource> ExtractSource() {
        return std::move(Source);
    }

    NOlap::NReader::NCommon::TFetchingScriptCursor ExtractCursor() {
        return std::move(Cursor);
    }
};

class TEvStepActionSuspended: public NActors::TEventLocal<TEvStepActionSuspended, TEvPrivate::EvStepActionSuspended> {
private:
    ui64 SourceId = 0;

public:
    explicit TEvStepActionSuspended(const ui64 sourceId)
        : SourceId(sourceId)
    {
    }

    ui64 GetSourceId() const {
        return SourceId;
    }
};

}   // namespace NKikimr::NColumnShard
