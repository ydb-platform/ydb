#pragma once

#include "priority.h"

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/defaults.h>

#include <ydb/core/util/struct_log/structured_message.h>

#include <utility>

struct TLogRecord {
    using TMetaFlags = TVector<std::pair<TString, TString>>;

    const char* Data;
    size_t Len;
    ELogPriority Priority;
    TMetaFlags MetaFlags;
    const TMaybe<NKikimr::NStructLog::TStructuredMessage> StructMessage;

    inline TLogRecord(ELogPriority priority, const char* data, size_t len, TMetaFlags metaFlags = {}) noexcept
        : Data(data)
        , Len(len)
        , Priority(priority)
        , MetaFlags(std::move(metaFlags))
    {
    }

    inline TLogRecord(ELogPriority priority, const char* data, size_t len, const TMaybe<NKikimr::NStructLog::TStructuredMessage>& structMessage) noexcept
        : Data(data)
        , Len(len)
        , Priority(priority)
        , StructMessage(structMessage)
    {
    }
};
