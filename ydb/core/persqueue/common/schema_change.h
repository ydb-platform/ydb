#pragma once

#include <ydb/core/base/row_version.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NKikimrPQ {
    class TSchemaChangeInfo;
}

namespace NKikimr::NPQ {

struct TSchemaChangeInfo {
    TRowVersion Version;
    TString Data;

    static TSchemaChangeInfo Parse(const NKikimrPQ::TSchemaChangeInfo& proto);
    void Serialize(NKikimrPQ::TSchemaChangeInfo& proto) const;
};

// True when a deferred schema-change write can be ACKed.
inline bool IsSchemaChangeVersionReleased(
    const TRowVersion& version,
    const TRowVersion& lastEmitted,
    const TRowVersion& committedFromStorage)
{
    return version <= Max(lastEmitted, committedFromStorage);
}

// Prefer an already-persisted newer LastSchemaChange over a deferred write's version.
inline TSchemaChangeInfo SelectSchemaChangeForAck(
    TSchemaChangeInfo proposed,
    const TMaybe<TSchemaChangeInfo>& existing)
{
    if (existing && existing->Version > proposed.Version) {
        return *existing;
    }
    return proposed;
}

}
