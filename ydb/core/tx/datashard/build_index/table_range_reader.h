#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/base/row_version.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/event_local.h>

#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NDataShard::NBuildIndex {

enum EReadRowsEvent {
    EvReadRowsRequest = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
    EvReadRowsResponse,
    EvReadRowsEventsEnd,
};

// A request to read all rows of one table range.
//
// The reader resolves the table through the scheme cache (target shards are
// discovered via TEvResolveKeySet) and caches both the navigation result and the
// partitioning per table path, so many ranges of the same table do not re-resolve
// columns and shards. Rows are streamed with TEvRead and are answered with a
// single TEvReadTableRangeResponse once the whole range has been read.
struct TEvReadTableRangeRequest : public TEventLocal<TEvReadTableRangeRequest, EvReadRowsRequest> {
    // Opaque id, echoed back in the response. Set by the caller when several
    // requests may be in flight at once.
    ui64 RequestId = 0;

    // Database name used for the scheme cache navigation.
    TString DatabaseName;

    // Full table path, e.g. "/Root/dir/table".
    TString TablePath;

    // Requested columns by name, in the order they should be returned.
    TVector<TString> Columns;

    // Requested primary key range. Key prefixes are allowed: an empty FromKey
    // means -inf and an empty ToKey means +inf.
    TSerializedCellVec FromKey;
    bool FromInclusive = true;
    TSerializedCellVec ToKey;
    bool ToInclusive = true;

    // Optional snapshot version. When unset the shard picks a repeatable version.
    TMaybe<TRowVersion> Snapshot;
};

// Result of one request. Rows reference the DataShard batches and stay valid
// only while the batches are alive; copy out anything that must outlive them.
struct TEvReadTableRangeResponse : public TEventLocal<TEvReadTableRangeResponse, EvReadRowsResponse> {
    ui64 RequestId = 0;
    bool Success = false;
    NYql::TIssues Issues;
    // PathId of the table the request was resolved to. Set on success.
    TPathId PathId;
    TVector<TEvDataShard::TEvReadResult::TPtr> Batches;

    explicit TEvReadTableRangeResponse(ui64 requestId)
        : RequestId(requestId)
    {}
};

NActors::IActor* CreateTableRangeReader();

} // namespace NKikimr::NDataShard::NBuildIndex
