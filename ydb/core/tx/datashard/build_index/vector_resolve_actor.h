#pragma once

#include "table_range_reader.h"

#include <ydb/core/base/row_version.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/scheme/scheme_tablecell.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NDataShard::NBuildIndex {

enum EVectorResolveEvent {
    EvVectorResolveRequest = EvReadRowsEventsEnd,
    EvVectorResolveResponse = EvReadRowsEventsEnd + 1,
};

// Settings of a resolver for one particular vector KMeans tree index.
//
// The level table columns are the regular index columns (__ydb_parent,
// __ydb_id, __ydb_centroid), so only the table path is needed. Prefix columns
// are the index key columns except the last (embedding) one; when they are
// present the index is considered prefixed and PrefixTablePath is required.
struct TVectorResolveSettings {
    TString DatabaseName;

    // Full path of indexImplLevelTable.
    TString LevelTablePath;

    // Full path of indexImplPrefixTable. Only used for prefixed indexes.
    TString PrefixTablePath;

    // Prefix key columns in the order they appear in the index key. Empty for a
    // non-prefixed index.
    TVector<TString> PrefixColumns;

    Ydb::Table::VectorIndexSettings IndexSettings;

    // Parameters for finding overlapping clusters (multiple clusters per row).
    ui32 OverlapClusters = 1;
    double OverlapRatio = 0;

    // Optional snapshot for level table reads. The prefix table is always read
    // from its latest version, so newly inserted prefixes become visible
    // immediately.
    TMaybe<TRowVersion> Snapshot;

    bool IsPrefixed() const {
        return !PrefixColumns.empty();
    }
};

// A batch of vectors to resolve. Vectors[i] corresponds to Prefixes[i]; for a
// non-prefixed index Prefixes may be empty. An empty vector (or a NULL one,
// which the caller should pass as an empty string) produces an empty cluster
// list in the response.
struct TEvVectorResolveRequest : public NActors::TEventLocal<TEvVectorResolveRequest, EvVectorResolveRequest> {
    ui64 RequestId = 0;

    TVector<TString> Vectors;

    // Prefix column values per row, in the order of Settings.PrefixColumns.
    TVector<TSerializedCellVec> Prefixes;
};

// A prefix that was missing from the prefix table and got a fresh id from the
// prefix sequence. A prefixed index build must insert it into the prefix table,
// otherwise the rows resolved to this id stay unreachable.
struct TNewPrefix {
    TSerializedCellVec Prefix;
    NTableIndex::NKMeans::TClusterId Id = 0;
};

// For every input row, the leaf (posting) cluster IDs the row belongs to.
// Rows that cannot be resolved into any cluster have an empty list.
struct TEvVectorResolveResponse : public NActors::TEventLocal<TEvVectorResolveResponse, EvVectorResolveResponse> {
    ui64 RequestId = 0;
    bool Success = false;
    NYql::TIssues Issues;
    TVector<TVector<NTableIndex::NKMeans::TClusterId>> Clusters;
    TVector<TNewPrefix> NewPrefixes;

    explicit TEvVectorResolveResponse(ui64 requestId)
        : RequestId(requestId)
    {}
};

// Creates a long-lived actor serving many resolve requests for one index. It
// owns a table range reader, so the index tables are resolved and cached once.
NActors::IActor* CreateVectorResolveActor(TVectorResolveSettings settings);

} // namespace NKikimr::NDataShard::NBuildIndex
