#pragma once

#include "defs.h"

#include <ydb/core/scheme/scheme_tabledefs.h>

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/stream/str.h>

namespace NKikimr {
namespace NDataShard {

using TKeyMap = TVector<std::pair<ui32, ui32>>; // index to main
using TIndexes = THashMap<TTableId, TKeyMap>;

// Description of one vector index whose posting table has to be cleaned up on TTL.
struct TVectorIndexDescription {
    // indexImplPostingTable.
    TTableId TableId;

    // Posting table key columns (except __ydb_parent) mapped to the main table
    // columns, in the posting key order.
    TKeyMap KeyMap;

    ui32 ParentColumnId = 0;
    ui32 EmbeddingColumnId = 0;
    TVector<ui32> PrefixColumnIds;
    TVector<TString> PrefixColumnNames;

    TString LevelTablePath;
    TString PrefixTablePath;

    Ydb::Table::VectorIndexSettings Settings;
    ui32 OverlapClusters = 1;
    double OverlapRatio = 0;
};

using TVectorIndexes = TVector<TVectorIndexDescription>;

IActor* CreateDistributedEraser(const TActorId& replyTo, const TString& databaseName,
    const TTableId& mainTableId, const TIndexes& indexes, const TVectorIndexes& vectorIndexes = {});

template <typename TBitMapType>
static TString SerializeBitMap(const TBitMapType& bitmap) {
    TString serialized;
    TStringOutput output(serialized);
    bitmap.Save(&output);
    return serialized;
}

template <typename TBitMapType>
static TBitMapType DeserializeBitMap(const TString& serialized) {
    TStringInput input(serialized);
    TBitMapType bitmap;
    bitmap.Load(&input);
    return bitmap;
}

} // NDataShard
} // NKikimr
