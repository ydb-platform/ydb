#pragma once

#include "barrier.h"

#include <ydb/core/scheme/scheme_pathid.h>

#include <util/generic/vector.h>

namespace NKikimr::NCdcAggregator {

struct TStreamPartition {
    ui32 PartitionId = 0;
    // PersQueue tablet that hosts the partition.
    ui64 TabletId = 0;

    bool operator==(const TStreamPartition&) const = default;
};

// Initial configuration of the stream given by SchemeShard.
//
// Sources is the initial set of sources: it recognizes a repeated
// configuration. The current set changes with split/merge and is owned by
// TStream.
struct TStreamConfig {
    TPathId StreamPathId;
    ui64 SchemeShardId = 0;
    ui64 Version = 0;
    // Sorted, unique.
    TVector<TSourceId> Sources;
    // Sorted by PartitionId, unique.
    TVector<TStreamPartition> Partitions;

    bool operator==(const TStreamConfig&) const = default;
};

// Sorts sources and partitions; returns false with an error for a
// configuration that cannot serve a stream.
bool NormalizeStreamConfig(TStreamConfig& config, TString& error);

} // namespace NKikimr::NCdcAggregator
