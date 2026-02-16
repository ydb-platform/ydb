#pragma once

#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_sorted_partitioner.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/tempfile.h>

namespace NYql::NFmr::NTestTools {

using TFingerprintCounts = THashMap<TString, ui64>;

struct TGeneratedSortedTable {
    TFmrTableId TableId;
    TString PartId;
    TVector<TTempFileHandle> ChunkFiles;
    TVector<TChunkStats> ChunkStats;
};

struct TBoundaryKeyTableSpec {
    TString Cluster = "c";
    TString Path;
    TString PartId;

    ui64 BoundaryKey = 5;

    ui64 LowStart = 1;
    ui64 LowEnd = 4;
    ui64 RowsPerLowKey = 100;
    ui64 BoundaryTailCount = 2000;

    ui64 BoundaryHeadCount1 = 2000;
    ui64 MidStart = 6;
    ui64 MidEnd = 8;
    ui64 RowsPerMidKey = 100;

    ui64 BoundaryHeadCount2 = 2000;
    ui64 HighStart = 9;
    ui64 HighEnd = 12;
    ui64 RowsPerHighKey = 100;

    ui64 ChunkWeight = 10;
};
TGeneratedSortedTable GenerateBoundaryKeySpanningMultipleChunksTable(const TBoundaryKeyTableSpec& spec, ui64 idBase);

TGeneratedSortedTable GenerateBoundaryKeySpanning3ChunksTable(const TBoundaryKeyTableSpec& spec, ui64 idBase);

void BuildPartitionerInputs(
    const TVector<TGeneratedSortedTable>& tables,
    std::unordered_map<TFmrTableId, std::vector<TString>>& partIdsForTables,
    std::unordered_map<TString, std::vector<TChunkStats>>& partIdStats
);

TFingerprintCounts CountSourceFingerprintsFromFiles(
    const TVector<TGeneratedSortedTable>& tables,
    const TVector<TString>& keyColumns
);

TFingerprintCounts CountTaskFingerprintsFromFiles(
    const std::vector<TTaskTableInputRef>& tasks,
    const TVector<TGeneratedSortedTable>& tables,
    const TVector<TString>& keyColumns
);

std::pair<TFingerprintCounts, TFingerprintCounts> DiffFingerprintMultisets(
    const TFingerprintCounts& src,
    const TFingerprintCounts& dst
);

} // namespace NYql::NFmr::NTestTools

