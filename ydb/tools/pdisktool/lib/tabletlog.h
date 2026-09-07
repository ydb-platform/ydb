#pragma once

#include "blobsource.h"

#include <ydb/core/base/tablet.h>

namespace NKikimr::NPDiskTool {

struct TTabletLogHistoryStats {
    ui32 CandidatesTried = 0;
    ui64 ZeroEntries = 0;
    ui64 LogEntries = 0;
    ui64 ParseFailures = 0;
    ui64 MissingReferences = 0;
    ui64 DeclinedEntries = 0;
    ui64 Gaps = 0;
    bool GapsTolerated = false;
};

struct TTabletLogHistory {
    bool Ok = false;
    TLogoBlobID KeyEntry;
    std::pair<ui32, ui32> Snapshot{0, 0};
    std::pair<ui32, ui32> Confirmed{0, 0};
    std::pair<ui32, ui32> Latest{0, 0};
    TIntrusivePtr<TEvTablet::TDependencyGraph> Graph;
    TTabletLogHistoryStats Stats;
};

// The offline counterpart of TTabletReqFindLatestLogEntry plus TTabletReqRebuildHistoryGraph: the key
// entry is the highest channel-0 blob instead of a TEvDiscover answer, the log range comes from the
// blob store instead of a TEvRange, and reference presence is a lookup instead of a TEvGet.
//
// Where the production code trusts the log and aborts, a value read out of a possibly damaged blob is
// checked and the candidate key entry is abandoned in favour of an older one. When no candidate yields
// a complete history, the best one is rebuilt while tolerating the gaps, and that is reported.
TTabletLogHistory RebuildTabletHistory(TBlobStore& store, ui64 tabletId, ui32 maxGeneration,
    TIssueLog& issues);

} // namespace NKikimr::NPDiskTool
