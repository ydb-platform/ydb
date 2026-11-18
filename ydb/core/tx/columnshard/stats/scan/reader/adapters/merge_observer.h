#pragma once

#include "decode_observer.h"

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NStats::NScan {

class TMergeObserver {
    TLocalStats* const LocalStats;
    YDB_READONLY_DEF(std::unique_ptr<TDecodeObserver>, DecodeObserver);
    TOperationScope PKScope;
    TOperationScope ReserveScope;
    TOperationScope MergeScope;

public:
    TMergeObserver(TLocalStats* localStats, ui64 requestedMemory);

    void OnMergeStarted(std::optional<ui64> sources, std::optional<ui64> inputRows);
    void OnInputRows(ui64 rows);
    void OnPKOutputRows(ui64 rows);
    void OnMergeFinished(ui64 outputRows);
    void OnMergeFinished();
    void OnPKResumed();
    void OnPKFinished();
    void OnMemoryAllocated(ui64 bytes);
    void OnAllocationImpossible();
};

}   // namespace NKikimr::NOlap::NStats::NScan
