#pragma once
#include <util/datetime/base.h>
#include <util/system/types.h>

namespace NKikimr::NOlap::NReader {

struct TReadStats {
    TInstant BeginTimestamp;
    ui64 IndexGranules{0};
    ui64 IndexPortions{0};
    ui64 IndexBatches{0};
    ui64 CommittedPortionsBytes = 0;
    ui64 InsertedPortionsBytes = 0;
    ui64 CompactedPortionsBytes = 0;
    ui64 DataFilterBytes{0};
    ui64 DataAdditionalBytes{0};

    ui32 SchemaColumns = 0;
    ui32 FilterColumns = 0;
    ui32 AdditionalColumns = 0;

    ui32 SelectedRows = 0;

    TReadStats()
        : BeginTimestamp(TInstant::Now()) {
    }

    void PrintToLog();

    ui64 GetReadBytes() const {
        return CompactedPortionsBytes + InsertedPortionsBytes + CompactedPortionsBytes;
    }

    TDuration Duration() {
        return TInstant::Now() - BeginTimestamp;
    }
};


}