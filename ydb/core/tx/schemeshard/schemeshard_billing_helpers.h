#pragma once

#include <ydb/core/metering/bill_record.h>
#include <ydb/core/protos/index_builder.pb.h>

namespace NKikimr::NSchemeShard {

using TMeteringStats = NKikimrIndexBuilder::TMeteringStats;

TMeteringStats operator + (const TMeteringStats& value, const TMeteringStats& other);
TMeteringStats operator - (const TMeteringStats& value, const TMeteringStats& other);
TMeteringStats& operator += (TMeteringStats& value, const TMeteringStats& other);
TMeteringStats& operator -= (TMeteringStats& value, const TMeteringStats& other);

struct TMeteringStatsHelper {
    static void TryFixOldFormat(TMeteringStats& value);
    static TMeteringStats ZeroValue();
    static bool IsZero(TMeteringStats& value);
};

struct TRUCalculator {
    static ui64 ReadTable(ui64 bytes);
    static ui64 BulkUpsert(ui64 bytes, ui64 rows);
    static ui64 CPU(ui64 сpuTimeUs);
    static ui64 Calculate(const TMeteringStats& stats, TString& explain);
};

}
