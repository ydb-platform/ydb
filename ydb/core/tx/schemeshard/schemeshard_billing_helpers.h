#pragma once

#include <ydb/core/metering/bill_record.h>
#include <ydb/core/protos/index_builder.pb.h>

namespace NKikimr::NSchemeShard {

using TMeteringStats = NKikimrIndexBuilder::TMeteringStats;

struct TMeteringStatsCalculator {
    static void TryFixOldFormat(TMeteringStats& value);
    static TMeteringStats Zero();
    static bool IsZero(TMeteringStats& value);
    static void AddTo(TMeteringStats& value, const TMeteringStats& other);
    static void SubFrom(TMeteringStats& value, const TMeteringStats& other);
};

struct TRUCalculator {
    static ui64 ReadTable(ui64 bytes);
    static ui64 BulkUpsert(ui64 bytes, ui64 rows);
    static ui64 Calculate(const TMeteringStats& stats, TString& explain);
};

}
