#pragma once

#include <ydb/core/metering/bill_record.h>
#include <ydb/core/protos/index_builder.pb.h>

namespace NKikimr::NSchemeShard {

using TBillingStats = NKikimrIndexBuilder::TBillingStats;

struct TBillingStatsCalculator {
    static void TryFixOldFormat(TBillingStats& value);
    static bool IsZero(TBillingStats& value);
    static void AddTo(TBillingStats& value, const TBillingStats& other);
    static void SubFrom(TBillingStats& value, const TBillingStats& other);
};

struct TRUCalculator {
    static ui64 ReadTable(ui64 bytes);
    static ui64 BulkUpsert(ui64 bytes, ui64 rows);
    static ui64 Calculate(const TBillingStats& stats);
};

}
