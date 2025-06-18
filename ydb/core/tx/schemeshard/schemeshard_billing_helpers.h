#pragma once

#include <ydb/core/metering/bill_record.h>
#include <ydb/core/protos/index_builder.pb.h>

namespace NKikimr::NSchemeShard {

struct TBillingStatsCalculator {
    static void TryFixOldFormat(NKikimrIndexBuilder::TBillingStats& value);
    static void AddTo(NKikimrIndexBuilder::TBillingStats& value, const NKikimrIndexBuilder::TBillingStats& other);
};

struct TRUCalculator {
    static ui64 ReadTable(ui64 bytes);
    static ui64 BulkUpsert(ui64 bytes, ui64 rows);
    static ui64 Calculate(const NKikimrIndexBuilder::TBillingStats& stats);
};

}
