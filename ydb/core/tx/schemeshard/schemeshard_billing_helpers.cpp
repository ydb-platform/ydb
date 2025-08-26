#include "schemeshard_billing_helpers.h"

#include <util/generic/size_literals.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NKikimr::NSchemeShard {

TMeteringStats operator + (const TMeteringStats& value, const TMeteringStats& other) {
    TMeteringStats result = value;
    result += other;
    return result;
}

TMeteringStats operator - (const TMeteringStats& value, const TMeteringStats& other) {
    TMeteringStats result = value;
    result -= other;
    return result;
}

TMeteringStats& operator += (TMeteringStats& value, const TMeteringStats& other) {
    value.SetUploadRows(value.GetUploadRows() + other.GetUploadRows());
    value.SetUploadBytes(value.GetUploadBytes() + other.GetUploadBytes());
    value.SetReadRows(value.GetReadRows() + other.GetReadRows());
    value.SetReadBytes(value.GetReadBytes() + other.GetReadBytes());
    value.SetCpuTimeUs(value.GetCpuTimeUs() + other.GetCpuTimeUs());
    return value;
}

TMeteringStats& operator -= (TMeteringStats& value, const TMeteringStats& other) {
    const auto safeSub = [](ui64 x, ui64 y) -> ui64 {
        if (Y_LIKELY(x >= y)) {
            return x - y;
        }
        Y_ASSERT(false);
        return 0;
    };

    value.SetUploadRows(safeSub(value.GetUploadRows(), other.GetUploadRows()));
    value.SetUploadBytes(safeSub(value.GetUploadBytes(), other.GetUploadBytes()));
    value.SetReadRows(safeSub(value.GetReadRows(), other.GetReadRows()));
    value.SetReadBytes(safeSub(value.GetReadBytes(), other.GetReadBytes()));
    value.SetCpuTimeUs(safeSub(value.GetCpuTimeUs(), other.GetCpuTimeUs()));
    return value;
}

void TMeteringStatsHelper::TryFixOldFormat(TMeteringStats& value) {
    // old format: assign upload to read
    if (value.GetReadRows() == 0 && value.GetUploadRows() != 0) {
        value.SetReadRows(value.GetUploadRows());
        value.SetReadBytes(value.GetUploadBytes());
    }
}

TMeteringStats TMeteringStatsHelper::ZeroValue() {
    // this method the only purpose is to beautifully print zero stats instead of empty protobuf or with missing fields
    TMeteringStats value;
    value.SetUploadRows(0);
    value.SetUploadBytes(0);
    value.SetReadRows(0);
    value.SetReadBytes(0);
    value.SetCpuTimeUs(0);
    return value;
}

bool TMeteringStatsHelper::IsZero(TMeteringStats& value) {
    return value.GetUploadRows() == 0
        && value.GetUploadBytes() == 0
        && value.GetReadRows() == 0
        && value.GetReadBytes() == 0
        && value.GetCpuTimeUs() == 0;
}

ui64 TRUCalculator::ReadTable(ui64 bytes) {
    // The ReadTable operation lets you efficiently read large ranges of data from a table.
    // The request cost only depends on the amount of data read based on the rate of 128 RU per 1 MB.
    // When calculating the cost, the amount is rounded up to a multiple of 1 MB.
    // https://yandex.cloud/en-ru/docs/ydb/pricing/ru-special#readtable
    return 128 * ((bytes + 1_MB - 1) / 1_MB);
}

ui64 TRUCalculator::BulkUpsert(ui64 bytes, ui64 rows) {
    // BulkUpsert lets you efficiently upload data to the database.
    // The cost of writing a row using the BulkUpsert operation is 0.5 RU per 1 KB of written data.
    // When calculating the cost, the data amount is rounded up to a multiple of 1 KB.
    // The total cost of the operation is calculated as the sum of costs for all rows written, with the result rounded up to the nearest integer.
    // https://yandex.cloud/en-ru/docs/ydb/pricing/ru-special#bulkupsert
    return (Max(rows, (bytes + 1_KB - 1) / 1_KB) + 1) / 2;
}

ui64 TRUCalculator::CPU(ui64 сpuTimeUs) {
    // The sum is divided by the CPU time increment of 1.5 ms, rounded down and converted to RU.
    // https://yandex.cloud/en-ru/docs/ydb/pricing/ru-yql
    return сpuTimeUs / 1500;
}

ui64 TRUCalculator::Calculate(const TMeteringStats& stats, TString& explain) {
    // The cost of building an index is the sum of the cost of ReadTable from the source table and BulkUpsert to the index table.
    // Note: in case of any cost changes, documentation is needed to be updated correspondingly.
    // https://yandex.cloud/ru/docs/ydb/pricing/ru-special#build-index

    // To evaluate the YDB API request cost, the CPU cost and the I/O cost are calculated. A maximum from the calculated values is selected.
    // https://yandex.cloud/en-ru/docs/ydb/pricing/ru-yql

    ui64 readTable = TRUCalculator::ReadTable(stats.GetReadBytes());
    ui64 bulkUpsert = TRUCalculator::BulkUpsert(stats.GetUploadBytes(), stats.GetUploadRows());
    ui64 cpu = TRUCalculator::CPU(stats.GetCpuTimeUs());
    explain = TStringBuilder()
        << "ReadTable: " << readTable
        << ", BulkUpsert: " << bulkUpsert
        << ", CPU: " << cpu;

    // CPU usage is intentionally excluded to simplify the documentation
    // as I/O operations are typically much more resource-intensive than CPU processing
    return readTable + bulkUpsert;
}

}
