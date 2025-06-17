#include "schemeshard_billing_helpers.h"

#include <util/generic/size_literals.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NKikimr::NSchemeShard {

TBillingStats::TBillingStats(ui64 readRows, ui64 readBytes, ui64 uploadRows, ui64 uploadBytes)
    : UploadRows{uploadRows}
    , UploadBytes{uploadBytes}
    , ReadRows{readRows}
    , ReadBytes{readBytes}
{
}

TBillingStats TBillingStats::operator -(const TBillingStats &other) const {
    Y_ENSURE(UploadRows >= other.UploadRows);
    Y_ENSURE(UploadBytes >= other.UploadBytes);
    Y_ENSURE(ReadRows >= other.ReadRows);
    Y_ENSURE(ReadBytes >= other.ReadBytes);

    return {UploadRows - other.UploadRows, UploadBytes - other.UploadBytes,
            ReadRows - other.ReadRows, ReadBytes - other.ReadBytes};
}

TBillingStats TBillingStats::operator +(const TBillingStats &other) const {
    return {UploadRows + other.UploadRows, UploadBytes + other.UploadBytes,
            ReadRows + other.ReadRows, ReadBytes + other.ReadBytes};
}

TString TBillingStats::ToString() const {
    return TStringBuilder()
            << "{"
            << " upload rows: " << UploadRows
            << ", upload bytes: " << UploadBytes
            << ", read rows: " << ReadRows
            << ", read bytes: " << ReadBytes
            << " }";
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

ui64 TRUCalculator::Calculate(const TBillingStats& stats) {
    // The cost of building an index is the sum of the cost of ReadTable from the source table and BulkUpsert to the index table.
    // https://yandex.cloud/en-ru/docs/ydb/pricing/ru-special#secondary-index
    return TRUCalculator::ReadTable(stats.GetReadBytes())
         + TRUCalculator::BulkUpsert(stats.GetUploadBytes(), stats.GetUploadRows());
}

}
