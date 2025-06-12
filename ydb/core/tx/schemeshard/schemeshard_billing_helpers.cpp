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
    return 128 * ((bytes + 1_MB - 1) / 1_MB);
}

ui64 TRUCalculator::BulkUpsert(ui64 bytes, ui64 rows) {
    return (Max(rows, (bytes + 1_KB - 1) / 1_KB) + 1) / 2;
}

ui64 TRUCalculator::Calculate(const TBillingStats& stats) {
    return TRUCalculator::ReadTable(stats.GetReadBytes())
         + TRUCalculator::BulkUpsert(stats.GetUploadBytes(), stats.GetUploadRows());
}

}
