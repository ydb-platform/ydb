#pragma once

#include <ydb/core/metering/bill_record.h>

namespace NKikimr::NSchemeShard {

class TBillingStats {
public:
    TBillingStats() = default;
    TBillingStats(const TBillingStats& other) = default;
    TBillingStats& operator = (const TBillingStats& other) = default;

    TBillingStats(ui64 uploadRows, ui64 uploadBytes, ui64 readRows, ui64 readBytes);

    TBillingStats operator - (const TBillingStats& other) const;
    TBillingStats& operator -= (const TBillingStats& other) {
        return *this = *this - other;
    }

    TBillingStats operator + (const TBillingStats& other) const;
    TBillingStats& operator += (const TBillingStats& other) {
        return *this = *this + other;
    }

    bool operator == (const TBillingStats& other) const = default;

    explicit operator bool () const {
        return *this != TBillingStats{};
    }

    TString ToString() const;

    ui64 GetUploadRows() const {
        return UploadRows;
    }
    ui64 GetUploadBytes() const {
        return UploadBytes;
    }

    ui64 GetReadRows() const {
        return ReadRows;
    }
    ui64 GetReadBytes() const {
        return ReadBytes;
    }

private:
    ui64 UploadRows = 0;
    ui64 UploadBytes = 0;
    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;
};

struct TRUCalculator {
    // https://a.yandex-team.ru/arc/trunk/arcadia/kikimr/docs/ru/pricing/serverless.md
    static ui64 ReadTable(ui64 bytes);
    static ui64 BulkUpsert(ui64 bytes, ui64 rows);
    static ui64 Calculate(const TBillingStats& stats);
};

}
