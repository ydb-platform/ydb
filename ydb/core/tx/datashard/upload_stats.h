#pragma once

#include "defs.h"

#include <ydb/core/base/counters.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr::NDataShard {

class IStatHolder {
public:
    virtual ~IStatHolder() = default;

    virtual ui64 GetRows() const = 0;
    virtual ui64 GetBytes() const = 0;
};

class TUploadMonStats: public IStatHolder {
public:
    explicit TUploadMonStats() = default;
    explicit TUploadMonStats(const TString service,
                             const TString subsystem);

    void Aggr(ui64 rows, ui64 bytes);
    void Aggr(IStatHolder const* other);

    TString ToString() const;

    ui64 GetRows() const override final {
        return RowsSent;
    }

    ui64 GetBytes() const override final {
        return BytesSent;
    }

private:
    ui64 RowsSent = 0;
    ui64 BytesSent = 0;

    ::NMonitoring::TDynamicCounters::TCounterPtr MonRows;
    ::NMonitoring::TDynamicCounters::TCounterPtr MonBytes;
};

struct TUploadStatus {
    Ydb::StatusIds::StatusCode StatusCode = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    NYql::TIssues Issues;

    bool IsNone() const {
        return StatusCode == Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    }

    bool IsSuccess() const {
        return StatusCode == Ydb::StatusIds::SUCCESS;
    }

    bool IsRetriable() const {
        return StatusCode == Ydb::StatusIds::UNAVAILABLE || StatusCode == Ydb::StatusIds::OVERLOADED;
    }

    TString ToString() const {
        return TStringBuilder()
               << "Status {"
               << " Code: " << Ydb::StatusIds_StatusCode_Name(StatusCode)
               << " Issues: " << Issues.ToString()
               << " }";
    }
};

struct TUploadRetryLimits {
    ui32 MaxUploadRowsRetryCount = 50;
    ui32 BackoffCeiling = 3;

    TDuration GetTimeoutBackouff(ui32 retryNo) const {
        return TDuration::Seconds(1u << Max(retryNo, BackoffCeiling));
    }
};

struct TUploadLimits: TUploadRetryLimits {
    ui64 BatchRowsLimit = 500;
    ui64 BatchBytesLimit = 1u << 23; // 8MB
};

}
