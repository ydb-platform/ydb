#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/fluent_settings_helpers.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/request_settings.h>

#include <library/cpp/regex/pcre/regexp.h>

#include <util/generic/size_literals.h>

class TLog;

namespace NYdb {

inline namespace Dev {
    class TDriver;
}

namespace NDump {

TString DataFileName(ui32 id);

/// dump
struct TDumpSettings: public TOperationRequestSettings<TDumpSettings> {
    using TSelf = TDumpSettings;

    enum class EConsistencyLevel {
        Table /* "table" */,
        Database /* "database" */,
    };

    TVector<TRegExMatch> ExclusionPatterns_;
    TSelf& ExclusionPatterns(TVector<TRegExMatch>&& value) {
        ExclusionPatterns_ = std::move(value);
        return *this;
    }

    FLUENT_SETTING(TString, Database);
    FLUENT_SETTING_DEFAULT(bool, SchemaOnly, false);
    FLUENT_SETTING_DEFAULT(EConsistencyLevel, ConsistencyLevel, EConsistencyLevel::Database);
    FLUENT_SETTING_DEFAULT(bool, AvoidCopy, false);
    FLUENT_SETTING_DEFAULT(bool, SavePartialResult, false);
    FLUENT_SETTING_DEFAULT(bool, PreservePoolKinds, false);
    FLUENT_SETTING_DEFAULT(bool, Ordered, false);

    bool UseConsistentCopyTable() const {
        switch (ConsistencyLevel_) {
            case EConsistencyLevel::Table:
                return false;
            case EConsistencyLevel::Database:
                return true;
        }
    }

}; // TDumpSettings

class TDumpResult: public TStatus {
public:
    TDumpResult(TStatus&& status);

}; // TDumpResult

/// restore
struct TRateLimiterSettings {
    using TSelf = TRateLimiterSettings;

    FLUENT_SETTING_DEFAULT(ui32, Rate, Max<ui32>());
    FLUENT_SETTING_DEFAULT(TDuration, Interval, TDuration::Seconds(1));
    FLUENT_SETTING_DEFAULT(TDuration, ReactionTime, TDuration::MilliSeconds(50));

    TSelf& WithBandwidth(ui64 bandwidth, ui64 batchSize) {
        return Rate(Max<ui64>(1, (Interval_.Seconds() * bandwidth + batchSize - 1) / batchSize));
    }

    TSelf& WithRps(ui64 maxUploadRps) {
        return Rate(Max<ui64>(1, maxUploadRps * Interval_.Seconds()));
    }

    ui32 GetRps() const {
        return Max<ui32>(1, Rate_ * TDuration::Seconds(1).MilliSeconds() / Interval_.MilliSeconds());
    }

}; // TRateLimiterSettings

struct TRestoreSettings: public TOperationRequestSettings<TRestoreSettings> {
    using TSelf = TRestoreSettings;

    enum class EMode {
        Yql,
        BulkUpsert,
        ImportData,
    };

    static constexpr ui64 MaxImportDataBytesPerRequest = 16_MB;

    FLUENT_SETTING_DEFAULT(EMode, Mode, EMode::Yql);
    FLUENT_SETTING_DEFAULT(bool, DryRun, false);
    FLUENT_SETTING_DEFAULT(bool, RestoreData, true);
    FLUENT_SETTING_DEFAULT(bool, RestoreIndexes, true);
    FLUENT_SETTING_DEFAULT(bool, RestoreChangefeeds, true);
    FLUENT_SETTING_DEFAULT(bool, RestoreACL, true);
    FLUENT_SETTING_DEFAULT(bool, SkipDocumentTables, false);
    FLUENT_SETTING_DEFAULT(bool, SavePartialResult, false);

    FLUENT_SETTING_DEFAULT(ui64, MemLimit, 32_MB);
    FLUENT_SETTING_DEFAULT(ui64, RowsPerRequest, 0);
    FLUENT_SETTING_DEFAULT(ui64, BytesPerRequest, 512_KB);
    FLUENT_SETTING_DEFAULT(ui64, RequestUnitsPerRequest, 0);
    FLUENT_SETTING_DEFAULT(ui64, FileBufferSize, 2_MB);
    FLUENT_SETTING_DEFAULT(ui32, MaxInFlight, 0);
    FLUENT_SETTING_DEFAULT(TRateLimiterSettings, RateLimiterSettings, {});

}; // TRestoreSettings

struct TRestoreClusterSettings {
    using TSelf = TRestoreClusterSettings;

    FLUENT_SETTING_DEFAULT(TDuration, WaitNodesDuration, TDuration::Minutes(1));
}; // TRestoreClusterSettings

struct TRestoreDatabaseSettings {
    using TSelf = TRestoreDatabaseSettings;

    FLUENT_SETTING_DEFAULT(TDuration, WaitNodesDuration, TDuration::Minutes(1));
    FLUENT_SETTING_OPTIONAL(TString, Database);
    FLUENT_SETTING_DEFAULT(bool, WithContent, true);
}; // TRestoreDatabaseSettings

class TRestoreResult: public TStatus {
public:
    TRestoreResult(TStatus&& status);

}; // TRestoreResult

/// client
class TClient {
private:
    class TImpl;

public:
    explicit TClient(const TDriver& driver);
    explicit TClient(const TDriver& driver, std::shared_ptr<TLog>&& log);

    TDumpResult Dump(const TString& dbPath, const TString& fsPath, const TDumpSettings& settings = {});
    TRestoreResult Restore(const TString& fsPath, const TString& dbPath, const TRestoreSettings& settings = {});

    TDumpResult DumpCluster(const TString& fsPath);
    TRestoreResult RestoreCluster(const TString& fsPath, const TRestoreClusterSettings& settings = {});

    TDumpResult DumpDatabase(const TString& database, const TString& fsPath);
    TRestoreResult RestoreDatabase(const TString& fsPath, const TRestoreDatabaseSettings& settings = {});

private:
    std::shared_ptr<TImpl> Impl_;

}; // TClient

} // NDump
} // NYdb
