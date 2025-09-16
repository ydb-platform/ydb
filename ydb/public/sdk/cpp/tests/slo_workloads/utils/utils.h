#pragma once

#include "statistics.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/random/random.h>

extern const TDuration DefaultReactionTime;
extern const TDuration ReactionTimeDelay;
extern const TDuration GlobalTimeout;
extern const ui64 PartitionsCount;

struct TRecordData {
    ui32 ObjectId;
    ui64 Timestamp;
    std::string Guid;
    std::string Payload;
};

struct TKeyValueRecordData {
    ui32 ObjectId;
    ui64 Timestamp;
    std::string Payload;
};

struct TDurationMeter {
    TDurationMeter(TDuration& value);
    ~TDurationMeter();

    TDuration& Value;
    TInstant StartTime;
};

struct TDatabaseOptions {
    NYdb::TDriver& Driver;
    const std::string& Prefix;
};

struct TCommonOptions {
    // Executor options:
    TDatabaseOptions DatabaseOptions;
    ui32 SecondsToRun = 10;
    ui32 Rps = 10;
    ui32 MaxInputThreads = 50;
    ui32 MaxCallbackThreads = 50;
    ui32 MaxInfly = 500;
    ui32 MaxRetries = 50;
    ui64 A_ReactionTime = 70; //ms
    TDuration ReactionTime = DefaultReactionTime;
    bool StopOnError = false;
    bool UseApplicationTimeout = false;
    bool SendPreventiveRequest = false;
    bool DoNotPrepare = false;

    //Generator options:
    ui32 MinLength = 20;
    ui32 MaxLength = 200;

    //Output options:
    bool DontPushMetrics = true;
    std::string ResultFileName = "slo_result.json";

    bool UseFollowers = false;
    bool RetryMode = false;
    bool SaveResult = false;
};

struct TCreateOptions {
    TCommonOptions CommonOptions;
    ui32 Count = 10000;
    ui32 PackSize = 100;
};

struct TRunOptions {
    TCommonOptions CommonOptions;
    bool DontRunA = false;
    bool DontRunB = false;
    bool DontRunC = false;
    ui32 Read_rps = 1000;
    ui32 Write_rps = 10;
};

class TRpsProvider {
public:
    TRpsProvider(ui64 rps);
    void Reset();
    void Use();
    bool TryUse();
    ui64 GetRps() const;

private:
    ui64 Rps;
    TDuration Period;
    TInstant ProcessedTime;
    TInstant LastCheck;
    ui32 Allowed = 0;
    TInstant StartTime;
};

enum class ECommandType {
    Unknown,
    Create,
    Run,
    Cleanup
};

struct TTableStats {
    ui64 RowCount = 0;
    ui32 MaxId = 0;
};

using TCreateCommand = std::function<int(TDatabaseOptions&, int, char**)>;
using TRunCommand = std::function<int(TDatabaseOptions&, int, char**)>;
using TCleanupCommand = std::function<int(TDatabaseOptions&, int)>;

int DoMain(int argc, char** argv, TCreateCommand create, TRunCommand run, TCleanupCommand cleanup);

std::string GetCmdList();
ECommandType ParseCommand(const char* cmd);

std::string JoinPath(const std::string& prefix, const std::string& path);

class TYdbErrorException : public yexception {
public:
    TYdbErrorException(NYdb::TStatus status)
        : Status(std::move(status))
    { }

    friend IOutputStream& operator<<(IOutputStream& out, const TYdbErrorException& e) {
        out << "Status: " << e.Status.GetStatus();
        if (e.Status.GetIssues()) {
            out << Endl;
            e.Status.GetIssues().PrintTo(out);
        }
        return out;
    }

private:
    NYdb::TStatus Status;
};

inline void ThrowOnError(NYdb::TStatus status) {
    if (!status.IsSuccess()) {
        throw TYdbErrorException(std::move(status));
    }
}

inline void RetryBackoff(
    NYdb::NTable::TTableClient& client
    , ui32 retries
    , const NYdb::NTable::TTableClient::TOperationSyncFunc& func
) {
    TDuration delay = TDuration::Seconds(5);
    while (retries) {
        NYdb::TStatus status = client.RetryOperationSync(func);
        if (status.IsSuccess()) {
            return;
        }
        --retries;
        if (!retries) {
            Cerr << "Create request failed after all retries." << Endl;
            Cerr << status << Endl;
            ThrowOnError(status);
        }
        Cerr << "Create request failed. Sleeping for " << delay << Endl;
        Sleep(delay);
        delay *= 2;
    }
}

std::string GenerateRandomString(ui32 minLength, ui32 maxLength);

NYdb::TParams PackValuesToParamsAsList(const std::vector<NYdb::TValue>& items, const std::string name = "$items");

// Returns special object_id within the same shard as given id
ui32 GetSpecialId(ui32 id);

// Returns special object_id for given shard
ui32 GetShardSpecialId(ui64 shardNo);

ui32 GetHash(ui32 value);

TTableStats GetTableStats(TDatabaseOptions& dbOptions, const std::string& tableName);

bool ParseOptionsCreate(int argc, char** argv, TCreateOptions& createOptions, bool followers = false);
bool ParseOptionsRun(int argc, char** argv, TRunOptions& runOptions, bool followers = false);
