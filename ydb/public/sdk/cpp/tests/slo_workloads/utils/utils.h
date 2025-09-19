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
extern const std::uint64_t PartitionsCount;

struct TRecordData {
    std::uint32_t ObjectId;
    std::uint64_t Timestamp;
    std::string Guid;
    std::string Payload;
};

struct TKeyValueRecordData {
    std::uint32_t ObjectId;
    std::uint64_t Timestamp;
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
    std::uint32_t SecondsToRun = 10;
    std::uint32_t Rps = 10;
    std::uint32_t MaxInputThreads = 50;
    std::uint32_t MaxCallbackThreads = 50;
    std::uint32_t MaxInfly = 500;
    std::uint32_t MaxRetries = 50;
    std::uint64_t A_ReactionTime = 70; //ms
    TDuration ReactionTime = DefaultReactionTime;
    bool StopOnError = false;
    bool UseApplicationTimeout = false;
    bool SendPreventiveRequest = false;

    //Generator options:
    std::uint32_t MinLength = 20;
    std::uint32_t MaxLength = 200;

    //Output options:
    bool DontPushMetrics = true;
    std::string ResultFileName = "slo_result.json";

    bool UseFollowers = false;
    bool RetryMode = false;
    bool SaveResult = false;
};

struct TCreateOptions {
    TCommonOptions CommonOptions;
    std::uint32_t Count = 10000;
    std::uint32_t PackSize = 100;
};

struct TRunOptions {
    TCommonOptions CommonOptions;
    bool DontRunA = false;
    bool DontRunB = false;
    bool DontRunC = false;
    std::uint32_t Read_rps = 1000;
    std::uint32_t Write_rps = 10;
};

class TRpsProvider {
public:
    TRpsProvider(std::uint64_t rps);
    void Reset();
    void Use();
    bool TryUse();
    std::uint64_t GetRps() const;

private:
    std::uint64_t Rps;
    TDuration Period;
    TInstant ProcessedTime;
    TInstant LastCheck;
    std::uint32_t Allowed = 0;
    TInstant StartTime;
};

enum class ECommandType {
    Unknown,
    Create,
    Run,
    Cleanup
};

struct TTableStats {
    std::uint64_t RowCount = 0;
    std::uint32_t MaxId = 0;
};

using TCreateCommand = std::function<int(TDatabaseOptions&, int, char**)>;
using TRunCommand = std::function<int(TDatabaseOptions&, int, char**)>;
using TCleanupCommand = std::function<int(TDatabaseOptions&, int)>;

int DoMain(int argc, char** argv, TCreateCommand create, TRunCommand run, TCleanupCommand cleanup);

std::string GetCmdList();
ECommandType ParseCommand(const char* cmd);

std::string JoinPath(const std::string& prefix, const std::string& path);

inline void RetryBackoff(
    NYdb::NTable::TTableClient& client,
    std::uint32_t retries,
    const NYdb::NTable::TTableClient::TOperationSyncFunc& func
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
            NYdb::NStatusHelpers::ThrowOnError(status);
        }
        Cerr << "Create request failed. Sleeping for " << delay << Endl;
        Sleep(delay);
        delay *= 2;
    }
}

std::string GenerateRandomString(std::uint32_t minLength, std::uint32_t maxLength);

NYdb::TParams PackValuesToParamsAsList(const std::vector<NYdb::TValue>& items, const std::string name = "$items");

// Returns special object_id within the same shard as given id
std::uint32_t GetSpecialId(std::uint32_t id);

// Returns special object_id for given shard
std::uint32_t GetShardSpecialId(std::uint64_t shardNo);

std::uint32_t GetHash(std::uint32_t value);

TTableStats GetTableStats(TDatabaseOptions& dbOptions, const std::string& tableName);

bool ParseOptionsCreate(int argc, char** argv, TCreateOptions& createOptions, bool followers = false);
bool ParseOptionsRun(int argc, char** argv, TRunOptions& runOptions, bool followers = false);
