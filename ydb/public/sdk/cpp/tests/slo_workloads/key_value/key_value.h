#pragma once

#include <ydb/public/sdk/cpp/tests/slo_workloads/utils/utils.h>
#include <ydb/public/sdk/cpp/tests/slo_workloads/utils/executor.h>
#include <ydb/public/sdk/cpp/tests/slo_workloads/utils/generator.h>
#include <ydb/public/sdk/cpp/tests/slo_workloads/utils/job.h>

extern const std::string TableName;

NYdb::TValue BuildValueFromRecord(const TKeyValueRecordData& recordData);

// Initial content generation
class TGenerateInitialContentJob : public TThreadJob {
public:
    TGenerateInitialContentJob(const TCreateOptions& createOpts, std::uint32_t maxId);
    void ShowProgress(TStringBuilder& report) override;
    void DoJob() override;
    void OnFinish() override;

private:
    TExecutor Executor;
    TPackGenerator<TKeyValueGenerator, TKeyValueRecordData> PackGenerator;
    std::uint64_t Total;
};

// Write workload job
class TWriteJob : public TThreadJob {
public:
    TWriteJob(const TCommonOptions& opts, std::uint32_t maxId);
    void ShowProgress(TStringBuilder& report) override;
    void DoJob() override;
    void OnFinish() override;

private:
    TExecutor Executor;
    TKeyValueGenerator Generator;
    std::atomic<std::uint64_t> ValuesGenerated = 0;
};

// Read workload job  
class TReadJob : public TThreadJob {
public:
    TReadJob(const TCommonOptions& opts, std::uint32_t maxId);
    void ShowProgress(TStringBuilder& report) override;
    void DoJob() override;
    void OnFinish() override;

private:
    std::unique_ptr<TExecutor> Executor;
    std::uint32_t ObjectIdRange;
    bool SaveResult;
};

int CreateTable(TDatabaseOptions& dbOptions);
int DropTable(TDatabaseOptions& dbOptions);

// Creates a table and fills it with initial content
int DoCreate(TDatabaseOptions& dbOptions, int argc, char** argv);
// Not implemented
int DoRun(TDatabaseOptions& dbOptions, int argc, char** argv);
// Drops the table
int DoCleanup(TDatabaseOptions& dbOptions, int argc);
