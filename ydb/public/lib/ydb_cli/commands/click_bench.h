#pragma once

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>
#include <ydb/public/lib/ydb_cli/common/root.h>
#include <util/generic/set.h>

#include "ydb_command.h"

struct IQueryRunner {
    virtual ~IQueryRunner() = default;
    virtual TString GetQueries(const TString& fullTablePath, const TString& externalQueries) = 0;
    virtual std::pair<TString, TString> Execute(const TString& query) = 0;
};

class TBenchContext {
protected:
    TSet<ui32> QueriesToRun;
    TSet<ui32> QueriesToSkip;
    bool DisableLlvm = false;
    bool EnablePushdown = false;
    TString ExternalQueries;
    TString ExternalQueriesFile;
public:
    TString OutFilePath;
    ui32 IterationsCount;
    TString JsonReportFileName;

    TString GetExternalQueries() const {
        TString externalQueries;
        if (ExternalQueries) {
            externalQueries = ExternalQueries;
        } else if (ExternalQueriesFile) {
            TFileInput fInput(ExternalQueriesFile);
            externalQueries = fInput.ReadAll();
        }
        return externalQueries;
    }

    TString PatchQuery(const TStringBuf& original) const;
    bool NeedRun(const ui32 queryIdx) const;
};

class TClickHouseBench {
public:
    TClickHouseBench(NYdb::TDriver& driver, const TString& db, const TString& path, const TString& table)
        : Driver(driver)
        , Database(db)
        , Path(path)
        , Table(table)
    {}

    void Init();
    bool RunBench(IQueryRunner& queryRunner, const TBenchContext& context);

private:
    struct TTestInfo {
        TDuration ColdTime;
        TDuration Min;
        TDuration Max;
        double Mean = 0;
        double Std = 0;
    };

    static TTestInfo AnalyzeTestRuns(const TVector<TDuration>& timings);
    TString FullTablePath() const;

    NYdb::TDriver& Driver;
    const TString& Database;
    const TString& Path;
    const TString& Table;
};


class TClickBenchCommandInit : public NYdb::NConsoleClient::TYdbCommand {
public:
    TClickBenchCommandInit();
    void Config(TConfig& config);
    int Run(TConfig& config);

private:
    TString Path;
    TString Table;
};


class TStreamQueryRunner : public IQueryRunner {
public:
    explicit TStreamQueryRunner(NYdb::TDriver& driver)
        : SqClient(driver)
    {}
    TString GetQueries(const TString& fullTablePath, const TString& externalQueries) override;
    std::pair<TString, TString> Execute(const TString& query) override;

private:
    std::pair<TString, TString> ResultToYson(NYdb::NTable::TScanQueryPartIterator& it);

private:
    NYdb::NTable::TTableClient SqClient;
};

class TClickBenchCommandRun : public NYdb::NConsoleClient::TYdbCommand, public TBenchContext {
public:
    TClickBenchCommandRun();
    void Config(TConfig& config);
    int Run(TConfig& config);
private:
    TString Path;
    TString Table;
};

class TCommandClickBench : public NYdb::NConsoleClient::TClientCommandTree {
public:
    TCommandClickBench();
};

