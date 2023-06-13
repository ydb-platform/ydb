#pragma once

#include <util/generic/set.h>

#include "ydb_command.h"

namespace NYdb::NConsoleClient {

class TClickBenchCommandInit : public NYdb::NConsoleClient::TYdbCommand {
public:
    TClickBenchCommandInit();
    void Config(TConfig& config);
    int Run(TConfig& config);

private:
    TString Table;
    TString StoreType;
};

class TClickBenchCommandClean : public NYdb::NConsoleClient::TYdbCommand {
public:
    TClickBenchCommandClean();
    void Config(TConfig& config);
    int Run(TConfig& config);

private:
    TString Table;
};

class TClickBenchCommandRun : public NYdb::NConsoleClient::TYdbCommand {
protected:
    TSet<ui32> QueriesToRun;
    TSet<ui32> QueriesToSkip;
    TVector<TString> QuerySettings;
    TString ExternalQueries;
    TString ExternalQueriesFile;
    TString ExternalQueriesDir;
    TString ExternalResultsDir;
    TString ExternalVariablesString;

    TMap<ui32, TString> LoadExternalResults() const;
public:
    TClickBenchCommandRun();
    void Config(TConfig& config);
    int Run(TConfig& config);
    TString PatchQuery(const TStringBuf& original) const;
    bool NeedRun(const ui32 queryIdx) const;
    bool RunBench(TConfig& config);

    class TQueryFullInfo {
    private:
        TString Query;
        TString ExpectedResult;
    public:
        TQueryFullInfo(const TString& query, const TString& expectedResult)
            : Query(query)
            , ExpectedResult(expectedResult)
        {

        }

        bool IsCorrectResult(const TString& result) const {
            if (!ExpectedResult) {
                return true;
            }
            return result == ExpectedResult;
        }

        const TString& GetQuery() const {
            return Query;
        }

        const TString& GetExpectedResult() const {
            return ExpectedResult;
        }
    };

    TVector<TQueryFullInfo> GetQueries(const TString& fullTablePath) const;

    TString OutFilePath;
    ui32 IterationsCount;
    TString JsonReportFileName;
    TString MiniStatFileName;
    TString Table;
};

class TCommandClickBench : public NYdb::NConsoleClient::TClientCommandTree {
public:
    TCommandClickBench();
};

}  // namespace NYdb::NConsoleClient
