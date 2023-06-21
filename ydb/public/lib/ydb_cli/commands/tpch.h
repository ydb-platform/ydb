#pragma once

#include <util/generic/set.h>

#include "ydb_command.h"

namespace NYdb::NConsoleClient {

class TTpchCommandInit : public NYdb::NConsoleClient::TYdbCommand {
public:
    TTpchCommandInit();
    void Config(TConfig& config);
    int Run(TConfig& config);

private:
    void SetPartitionByCols(TString& createSql);

    TString TablesPath;
    TString StoreType;
};

class TTpchCommandClean : public NYdb::NConsoleClient::TYdbCommand {
public:
    TTpchCommandClean();
    void Config(TConfig& config);
    int Run(TConfig& config);

private:
    std::vector<TString> Tables = {"customer", "lineitem", "nation", "orders",
        "region", "part", "partsupp", "supplier"};
};

class TTpchCommandRun : public NYdb::NConsoleClient::TYdbCommand {
protected:
    TSet<ui32> QueriesToRun;
    TSet<ui32> QueriesToSkip;
    TVector<TString> QuerySettings;
    TString ExternalQueries;
    TString ExternalQueriesFile;
    TString ExternalQueriesDir;
    TString ExternalVariablesString;
public:
    TTpchCommandRun();
    void Config(TConfig& config);
    int Run(TConfig& config);
    TString PatchQuery(const TStringBuf& original) const;
    bool NeedRun(const ui32 queryIdx) const;
    bool RunBench(TConfig& config);

    TVector<TString> GetQueries() const;

    TString TablesPath;
    TString OutFilePath;
    ui32 IterationsCount;
    TString JsonReportFileName;
    TString MiniStatFileName;
    TString Table;
};

class TCommandTpch : public NYdb::NConsoleClient::TClientCommandTree {
public:
    TCommandTpch();
};

}  // namespace NYdb::NConsoleClient
