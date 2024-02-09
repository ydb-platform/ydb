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
    TString S3Endpoint;
    TString S3Prefix;
};

class TTpchCommandClean : public NYdb::NConsoleClient::TYdbCommand {
public:
    TTpchCommandClean();
    void Config(TConfig& config);
    int Run(TConfig& config);

private:
    std::vector<TString> Tables = {"customer", "lineitem", "nation", "orders",
        "region", "part", "partsupp", "supplier"};
    bool IsExternal = false;
    TString TablesPath;
};

class TTpchCommandRun : public NYdb::NConsoleClient::TYdbCommand {
protected:
    TSet<ui32> QueriesToRun;
    TSet<ui32> QueriesToSkip;
    TVector<TString> QuerySettings;
    TString ExternalQueriesDir;
    TString ExternalVariablesString;
    TString QueryExecuterType;

public:
    TTpchCommandRun();
    void Config(TConfig& config);
    int Run(TConfig& config);
    TString PatchQuery(const TStringBuf& original) const;
    bool NeedRun(const ui32 queryIdx) const;

    template <typename TClient>
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
