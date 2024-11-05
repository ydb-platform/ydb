#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/lib/ydb_cli/common/root.h>


namespace NYdb::NTpch {

class TTpchCommandBase : public NConsoleClient::TYdbCommand {
public:
    TTpchCommandBase(const TString& name, const std::initializer_list<TString>& aliases, const TString& description)
        : NConsoleClient::TYdbCommand(name, aliases, description)
    {}

    void SetPath(const TString& path) {
        Path = path;
    }
protected:
    TString Path; // absolute path to TPC-H tables dir
};

class TClientCommandTpchRoot : public NConsoleClient::TClientCommandRootBase {
public:
    TClientCommandTpchRoot();

    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    void ParseAddress(TConfig& config) override;
    void Validate(TConfig& config) override;
    int Run(TConfig& config) override;
private:
    TString Database;
    TString Path; // relative path to TPC-H tables dir
};


int NewTpchClient(int argc, char** argv);

} // NYdb::NTpch
