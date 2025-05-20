#pragma once

#include "ydb_command.h"
#include "ydb_common.h"

#include <ydb/public/lib/ydb_cli/common/format.h>

namespace NYdb::NConsoleClient {

class TCommandTableInfer : public TClientCommandTree {
public:
    TCommandTableInfer();
};

class TCommandTableInferFile : public TClientCommandTree {
public:
    TCommandTableInferFile();
};

class TCommandTableInferCsv : public TYdbCommand, public TCommandWithPath, public TCommandWithInput {
public:
    TCommandTableInferCsv();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    std::vector<std::string> FilePaths;
    bool ReadingFromStdin = false;
    TString ColumnNames;
    int RowsToAnalyze;
    bool Header = false;
};

}
