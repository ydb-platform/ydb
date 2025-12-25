#pragma once

#include "ydb_command.h"
#include "ydb_common.h"
#include <library/cpp/streams/bzip2/bzip2.h>
#include <ydb/public/lib/ydb_cli/common/format.h>

#include <util/generic/string.h>
#include <vector>

namespace NYdb::NConsoleClient {

class TCommandClusterDiagnostics : public TClientCommandTree {
public:
    TCommandClusterDiagnostics();
};

class TCommandClusterDiagnosticsCollect : public TYdbReadOnlyCommand, public TCommandWithOutput {
public:
    TCommandClusterDiagnosticsCollect();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    void ProcessState(TConfig& config, TBZipCompress& compress, ui32 index = 0);

    ui32 DurationSeconds = 0;
    ui32 PeriodSeconds = 0;
    TString FileName;
    bool NoSanitize = false;
};

}
