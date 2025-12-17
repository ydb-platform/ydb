#pragma once

#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_log.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

#include <util/generic/string.h>

#include <library/cpp/logger/log.h>

namespace NYdb::NConsoleClient {

class TInteractiveCLI {
    inline const static NColorizer::TColors Colors = NConsoleClient::AutoColors(Cout);

    struct TVersionInfo {
        TString CliVersion;
        TString ServerVersion;
        TString ServerAvailableCheckFail;
    };

public:
    TInteractiveCLI(const TString& profileName, const TString& ydbPath);

    int Run(TClientCommand::TConfig& config);

private:
    TVersionInfo ResolveVersionInfo(const TDriver& driver) const;

private:
    const TString Profile;
    const TString YdbPath;
    TInteractiveLogger Log;
};

} // namespace NYdb::NConsoleClient
