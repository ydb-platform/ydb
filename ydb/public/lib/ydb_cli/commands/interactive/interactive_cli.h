#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/lib/ydb_cli/common/colors.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

#include <util/generic/string.h>

namespace NYdb::NConsoleClient {

class TInteractiveCLI {
    inline const static NColorizer::TColors Colors = NConsoleClient::AutoColors(Cout);

    struct TVersionInfo {
        TString CliVersion;
        TString ServerVersion;
        TString ServerAvailableCheckFail;
    };

public:
    explicit TInteractiveCLI(const TString& profileName);

    int Run(TClientCommand::TConfig& config);

private:
    TVersionInfo ResolveVersionInfo(const TDriver& driver) const;

private:
    const TString Profile;
};

} // namespace NYdb::NConsoleClient
