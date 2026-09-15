#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NTPCC {

// do a very basic data check before doing anything else,
// in case of any issues print error and exit

void CheckPathForInit(
    const NConsoleClient::TClientCommand::TConfig& connectionConfig,
    const TString& path) noexcept;

void CheckPathForImport(
    const NConsoleClient::TClientCommand::TConfig& connectionConfig,
    const TString& path) noexcept;

void CheckPathForRun(
    const NConsoleClient::TClientCommand::TConfig& connectionConfig,
    const TString& path,
    int expectedWhCount) noexcept;

} // namespace NYdb::NTPCC
