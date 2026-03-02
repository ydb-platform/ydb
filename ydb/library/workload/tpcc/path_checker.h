#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NTPCC {

// do a very basic data check before doing anything else,
// in case of any issues print error and exit
struct TRunConfig;

void CheckPathForInit(
    const NConsoleClient::TClientCommand::TConfig& connectionConfig,
    const TString& path) noexcept;

void CheckPathForImport(
    const NConsoleClient::TClientCommand::TConfig& connectionConfig,
    const TRunConfig& runConfig) noexcept;

void CheckPathForRun(
    const NConsoleClient::TClientCommand::TConfig& connectionConfig,
    const TString& path,
    int expectedWhCount) noexcept;

int GetTableSize(TDriver& driver, const TString& path, const char* table) noexcept;

} // namespace NYdb::NTPCC
