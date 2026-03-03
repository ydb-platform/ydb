#pragma once

#include <ydb/public/lib/ydb_cli/common/command.h>
#include <memory>

namespace NKikimr::NDriverClient {

// Fully refactored commands (Config/Run pattern)
std::unique_ptr<NYdb::NConsoleClient::TClientCommand> NewCommandFormatInfo();
std::unique_ptr<NYdb::NConsoleClient::TClientCommand> NewCommandFormatUtil();
std::unique_ptr<NYdb::NConsoleClient::TClientCommand> NewCommandNodeByHost();

// Delegate wrappers (forward to existing Main*() functions)
std::unique_ptr<NYdb::NConsoleClient::TClientCommand> NewCommandSchemeInitRoot();
std::unique_ptr<NYdb::NConsoleClient::TClientCommand> NewCommandPersQueueRequest();
std::unique_ptr<NYdb::NConsoleClient::TClientCommand> NewCommandPersQueueStress();
std::unique_ptr<NYdb::NConsoleClient::TClientCommand> NewCommandPersQueueDiscoverClusters();
std::unique_ptr<NYdb::NConsoleClient::TClientCommand> NewCommandActorsysPerfTest();

} // namespace NKikimr::NDriverClient
