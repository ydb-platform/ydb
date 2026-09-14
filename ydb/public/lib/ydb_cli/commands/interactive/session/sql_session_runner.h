#pragma once

#include "session_runner_interface.h"

#include <ydb/public/lib/ydb_cli/common/lazy_driver.h>

namespace NYdb::NConsoleClient {

struct TSqlSessionSettings {
    // Lazy driver dedicated to YQL query execution; stopped at the end of
    // every HandleLine and re-created on the next turn.
    TLazyDriver::TPtr SqlLazyDriver;

    // Lazy driver dedicated to interactive transactions: a Query service
    // session opened on BEGIN lives on this driver until COMMIT/ROLLBACK
    // (or an error that closes the transaction).
    TLazyDriver::TPtr SqlTxLazyDriver;

    // Lazy driver used by the YQL completer for schema lookups; must be set,
    // since SQL session always enables YQL completion.
    TLazyDriver::TPtr CompleterLazyDriver;

    TString Database;
    bool EnableAiInteractive = false;
    bool EnableInteractiveTransactions = false;
};

ISessionRunner::TPtr CreateSqlSessionRunner(const TSqlSessionSettings& settings);

} // namespace NYdb::NConsoleClient
