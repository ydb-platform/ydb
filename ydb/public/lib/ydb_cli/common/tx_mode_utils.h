#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/tx.h>

#include <util/generic/fwd.h>
#include <util/generic/strbuf.h>

#include <optional>

namespace NYdb::NConsoleClient {

// YDB transaction mode names accepted by CLI (e.g. ydb table query --tx-mode).
constexpr TStringBuf kTxModeSerializableRw = "serializable-rw";
constexpr TStringBuf kTxModeOnlineRo = "online-ro";
constexpr TStringBuf kTxModeStaleRo = "stale-ro";
constexpr TStringBuf kTxModeSnapshotRo = "snapshot-ro";
constexpr TStringBuf kTxModeSnapshotRw = "snapshot-rw";
constexpr TStringBuf kTxModeReadCommittedRw = "read-committed-rw";

// Parse a transaction mode string into TTxSettings.
// Accepts YDB mode names (serializable-rw, read-committed-rw, ...) and
// PostgreSQL-style isolation level names (READ COMMITTED, SERIALIZABLE, ...).
std::optional<NQuery::TTxSettings> ParseTxSettings(TStringBuf mode);

// Human-readable list of supported mode names for help messages.
TString GetTxModeNamesForHelp();

// Parse isolation level from a BEGIN / START TRANSACTION command line.
std::optional<NQuery::TTxSettings> ParseBeginTransactionIsolation(TStringBuf line);

bool IsBeginCommand(TStringBuf line);
bool IsCommitCommand(TStringBuf line);
bool IsRollbackCommand(TStringBuf line);

} // namespace NYdb::NConsoleClient
