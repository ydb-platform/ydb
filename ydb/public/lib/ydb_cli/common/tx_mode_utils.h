#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/tx.h>

#include <util/generic/fwd.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>

#include <optional>

namespace NYdb::NConsoleClient {

// YDB transaction mode names supported by interactive BEGIN.
//
// Not the full set of CLI-recognized tx-mode strings (online-ro / stale-ro
// remain valid for one-shot --tx-mode flags; they are simply not openable
// via Query.BeginTransaction and therefore not listed here).
constexpr TStringBuf kTxModeSerializableRw = "serializable-rw";
constexpr TStringBuf kTxModeSnapshotRo = "snapshot-ro";
constexpr TStringBuf kTxModeSnapshotRw = "snapshot-rw";
constexpr TStringBuf kTxModeReadCommittedRw = "read-committed-rw";

// YDB transaction mode names accepted by interactive BEGIN.
//
// Note: only modes that the Query service accepts via BeginTransaction are
// listed here. online-ro and stale-ro are intentionally excluded because the
// server rejects open transactions for them ("open transactions not supported
// for transaction mode ..."); they only make sense as one-shot tx_control
// values for individual ExecuteQuery requests.
TVector<TStringBuf> GetSupportedTxModeNames();

// Parse a single transaction mode token (e.g. "read-committed-rw") into TTxSettings.
// Returns std::nullopt if the token is not a recognized interactive mode
// (online-ro / stale-ro are not interactive — see GetSupportedTxModeNames).
std::optional<NQuery::TTxSettings> ParseTxModeName(TStringBuf mode);

// Parse the isolation specification of a BEGIN command line.
//
// Accepted forms (case-insensitive, trailing semicolon allowed):
//   BEGIN
//   BEGIN TRANSACTION
//   BEGIN [TRANSACTION] <mode>
//
// Returns:
//   - SerializableRW() for the bare command (no mode specified);
//   - the parsed TTxSettings otherwise;
//   - std::nullopt if the line is not a BEGIN command, the mode is unknown,
//     or trailing tokens are unrecognized.
std::optional<NQuery::TTxSettings> ParseBeginTransactionIsolation(TStringBuf line);

// Extracts the user-facing mode token from a BEGIN line, if any.
//
// Returns the lowercased mode token (e.g. "snapshot-ro") for inputs like
// "BEGIN snapshot-ro" or "BEGIN TRANSACTION read-committed-rw".
// Returns std::nullopt if the line is not a BEGIN command or has no mode
// token after the prefix. Used to produce targeted diagnostics for known but
// unsupported modes.
std::optional<TString> ExtractBeginModeToken(TStringBuf line);

// True if `mode` is a YDB tx mode that is valid only as a one-shot per-query
// tx_control (--tx-mode), not as an open interactive transaction. Today:
// online-ro and stale-ro.
bool IsOneShotOnlyTxMode(TStringBuf mode);

// True if a BEGIN line has tokens after the mode (e.g. "BEGIN serializable-rw
// extra"). Such lines are rejected as malformed rather than with the
// one-shot-mode hint.
bool HasTrailingBeginTokens(TStringBuf line);

// Human-readable list of supported mode names for help / error messages.
TString GetTxModeNamesForHelp();

// Returns the full set of TCL command-line forms accepted by the interactive
// transaction engine, suitable for use as TAB completion candidates.
TVector<TString> GetTransactionControlCompletions();

bool IsBeginCommand(TStringBuf line);
bool IsCommitCommand(TStringBuf line);
bool IsRollbackCommand(TStringBuf line);

} // namespace NYdb::NConsoleClient
