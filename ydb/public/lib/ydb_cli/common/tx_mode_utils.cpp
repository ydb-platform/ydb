#include "tx_mode_utils.h"

#include <util/string/builder.h>
#include <util/string/strip.h>

namespace NYdb::NConsoleClient {

namespace {

TString NormalizeModeToken(TStringBuf token) {
    TString normalized = to_lower(TString(token));
    for (auto& c : normalized) {
        if (c == ' ') {
            c = '-';
        }
    }
    return normalized;
}

std::optional<NQuery::TTxSettings> ParseYdbTxModeName(TStringBuf mode) {
    const TString normalized = NormalizeModeToken(mode);
    if (normalized == kTxModeSerializableRw || normalized == "serializable") {
        return NQuery::TTxSettings::SerializableRW();
    }
    if (normalized == kTxModeOnlineRo || normalized == "online") {
        return NQuery::TTxSettings::OnlineRO();
    }
    if (normalized == kTxModeStaleRo || normalized == "stale") {
        return NQuery::TTxSettings::StaleRO();
    }
    if (normalized == kTxModeSnapshotRo) {
        return NQuery::TTxSettings::SnapshotRO();
    }
    if (normalized == kTxModeSnapshotRw) {
        return NQuery::TTxSettings::SnapshotRW();
    }
    if (normalized == kTxModeReadCommittedRw || normalized == "read-committed") {
        return NQuery::TTxSettings::ReadCommittedRW();
    }
    return {};
}

std::optional<NQuery::TTxSettings> ParsePsqlIsolationLevel(TStringBuf level) {
    const auto normalized = to_upper(TString(level));
    if (normalized == "READ UNCOMMITTED") {
        return NQuery::TTxSettings::OnlineRO(
            NQuery::TTxOnlineSettings().AllowInconsistentReads(true));
    }
    if (normalized == "READ COMMITTED") {
        return NQuery::TTxSettings::ReadCommittedRW();
    }
    if (normalized == "REPEATABLE READ") {
        return NQuery::TTxSettings::SnapshotRW();
    }
    if (normalized == "SERIALIZABLE") {
        return NQuery::TTxSettings::SerializableRW();
    }
    return {};
}

TString StripTransactionKeywords(TStringBuf line) {
    TString normalized = to_upper(TString(line));
    StripInPlace(normalized);

    if (normalized.StartsWith("START TRANSACTION")) {
        normalized = normalized.substr(strlen("START TRANSACTION"));
    } else if (normalized.StartsWith("BEGIN TRANSACTION")) {
        normalized = normalized.substr(strlen("BEGIN TRANSACTION"));
    } else if (normalized.StartsWith("BEGIN WORK")) {
        normalized = normalized.substr(strlen("BEGIN WORK"));
    } else if (normalized.StartsWith("BEGIN")) {
        normalized = normalized.substr(strlen("BEGIN"));
    } else {
        return {};
    }

    StripInPlace(normalized);
    return normalized;
}

} // anonymous namespace

std::optional<NQuery::TTxSettings> ParseTxSettings(TStringBuf mode) {
    if (mode.empty()) {
        return NQuery::TTxSettings::SerializableRW();
    }

    if (auto settings = ParseYdbTxModeName(mode)) {
        return settings;
    }
    return ParsePsqlIsolationLevel(mode);
}

TString GetTxModeNamesForHelp() {
    return TStringBuilder()
        << kTxModeSerializableRw << ", "
        << kTxModeReadCommittedRw << ", "
        << kTxModeOnlineRo << ", "
        << kTxModeStaleRo << ", "
        << kTxModeSnapshotRo << ", "
        << kTxModeSnapshotRw
        << " (or PostgreSQL-style ISOLATION LEVEL: READ COMMITTED, SERIALIZABLE, REPEATABLE READ, READ UNCOMMITTED)";
}

std::optional<NQuery::TTxSettings> ParseBeginTransactionIsolation(TStringBuf line) {
    const TString remainder = StripTransactionKeywords(line);
    if (remainder.empty()) {
        return NQuery::TTxSettings::SerializableRW();
    }

    TString spec = remainder;
    const auto upperSpec = to_upper(spec);
    const char* isolationPrefix = "ISOLATION LEVEL ";
    if (upperSpec.StartsWith(isolationPrefix)) {
        spec = spec.substr(strlen(isolationPrefix));
        StripInPlace(spec);
        return ParsePsqlIsolationLevel(spec);
    }

    return ParseTxSettings(spec);
}

bool IsCommitCommand(TStringBuf line) {
    TString normalized = to_upper(TString(line));
    StripInPlace(normalized);
    return normalized == "COMMIT"
        || normalized == "COMMIT WORK"
        || normalized == "COMMIT TRANSACTION"
        || normalized == "END"
        || normalized == "END TRANSACTION";
}

bool IsRollbackCommand(TStringBuf line) {
    TString normalized = to_upper(TString(line));
    StripInPlace(normalized);
    return normalized == "ROLLBACK"
        || normalized == "ROLLBACK WORK"
        || normalized == "ROLLBACK TRANSACTION";
}

bool IsBeginCommand(TStringBuf line) {
    TString normalized = to_upper(TString(line));
    StripInPlace(normalized);
    return normalized.StartsWith("BEGIN")
        || normalized.StartsWith("START TRANSACTION");
}

} // namespace NYdb::NConsoleClient
