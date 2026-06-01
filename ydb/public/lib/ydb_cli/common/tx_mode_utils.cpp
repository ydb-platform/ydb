#include "tx_mode_utils.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <cctype>

namespace NYdb::NConsoleClient {

namespace {

// Tokenize a TCL command line into uppercased word tokens.
// Trailing semicolons (any number, possibly separated by whitespace) are
// dropped. Whitespace between tokens is collapsed, semicolons are treated as
// separators, all other characters become part of the current token.
//
// Examples:
//   "BEGIN"                              -> ["BEGIN"]
//   "begin transaction"                  -> ["BEGIN", "TRANSACTION"]
//   "BEGIN read-committed-rw;"           -> ["BEGIN", "READ-COMMITTED-RW"]
//   "BEGIN snapshot-ro"                  -> ["BEGIN", "SNAPSHOT-RO"]
TVector<TString> TokenizeUpper(TStringBuf line) {
    TVector<TString> tokens;
    TString current;
    auto flush = [&]() {
        if (!current.empty()) {
            tokens.push_back(std::move(current));
            current.clear();
        }
    };
    for (char c : line) {
        const unsigned char uc = static_cast<unsigned char>(c);
        if (std::isspace(uc) || c == ';') {
            flush();
        } else {
            current.push_back(static_cast<char>(std::toupper(uc)));
        }
    }
    flush();
    return tokens;
}

// Returns true if tokens start with the given uppercase prefix.
bool TokensStartWith(const TVector<TString>& tokens, std::initializer_list<TStringBuf> prefix) {
    if (tokens.size() < prefix.size()) {
        return false;
    }
    size_t i = 0;
    for (auto p : prefix) {
        if (tokens[i++] != p) {
            return false;
        }
    }
    return true;
}

// How many leading tokens form the BEGIN/START preamble. Returns 0 if the
// line does not start with a BEGIN-style preamble.
size_t MatchBeginPrefix(const TVector<TString>& tokens) {
    if (tokens.empty()) {
        return 0;
    }
    if (TokensStartWith(tokens, {"START", "TRANSACTION"})) {
        return 2;
    }
    if (TokensStartWith(tokens, {"BEGIN", "TRANSACTION"})) {
        return 2;
    }
    if (TokensStartWith(tokens, {"BEGIN", "WORK"})) {
        return 2;
    }
    if (tokens.front() == "BEGIN") {
        return 1;
    }
    return 0;
}

bool MatchExactCommand(const TVector<TString>& tokens, std::initializer_list<TStringBuf> expected) {
    if (tokens.size() != expected.size()) {
        return false;
    }
    return TokensStartWith(tokens, expected);
}

} // anonymous namespace

TVector<TStringBuf> GetSupportedTxModeNames() {
    // Only modes that can be opened via Query.BeginTransaction. online-ro
    // and stale-ro are intentionally excluded — server-side BeginTransaction
    // rejects them with "open transactions not supported for transaction
    // mode ..."; they are usable only as one-shot tx_control values.
    return {
        kTxModeSerializableRw,
        kTxModeSnapshotRo,
        kTxModeSnapshotRw,
        kTxModeReadCommittedRw,
    };
}

std::optional<NQuery::TTxSettings> ParseTxModeName(TStringBuf mode) {
    const TString normalized = to_lower(TString(mode));
    if (normalized == kTxModeSerializableRw) {
        return NQuery::TTxSettings::SerializableRW();
    }
    if (normalized == kTxModeSnapshotRo) {
        return NQuery::TTxSettings::SnapshotRO();
    }
    if (normalized == kTxModeSnapshotRw) {
        return NQuery::TTxSettings::SnapshotRW();
    }
    if (normalized == kTxModeReadCommittedRw) {
        return NQuery::TTxSettings::ReadCommittedRW();
    }
    return {};
}

TString GetTxModeNamesForHelp() {
    TStringBuilder builder;
    bool first = true;
    for (const auto name : GetSupportedTxModeNames()) {
        if (!first) {
            builder << ", ";
        }
        first = false;
        builder << name;
    }
    return std::move(builder);
}

TVector<TString> GetTransactionControlCompletions() {
    TVector<TString> result;

    // Every BEGIN / START prefix can optionally be followed by a tx mode.
    static constexpr TStringBuf kBeginPrefixes[] = {
        "BEGIN",
        "BEGIN TRANSACTION",
        "BEGIN WORK",
        "START TRANSACTION",
    };
    const auto modes = GetSupportedTxModeNames();
    for (auto prefix : kBeginPrefixes) {
        result.emplace_back(prefix);
        for (auto mode : modes) {
            result.emplace_back(TStringBuilder() << prefix << " " << mode);
        }
    }

    static constexpr TStringBuf kCommitForms[] = {
        "COMMIT",
        "COMMIT TRANSACTION",
        "COMMIT WORK",
        "END",
        "END TRANSACTION",
    };
    for (auto form : kCommitForms) {
        result.emplace_back(form);
    }

    static constexpr TStringBuf kRollbackForms[] = {
        "ROLLBACK",
        "ROLLBACK TRANSACTION",
        "ROLLBACK WORK",
    };
    for (auto form : kRollbackForms) {
        result.emplace_back(form);
    }

    return result;
}

std::optional<NQuery::TTxSettings> ParseBeginTransactionIsolation(TStringBuf line) {
    const auto tokens = TokenizeUpper(line);
    const size_t skip = MatchBeginPrefix(tokens);
    if (skip == 0) {
        return {};
    }

    // Bare BEGIN/START TRANSACTION/etc. defaults to SerializableRW.
    if (skip == tokens.size()) {
        return NQuery::TTxSettings::SerializableRW();
    }

    const TString& modeToken = tokens[skip];
    auto settings = ParseTxModeName(modeToken);
    if (!settings) {
        return {};
    }

    // No trailing tokens are allowed after the mode.
    if (skip + 1 != tokens.size()) {
        return {};
    }
    return settings;
}

std::optional<TString> ExtractBeginModeToken(TStringBuf line) {
    const auto tokens = TokenizeUpper(line);
    const size_t skip = MatchBeginPrefix(tokens);
    if (skip == 0 || skip >= tokens.size()) {
        return {};
    }
    return to_lower(tokens[skip]);
}

bool IsOneShotOnlyTxMode(TStringBuf mode) {
    const TString normalized = to_lower(TString(mode));
    return normalized == "online-ro" || normalized == "stale-ro";
}

bool IsBeginCommand(TStringBuf line) {
    const auto tokens = TokenizeUpper(line);
    return MatchBeginPrefix(tokens) > 0;
}

bool IsCommitCommand(TStringBuf line) {
    const auto tokens = TokenizeUpper(line);
    return MatchExactCommand(tokens, {"COMMIT"})
        || MatchExactCommand(tokens, {"COMMIT", "TRANSACTION"})
        || MatchExactCommand(tokens, {"COMMIT", "WORK"})
        || MatchExactCommand(tokens, {"END"})
        || MatchExactCommand(tokens, {"END", "TRANSACTION"});
}

bool IsRollbackCommand(TStringBuf line) {
    const auto tokens = TokenizeUpper(line);
    return MatchExactCommand(tokens, {"ROLLBACK"})
        || MatchExactCommand(tokens, {"ROLLBACK", "TRANSACTION"})
        || MatchExactCommand(tokens, {"ROLLBACK", "WORK"});
}

} // namespace NYdb::NConsoleClient
