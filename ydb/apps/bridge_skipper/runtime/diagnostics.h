#pragma once

#include "commands.h"
#include <util/datetime/base.h>
#include <deque>
#include <mutex>
#include <optional>

namespace NBridgeSkipper {

// Bounded process-local diagnostics, deliberately outside the per-tick snapshot.
class TDiagnosticStore {
public:
    explicit TDiagnosticStore(TString bootId, TString captureLevel = "info");

    void History(TString type, NJson::TJsonValue details, TInstant at);
    void Log(TString level, TString message, TInstant at);
    void Operation(TString id, NJson::TJsonValue value, bool inFlight);
    // Tail limits bound returned row copies; metadata always describes the
    // whole retained stream. Full API/export reads leave the limit unset.
    // tail_clipped describes omitted retained rows, separately from a lost
    // cursor or an initial read whose older stream records were pruned.
    NJson::TJsonValue ReadHistory(TStringBuf after = {}, std::optional<size_t> tailLimit = {}) const;
    NJson::TJsonValue ReadLogs(
        TStringBuf after = {},
        TStringBuf level = {},
        TStringBuf search = {},
        TStringBuf pile = {},
        std::optional<size_t> tailLimit = {}) const;
    std::optional<NJson::TJsonValue> ReadOperation(const TString& id) const;
    const TString& CaptureLevel() const;

private:
    NJson::TJsonValue Read(
        const std::deque<NJson::TJsonValue>& rows,
        ui64 produced,
        TStringBuf after,
        TStringBuf level,
        TStringBuf search,
        TStringBuf pile,
        std::optional<size_t> tailLimit) const;
    TString BootId;
    TString Level;
    mutable std::mutex Mutex;
    ui64 NextHistory = 0;
    ui64 NextLog = 0;
    std::deque<NJson::TJsonValue> HistoryRows;
    std::deque<NJson::TJsonValue> LogRows;
    std::map<TString, NJson::TJsonValue> Operations;
    std::deque<TString> Completed;
};

} // namespace NBridgeSkipper
