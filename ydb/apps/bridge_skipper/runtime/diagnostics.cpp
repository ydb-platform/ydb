#include "diagnostics.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <util/string/cast.h>

#include <algorithm>
#include <vector>

namespace NBridgeSkipper {
namespace {

int Rank(TStringBuf level) {
    if (level == "trace") {
        return 0;
    }
    if (level == "debug") {
        return 1;
    }
    if (level == "info") {
        return 2;
    }
    if (level == "warn") {
        return 3;
    }
    if (level == "error") {
        return 4;
    }
    if (level == "fatal") {
        return 5;
    }
    return 2;
}

bool ContainsFolded(TString haystack, TStringBuf needle) {
    TString pattern(needle);
    haystack.to_lower();
    pattern.to_lower();
    return haystack.Contains(pattern);
}

TString LogSummary(const TString& message) {
    NJson::TJsonValue structured;
    if (!NJson::ReadJsonTree(message, &structured) || !structured.IsMap() || !structured["message"].IsString()) {
        return message;
    }
    TString summary;
    for (const TStringBuf key : {"component", "event", "message"}) {
        const auto& part = structured[TString(key)];
        if (part.IsString() && !part.GetString().empty()) {
            if (!summary.empty()) {
                summary += " · ";
            }
            summary += part.GetString();
        }
    }
    return summary;
}

TString HistoryOriginLabel(TStringBuf origin) {
    if (origin == "baseline") {
        return "Baseline";
    }
    if (origin == "inferred") {
        return "Inferred";
    }
    if (origin == "manual") {
        return "Manual";
    }
    if (origin == "auto") {
        return "Auto";
    }
    if (origin == "operator") {
        return "Operator";
    }
    if (origin == "skipper") {
        return "Skipper";
    }
    return "External/unknown";
}

TString HistorySummary(const NJson::TJsonValue& row) {
    TString summary;
    const auto& type = row["type"].GetString();
    if (row["message"].IsString()) {
        summary = row["message"].GetString();
    } else if (row["reason"].IsString()) {
        summary = row["reason"].GetString();
    } else if (type == "mode") {
        summary = "Mode " + row["mode"].GetString();
    } else if (type == "operation_sent") {
        summary = "Requested " + (row["kind"].IsString() ? row["kind"].GetString() : TString("operation"));
    } else if (type == "operation_outcome") {
        summary = row["outcome"].IsString() ? row["outcome"].GetString() : TString("Request outcome");
    } else if (type == "view") {
        summary = "Observed state";
    } else {
        summary = type.empty() ? TString("Event") : type;
    }
    if (row["coverage_gap"].GetBoolean()) {
        summary += " · " + (row["coverage_gap_text"].IsString() ? row["coverage_gap_text"].GetString()
            : TString("Coverage gap; intermediate events were not observed."));
    }
    return summary;
}

} // namespace


TDiagnosticStore::TDiagnosticStore(TString bootId, TString captureLevel)
    : BootId(std::move(bootId))
    , Level(std::move(captureLevel)) {
}

void TDiagnosticStore::History(TString type, NJson::TJsonValue details, TInstant at) {
    details["type"] = std::move(type);
    details["at"] = at.ToString();
    if (!details["origin"].IsString()) {
        if (details["type"].GetString() == "mode") {
            details["origin"] = "operator";
        } else if (details["type"].GetString() == "decision") {
            details["origin"] = "skipper";
        }
    }
    details["origin_label"] = HistoryOriginLabel(details["origin"].GetString());
    details["summary"] = HistorySummary(details);
    std::lock_guard lock(Mutex);
    details["cursor"] = BootId + ":" + ::ToString(++NextHistory);
    HistoryRows.push_back(std::move(details));
    if (HistoryRows.size() > 1000) {
        HistoryRows.pop_front();
    }
}

void TDiagnosticStore::Log(TString level, TString message, TInstant at) {
    NJson::TJsonValue row(NJson::JSON_MAP);
    row["level"] = std::move(level);
    row["summary"] = LogSummary(message);
    row["message"] = std::move(message);
    row["at"] = at.ToString();
    std::lock_guard lock(Mutex);
    row["cursor"] = BootId + ":" + ::ToString(++NextLog);
    LogRows.push_back(std::move(row));
    if (LogRows.size() > 5000) {
        LogRows.pop_front();
    }
}

void TDiagnosticStore::Operation(TString id, NJson::TJsonValue value, bool inFlight) {
    std::lock_guard lock(Mutex);
    value["operation_id"] = id;
    Operations[id] = std::move(value);
    if (!inFlight) {
        Completed.push_back(std::move(id));
        if (Completed.size() > 1000) {
            Operations.erase(Completed.front());
            Completed.pop_front();
        }
    }
}

NJson::TJsonValue TDiagnosticStore::Read(
    const std::deque<NJson::TJsonValue>& rows,
    ui64 produced,
    TStringBuf after,
    TStringBuf level,
    TStringBuf search,
    TStringBuf pile,
    std::optional<size_t> tailLimit) const
{
    NJson::TJsonValue result(NJson::JSON_MAP);
    result["boot_id"] = BootId;
    // Counts describe the whole current-boot retained stream before filtering.
    result["retained_count"] = static_cast<ui64>(rows.size());
    result["total_count"] = produced;
    result["dropped_count"] = produced - rows.size();
    result["oldest_at"] = rows.empty() ? NJson::TJsonValue() : rows.front()["at"];
    result["newest_at"] = rows.empty() ? NJson::TJsonValue() : rows.back()["at"];
    result["rows"] = NJson::TJsonValue(NJson::JSON_ARRAY);
    bool found = after.empty();
    size_t first = 0;
    if (!after.empty()) {
        for (size_t i = 0; i < rows.size(); ++i) {
            if (rows[i]["cursor"].GetString() == after) {
                found = true;
                first = i + 1;
                break;
            }
        }
    }
    const bool gap = !found || (after.empty() && produced > rows.size());
    bool tailClipped = false;
    const size_t limit = tailLimit.value_or(rows.size());
    std::vector<const NJson::TJsonValue*> selected;
    selected.reserve(std::min(limit, rows.size()));
    // Select pointers before copying: state excerpts never materialize the full
    // captured ring. One additional match detects omitted cursor coverage.
    for (size_t i = rows.size(); i > first; --i) {
        const auto& row = rows[i - 1];
        if ((level.empty() || Rank(row["level"].GetString()) >= Rank(level))
            && (search.empty() || ContainsFolded(NJson::WriteJson(row, false, true), search))
            && (pile.empty() || ContainsFolded(row["message"].GetString(), pile)))
        {
            if (selected.size() == limit) {
                tailClipped = true;
                break;
            }
            selected.push_back(&row);
        }
    }
    for (auto it = selected.rbegin(); it != selected.rend(); ++it) {
        result["rows"].AppendValue(**it);
    }
    result["coverage_gap"] = gap;
    result["tail_clipped"] = tailClipped;
    result["next_cursor"] = rows.empty() ? TString(after) : rows.back()["cursor"].GetString();
    return result;
}

NJson::TJsonValue TDiagnosticStore::ReadHistory(TStringBuf after, std::optional<size_t> tailLimit) const {
    std::lock_guard lock(Mutex);
    return Read(HistoryRows, NextHistory, after, {}, {}, {}, tailLimit);
}

NJson::TJsonValue TDiagnosticStore::ReadLogs(
    TStringBuf after,
    TStringBuf level,
    TStringBuf search,
    TStringBuf pile,
    std::optional<size_t> tailLimit) const
{
    std::lock_guard lock(Mutex);
    auto result = Read(LogRows, NextLog, after, level, search, pile, tailLimit);
    result["capture_level"] = Level;
    result["debug_available"] = Level == "debug" || Level == "trace";
    return result;
}

std::optional<NJson::TJsonValue> TDiagnosticStore::ReadOperation(const TString& id) const {
    std::lock_guard lock(Mutex);
    const auto it = Operations.find(id);
    return it == Operations.end() ? std::nullopt : std::optional(it->second);
}

const TString& TDiagnosticStore::CaptureLevel() const {
    return Level;
}

} // namespace NBridgeSkipper
