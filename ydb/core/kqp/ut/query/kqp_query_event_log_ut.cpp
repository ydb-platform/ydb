#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <library/cpp/json/json_reader.h>

#include <util/stream/str.h>

namespace NKikimr::NKqp {
namespace {

constexpr TStringBuf REQ_JSON_MARKER = "[REQ_JSON]";

struct TReqJsonEntry {
    TString RawLine;
    NJson::TJsonValue Json;
    TString Event;
    int Part = 0;
    int Total = 0;
    TString Priority;  // "DEBUG" | "WARN" | ...
};

// The test backend writes records into one continuous blob without `\n`
// separators, so we extract each [REQ_JSON] object by tracking brace balance
// with proper string-escape handling.
size_t FindJsonObjectEnd(TStringBuf data, size_t start) {
    int depth = 0;
    bool inStr = false;
    bool esc = false;
    for (size_t i = start; i < data.size(); ++i) {
        const char c = data[i];
        if (esc) {
            esc = false;
            continue;
        }
        if (inStr) {
            if (c == '\\') {
                esc = true;
            } else if (c == '"') {
                inStr = false;
            }
            continue;
        }
        if (c == '"') {
            inStr = true;
        } else if (c == '{') {
            ++depth;
        } else if (c == '}') {
            --depth;
            if (depth == 0) {
                return i + 1;
            }
        }
    }
    return TStringBuf::npos;
}

// Scan back from `markerPos` to the preceding ":KQP_REQUEST <PRIO>:" token.
TString ExtractPriority(TStringBuf blob, size_t markerPos) {
    constexpr size_t WINDOW = 256;
    const size_t winStart = markerPos > WINDOW ? markerPos - WINDOW : 0;
    const TStringBuf window = blob.SubStr(winStart, markerPos - winStart);
    constexpr TStringBuf NEEDLE = ":KQP_REQUEST ";
    const size_t kqpPos = window.rfind(NEEDLE);
    if (kqpPos == TStringBuf::npos) {
        return {};
    }
    const size_t prioStart = kqpPos + NEEDLE.size();
    const size_t prioEnd = window.find(':', prioStart);
    if (prioEnd == TStringBuf::npos) {
        return {};
    }
    return TString(window.SubStr(prioStart, prioEnd - prioStart));
}

TVector<TReqJsonEntry> CollectReqJson(TStringBuf blob) {
    TVector<TReqJsonEntry> entries;
    size_t pos = 0;
    while (true) {
        const size_t markerPos = blob.find(REQ_JSON_MARKER, pos);
        if (markerPos == TStringBuf::npos) {
            break;
        }
        const size_t jsonStart = blob.find('{', markerPos + REQ_JSON_MARKER.size());
        if (jsonStart == TStringBuf::npos) {
            break;
        }
        const size_t jsonEnd = FindJsonObjectEnd(blob, jsonStart);
        if (jsonEnd == TStringBuf::npos) {
            break;
        }

        TReqJsonEntry entry;
        const TStringBuf jsonView = blob.SubStr(jsonStart, jsonEnd - jsonStart);
        const TString jsonStr(jsonView);
        if (NJson::ReadJsonTree(jsonStr, &entry.Json, /*throwOnError=*/false)) {
            entry.RawLine = TString(blob.SubStr(markerPos, jsonEnd - markerPos));
            entry.Priority = ExtractPriority(blob, markerPos);
            const auto& req = entry.Json["request"];
            entry.Event = req["event"].GetStringSafe("");
            entry.Part = entry.Json["part"].GetIntegerSafe(0);
            entry.Total = entry.Json["total"].GetIntegerSafe(0);
            entries.push_back(std::move(entry));
        }
        pos = jsonEnd;
    }
    return entries;
}

void DumpEntries(const char* caseName, const TVector<TReqJsonEntry>& entries, const TString& fullLog) {
    Cerr << "=== " << caseName << " (" << entries.size() << " [REQ_JSON] lines, "
         << fullLog.size() << " total log bytes) ===" << Endl;
    if (entries.empty()) {
        Cerr << "--- log head (first 40 lines) ---" << Endl;
        TStringInput in(fullLog);
        TString line;
        for (size_t i = 0; i < 40 && in.ReadLine(line); ++i) {
            Cerr << line << Endl;
        }
        return;
    }
    for (size_t i = 0; i < entries.size(); ++i) {
        Cerr << "[" << i << "] prio=" << entries[i].Priority
             << " event=" << entries[i].Event
             << " part=" << entries[i].Part << "/" << entries[i].Total << Endl
             << "    " << entries[i].RawLine << Endl;
    }
}

const TReqJsonEntry* FindCompleted(const TVector<TReqJsonEntry>& entries) {
    for (const auto& e : entries) {
        if (e.Event == "completed" && e.Part == 1) {
            return &e;
        }
    }
    return nullptr;
}

TKikimrSettings MakeStreamSettings(TStringStream& logStream) {
    TKikimrSettings settings;
    settings.LogStream = &logStream;
    return settings;
}

void SetKqpRequestLevel(TKikimrRunner& kikimr, NLog::EPriority prio) {
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_REQUEST, prio);
}

TStringBuf LogSince(const TStringStream& logStream, size_t offset) {
    const TStringBuf blob = logStream.Str();
    return offset < blob.size() ? blob.SubStr(offset) : TStringBuf{};
}

Y_UNIT_TEST_SUITE(KqpQueryEventLog) {

// At KQP_REQUEST=DEBUG a successful query emits one completed envelope at
// DEBUG with the full per-query field set.
Y_UNIT_TEST(ExecuteSuccessAtDebugLogsCompleted) {
    TStringStream logStream;
    size_t logStart = 0;
    {
        TKikimrRunner kikimr(MakeStreamSettings(logStream));
        SetKqpRequestLevel(kikimr, NLog::EPriority::PRI_DEBUG);
        logStart = logStream.Size();

        auto db = kikimr.GetQueryClient();
        auto result = db.ExecuteQuery(
            "SELECT 1 AS x",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
    const auto fullLog = logStream.Str();
    const auto entries = CollectReqJson(LogSince(logStream, logStart));
    DumpEntries("ExecuteSuccessAtDebug", entries, fullLog);
    UNIT_ASSERT_C(!entries.empty(), "expected REQ_JSON entries on DEBUG");

    for (const auto& e : entries) {
        UNIT_ASSERT_VALUES_EQUAL_C(e.Event, "completed",
            TStringBuilder() << "only completed envelopes are allowed now: " << e.RawLine);
    }

    const auto* completed = FindCompleted(entries);
    UNIT_ASSERT_C(completed, "completed envelope (part=1) must be present");
    UNIT_ASSERT_VALUES_EQUAL_C(completed->Priority, "DEBUG",
        "successful completed must be emitted at DEBUG");
    UNIT_ASSERT_C(!completed->Json["req_id"].GetStringSafe("").empty(),
        "req_id must be ProxyRequestId (non-empty)");

    const auto& req = completed->Json["request"];
    UNIT_ASSERT_VALUES_EQUAL(req["event"].GetStringSafe(""), "completed");
    UNIT_ASSERT_VALUES_EQUAL(req["status"].GetStringSafe(""), "SUCCESS");
    UNIT_ASSERT_C(req["action"].GetStringSafe("").Contains("EXECUTE"), req["action"].GetStringSafe(""));
    UNIT_ASSERT_C(req.Has("type"), "type field");
    UNIT_ASSERT_C(req.Has("duration_us"), "duration_us field");
    UNIT_ASSERT_C(req.Has("query_len"), "query_len field");
    UNIT_ASSERT_C(req.Has("results_size"), "results_size field");
    UNIT_ASSERT_C(req.Has("database"), "database field");
    UNIT_ASSERT_C(req.Has("trace_id"), "trace_id field");
    UNIT_ASSERT_C(req.Has("started_at_us"), "started_at_us field");
}

// At KQP_REQUEST=WARN successful completed is silent but failures still
// emit the full envelope at WARN.
Y_UNIT_TEST(SuccessSilentAtWarnButFailureLogged) {
    TStringStream logStream;
    size_t logStart = 0;
    {
        TKikimrRunner kikimr(MakeStreamSettings(logStream));
        SetKqpRequestLevel(kikimr, NLog::EPriority::PRI_WARN);
        logStart = logStream.Size();

        auto db = kikimr.GetQueryClient();
        {
            auto ok = db.ExecuteQuery(
                "SELECT 1 AS x",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(ok.IsSuccess(), ok.GetIssues().ToString());
        }
        {
            auto bad = db.ExecuteQuery(
                "SELECT FROM broken_syntax",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(!bad.IsSuccess(), "syntax-broken query must fail");
        }
    }
    const auto fullLog = logStream.Str();
    const auto entries = CollectReqJson(LogSince(logStream, logStart));
    DumpEntries("SuccessSilentAtWarn", entries, fullLog);

    bool foundFailure = false;
    for (const auto& e : entries) {
        if (e.Event != "completed" || e.Part != 1) {
            continue;
        }
        const auto data = e.Json["request"]["data"].GetStringSafe("");
        if (!data.Contains("broken_syntax")) {
            continue;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(e.Priority, "WARN", e.RawLine);
        const auto& req = e.Json["request"];
        UNIT_ASSERT_C(req.Has("database"), e.RawLine);
        UNIT_ASSERT_C(req.Has("started_at_us"), e.RawLine);
        UNIT_ASSERT_C(req.Has("duration_us"), e.RawLine);
        foundFailure = true;
    }
    UNIT_ASSERT_C(foundFailure, "expected failure completed for broken_syntax at WARN");
}

// Extra fields appear only in part=1; at TRACE the SQL is not truncated.
Y_UNIT_TEST(ExtraFieldsOnlyInFirstPart) {
    TStringStream logStream;
    size_t logStart = 0;
    {
        TKikimrRunner kikimr(MakeStreamSettings(logStream));
        SetKqpRequestLevel(kikimr, NLog::EPriority::PRI_TRACE);
        logStart = logStream.Size();

        auto db = kikimr.GetQueryClient();

        // SQL longer than SQL_TEXT_MAX_SIZE (4000), padded inside a comment.
        TStringBuilder sb;
        sb << "/*";
        for (size_t i = 0; i < 5000; ++i) {
            sb << 'x';
        }
        sb << "*/ SELECT 1 AS x";

        auto result = db.ExecuteQuery(
            sb,
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
    const auto fullLog = logStream.Str();
    const auto entries = CollectReqJson(LogSince(logStream, logStart));
    DumpEntries("ExtraFieldsOnlyInFirstPart", entries, fullLog);

    bool sawMulti = false;
    for (const auto& e : entries) {
        if (e.Event != "completed") {
            continue;
        }
        if (e.Total > 1) {
            sawMulti = true;
            const auto& req = e.Json["request"];
            if (e.Part == 1) {
                UNIT_ASSERT_C(req.Has("status"), "part=1 must have extra fields");
                UNIT_ASSERT_C(!req.Has("data_truncated"),
                    TStringBuilder() << "TRACE must NOT truncate SQL: " << e.RawLine);
            } else {
                UNIT_ASSERT_C(!req.Has("status"),
                    TStringBuilder() << "non-first part must not have extra fields: " << e.RawLine);
                UNIT_ASSERT_C(!req.Has("duration_us"),
                    TStringBuilder() << "non-first part must not have duration_us: " << e.RawLine);
                UNIT_ASSERT_C(!req.Has("data_truncated"),
                    TStringBuilder() << "non-first part must not carry data_truncated: " << e.RawLine);
            }
        }
    }
    UNIT_ASSERT_C(sawMulti, "long SQL must produce a multi-part envelope");
}

// At DEBUG long SQL is truncated to QUERY_TEXT_LIMIT (10240) bytes; the
// original length is preserved in `query_len` and the cut is flagged by
// `data_truncated: true` in part=1.
Y_UNIT_TEST(LongQueryTruncatedAtDebug) {
    constexpr size_t QUERY_TEXT_LIMIT = 10240;

    TStringStream logStream;
    size_t logStart = 0;
    {
        TKikimrRunner kikimr(MakeStreamSettings(logStream));
        SetKqpRequestLevel(kikimr, NLog::EPriority::PRI_DEBUG);
        logStart = logStream.Size();

        auto db = kikimr.GetQueryClient();

        // ~30 KB of SQL, well past the 10 KB cap.
        TStringBuilder sb;
        sb << "/*";
        for (size_t i = 0; i < 30000; ++i) {
            sb << 'x';
        }
        sb << "*/ SELECT 1 AS x";

        auto result = db.ExecuteQuery(
            sb,
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
    const auto fullLog = logStream.Str();
    const auto entries = CollectReqJson(LogSince(logStream, logStart));
    DumpEntries("LongQueryTruncatedAtDebug", entries, fullLog);

    bool sawTruncated = false;
    for (const auto& e : entries) {
        if (e.Event != "completed" || e.Part != 1) {
            continue;
        }
        const auto& req = e.Json["request"];
        const auto queryLen = req["query_len"].GetUIntegerSafe(0);
        if (queryLen <= QUERY_TEXT_LIMIT) {
            continue;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(e.Priority, "DEBUG", e.RawLine);
        UNIT_ASSERT_C(req.Has("data_truncated"), e.RawLine);
        UNIT_ASSERT_VALUES_EQUAL_C(req["data_truncated"].GetBooleanSafe(false), true, e.RawLine);
        UNIT_ASSERT_C(e.Total <= 3,
            TStringBuilder() << "truncated SQL must produce <= 3 chunks, got " << e.Total);
        sawTruncated = true;
    }
    UNIT_ASSERT_C(sawTruncated, "expected a truncated completed envelope at DEBUG");
}


Y_UNIT_TEST(UiExcludedSuccessSilentButFailureLogged) {
    TStringStream logStream;
    size_t logStart = 0;
    {
        TKikimrRunner kikimr(MakeStreamSettings(logStream));
        SetKqpRequestLevel(kikimr, NLog::EPriority::PRI_DEBUG);
        logStart = logStream.Size();

        auto db = kikimr.GetQueryClient();
        {
            auto ok = db.ExecuteQuery(
                "/*UI-QUERY-EXCLUDE*/\nSELECT 1 AS x",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(ok.IsSuccess(), ok.GetIssues().ToString());
        }
        {
            auto bad = db.ExecuteQuery(
                "/*UI-QUERY-EXCLUDE*/\nSELECT FROM broken_syntax",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(!bad.IsSuccess(), "excluded syntax-broken query must fail");
        }
        {
            auto control = db.ExecuteQuery(
                "SELECT 2 AS y",
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(control.IsSuccess(), control.GetIssues().ToString());
        }
    }
    const auto fullLog = logStream.Str();
    const auto entries = CollectReqJson(LogSince(logStream, logStart));
    DumpEntries("UiExcludedSuccessSilentButFailureLogged", entries, fullLog);

    bool excludedSuccessSeen = false;
    bool excludedFailureSeen = false;
    bool controlSuccessSeen = false;
    for (const auto& e : entries) {
        if (e.Event != "completed" || e.Part != 1) {
            continue;
        }
        const auto data = e.Json["request"]["data"].GetStringSafe("");
        const auto status = e.Json["request"]["status"].GetStringSafe("");
        const bool isExcluded = data.Contains("/*UI-QUERY-EXCLUDE*/");
        const bool isControl = !isExcluded && data.Contains("SELECT 2 AS y");
        if (isExcluded) {
            if (status == "SUCCESS") {
                excludedSuccessSeen = true;
            } else {
                excludedFailureSeen = true;
                UNIT_ASSERT_VALUES_EQUAL_C(e.Priority, "WARN", e.RawLine);
            }
        } else if (isControl && status == "SUCCESS") {
            controlSuccessSeen = true;
            UNIT_ASSERT_VALUES_EQUAL_C(e.Priority, "DEBUG", e.RawLine);
        }
    }
    UNIT_ASSERT_C(controlSuccessSeen,
        "non-excluded success must log at DEBUG — otherwise the excluded-success "
        "assertion below would pass trivially via the priority gate");
    UNIT_ASSERT_C(!excludedSuccessSeen, "UI-excluded successful query must not log completed");
    UNIT_ASSERT_C(excludedFailureSeen, "UI-excluded failure must log completed at WARN");
}

} // Y_UNIT_TEST_SUITE

} // namespace
} // namespace NKikimr::NKqp
