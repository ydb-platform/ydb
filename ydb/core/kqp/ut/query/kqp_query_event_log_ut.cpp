#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/library/aclib/aclib.h>

#include <library/cpp/json/json_reader.h>

#include <util/stream/str.h>

namespace NKikimr::NKqp {
namespace {

constexpr TStringBuf REQ_JSON_MARKER = "[REQ_JSON]";

struct TReqJsonEntry {
    TString RawLine;
    NJson::TJsonValue Json;
    TString Event;
    TString Kind;
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
            entry.Kind = entry.Json["kind"].GetStringSafe("");
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
             << " kind=" << entries[i].Kind
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

// Drives KqpProxy with an explicit UserToken — SDK clients can't set the
// SID, but metadata-service local RPCs do exactly this internally.
void SendKqpQueryAsUser(TTestActorRuntime& runtime,
                       const TActorId& edge,
                       const TString& userSid,
                       const TString& query) {
    auto ev = std::make_unique<NKqp::TEvKqp::TEvQueryRequest>();
    ev->Record.SetUserToken(NACLib::TUserToken(userSid, {}).SerializeAsString());
    ActorIdToProto(edge, ev->Record.MutableRequestActorId());
    auto& req = *ev->Record.MutableRequest();
    req.SetDatabase("/Root");
    req.SetQuery(query);
    req.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
    req.SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
    auto* txControl = req.MutableTxControl();
    txControl->mutable_begin_tx()->mutable_serializable_read_write();
    txControl->set_commit_tx(true);

    runtime.Send(new IEventHandle(MakeKqpProxyID(runtime.GetNodeId(0)), edge, ev.release()));
    auto reply = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(edge);
    UNIT_ASSERT_C(reply, "no TEvQueryResponse for metadata-user query");
}

Y_UNIT_TEST_SUITE(KqpQueryEventLog) {

// At KQP_REQUEST=DEBUG a successful query emits one completed envelope at
// DEBUG with the full per-query field set.
Y_UNIT_TEST(ExecuteSuccessAtDebugLogsCompleted) {
    TStringStream logStream;
    {
        TKikimrRunner kikimr(MakeStreamSettings(logStream));
        SetKqpRequestLevel(kikimr, NLog::EPriority::PRI_DEBUG);

        auto db = kikimr.GetQueryClient();
        auto result = db.ExecuteQuery(
            "SELECT 1 AS x",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
    const auto fullLog = logStream.Str();
    const auto entries = CollectReqJson(fullLog);
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
    UNIT_ASSERT_VALUES_EQUAL_C(completed->Kind, "completed",
        "single-envelope completed must carry kind=completed marker");
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
    {
        TKikimrRunner kikimr(MakeStreamSettings(logStream));
        SetKqpRequestLevel(kikimr, NLog::EPriority::PRI_WARN);

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
    const auto entries = CollectReqJson(fullLog);
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

// WARN failure with empty query text must still carry issues (dataChunks==0 path).
Y_UNIT_TEST(FailureWithEmptyQueryTextLogsIssuesAtWarn) {
    TStringStream logStream;
    {
        TKikimrRunner kikimr(MakeStreamSettings(logStream));
        SetKqpRequestLevel(kikimr, NLog::EPriority::PRI_WARN);

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        const auto edge = runtime.AllocateEdgeActor();
        SendKqpQueryAsUser(runtime, edge, "user@domain", "");
    }
    const auto fullLog = logStream.Str();
    const auto entries = CollectReqJson(fullLog);
    DumpEntries("FailureWithEmptyQueryTextLogsIssuesAtWarn", entries, fullLog);

    bool found = false;
    for (const auto& e : entries) {
        if (e.Event != "completed" || e.Part != 1) {
            continue;
        }
        // Whole-log scan now sees startup/background requests too — scope to ours.
        if (e.Json["user"].GetStringSafe("") != "user@domain") {
            continue;
        }
        const auto& req = e.Json["request"];
        if (req["status"].GetStringSafe("") == "SUCCESS") {
            continue;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(e.Total, 1, e.RawLine);
        UNIT_ASSERT_C(!req.Has("data") || req["data"].GetStringSafe("").empty(),
            TStringBuilder() << "empty query must not carry SQL data: " << e.RawLine);
        UNIT_ASSERT_C(req.Has("issues"),
            TStringBuilder() << "failure without query text must still log issues: " << e.RawLine);
        UNIT_ASSERT_C(!req["issues"].GetStringSafe("").empty(), e.RawLine);
        found = true;
    }
    UNIT_ASSERT_C(found, "expected a single-envelope failure with issues for empty query at WARN");
}

// At TRACE successful queries are emitted as a multi-part envelope tagged
// TRACE (not DEBUG), the SQL is not truncated, and the per-query extra
// fields appear only in part=1.
Y_UNIT_TEST(ExtraFieldsOnlyInFirstPart) {
    TStringStream logStream;
    {
        TKikimrRunner kikimr(MakeStreamSettings(logStream));
        SetKqpRequestLevel(kikimr, NLog::EPriority::PRI_TRACE);

        auto db = kikimr.GetQueryClient();

        TStringBuilder sb;
        sb << "/*";
        for (size_t i = 0; i < 15000; ++i) {
            sb << 'x';
        }
        sb << "*/ SELECT 1 AS x";

        auto result = db.ExecuteQuery(
            sb,
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
    const auto fullLog = logStream.Str();
    const auto entries = CollectReqJson(fullLog);
    DumpEntries("ExtraFieldsOnlyInFirstPart", entries, fullLog);

    bool sawPart1OfMulti = false;
    bool sawNonFirst = false;
    TString multiReqId;
    int multiTotal = 0;
    for (const auto& e : entries) {
        if (e.Total <= 1) {
            continue;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(e.Priority, "TRACE",
            TStringBuilder() << "multi-part envelopes must be emitted at TRACE: " << e.RawLine);
        const auto& req = e.Json["request"];
        if (e.Part == 1) {
            sawPart1OfMulti = true;
            multiReqId = e.Json["req_id"].GetStringSafe("");
            multiTotal = e.Total;
            UNIT_ASSERT_VALUES_EQUAL_C(e.Kind, "completed",
                TStringBuilder() << "part=1 must carry kind=completed marker: " << e.RawLine);
            UNIT_ASSERT_VALUES_EQUAL_C(req["event"].GetStringSafe(""), "completed",
                TStringBuilder() << "part=1 must mark itself as completed envelope: " << e.RawLine);
            UNIT_ASSERT_C(req.Has("status"),
                TStringBuilder() << "part=1 must carry completed metadata: " << e.RawLine);
            UNIT_ASSERT_C(req.Has("data"),
                TStringBuilder() << "part=1 must carry first data chunk: " << e.RawLine);
            UNIT_ASSERT_C(!req.Has("data_truncated"),
                TStringBuilder() << "TRACE must NOT truncate SQL: " << e.RawLine);
        } else {
            sawNonFirst = true;
            UNIT_ASSERT_VALUES_EQUAL_C(e.Kind, "continuation",
                TStringBuilder() << "part>1 must carry kind=continuation marker: " << e.RawLine);
            UNIT_ASSERT_C(!req.Has("event"),
                TStringBuilder() << "non-first part must not duplicate event: " << e.RawLine);
            UNIT_ASSERT_C(!req.Has("status"),
                TStringBuilder() << "non-first part must not duplicate completed fields: " << e.RawLine);
            UNIT_ASSERT_C(!req.Has("duration_us"),
                TStringBuilder() << "non-first part must not duplicate duration_us: " << e.RawLine);
            UNIT_ASSERT_C(!req.Has("issues"),
                TStringBuilder() << "non-first part must not duplicate issues: " << e.RawLine);
            UNIT_ASSERT_C(!req.Has("data_truncated"),
                TStringBuilder() << "non-first part must not carry data_truncated: " << e.RawLine);
            UNIT_ASSERT_C(req.Has("data"),
                TStringBuilder() << "non-first part must carry data continuation: " << e.RawLine);
        }
    }
    UNIT_ASSERT_C(sawPart1OfMulti, "long SQL must produce a multi-part envelope (part=1)");
    UNIT_ASSERT_C(sawNonFirst, "long SQL must produce continuation chunks (part>1)");

    int chunksSeen = 0;
    for (const auto& e : entries) {
        if (e.Json["req_id"].GetStringSafe("") == multiReqId) {
            ++chunksSeen;
        }
    }
    UNIT_ASSERT_VALUES_EQUAL_C(chunksSeen, multiTotal,
        TStringBuilder() << "must see exactly total=" << multiTotal
                         << " chunks for the multi-part req, got " << chunksSeen);
}

// Multi-part FAILURE keeps issues in part==1 only — no per-chunk duplication.
Y_UNIT_TEST(IssuesOnlyInFirstPartOnFailure) {
    TStringStream logStream;
    {
        TKikimrRunner kikimr(MakeStreamSettings(logStream));
        SetKqpRequestLevel(kikimr, NLog::EPriority::PRI_TRACE);

        auto db = kikimr.GetQueryClient();

        TStringBuilder sb;
        sb << "/*";
        for (size_t i = 0; i < 15000; ++i) {
            sb << 'x';
        }
        sb << "*/ SELECT FROM broken_chunk_marker_xyz";

        auto bad = db.ExecuteQuery(
            sb,
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(!bad.IsSuccess(), "syntax-broken query must fail");
    }
    const auto fullLog = logStream.Str();
    const auto entries = CollectReqJson(fullLog);
    DumpEntries("IssuesOnlyInFirstPartOnFailure", entries, fullLog);

    TString targetReqId;
    for (const auto& e : entries) {
        if (e.Total <= 1 || e.Part != 1) {
            continue;
        }
        const auto& req = e.Json["request"];
        const auto status = req["status"].GetStringSafe("");
        if (status.empty() || status == "SUCCESS") {
            continue;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(e.Priority, "WARN",
            TStringBuilder() << "failing envelope must log at WARN: " << e.RawLine);
        UNIT_ASSERT_C(req.Has("issues"),
            TStringBuilder() << "multi-part FAILURE part==1 must carry issues: " << e.RawLine);
        const auto issuesStr = req["issues"].GetStringSafe("");
        UNIT_ASSERT_C(!issuesStr.empty(),
            TStringBuilder() << "issues string must not be empty for parse error: " << e.RawLine);
        targetReqId = e.Json["req_id"].GetStringSafe("");
        break;
    }
    UNIT_ASSERT_C(!targetReqId.empty(),
        "expected a multi-part FAILURE envelope at TRACE for long broken SQL");

    bool sawNonFirst = false;
    for (const auto& e : entries) {
        if (e.Json["req_id"].GetStringSafe("") != targetReqId || e.Part == 1) {
            continue;
        }
        sawNonFirst = true;
        const auto& req = e.Json["request"];
        UNIT_ASSERT_C(!req.Has("issues"),
            TStringBuilder() << "non-first part of FAILURE must NOT duplicate issues: " << e.RawLine);
        UNIT_ASSERT_C(!req.Has("event"),
            TStringBuilder() << "non-first part of FAILURE must NOT duplicate event: " << e.RawLine);
        UNIT_ASSERT_C(!req.Has("status"),
            TStringBuilder() << "non-first part of FAILURE must NOT duplicate status: " << e.RawLine);
        UNIT_ASSERT_C(req.Has("data"),
            TStringBuilder() << "non-first part of FAILURE must carry data: " << e.RawLine);
    }
    UNIT_ASSERT_C(sawNonFirst,
        "long broken SQL must produce continuation chunks (part>1)");
}

// Long issues span multiple chunks instead of being silently truncated to 1 KB.
Y_UNIT_TEST(LongIssuesChunkedAcrossPartsOnFailure) {
    TStringStream logStream;
    {
        TKikimrRunner kikimr(MakeStreamSettings(logStream));
        SetKqpRequestLevel(kikimr, NLog::EPriority::PRI_TRACE);

        TStringBuilder bad;
        for (size_t i = 0; i < 10000; ++i) {
            bad << 'z';
        }
        const TString sql = TStringBuilder() << "SELECT * FROM `" << bad << "` LIMIT 1;";
        auto fail = kikimr.GetQueryClient().ExecuteQuery(
            sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(!fail.IsSuccess(), "missing-table query must fail");
    }
    const auto fullLog = logStream.Str();
    const auto entries = CollectReqJson(fullLog);
    DumpEntries("LongIssuesChunkedAcrossPartsOnFailure", entries, fullLog);

    TString targetReqId;
    for (const auto& e : entries) {
        const auto status = e.Json["request"]["status"].GetStringSafe("");
        // Only our long broken SQL fans out into a multi-part failure — that
        // Total>1 keeps startup/background single-part failures out of scope.
        if (e.Total > 1 && e.Part == 1 && !status.empty() && status != "SUCCESS") {
            targetReqId = e.Json["req_id"].GetStringSafe("");
            break;
        }
    }
    UNIT_ASSERT_C(!targetReqId.empty(), "expected a failing envelope with issues");

    int partsWithIssues = 0;
    size_t issuesBytesSeen = 0;
    for (const auto& e : entries) {
        if (e.Json["req_id"].GetStringSafe("") != targetReqId) {
            continue;
        }
        const auto& req = e.Json["request"];
        if (req.Has("issues")) {
            ++partsWithIssues;
            issuesBytesSeen += req["issues"].GetStringSafe("").size();
        }
    }
    UNIT_ASSERT_C(partsWithIssues > 1,
        TStringBuilder() << "long issues must span multiple chunks, got " << partsWithIssues);
    UNIT_ASSERT_C(issuesBytesSeen > 4096,
        TStringBuilder() << "concatenated issues must exceed 4 KB, got " << issuesBytesSeen);
}

// At WARN long issues are truncated to 1 KB inside a single envelope — no multi-part.
Y_UNIT_TEST(LongIssuesTruncatedAtWarn) {
    TStringStream logStream;
    {
        TKikimrRunner kikimr(MakeStreamSettings(logStream));
        SetKqpRequestLevel(kikimr, NLog::EPriority::PRI_WARN);

        TStringBuilder bad;
        for (size_t i = 0; i < 10000; ++i) {
            bad << 'z';
        }
        const TString sql = TStringBuilder() << "SELECT * FROM `" << bad << "` LIMIT 1;";
        auto fail = kikimr.GetQueryClient().ExecuteQuery(
            sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(!fail.IsSuccess(), "missing-table query must fail");
    }
    const auto fullLog = logStream.Str();
    const auto entries = CollectReqJson(fullLog);
    DumpEntries("LongIssuesTruncatedAtWarn", entries, fullLog);

    bool found = false;
    for (const auto& e : entries) {
        if (e.Event != "completed" || e.Part != 1) {
            continue;
        }
        const auto& req = e.Json["request"];
        if (req["status"].GetStringSafe("") == "SUCCESS") {
            continue;
        }
        // Whole-log scan sees startup/background WARN failures too — ours is the
        // only one whose SQL carries the giant missing-table marker.
        if (!req["data"].GetStringSafe("").Contains("zzzz")) {
            continue;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(e.Total, 1, e.RawLine);
        UNIT_ASSERT_C(req.Has("issues_truncated"), e.RawLine);
        UNIT_ASSERT_VALUES_EQUAL_C(req["issues_truncated"].GetBooleanSafe(false), true, e.RawLine);
        UNIT_ASSERT_C(req["issues"].GetStringSafe("").size() <= 1024,
            TStringBuilder() << "WARN issues must be capped at 1 KB, got "
                             << req["issues"].GetStringSafe("").size());
        found = true;
    }
    UNIT_ASSERT_C(found, "expected a single-envelope failure with truncated issues at WARN");
}

// At DEBUG long SQL is truncated to QUERY_TEXT_LIMIT (6 KB) bytes and
// emitted as a single envelope (`total=1`) — operators of the always-on
// stream consume "one record per query". The original length is preserved
// in `query_len` and the cut is flagged by `data_truncated: true`.
Y_UNIT_TEST(LongQueryTruncatedAtDebug) {
    constexpr size_t QUERY_TEXT_LIMIT = 6 * 1024;

    TStringStream logStream;
    {
        TKikimrRunner kikimr(MakeStreamSettings(logStream));
        SetKqpRequestLevel(kikimr, NLog::EPriority::PRI_DEBUG);

        auto db = kikimr.GetQueryClient();

        // ~30 KB of SQL, well past the 6 KB cap.
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
    const auto entries = CollectReqJson(fullLog);
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
        UNIT_ASSERT_VALUES_EQUAL_C(e.Total, 1,
            TStringBuilder() << "truncated SQL at DEBUG must be a single envelope, got total=" << e.Total);
        const auto data = req["data"].GetStringSafe("");
        UNIT_ASSERT_C(data.size() <= QUERY_TEXT_LIMIT,
            TStringBuilder() << "DEBUG envelope data must be capped at QUERY_TEXT_LIMIT, got " << data.size());
        sawTruncated = true;
    }
    UNIT_ASSERT_C(sawTruncated, "expected a truncated completed envelope at DEBUG");
}

Y_UNIT_TEST(MetadataSystemUserSuccessSilentButFailureLogged) {
    TStringStream logStream;
    {
        TKikimrRunner kikimr(MakeStreamSettings(logStream));
        SetKqpRequestLevel(kikimr, NLog::EPriority::PRI_DEBUG);

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        const auto edge = runtime.AllocateEdgeActor();

        SendKqpQueryAsUser(runtime, edge, BUILTIN_ACL_METADATA,
            "/*meta-ok*/ SELECT 1 AS x_meta_ok");
        SendKqpQueryAsUser(runtime, edge, BUILTIN_ACL_METADATA,
            "/*meta-fail*/ SELECT FROM broken_syntax_meta");

        // Non-metadata SUCCESS must log at DEBUG — otherwise the
        // metadata-silence assertion below passes via the priority gate.
        auto db = kikimr.GetQueryClient();
        auto control = db.ExecuteQuery(
            "SELECT 2 AS y_meta_ctrl",
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(control.IsSuccess(), control.GetIssues().ToString());
    }
    const auto fullLog = logStream.Str();
    const auto entries = CollectReqJson(fullLog);
    DumpEntries("MetadataSystemUserSuccessSilentButFailureLogged", entries, fullLog);

    bool metaSuccessSeen = false;
    bool metaFailureSeen = false;
    bool controlSuccessSeen = false;
    for (const auto& e : entries) {
        if (e.Event != "completed" || e.Part != 1) {
            continue;
        }
        const auto& req = e.Json["request"];
        const auto user = e.Json["user"].GetStringSafe("");
        const auto data = req["data"].GetStringSafe("");
        const auto status = req["status"].GetStringSafe("");
        const bool isMetaUser = user == BUILTIN_ACL_METADATA;
        if (isMetaUser && data.Contains("/*meta-ok*/")) {
            metaSuccessSeen = true;
        } else if (isMetaUser && data.Contains("/*meta-fail*/")) {
            metaFailureSeen = true;
            UNIT_ASSERT_VALUES_EQUAL_C(e.Priority, "WARN", e.RawLine);
            UNIT_ASSERT_C(status != "SUCCESS",
                TStringBuilder() << "meta-fail must not be SUCCESS: " << e.RawLine);
        } else if (!isMetaUser && data.Contains("y_meta_ctrl") && status == "SUCCESS") {
            controlSuccessSeen = true;
            UNIT_ASSERT_VALUES_EQUAL_C(e.Priority, "DEBUG", e.RawLine);
        }
    }
    UNIT_ASSERT_C(controlSuccessSeen,
        "non-metadata success must log at DEBUG (priority gate sanity)");
    UNIT_ASSERT_C(!metaSuccessSeen,
        "metadata@system successful query must not log a completed envelope");
    UNIT_ASSERT_C(metaFailureSeen,
        "metadata@system failed query must log a completed envelope at WARN");
}

// Streaming query over real tables must report results_size > 0 — bytes
// pushed to the client are accounted, not just the empty ExecuteQuery payload.
Y_UNIT_TEST(StreamingBigResultReportsResultsSize) {
    TStringStream logStream;
    {
        TKikimrRunner kikimr(MakeStreamSettings(logStream));
        SetKqpRequestLevel(kikimr, NLog::EPriority::PRI_DEBUG);

        auto db = kikimr.GetQueryClient();
        auto it = db.StreamExecuteQuery(R"(
            SELECT a.Key AS k_marker_big, a.Text AS t1, b.Text AS t2, c.Text AS t3
            FROM `EightShard` AS a
            CROSS JOIN `EightShard` AS b
            CROSS JOIN `EightShard` AS c
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        auto streamPart = it.ReadNext().GetValueSync();
        UNIT_ASSERT_C(streamPart.IsSuccess(), streamPart.GetIssues().ToString());
    }
    const auto fullLog = logStream.Str();
    const auto entries = CollectReqJson(fullLog);
    DumpEntries("StreamingBigResultReportsResultsSize", entries, fullLog);

    bool found = false;
    for (const auto& e : entries) {
        if (e.Event != "completed" || e.Part != 1) {
            continue;
        }
        const auto& req = e.Json["request"];
        const auto data = req["data"].GetStringSafe("");
        if (!data.Contains("k_marker_big")) {
            continue;
        }
        const auto status = req["status"].GetStringSafe("");
        const auto durationUs = req["duration_us"].GetUIntegerSafe(0);
        const auto resultsSize = req["results_size"].GetUIntegerSafe(0);
        Cerr << "EightShard^3 cross-join: status=" << status
             << " duration_us=" << durationUs
             << " results_size=" << resultsSize << Endl;
        UNIT_ASSERT_C(resultsSize > 0,
            TStringBuilder() << "results_size must be > 0, got " << resultsSize);
        found = true;
    }
    UNIT_ASSERT_C(found, "completed envelope for big cross-join must be present");
}

} // Y_UNIT_TEST_SUITE

} // namespace
} // namespace NKikimr::NKqp
