#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/test_tli.h>
#include <ydb/core/protos/data_integrity_trails.pb.h>

#include <algorithm>
#include <optional>
#include <regex>
#include <util/string/escape.h>
#include <util/string/cast.h>
#include <util/string/split.h>


namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {
    // ==================== Low-level TLI parsing helpers ====================

    std::vector<TString> ExtractTliRecords(const TString& logs) {
        const TString delimiter = "TLI ";
        std::vector<TString> result;
        size_t pos = 0;
        while (true) {
            const size_t found = logs.find(delimiter, pos);
            if (found == TString::npos) {
                break;
            }
            size_t recordEnd = logs.size();
            size_t timestampPos = found;
            while (timestampPos < logs.size()) {
                timestampPos = logs.find("202", timestampPos + 1);
                if (timestampPos == TString::npos) {
                    break;
                }
                if (timestampPos + 30 < logs.size() &&
                    logs[timestampPos + 4] == '-' &&
                    logs[timestampPos + 7] == '-' &&
                    logs[timestampPos + 10] == 'T' &&
                    logs[timestampPos + 13] == ':' &&
                    logs[timestampPos + 16] == ':' &&
                    logs[timestampPos + 19] == '.' &&
                    logs[timestampPos + 26] == 'Z' &&
                    logs.find(" node ", timestampPos) == timestampPos + 27) {
                    recordEnd = std::min(recordEnd, timestampPos);
                    break;
                }
            }
            size_t nextDelimiter = logs.find(delimiter, found + delimiter.size());
            if (nextDelimiter != TString::npos) {
                recordEnd = std::min(recordEnd, nextDelimiter);
            }
            size_t nextNewline = logs.find('\n', found);
            if (nextNewline != TString::npos) {
                recordEnd = std::min(recordEnd, nextNewline);
            }
            result.push_back(logs.substr(found, recordEnd - found));
            pos = recordEnd;
        }
        return result;
    }

    bool MatchesMessage(const TString& record, const TString& messagePattern) {
        if (messagePattern.empty()) {
            return true;
        }
        const size_t messagePos = record.find("Message: ");
        if (messagePos == TString::npos) {
            return false;
        }
        const size_t messageStart = messagePos + 9;
        const size_t messageEnd = record.find(',', messageStart);
        const TString message = record.substr(messageStart, messageEnd == TString::npos ? record.size() : messageEnd - messageStart);
        std::regex messageRegex(messagePattern.c_str());
        std::smatch match;
        return std::regex_search(message.cbegin(), message.cend(), match, messageRegex);
    }

    std::optional<TString> ExtractQueryText(const TString& logs, const TString& messagePattern) {
        for (const auto& record : ExtractTliRecords(logs)) {
            if (!record.Contains("Component: SessionActor") || !MatchesMessage(record, messagePattern)) {
                continue;
            }
            const size_t allPos = record.find("QueryText: ");
            if (allPos == TString::npos) {
                continue;
            }
            TString result = record.substr(allPos + 11);
            const size_t allQueryTextsPos = result.find(", AllQueryTexts:");
            if (allQueryTextsPos != TString::npos) {
                result = result.substr(0, allQueryTextsPos);
            }
            return UnescapeC(result);
        }
        return std::nullopt;
    }

    // Extract VictimQueryText field (the original query whose locks were broken)
    std::optional<TString> ExtractVictimQueryText(const TString& logs, const TString& messagePattern) {
        for (const auto& record : ExtractTliRecords(logs)) {
            if (!record.Contains("Component: SessionActor") || !MatchesMessage(record, messagePattern)) {
                continue;
            }
            const size_t victimPos = record.find("VictimQueryText: ");
            if (victimPos == TString::npos) {
                continue;
            }
            TString result = record.substr(victimPos + 17);
            // Stop at comma followed by QueryText:
            const size_t nextFieldPos = result.find(", QueryText:");
            if (nextFieldPos != TString::npos) {
                result = result.substr(0, nextFieldPos);
            }
            return UnescapeC(result);
        }
        return std::nullopt;
    }

    std::optional<TString> ExtractAllQueryTexts(const TString& logs, const TString& messagePattern) {
        for (const auto& record : ExtractTliRecords(logs)) {
            if (!record.Contains("Component: SessionActor") || !MatchesMessage(record, messagePattern)) {
                continue;
            }
            const size_t allPos = record.find("AllQueryTexts: ");
            if (allPos == TString::npos) {
                continue;
            }
            TString result = record.substr(allPos + 15);
            if (result.EndsWith(",")) {
                result.pop_back();
            }
            return UnescapeC(result);
        }
        return std::nullopt;
    }

    std::optional<ui64> ExtractNumericField(const TString& record, const TString& fieldName) {
        const TString prefix = fieldName + ": ";
        const size_t pos = record.find(prefix);
        if (pos == TString::npos) {
            return std::nullopt;
        }
        const size_t start = pos + prefix.size();
        size_t end = record.find_first_not_of("0123456789", start);
        if (end == TString::npos) {
            end = record.size();
        }
        const TString value = record.substr(start, end - start);
        return value.empty() ? std::nullopt : std::make_optional(FromString<ui64>(value));
    }

    std::optional<ui64> ExtractQueryTraceId(const TString& logs, const TString& component, const TString& messagePattern) {
        for (const auto& record : ExtractTliRecords(logs)) {
            if (!record.Contains("Component: " + component) || !MatchesMessage(record, messagePattern)) {
                continue;
            }
            return ExtractNumericField(record, "QueryTraceId");
        }
        return std::nullopt;
    }

    std::optional<ui64> ExtractCurrentQueryTraceId(const TString& logs, const TString& component, const TString& messagePattern) {
        for (const auto& record : ExtractTliRecords(logs)) {
            if (!record.Contains("Component: " + component) || !MatchesMessage(record, messagePattern)) {
                continue;
            }
            return ExtractNumericField(record, "CurrentQueryTraceId");
        }
        return std::nullopt;
    }


    std::optional<ui64> ExtractBreakerQueryTraceId(const TString& logs, const TString& component, const TString& messagePattern) {
        for (const auto& record : ExtractTliRecords(logs)) {
            if (!record.Contains("Component: " + component) || !MatchesMessage(record, messagePattern)) {
                continue;
            }
            return ExtractNumericField(record, "BreakerQueryTraceId");
        }
        return std::nullopt;
    }

    std::optional<ui64> ExtractVictimQueryTraceId(const TString& logs, const TString& component, const TString& messagePattern) {
        for (const auto& record : ExtractTliRecords(logs)) {
            if (!record.Contains("Component: " + component) || !MatchesMessage(record, messagePattern)) {
                continue;
            }
            return ExtractNumericField(record, "VictimQueryTraceId");
        }
        return std::nullopt;
    }

    std::optional<std::vector<ui64>> ExtractVictimQueryTraceIds(const TString& logs, const TString& component, const TString& messagePattern) {
        std::vector<ui64> result;
        bool foundField = false;
        for (const auto& record : ExtractTliRecords(logs)) {
            if (!record.Contains("Component: " + component) || !MatchesMessage(record, messagePattern)) {
                continue;
            }
            const size_t idsPos = record.find("VictimQueryTraceIds: [");
            if (idsPos == TString::npos) {
                continue;
            }
            foundField = true;
            const size_t listStart = idsPos + 22;
            const size_t listEnd = record.find(']', listStart);
            if (listEnd == TString::npos) {
                continue;
            }
            for (const auto& part : StringSplitter(record.substr(listStart, listEnd - listStart)).Split(' ').SkipEmpty()) {
                result.emplace_back(FromString<ui64>(part));
            }
        }
        return foundField ? std::make_optional(result) : std::nullopt;
    }

    void DumpTliRecords(const TString& logs) {
        for (const auto& record : ExtractTliRecords(logs)) {
            Cerr << record << Endl;
        }
    }

    // ==================== TLI log patterns ====================

    struct TTliLogPatterns {
        TString BreakerSessionActorMessagePattern;
        TString VictimSessionActorMessagePattern;
        TString BreakerDatashardMessage;
        TString VictimDatashardMessage;
    };

    TTliLogPatterns MakeTliLogPatterns(bool useSink) {
        return {
            "(Query|Commit) had broken other locks",
            "(Query|Commit) was a victim of broken locks",
            useSink ? "Write transaction broke other locks" : "KQP data transaction broke other locks",
            useSink ? "Write transaction was a victim of broken locks" : "KQP data transaction was a victim of broken locks",
        };
    }

    // ==================== Extracted TLI data struct ====================

    struct TExtractedTliData {
        std::optional<TString> BreakerAllQueryTexts;
        std::optional<TString> VictimAllQueryTexts;
        std::optional<TString> BreakerQueryText;
        std::optional<TString> VictimQueryText;
        std::optional<ui64> BreakerSessionQueryTraceId;
        std::optional<ui64> BreakerShardQueryTraceId;
        std::optional<ui64> BreakerSessionBreakerQueryTraceId;
        std::optional<ui64> BreakerShardBreakerQueryTraceId;
        std::optional<std::vector<ui64>> BreakerShardVictimQueryTraceIds;

        std::optional<ui64> VictimSessionQueryTraceId;
        std::optional<ui64> VictimShardQueryTraceId;
        std::optional<ui64> VictimSessionCurrentQueryTraceId;
        std::optional<ui64> VictimShardCurrentQueryTraceId;
        std::optional<ui64> VictimSessionVictimQueryTraceId;
        std::optional<ui64> VictimShardVictimQueryTraceId;
    };

    TExtractedTliData ExtractAllTliData(const TString& logs, const TTliLogPatterns& patterns) {
        TExtractedTliData data;
        data.BreakerAllQueryTexts = ExtractAllQueryTexts(logs, patterns.BreakerSessionActorMessagePattern);
        data.VictimAllQueryTexts = ExtractAllQueryTexts(logs, patterns.VictimSessionActorMessagePattern);
        data.BreakerQueryText = ExtractQueryText(logs, patterns.BreakerSessionActorMessagePattern);
        data.VictimQueryText = ExtractVictimQueryText(logs, patterns.VictimSessionActorMessagePattern);
        data.BreakerSessionQueryTraceId = ExtractQueryTraceId(logs, "SessionActor", patterns.BreakerSessionActorMessagePattern);
        data.BreakerShardQueryTraceId = ExtractQueryTraceId(logs, "DataShard", patterns.BreakerDatashardMessage);
        data.BreakerSessionBreakerQueryTraceId = ExtractBreakerQueryTraceId(logs, "SessionActor", patterns.BreakerSessionActorMessagePattern);
        data.BreakerShardBreakerQueryTraceId = ExtractBreakerQueryTraceId(logs, "DataShard", patterns.BreakerDatashardMessage);
        data.BreakerShardVictimQueryTraceIds = ExtractVictimQueryTraceIds(logs, "DataShard", patterns.BreakerDatashardMessage);
        data.VictimSessionQueryTraceId = ExtractQueryTraceId(logs, "SessionActor", patterns.VictimSessionActorMessagePattern);
        data.VictimShardQueryTraceId = ExtractQueryTraceId(logs, "DataShard", patterns.VictimDatashardMessage);
        data.VictimSessionCurrentQueryTraceId = ExtractCurrentQueryTraceId(logs, "SessionActor", patterns.VictimSessionActorMessagePattern);
        data.VictimShardCurrentQueryTraceId = ExtractCurrentQueryTraceId(logs, "DataShard", patterns.VictimDatashardMessage);
        data.VictimSessionVictimQueryTraceId = ExtractVictimQueryTraceId(logs, "SessionActor", patterns.VictimSessionActorMessagePattern);
        data.VictimShardVictimQueryTraceId = ExtractVictimQueryTraceId(logs, "DataShard", patterns.VictimDatashardMessage);

        return data;
    }

    void AssertCommonTliAsserts(
        const TExtractedTliData& data,
        const TString& breakerQueryText,
        const TString& victimQueryText,
        const std::optional<TString>& victimExtraQueryText = std::nullopt)
    {
        // ==================== QueryTraceId Linkage Assertions ====================

        // 1. DS Breaker ↔ KQP Breaker: Match BreakerQueryTraceId
        UNIT_ASSERT_C(data.BreakerSessionBreakerQueryTraceId, "breaker SessionActor BreakerQueryTraceId should be present");
        UNIT_ASSERT_C(data.BreakerShardBreakerQueryTraceId, "breaker DataShard BreakerQueryTraceId should be present");
        UNIT_ASSERT_VALUES_EQUAL_C(*data.BreakerSessionBreakerQueryTraceId, *data.BreakerShardBreakerQueryTraceId,
            "DS Breaker ↔ KQP Breaker: BreakerQueryTraceId should match");

        // 2. DS Victim ↔ KQP Victim: Match VictimQueryTraceId
        UNIT_ASSERT_C(data.VictimSessionVictimQueryTraceId, "victim SessionActor VictimQueryTraceId should be present");
        UNIT_ASSERT_C(data.VictimShardVictimQueryTraceId, "victim DataShard VictimQueryTraceId should be present");
        UNIT_ASSERT_VALUES_EQUAL_C(*data.VictimSessionVictimQueryTraceId, *data.VictimShardVictimQueryTraceId,
            "DS Victim ↔ KQP Victim: VictimQueryTraceId should match");

        // 3. DS Breaker ↔ DS Victim: Match VictimQueryTraceId in VictimQueryTraceIds array
        bool foundVictimQueryTraceId = std::find(data.BreakerShardVictimQueryTraceIds->begin(),
            data.BreakerShardVictimQueryTraceIds->end(), *data.VictimShardVictimQueryTraceId)
            != data.BreakerShardVictimQueryTraceIds->end();
        UNIT_ASSERT_C(foundVictimQueryTraceId, "DS Breaker ↔ DS Victim: victim VictimQueryTraceId should be in breaker VictimQueryTraceIds");

        // BreakerQueryTraceId consistency - breaker's QueryTraceId should match BreakerQueryTraceId
        UNIT_ASSERT_VALUES_EQUAL_C(*data.BreakerSessionQueryTraceId, *data.BreakerSessionBreakerQueryTraceId,
            "breaker SessionActor QueryTraceId should match BreakerQueryTraceId");

        UNIT_ASSERT_VALUES_EQUAL_C(*data.BreakerShardQueryTraceId, *data.BreakerShardBreakerQueryTraceId,
            "breaker DataShard QueryTraceId should match BreakerQueryTraceId");

        // VictimQueryTraceId consistency - victim's first query QueryTraceId should match VictimQueryTraceId
        if (data.VictimSessionQueryTraceId && data.VictimSessionVictimQueryTraceId) {
            UNIT_ASSERT_VALUES_EQUAL_C(*data.VictimSessionQueryTraceId, *data.VictimSessionVictimQueryTraceId,
                "victim SessionActor QueryTraceId should match VictimQueryTraceId");
        }
        if (data.VictimShardQueryTraceId && data.VictimShardVictimQueryTraceId) {
            UNIT_ASSERT_VALUES_EQUAL_C(*data.VictimShardQueryTraceId, *data.VictimShardVictimQueryTraceId,
                "victim DataShard QueryTraceId should match VictimQueryTraceId");
        }

        // Query text assertions
        UNIT_ASSERT_C(data.BreakerAllQueryTexts && data.BreakerAllQueryTexts->Contains(breakerQueryText),
            "breaker SessionActor AllQueryTexts should contain breaker query");
        UNIT_ASSERT_C(data.VictimAllQueryTexts && data.VictimAllQueryTexts->Contains(victimQueryText),
            "victim SessionActor AllQueryTexts should contain victim query");
        UNIT_ASSERT_C(!data.BreakerQueryText || *data.BreakerQueryText == breakerQueryText || *data.BreakerQueryText == "Commit",
            "breaker SessionActor QueryText should match breaker query or be Commit: " << *data.BreakerQueryText);
        UNIT_ASSERT_VALUES_EQUAL_C(data.VictimQueryText, victimQueryText,
            "victim SessionActor QueryText should match victim query");
        if (victimExtraQueryText) {
            UNIT_ASSERT_C(data.VictimAllQueryTexts->Contains(*victimExtraQueryText),
                "AllQueryTexts should contain victim extra query");
        }
    }

    void AssertNoTliLogsWhenDisabled(const TString& logs, bool logEnabled) {
        if (!logEnabled) {
            UNIT_ASSERT_C(logs.find("TLI INFO") == TString::npos,
                "no TLI INFO logs expected when LogEnabled=false");
        }
    }

    // ==================== Test context and table helpers ====================

    TKikimrSettings MakeKikimrSettings(bool useSink, TStringStream& ss) {
        TKikimrSettings settings;
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(useSink);
        settings.AppConfig.MutableLogTliConfig()->SetQueryTextLogMode(NKikimrProto::TLogTliConfig_ELogMode_ORIGINAL);
        settings.LogStream = &ss;
        settings.SetWithSampleTables(false);
        return settings;
    }

    void ConfigureKikimrForTli(TKikimrRunner& kikimr, bool logEnabled) {
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).LogTliConfig.SetQueryTextLogMode(NKikimrProto::TLogTliConfig_ELogMode_ORIGINAL);
        if (logEnabled) {
            kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TLI, NLog::PRI_INFO);
        }
    }

    struct TTliTestContext {
        TKikimrRunner Kikimr;
        TTableClient Client;
        TSession Session;
        TSession VictimSession;

        TTliTestContext(bool logEnabled, bool useSink, TStringStream& ss)
            : Kikimr(MakeKikimrSettings(useSink, ss))
            , Client(Kikimr.GetTableClient())
            , Session(Client.CreateSession().GetValueSync().GetSession())
            , VictimSession(Client.CreateSession().GetValueSync().GetSession())
        {
            ConfigureKikimrForTli(Kikimr, logEnabled);
        }

        void CreateTable(const TString& tableName) {
            NKqp::AssertSuccessResult(Session.ExecuteSchemeQuery(
                Sprintf(R"(CREATE TABLE `%s` (Key Uint64, Value String, PRIMARY KEY (Key));)", tableName.c_str())
            ).GetValueSync());
        }

        void SeedTable(const TString& tableName, const TVector<std::pair<ui64, TString>>& rows) {
            for (const auto& [key, value] : rows) {
                NKqp::AssertSuccessResult(Session.ExecuteDataQuery(
                    Sprintf("UPSERT INTO `%s` (Key, Value) VALUES (%luu, \"%s\")", tableName.c_str(), key, value.c_str()),
                    TTxControl::BeginTx().CommitTx()
                ).GetValueSync());
            }
        }

        void ExecuteQuery(const TString& query) {
            NKqp::AssertSuccessResult(Session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync());
        }
    };

    TTransaction BeginReadTx(TSession& session, const TString& queryText) {
        while (true) {
            auto result = session.ExecuteDataQuery(queryText, TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
            if (FormatResultSetYson(result.GetResultSet(0)) != "[]") {
                auto tx = result.GetTransaction();
                UNIT_ASSERT(tx);
                return *tx;
            }
        }
    }

    EStatus ExecuteVictimCommit(TSession& session, TTransaction& tx, const TString& query) {
        return session.ExecuteDataQuery(query, TTxControl::Tx(tx).CommitTx()).ExtractValueSync().GetStatus();
    }

    void VerifyTliLogsAndAssert(
        TStringStream& ss,
        bool LogEnabled,
        bool UseSink,
        const TString& breakerQueryText,
        const TString& victimQueryText,
        const std::optional<TString>& victimExtraQueryText = std::nullopt)
    {
        DumpTliRecords(ss.Str());
        const auto patterns = MakeTliLogPatterns(UseSink);

        if (LogEnabled) {
            const auto data = ExtractAllTliData(ss.Str(), patterns);
            AssertCommonTliAsserts(data, breakerQueryText, victimQueryText, victimExtraQueryText);
        } else {
            AssertNoTliLogsWhenDisabled(ss.Str(), LogEnabled);
        }
    }
} // namespace

Y_UNIT_TEST_SUITE(KqpTli) {

    Y_UNIT_TEST_QUAD(Basic, LogEnabled, UseSink) {
        TStringStream ss;
        TTliTestContext ctx(LogEnabled, UseSink, ss);
        ctx.CreateTable("/Root/Tenant1/TableLocks");
        ctx.SeedTable("/Root/Tenant1/TableLocks", {{1, "Initial"}});

        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/TableLocks` (Key, Value) VALUES (1u, \"BreakerValue\")";
        const TString victimQueryText = "SELECT * FROM `/Root/Tenant1/TableLocks` WHERE Key = 1u";
        const TString victimCommitText = "UPSERT INTO `/Root/Tenant1/TableLocks` (Key, Value) VALUES (1u, \"VictimValue\")";

        auto victimTx = BeginReadTx(ctx.VictimSession, victimQueryText);
        ctx.ExecuteQuery(breakerQueryText);
        UNIT_ASSERT_VALUES_EQUAL(ExecuteVictimCommit(ctx.VictimSession, victimTx, victimCommitText), EStatus::ABORTED);

        VerifyTliLogsAndAssert(ss, LogEnabled, UseSink, breakerQueryText, victimQueryText, victimCommitText);
    }

    Y_UNIT_TEST_QUAD(SeparateCommit, LogEnabled, UseSink) {
        TStringStream ss;
        TTliTestContext ctx(LogEnabled, UseSink, ss);
        ctx.CreateTable("/Root/Tenant1/TableLocks");
        ctx.SeedTable("/Root/Tenant1/TableLocks", {{1, "Initial"}});

        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/TableLocks` (Key, Value) VALUES (1u, \"BreakerValue\")";
        const TString victimQueryText = "SELECT * FROM `/Root/Tenant1/TableLocks` WHERE Key = 1u";
        const TString victimCommitText = "UPSERT INTO `/Root/Tenant1/TableLocks` (Key, Value) VALUES (1u, \"VictimValue\")";

        auto victimTx = BeginReadTx(ctx.VictimSession, victimQueryText);

        // Breaker: begin tx, write key 1, write key 2, then separate commit
        std::optional<TTransaction> breakerTx;
        {
            auto result = ctx.Session.ExecuteDataQuery(breakerQueryText, TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
            breakerTx = result.GetTransaction();
        }
        NKqp::AssertSuccessResult(ctx.Session.ExecuteDataQuery(
            "UPSERT INTO `/Root/Tenant1/TableLocks` (Key, Value) VALUES (2u, \"UsualValue\")",
            TTxControl::Tx(*breakerTx)).GetValueSync());
        NKqp::AssertSuccessResult(breakerTx->Commit().ExtractValueSync());

        UNIT_ASSERT_VALUES_EQUAL(ExecuteVictimCommit(ctx.VictimSession, victimTx, victimCommitText), EStatus::ABORTED);

        VerifyTliLogsAndAssert(ss, LogEnabled, UseSink, breakerQueryText, victimQueryText, victimCommitText);
    }

    Y_UNIT_TEST_QUAD(ManyTables, LogEnabled, UseSink) {
        TStringStream ss;
        TTliTestContext ctx(LogEnabled, UseSink, ss);
        for (int i = 1; i <= 6; ++i) {
            ctx.CreateTable(Sprintf("/Root/Tenant1/Table%d", i));
            ctx.SeedTable(Sprintf("/Root/Tenant1/Table%d", i), {{1, Sprintf("Init%d", i)}});
        }

        const TString victimSelectTable1 = "SELECT * FROM `/Root/Tenant1/Table1` WHERE Key = 1u";
        const TString victimSelectTable2 = "SELECT * FROM `/Root/Tenant1/Table2` WHERE Key = 1u";
        const TString victimSelectTable3 = "SELECT * FROM `/Root/Tenant1/Table3` WHERE Key = 1u";
        const TString victimUpdateTable4 = "UPDATE `/Root/Tenant1/Table4` SET Value = \"VictimUpdate\" WHERE Key = 1u";
        const TString breakerUpdateTable2 = "UPDATE `/Root/Tenant1/Table2` SET Value = \"BreakerUpdate2\" WHERE Key = 1u";
        const TString breakerUpdateTable5 = "UPDATE `/Root/Tenant1/Table5` SET Value = \"BreakerUpdate5\" WHERE Key = 1u";
        const TString breakerUpdateTable6 = "UPDATE `/Root/Tenant1/Table6` SET Value = \"BreakerUpdate6\" WHERE Key = 1u";

        // Victim: read tables 1,2,3, then update table 4 (without commit)
        auto victimTx = BeginReadTx(ctx.VictimSession, victimSelectTable1);
        NKqp::AssertSuccessResult(ctx.VictimSession.ExecuteDataQuery(victimSelectTable2, TTxControl::Tx(victimTx)).GetValueSync());
        NKqp::AssertSuccessResult(ctx.VictimSession.ExecuteDataQuery(victimSelectTable3, TTxControl::Tx(victimTx)).GetValueSync());
        NKqp::AssertSuccessResult(ctx.VictimSession.ExecuteDataQuery(victimUpdateTable4, TTxControl::Tx(victimTx)).GetValueSync());

        // Breaker: update tables 5,2,6, then commit (breaks victim's lock on table 2)
        std::optional<TTransaction> breakerTx;
        {
            auto result = ctx.Session.ExecuteDataQuery(breakerUpdateTable5, TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
            breakerTx = result.GetTransaction();
        }
        NKqp::AssertSuccessResult(ctx.Session.ExecuteDataQuery(breakerUpdateTable2, TTxControl::Tx(*breakerTx)).GetValueSync());
        NKqp::AssertSuccessResult(ctx.Session.ExecuteDataQuery(breakerUpdateTable6, TTxControl::Tx(*breakerTx)).GetValueSync());
        NKqp::AssertSuccessResult(breakerTx->Commit().ExtractValueSync());

        UNIT_ASSERT_VALUES_EQUAL(victimTx.Commit().ExtractValueSync().GetStatus(), EStatus::ABORTED);

        VerifyTliLogsAndAssert(ss, LogEnabled, UseSink, breakerUpdateTable2, victimSelectTable2);
    }

    // Test: Victim reads key 1, breaker writes key 1, victim writes key 2
    Y_UNIT_TEST_QUAD(DifferentKeys, LogEnabled, UseSink) {
        TStringStream ss;
        TTliTestContext ctx(LogEnabled, UseSink, ss);
        ctx.CreateTable("/Root/Tenant1/TableDiffKeys");
        ctx.SeedTable("/Root/Tenant1/TableDiffKeys", {{1, "V1"}, {2, "V2"}});

        const TString victimQueryText = "SELECT * FROM `/Root/Tenant1/TableDiffKeys` WHERE Key = 1u";
        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/TableDiffKeys` (Key, Value) VALUES (1u, \"Breaker\")";
        const TString victimCommitText = "UPSERT INTO `/Root/Tenant1/TableDiffKeys` (Key, Value) VALUES (2u, \"VictimWrite\")";

        auto victimTx = BeginReadTx(ctx.VictimSession, victimQueryText);
        ctx.ExecuteQuery(breakerQueryText);
        UNIT_ASSERT_VALUES_EQUAL(ExecuteVictimCommit(ctx.VictimSession, victimTx, victimCommitText), EStatus::ABORTED);

        VerifyTliLogsAndAssert(ss, LogEnabled, UseSink, breakerQueryText, victimQueryText);
    }

    // Test: Victim reads multiple keys, breaker writes them all
    Y_UNIT_TEST_QUAD(MultipleKeys, LogEnabled, UseSink) {
        TStringStream ss;
        TTliTestContext ctx(LogEnabled, UseSink, ss);
        ctx.CreateTable("/Root/Tenant1/TableMulti");
        ctx.SeedTable("/Root/Tenant1/TableMulti", {{1, "V1"}, {2, "V2"}, {3, "V3"}});

        const TString victimQueryText = "SELECT * FROM `/Root/Tenant1/TableMulti` WHERE Key IN (1u, 2u, 3u)";
        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/TableMulti` (Key, Value) VALUES (1u, \"B1\"), (2u, \"B2\"), (3u, \"B3\")";
        const TString victimCommitText = "UPSERT INTO `/Root/Tenant1/TableMulti` (Key, Value) VALUES (1u, \"Victim\")";

        auto victimTx = BeginReadTx(ctx.VictimSession, victimQueryText);
        ctx.ExecuteQuery(breakerQueryText);
        UNIT_ASSERT_VALUES_EQUAL(ExecuteVictimCommit(ctx.VictimSession, victimTx, victimCommitText), EStatus::ABORTED);

        VerifyTliLogsAndAssert(ss, LogEnabled, UseSink, breakerQueryText, victimQueryText);
    }

    // Test: Cross-table lock breakage - victim reads TableA, breaker writes TableA, victim writes TableB
    Y_UNIT_TEST_QUAD(CrossTables, LogEnabled, UseSink) {
        TStringStream ss;
        TTliTestContext ctx(LogEnabled, UseSink, ss);
        ctx.CreateTable("/Root/Tenant1/TableA");
        ctx.CreateTable("/Root/Tenant1/TableB");
        ctx.SeedTable("/Root/Tenant1/TableA", {{1, "ValA"}});

        const TString victimQueryText = "SELECT * FROM `/Root/Tenant1/TableA` WHERE Key = 1u";
        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/TableA` (Key, Value) VALUES (1u, \"Breaker\")";
        const TString victimCommitText = "UPSERT INTO `/Root/Tenant1/TableB` (Key, Value) VALUES (1u, \"DstVal\")";

        auto victimTx = BeginReadTx(ctx.VictimSession, victimQueryText);
        ctx.ExecuteQuery(breakerQueryText);
        UNIT_ASSERT_VALUES_EQUAL(ExecuteVictimCommit(ctx.VictimSession, victimTx, victimCommitText), EStatus::ABORTED);

        VerifyTliLogsAndAssert(ss, LogEnabled, UseSink, breakerQueryText, victimQueryText);
    }

    // Test: Two victims and one breaker scenario
    Y_UNIT_TEST_QUAD(TwoVictimsOneBreaker, LogEnabled, UseSink) {
        TStringStream ss;
        TTliTestContext ctx(LogEnabled, UseSink, ss);
        ctx.CreateTable("/Root/Tenant1/TableLocks");
        ctx.SeedTable("/Root/Tenant1/TableLocks", {{1, "Initial"}});

        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/TableLocks` (Key, Value) VALUES (1u, \"BreakerValue\")";
        const TString victimQueryText = "SELECT * FROM `/Root/Tenant1/TableLocks` WHERE Key = 1u";
        const TString victimCommitText = "UPSERT INTO `/Root/Tenant1/TableLocks` (Key, Value) VALUES (1u, \"VictimValue\")";

        // Create two victim sessions
        TSession victim1Session = ctx.Client.CreateSession().GetValueSync().GetSession();
        TSession victim2Session = ctx.Client.CreateSession().GetValueSync().GetSession();

        // Victim1: read key 1 and prepare commit
        auto victim1Tx = BeginReadTx(victim1Session, victimQueryText);

        // Victim2: read key 1 and prepare commit
        auto victim2Tx = BeginReadTx(victim2Session, victimQueryText);

        // Breaker: write key 1 (breaks both victims' locks)
        ctx.ExecuteQuery(breakerQueryText);

        // Both victims try to commit - both should be aborted
        UNIT_ASSERT_VALUES_EQUAL(ExecuteVictimCommit(victim1Session, victim1Tx, victimCommitText), EStatus::ABORTED);
        UNIT_ASSERT_VALUES_EQUAL(ExecuteVictimCommit(victim2Session, victim2Tx, victimCommitText), EStatus::ABORTED);

        // Verify TLI logs with additional assertions for multiple victims
        VerifyTliLogsAndAssert(ss, LogEnabled, UseSink, breakerQueryText, victimQueryText, victimCommitText);
    }

}

} // namespace NKqp
} // namespace NKikimr
