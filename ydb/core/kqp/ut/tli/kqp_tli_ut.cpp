#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/test_tli.h>
#include <ydb/core/protos/data_integrity_trails.pb.h>

#include <algorithm>
#include <memory>
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

    // Check if position starts an ISO 8601 timestamp followed by " node " (log record boundary).
    // Format: YYYY-MM-DDTHH:MM:SS.xxxxxxZ node
    bool IsTimestampBoundary(const TString& logs, size_t pos) {
        if (pos + 30 >= logs.size()) {
            return false;
        }
        return std::isdigit(logs[pos]) && std::isdigit(logs[pos + 1]) &&
               std::isdigit(logs[pos + 2]) && std::isdigit(logs[pos + 3]) &&
               logs[pos + 4] == '-' &&
               std::isdigit(logs[pos + 5]) && std::isdigit(logs[pos + 6]) &&
               logs[pos + 7] == '-' &&
               std::isdigit(logs[pos + 8]) && std::isdigit(logs[pos + 9]) &&
               logs[pos + 10] == 'T' &&
               logs[pos + 13] == ':' &&
               logs[pos + 16] == ':' &&
               logs[pos + 19] == '.' &&
               logs[pos + 26] == 'Z' &&
               logs.find(" node ", pos) == pos + 27;
    }

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
            // Scan for the next log record timestamp boundary
            for (size_t i = found + 1; i < logs.size(); ++i) {
                if (IsTimestampBoundary(logs, i)) {
                    recordEnd = std::min(recordEnd, i);
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

    // Extract BreakerQueryText from a single TLI record
    std::optional<TString> ExtractBreakerQueryTextFromRecord(const TString& record) {
        const size_t allPos = record.find("BreakerQueryText: ");
        if (allPos == TString::npos) {
            return std::nullopt;
        }
        TString result = record.substr(allPos + 18);
        size_t nextFieldPos = result.find(", BreakerQueryTexts:");
        if (nextFieldPos != TString::npos) {
            result = result.substr(0, nextFieldPos);
        }
        return UnescapeC(result);
    }

    std::optional<TString> ExtractQueryText(const TString& logs, const TString& messagePattern,
        const std::optional<TString>& expectedText = std::nullopt)
    {
        for (const auto& record : ExtractTliRecords(logs)) {
            if (!record.Contains("Component: SessionActor") || !MatchesMessage(record, messagePattern)) {
                continue;
            }
            auto text = ExtractBreakerQueryTextFromRecord(record);
            if (!text) {
                continue;
            }
            if (expectedText && *text != *expectedText) {
                continue;
            }
            return text;
        }
        return std::nullopt;
    }

    // Extract VictimQueryText from a single TLI record
    std::optional<TString> ExtractVictimQueryTextFromRecord(const TString& record) {
        const size_t victimPos = record.find("VictimQueryText: ");
        if (victimPos == TString::npos) {
            return std::nullopt;
        }
        TString result = record.substr(victimPos + 17);
        const size_t nextFieldPos = result.find(", VictimQueryTexts:");
        if (nextFieldPos != TString::npos) {
            result = result.substr(0, nextFieldPos);
        }
        return UnescapeC(result);
    }

    std::optional<TString> ExtractVictimQueryText(const TString& logs, const TString& messagePattern,
        const std::optional<TString>& expectedText = std::nullopt)
    {
        for (const auto& record : ExtractTliRecords(logs)) {
            if (!record.Contains("Component: SessionActor") || !MatchesMessage(record, messagePattern)) {
                continue;
            }
            auto text = ExtractVictimQueryTextFromRecord(record);
            if (!text) {
                continue;
            }
            if (expectedText && *text != *expectedText) {
                continue;
            }
            return text;
        }
        return std::nullopt;
    }

    // Extract query texts field (BreakerQueryTexts or VictimQueryTexts based on context)
    // When expectedContainedText is provided, only returns from records containing that text.
    std::optional<TString> ExtractQueryTextsField(const TString& logs, const TString& messagePattern,
        const TString& fieldName, const std::optional<TString>& expectedContainedText = std::nullopt)
    {
        for (const auto& record : ExtractTliRecords(logs)) {
            if (!record.Contains("Component: SessionActor") || !MatchesMessage(record, messagePattern)) {
                continue;
            }
            const TString prefix = fieldName + ": ";
            const size_t allPos = record.find(prefix);
            if (allPos == TString::npos) {
                continue;
            }
            TString result = record.substr(allPos + prefix.size());
            if (result.EndsWith(",")) {
                result.pop_back();
            }
            TString unescaped = UnescapeC(result);
            if (expectedContainedText && !unescaped.Contains(*expectedContainedText)) {
                continue;
            }
            return unescaped;
        }
        return std::nullopt;
    }

    std::optional<TString> ExtractBreakerQueryTexts(const TString& logs, const TString& messagePattern,
        const std::optional<TString>& expectedContainedText = std::nullopt)
    {
        return ExtractQueryTextsField(logs, messagePattern, "BreakerQueryTexts", expectedContainedText);
    }

    std::optional<TString> ExtractVictimQueryTexts(const TString& logs, const TString& messagePattern,
        const std::optional<TString>& expectedContainedText = std::nullopt)
    {
        return ExtractQueryTextsField(logs, messagePattern, "VictimQueryTexts", expectedContainedText);
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

    std::optional<ui64> ExtractCurrentQueryTraceId(const TString& logs, const TString& component, const TString& messagePattern) {
        for (const auto& record : ExtractTliRecords(logs)) {
            if (!record.Contains("Component: " + component) || !MatchesMessage(record, messagePattern)) {
                continue;
            }
            return ExtractNumericField(record, "CurrentQueryTraceId");
        }
        return std::nullopt;
    }


    std::optional<ui64> ExtractBreakerQueryTraceId(const TString& logs, const TString& component, const TString& messagePattern,
        const std::optional<TString>& expectedBreakerQueryText = std::nullopt)
    {
        for (const auto& record : ExtractTliRecords(logs)) {
            if (!record.Contains("Component: " + component) || !MatchesMessage(record, messagePattern)) {
                continue;
            }
            if (expectedBreakerQueryText && component == "SessionActor") {
                auto text = ExtractBreakerQueryTextFromRecord(record);
                if (!text || *text != *expectedBreakerQueryText) {
                    continue;
                }
            }
            return ExtractNumericField(record, "BreakerQueryTraceId");
        }
        return std::nullopt;
    }



    std::optional<ui64> ExtractVictimQueryTraceId(const TString& logs, const TString& component, const TString& messagePattern,
        const std::optional<TString>& expectedVictimQueryText = std::nullopt)
    {
        for (const auto& record : ExtractTliRecords(logs)) {
            if (!record.Contains("Component: " + component) || !MatchesMessage(record, messagePattern)) {
                continue;
            }
            if (expectedVictimQueryText && component == "SessionActor") {
                auto text = ExtractVictimQueryTextFromRecord(record);
                if (!text || *text != *expectedVictimQueryText) {
                    continue;
                }
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

    std::optional<std::vector<ui64>> ExtractVictimQueryTraceIdOccurrences(
        const TString& logs,
        const TString& component,
        const TString& messagePattern)
    {
        std::vector<ui64> result;
        for (const auto& record : ExtractTliRecords(logs)) {
            if (!record.Contains("Component: " + component) || !MatchesMessage(record, messagePattern)) {
                continue;
            }
            auto value = ExtractNumericField(record, "VictimQueryTraceId");
            if (value) {
                result.push_back(*value);
            }
        }
        if (result.empty()) {
            return std::nullopt;
        }
        return result;
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

    TTliLogPatterns MakeTliLogPatterns() {
        return {
            "(Query|Commit) had broken other locks",
            "(Query|Commit) was a victim of broken locks",
            "Write transaction broke other locks",
            "(Write|Read) transaction was a victim of broken locks",
        };
    }

    // ==================== Extracted TLI data struct ====================

    struct TExtractedTliData {
        std::optional<TString> BreakerQueryTexts;
        std::optional<TString> VictimQueryTexts;
        std::optional<TString> BreakerQueryText;
        std::optional<TString> VictimQueryText;
        std::optional<ui64> BreakerSessionBreakerQueryTraceId;
        std::optional<ui64> BreakerShardBreakerQueryTraceId;
        std::optional<std::vector<ui64>> BreakerShardVictimQueryTraceIds;

        std::optional<ui64> VictimSessionCurrentQueryTraceId;
        std::optional<ui64> VictimShardCurrentQueryTraceId;
        std::optional<ui64> VictimSessionVictimQueryTraceId;
        std::optional<ui64> VictimShardVictimQueryTraceId;
        std::optional<std::vector<ui64>> VictimSessionVictimQueryTraceIdOccurrences;

        bool FoundBreakerRecordInDatashard = false;
        std::optional<std::vector<ui64>> MatchingDsBreakerVictimQueryTraceIds;
        bool FoundVictimRecordInDatashard = false;
    };

    std::pair<bool, std::optional<std::vector<ui64>>> ExtractMatchingFromBreakerDatashard(
        const TString& logs,
        const TString& messagePattern,
        std::optional<ui64> breakerQueryTraceIdFromKQP)
    {
        if (!breakerQueryTraceIdFromKQP) {
            return {false, std::nullopt};
        }

        for (const auto& record : ExtractTliRecords(logs)) {
            if (!record.Contains("Component: DataShard") || !MatchesMessage(record, messagePattern)) {
                continue;
            }
            auto breakerQueryTraceId = ExtractNumericField(record, "BreakerQueryTraceId");
            if (breakerQueryTraceId && *breakerQueryTraceId == *breakerQueryTraceIdFromKQP) {
                std::optional<std::vector<ui64>> matchingVictimIds;
                const size_t idsPos = record.find("VictimQueryTraceIds: [");
                if (idsPos != TString::npos) {
                    const size_t listStart = idsPos + 22;
                    const size_t listEnd = record.find(']', listStart);
                    if (listEnd != TString::npos) {
                        matchingVictimIds.emplace();
                        for (const auto& part : StringSplitter(record.substr(listStart, listEnd - listStart)).Split(' ').SkipEmpty()) {
                            matchingVictimIds->emplace_back(FromString<ui64>(part));
                        }
                    }
                }
                return {true, matchingVictimIds};
            }
        }
        return {false, std::nullopt};
    }

    bool CheckMatchingInVictimDatashard(
        const TString& logs,
        const TString& messagePattern,
        std::optional<ui64> victimQueryTraceIdFromKQP)
    {
        if (!victimQueryTraceIdFromKQP) {
            return false;
        }
        for (const auto& record : ExtractTliRecords(logs)) {
            if (!record.Contains("Component: DataShard") || !MatchesMessage(record, messagePattern)) {
                continue;
            }
            auto victimQueryTraceId = ExtractNumericField(record, "VictimQueryTraceId");
            if (victimQueryTraceId && *victimQueryTraceId == *victimQueryTraceIdFromKQP) {
                return true;
            }
        }
        return false;
    }

    TExtractedTliData ExtractAllTliData(const TString& logs, const TTliLogPatterns& patterns,
        const std::optional<TString>& expectedBreakerQueryText = std::nullopt,
        const std::optional<TString>& expectedVictimQueryText = std::nullopt)
    {
        TExtractedTliData data;
        data.BreakerQueryTexts = ExtractBreakerQueryTexts(logs, patterns.BreakerSessionActorMessagePattern, expectedBreakerQueryText);
        data.VictimQueryTexts = ExtractVictimQueryTexts(logs, patterns.VictimSessionActorMessagePattern, expectedVictimQueryText);
        data.BreakerQueryText = ExtractQueryText(logs, patterns.BreakerSessionActorMessagePattern, expectedBreakerQueryText);
        data.VictimQueryText = ExtractVictimQueryText(logs, patterns.VictimSessionActorMessagePattern, expectedVictimQueryText);
        data.BreakerSessionBreakerQueryTraceId = ExtractBreakerQueryTraceId(logs, "SessionActor", patterns.BreakerSessionActorMessagePattern, expectedBreakerQueryText);
        data.BreakerShardBreakerQueryTraceId = ExtractBreakerQueryTraceId(logs, "DataShard", patterns.BreakerDatashardMessage);
        data.BreakerShardVictimQueryTraceIds = ExtractVictimQueryTraceIds(logs, "DataShard", patterns.BreakerDatashardMessage);
        data.VictimSessionCurrentQueryTraceId = ExtractCurrentQueryTraceId(logs, "SessionActor", patterns.VictimSessionActorMessagePattern);
        data.VictimShardCurrentQueryTraceId = ExtractCurrentQueryTraceId(logs, "DataShard", patterns.VictimDatashardMessage);
        data.VictimSessionVictimQueryTraceId = ExtractVictimQueryTraceId(logs, "SessionActor", patterns.VictimSessionActorMessagePattern, expectedVictimQueryText);
        data.VictimShardVictimQueryTraceId = ExtractVictimQueryTraceId(logs, "DataShard", patterns.VictimDatashardMessage);
        data.VictimSessionVictimQueryTraceIdOccurrences = ExtractVictimQueryTraceIdOccurrences(
            logs, "SessionActor", patterns.VictimSessionActorMessagePattern);

        auto [foundBreaker, matchingVictimIds] = ExtractMatchingFromBreakerDatashard(logs, patterns.BreakerDatashardMessage, data.BreakerSessionBreakerQueryTraceId);
        data.FoundBreakerRecordInDatashard = foundBreaker;
        data.MatchingDsBreakerVictimQueryTraceIds = matchingVictimIds;

        data.FoundVictimRecordInDatashard = CheckMatchingInVictimDatashard(logs, patterns.VictimDatashardMessage, data.VictimSessionVictimQueryTraceId);

        return data;
    }

    void AssertCommonTliAsserts(
        const TExtractedTliData& data,
        const TString& breakerQueryText,
        const TString& victimQueryText,
        const std::optional<TString>& victimExtraQueryText = std::nullopt)
    {
        // ==================== QueryTraceId Linkage Assertions ====================

        UNIT_ASSERT_C(data.BreakerSessionBreakerQueryTraceId, "breaker SessionActor BreakerQueryTraceId should be present");
        UNIT_ASSERT_C(data.VictimSessionVictimQueryTraceId, "victim SessionActor VictimQueryTraceId should be present");

        // 1. DS Breaker ↔ KQP Breaker: Find the DataShard breaker record matching the SessionActor's BreakerQueryTraceId
        UNIT_ASSERT_C(data.FoundBreakerRecordInDatashard,
            "SessionActor BreakerQueryTraceId should exist in some DataShard breaker record");

        // 2. DS Victim ↔ KQP Victim: Find the DataShard victim record matching the SessionActor's VictimQueryTraceId
        UNIT_ASSERT_C(data.FoundVictimRecordInDatashard,
            "SessionActor VictimQueryTraceId should exist in some DataShard victim record");

        // 3. DS Breaker ↔ DS Victim: The matching DataShard breaker record should contain the VictimQueryTraceId
        UNIT_ASSERT_C(data.MatchingDsBreakerVictimQueryTraceIds.has_value(), "matching DataShard breaker record should have VictimQueryTraceIds");
        bool victimInBreaker = std::find(data.MatchingDsBreakerVictimQueryTraceIds->begin(), data.MatchingDsBreakerVictimQueryTraceIds->end(),
            *data.VictimSessionVictimQueryTraceId) != data.MatchingDsBreakerVictimQueryTraceIds->end();
        UNIT_ASSERT_C(victimInBreaker,
            "victim VictimQueryTraceId should be in matching DataShard breaker's VictimQueryTraceIds");

        // Query text assertions
        UNIT_ASSERT_C(data.BreakerQueryTexts && data.BreakerQueryTexts->Contains(breakerQueryText),
            "breaker SessionActor BreakerQueryTexts should contain breaker query");
        UNIT_ASSERT_C(data.VictimQueryTexts && data.VictimQueryTexts->Contains(victimQueryText),
            "victim SessionActor VictimQueryTexts should contain victim query");
        UNIT_ASSERT_VALUES_EQUAL_C(data.BreakerQueryText, breakerQueryText,
            "breaker SessionActor QueryText should match breaker query");
        UNIT_ASSERT_VALUES_EQUAL_C(data.VictimQueryText, victimQueryText,
            "victim SessionActor QueryText should match victim query");
        if (victimExtraQueryText) {
            UNIT_ASSERT_C(data.VictimQueryTexts->Contains(*victimExtraQueryText),
                "VictimQueryTexts should contain victim extra query");
        }
    }

    // ==================== Test context and table helpers ====================

    TKikimrSettings MakeKikimrSettings(TStringStream& ss) {
        TKikimrSettings settings;
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
        settings.LogStream = &ss;
        settings.SetWithSampleTables(false);
        return settings;
    }

    void ConfigureKikimrForTli(TKikimrRunner& kikimr, bool logEnabled = true) {
        if (logEnabled) {
            kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TLI, NLog::PRI_INFO);
        }
    }

    struct TTliTestContext {
        TKikimrRunner Kikimr;
        TTableClient Client;
        TSession Session;
        TSession VictimSession;

        TTliTestContext(TStringStream& ss, bool logEnabled = true)
            : Kikimr(MakeKikimrSettings(ss))
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

    // 2-node test context: victim session on node 0, breaker session on node 1.
    // Logs from both nodes are captured via per-node log backends.
    struct TTli2NodeTestContext {
        TKikimrRunner Kikimr;
        std::unique_ptr<NYdb::TDriver> VictimDriver;
        std::unique_ptr<NYdb::TDriver> BreakerDriver;
        std::unique_ptr<TTableClient> VictimClient;
        std::unique_ptr<TTableClient> BreakerClient;
        std::optional<TSession> VictimSession;
        std::optional<TSession> BreakerSession;

        TTli2NodeTestContext(TStringStream& ss)
            : Kikimr(MakeKikimrSettings(ss).SetNodeCount(2))
        {
            ConfigureKikimrForTli(Kikimr);

            auto& portManager = Kikimr.GetTestServer().GetRuntime()->GetPortManager();
            const ui16 breakerPort = portManager.GetPort();
            Kikimr.GetTestServer().EnableGRpc(breakerPort, 1);

            const auto baseConfig = Kikimr.GetDriverConfig();
            auto victimConfig = baseConfig;
            auto breakerConfig = baseConfig;

            victimConfig.SetEndpoint(Kikimr.GetEndpoint());
            breakerConfig.SetEndpoint(TStringBuilder() << "localhost:" << breakerPort);

            VictimDriver = std::make_unique<NYdb::TDriver>(victimConfig);
            BreakerDriver = std::make_unique<NYdb::TDriver>(breakerConfig);

            VictimClient = std::make_unique<TTableClient>(*VictimDriver);
            BreakerClient = std::make_unique<TTableClient>(*BreakerDriver);

            VictimSession = VictimClient->CreateSession().GetValueSync().GetSession();
            BreakerSession = BreakerClient->CreateSession().GetValueSync().GetSession();
        }

        void CreateTable(const TString& tableName) {
            NKqp::AssertSuccessResult(BreakerSession->ExecuteSchemeQuery(
                Sprintf(R"(CREATE TABLE `%s` (Key Uint64, Value String, PRIMARY KEY (Key));)", tableName.c_str())
            ).GetValueSync());
        }

        void SeedTable(const TString& tableName, const TVector<std::pair<ui64, TString>>& rows) {
            for (const auto& [key, value] : rows) {
                NKqp::AssertSuccessResult(BreakerSession->ExecuteDataQuery(
                    Sprintf("UPSERT INTO `%s` (Key, Value) VALUES (%luu, \"%s\")", tableName.c_str(), key, value.c_str()),
                    TTxControl::BeginTx().CommitTx()
                ).GetValueSync());
            }
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

    std::optional<TTransaction> BeginTx(TSession& session, const TString& query) {
        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
        return result.GetTransaction();
    }

    // Execute victim commit and return both status and issues for verification
    std::pair<EStatus, TString> ExecuteVictimCommitWithIssues(TSession& session, TTransaction& tx, const TString& query) {
        auto result = session.ExecuteDataQuery(query, TTxControl::Tx(tx).CommitTx()).ExtractValueSync();
        return {result.GetStatus(), result.GetIssues().ToString()};
    }

    // Execute a direct commit (no query) and return both status and issues
    std::pair<EStatus, TString> CommitTxWithIssues(TTransaction& tx) {
        auto result = tx.Commit().ExtractValueSync();
        return {result.GetStatus(), result.GetIssues().ToString()};
    }

    // Extract VictimQueryTraceId from issue message
    std::optional<ui64> ExtractVictimQueryTraceIdFromIssue(const TString& issues) {
        const TString prefix = "VictimQueryTraceId: ";
        size_t pos = issues.find(prefix);
        if (pos == TString::npos) {
            return std::nullopt;
        }
        pos += prefix.size();
        size_t endPos = issues.find('.', pos);
        if (endPos == TString::npos) {
            return std::nullopt;
        }
        return FromString<ui64>(issues.substr(pos, endPos - pos));
    }

    size_t CountTliRecords(const TString& logs, const TString& component, const TString& messagePattern) {
        size_t count = 0;
        for (const auto& record : ExtractTliRecords(logs)) {
            if (record.Contains("Component: " + component) && MatchesMessage(record, messagePattern)) {
                ++count;
            }
        }
        return count;
    }

    void AssertTliRecordCounts(
        const TString& logs,
        const TTliLogPatterns& patterns,
        size_t expectedBreakerCount,
        size_t expectedVictimCount)
    {
        size_t actualBreakerSessionActorCount = CountTliRecords(logs, "SessionActor", patterns.BreakerSessionActorMessagePattern);
        UNIT_ASSERT_VALUES_EQUAL_C(actualBreakerSessionActorCount, expectedBreakerCount,
            "breaker SessionActor TLI record count mismatch");

        size_t actualVictimSessionActorCount = CountTliRecords(logs, "SessionActor", patterns.VictimSessionActorMessagePattern);
        UNIT_ASSERT_VALUES_EQUAL_C(actualVictimSessionActorCount, expectedVictimCount,
            "victim SessionActor TLI record count mismatch");

        size_t actualBreakerDatashardCount = CountTliRecords(logs, "DataShard", patterns.BreakerDatashardMessage);
        UNIT_ASSERT_VALUES_EQUAL_C(actualBreakerDatashardCount, expectedBreakerCount,
            "breaker DataShard TLI record count mismatch");

        size_t actualVictimDatashardCount = CountTliRecords(logs, "DataShard", patterns.VictimDatashardMessage);
        UNIT_ASSERT_VALUES_EQUAL_C(actualVictimDatashardCount, expectedVictimCount,
            "victim DataShard TLI record count mismatch");
    }

    // Verify TLI issue content
    void VerifyTliIssueContent(const TString& issues) {
        UNIT_ASSERT_C(issues.Contains("Transaction locks invalidated"),
            "Issue should contain 'Transaction locks invalidated': " << issues);
        UNIT_ASSERT_C(!issues.Contains("BreakerQueryTraceId:"),
            "Issue should NOT contain 'BreakerQueryTraceId:': " << issues);

        auto victimQueryTraceId = ExtractVictimQueryTraceIdFromIssue(issues);
        UNIT_ASSERT_C(victimQueryTraceId.has_value(),
            "Issue should contain 'VictimQueryTraceId:': " << issues);
        UNIT_ASSERT_C(*victimQueryTraceId != 0,
            "VictimQueryTraceId should not be 0: " << issues);
    }

    void VerifyTliIssueAndLogs(
        const TString& issues,
        TStringStream& ss,
        const TString& breakerQueryText,
        const TString& victimQueryText,
        const std::optional<TString>& victimExtraQueryText = std::nullopt,
        size_t expectedBreakerCount = 1,
        size_t expectedVictimCount = 1
    )
    {
        DumpTliRecords(ss.Str());

        VerifyTliIssueContent(issues);

        const auto patterns = MakeTliLogPatterns();
        const auto data = ExtractAllTliData(ss.Str(), patterns, breakerQueryText, victimQueryText);
        AssertCommonTliAsserts(data, breakerQueryText, victimQueryText, victimExtraQueryText);

        auto victimQueryTraceId = ExtractVictimQueryTraceIdFromIssue(issues);
        UNIT_ASSERT_C(data.VictimSessionVictimQueryTraceIdOccurrences.has_value(),
            "victim SessionActor VictimQueryTraceId should be present");
        const auto& occurrences = *data.VictimSessionVictimQueryTraceIdOccurrences;
        UNIT_ASSERT_C(std::find(occurrences.begin(), occurrences.end(), *victimQueryTraceId) != occurrences.end(),
            "VictimQueryTraceId should match between issue and victim SessionActor log");

        AssertTliRecordCounts(ss.Str(), patterns, expectedBreakerCount, expectedVictimCount);
    }

    void VerifyTliIssueAndLogsWhenDisabled(
        const TString& issues,
        TStringStream& ss)
    {
        UNIT_ASSERT_C(issues.Contains("Transaction locks invalidated"),
            "Issue should contain 'Transaction locks invalidated': " << issues);

        // BreakerQueryTraceId should NOT be present in the issue
        UNIT_ASSERT_C(!issues.Contains("BreakerQueryTraceId:"),
            "Issue should NOT contain 'BreakerQueryTraceId:': " << issues);

        auto victimQueryTraceId = ExtractVictimQueryTraceIdFromIssue(issues);

        UNIT_ASSERT_C(!victimQueryTraceId.has_value(),
            "Issue should not contain 'VictimQueryTraceId:' when TLI logs are disabled: " << issues);

        UNIT_ASSERT_C(ss.Str().find("TLI INFO") == TString::npos,
            "no TLI INFO logs expected when TLI logs are disabled");
    }

} // namespace

Y_UNIT_TEST_SUITE(KqpTli) {

    Y_UNIT_TEST(LogDisabled) {
        TStringStream ss;
        TTliTestContext ctx(ss, false);
        ctx.CreateTable("/Root/Tenant1/TableLocks");
        ctx.SeedTable("/Root/Tenant1/TableLocks", {{1, "Initial"}});

        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/TableLocks` (Key, Value) VALUES (1u, \"BreakerValue\")";
        const TString victimQueryText = "SELECT * FROM `/Root/Tenant1/TableLocks` WHERE Key = 1u";
        const TString victimCommitText = "UPSERT INTO `/Root/Tenant1/TableLocks` (Key, Value) VALUES (1u, \"VictimValue\")";

        auto victimTx = BeginReadTx(ctx.VictimSession, victimQueryText);
        ctx.ExecuteQuery(breakerQueryText);
        auto [status, issues] = ExecuteVictimCommitWithIssues(ctx.VictimSession, victimTx, victimCommitText);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogsWhenDisabled(issues, ss);
    }

    Y_UNIT_TEST(Basic) {
        TStringStream ss;
        TTliTestContext ctx(ss);
        ctx.CreateTable("/Root/Tenant1/TableLocks");
        ctx.SeedTable("/Root/Tenant1/TableLocks", {{1, "Initial"}});

        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/TableLocks` (Key, Value) VALUES (1u, \"BreakerValue\")";
        const TString victimQueryText = "SELECT * FROM `/Root/Tenant1/TableLocks` WHERE Key = 1u";
        const TString victimCommitText = "UPSERT INTO `/Root/Tenant1/TableLocks` (Key, Value) VALUES (1u, \"VictimValue\")";

        auto victimTx = BeginReadTx(ctx.VictimSession, victimQueryText);
        ctx.ExecuteQuery(breakerQueryText);
        auto [status, issues] = ExecuteVictimCommitWithIssues(ctx.VictimSession, victimTx, victimCommitText);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerQueryText, victimQueryText, victimCommitText);
    }

    Y_UNIT_TEST(SeparateCommit) {
        TStringStream ss;
        TTliTestContext ctx(ss);
        ctx.CreateTable("/Root/Tenant1/TableLocks");
        ctx.SeedTable("/Root/Tenant1/TableLocks", {{1, "Initial"}});

        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/TableLocks` (Key, Value) VALUES (1u, \"BreakerValue\")";
        const TString victimQueryText = "SELECT * FROM `/Root/Tenant1/TableLocks` WHERE Key = 1u";
        const TString victimCommitText = "UPSERT INTO `/Root/Tenant1/TableLocks` (Key, Value) VALUES (1u, \"VictimValue\")";

        auto victimTx = BeginReadTx(ctx.VictimSession, victimQueryText);

        // Breaker: begin tx, write key 1, write key 2, then separate commit
        auto breakerTx = BeginTx(ctx.Session, breakerQueryText);
        NKqp::AssertSuccessResult(ctx.Session.ExecuteDataQuery(
            "UPSERT INTO `/Root/Tenant1/TableLocks` (Key, Value) VALUES (2u, \"UsualValue\")",
            TTxControl::Tx(*breakerTx)).GetValueSync());
        NKqp::AssertSuccessResult(breakerTx->Commit().ExtractValueSync());

        auto [status, issues] = ExecuteVictimCommitWithIssues(ctx.VictimSession, victimTx, victimCommitText);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerQueryText, victimQueryText, victimCommitText);
    }

    // Test: Many upserts in a single transaction, the breaker is the middle upsert
    Y_UNIT_TEST(ManyUpserts) {
        TStringStream ss;
        TTliTestContext ctx(ss);
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
        auto breakerTx = BeginTx(ctx.Session, breakerUpdateTable5);
        NKqp::AssertSuccessResult(ctx.Session.ExecuteDataQuery(breakerUpdateTable2, TTxControl::Tx(*breakerTx)).GetValueSync());
        NKqp::AssertSuccessResult(ctx.Session.ExecuteDataQuery(breakerUpdateTable6, TTxControl::Tx(*breakerTx).CommitTx()).GetValueSync());

        auto [status, issues] = CommitTxWithIssues(victimTx);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerUpdateTable2, victimSelectTable2);
    }

    // Test: Many upserts in a single transaction, the breaker is the middle upsert, separate commit
    Y_UNIT_TEST(ManyUpsertsSeparateCommit) {
        TStringStream ss;
        TTliTestContext ctx(ss);
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

        // Breaker: update tables 5,2,6, then commit separately (breaks victim's lock on table 2)
        auto breakerTx = BeginTx(ctx.Session, breakerUpdateTable5);
        NKqp::AssertSuccessResult(ctx.Session.ExecuteDataQuery(breakerUpdateTable2, TTxControl::Tx(*breakerTx)).GetValueSync());
        NKqp::AssertSuccessResult(ctx.Session.ExecuteDataQuery(breakerUpdateTable6, TTxControl::Tx(*breakerTx).CommitTx()).GetValueSync());

        auto [status, issues] = CommitTxWithIssues(victimTx);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerUpdateTable2, victimSelectTable2);
    }

    // Test: Multi-table writes with standalone COMMIT_TX (TPCC-like scenario)
    // Breaker writes to multiple tables in separate queries, then uses breakerTx->Commit() (QUERY_ACTION_COMMIT_TX)
    // This is different from CommitTx() on the last query (QUERY_ACTION_EXECUTE_PREPARED with commit flag)
    Y_UNIT_TEST(ManyUpsertsStandaloneCommit) {
        TStringStream ss;
        TTliTestContext ctx(ss);
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

        // Breaker: update tables 5,2,6, then standalone COMMIT_TX (no query, just commit)
        auto breakerTx = BeginTx(ctx.Session, breakerUpdateTable5);
        NKqp::AssertSuccessResult(ctx.Session.ExecuteDataQuery(breakerUpdateTable2, TTxControl::Tx(*breakerTx)).GetValueSync());
        NKqp::AssertSuccessResult(ctx.Session.ExecuteDataQuery(breakerUpdateTable6, TTxControl::Tx(*breakerTx)).GetValueSync());
        // Standalone COMMIT_TX (QUERY_ACTION_COMMIT_TX, unlike CommitTx() which is QUERY_ACTION_EXECUTE_PREPARED)
        NKqp::AssertSuccessResult(breakerTx->Commit().ExtractValueSync());

        auto [status, issues] = CommitTxWithIssues(victimTx);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerUpdateTable2, victimSelectTable2);
    }

    // Test: Victim reads key 1, breaker writes key 1, victim writes key 2
    Y_UNIT_TEST(DifferentKeys) {
        TStringStream ss;
        TTliTestContext ctx(ss);
        ctx.CreateTable("/Root/Tenant1/TableDiffKeys");
        ctx.SeedTable("/Root/Tenant1/TableDiffKeys", {{1, "V1"}, {2, "V2"}});

        const TString victimQueryText = "SELECT * FROM `/Root/Tenant1/TableDiffKeys` WHERE Key = 1u";
        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/TableDiffKeys` (Key, Value) VALUES (1u, \"Breaker\")";
        const TString victimCommitText = "UPSERT INTO `/Root/Tenant1/TableDiffKeys` (Key, Value) VALUES (2u, \"VictimWrite\")";

        auto victimTx = BeginReadTx(ctx.VictimSession, victimQueryText);
        ctx.ExecuteQuery(breakerQueryText);
        auto [status, issues] = ExecuteVictimCommitWithIssues(ctx.VictimSession, victimTx, victimCommitText);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerQueryText, victimQueryText);
    }

    // Test: Victim reads multiple keys, breaker writes them all
    Y_UNIT_TEST(MultipleKeys) {
        TStringStream ss;
        TTliTestContext ctx(ss);
        ctx.CreateTable("/Root/Tenant1/TableMulti");
        ctx.SeedTable("/Root/Tenant1/TableMulti", {{1, "V1"}, {2, "V2"}, {3, "V3"}});

        const TString victimQueryText = "SELECT * FROM `/Root/Tenant1/TableMulti` WHERE Key IN (1u, 2u, 3u)";
        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/TableMulti` (Key, Value) VALUES (1u, \"B1\"), (2u, \"B2\"), (3u, \"B3\")";
        const TString victimCommitText = "UPSERT INTO `/Root/Tenant1/TableMulti` (Key, Value) VALUES (1u, \"Victim\")";

        auto victimTx = BeginReadTx(ctx.VictimSession, victimQueryText);
        ctx.ExecuteQuery(breakerQueryText);
        auto [status, issues] = ExecuteVictimCommitWithIssues(ctx.VictimSession, victimTx, victimCommitText);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerQueryText, victimQueryText);
    }

    // Test: Cross-table lock breakage - victim reads TableA, breaker writes TableA, victim writes TableB
    Y_UNIT_TEST(CrossTables) {
        TStringStream ss;
        TTliTestContext ctx(ss);
        ctx.CreateTable("/Root/Tenant1/TableA");
        ctx.CreateTable("/Root/Tenant1/TableB");
        ctx.SeedTable("/Root/Tenant1/TableA", {{1, "ValA"}});

        const TString victimQueryText = "SELECT * FROM `/Root/Tenant1/TableA` WHERE Key = 1u";
        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/TableA` (Key, Value) VALUES (1u, \"Breaker\")";
        const TString victimCommitText = "UPSERT INTO `/Root/Tenant1/TableB` (Key, Value) VALUES (1u, \"DstVal\")";

        auto victimTx = BeginReadTx(ctx.VictimSession, victimQueryText);
        ctx.ExecuteQuery(breakerQueryText);
        auto [status, issues] = ExecuteVictimCommitWithIssues(ctx.VictimSession, victimTx, victimCommitText);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerQueryText, victimQueryText);
    }

    // Test: Two victims on two different tables, one breaker writes to both tables.
    // The breaker's SessionActor should emit two TLI log entries with different BreakerQueryTraceIds,
    // each matching the corresponding DataShard's BreakerQueryTraceId.
    Y_UNIT_TEST(TwoVictimsOneBreaker) {
        TStringStream ss;
        TTliTestContext ctx(ss);

        // Create two victim sessions
        TSession victim1Session = ctx.Client.CreateSession().GetValueSync().GetSession();
        TSession victim2Session = ctx.Client.CreateSession().GetValueSync().GetSession();

        ctx.CreateTable("/Root/Tenant1/TableA");
        ctx.CreateTable("/Root/Tenant1/TableB");
        ctx.SeedTable("/Root/Tenant1/TableA", {{1, "InitA"}});
        ctx.SeedTable("/Root/Tenant1/TableB", {{1, "InitB"}});

        const TString victim1QueryText = "SELECT * FROM `/Root/Tenant1/TableA` WHERE Key = 1u";
        const TString victim2QueryText = "SELECT * FROM `/Root/Tenant1/TableB` WHERE Key = 1u";
        const TString breakerUpdate1 = "UPDATE `/Root/Tenant1/TableA` SET Value = \"BreakerA\" WHERE Key = 1u";
        const TString breakerUpdate2 = "UPDATE `/Root/Tenant1/TableB` SET Value = \"BreakerB\" WHERE Key = 1u";
        const TString victim1CommitText = "UPSERT INTO `/Root/Tenant1/TableA` (Key, Value) VALUES (1u, \"VictimA\")";
        const TString victim2CommitText = "UPSERT INTO `/Root/Tenant1/TableB` (Key, Value) VALUES (1u, \"VictimB\")";

        // Both victims read their respective tables
        auto victim1Tx = BeginReadTx(victim1Session, victim1QueryText);
        auto victim2Tx = BeginReadTx(victim2Session, victim2QueryText);

        // Breaker: write to both tables in a single transaction
        auto breakerTx = BeginTx(ctx.Session, breakerUpdate1);
        NKqp::AssertSuccessResult(ctx.Session.ExecuteDataQuery(breakerUpdate2, TTxControl::Tx(*breakerTx).CommitTx()).GetValueSync());

        // Both victims try to commit - both should be aborted
        auto [status1, issues1] = ExecuteVictimCommitWithIssues(victim1Session, victim1Tx, victim1CommitText);
        UNIT_ASSERT_VALUES_EQUAL(status1, EStatus::ABORTED);

        auto [status2, issues2] = ExecuteVictimCommitWithIssues(victim2Session, victim2Tx, victim2CommitText);
        UNIT_ASSERT_VALUES_EQUAL(status2, EStatus::ABORTED);

        // Verify each victim independently
        VerifyTliIssueAndLogs(issues1, ss, breakerUpdate1, victim1QueryText, victim1CommitText,
            /* expectedBreakerCount */ 2, /* expectedVictimCount */ 2);
        VerifyTliIssueAndLogs(issues2, ss, breakerUpdate2, victim2QueryText, victim2CommitText,
            /* expectedBreakerCount */ 2, /* expectedVictimCount */ 2);
    }

    // Test: InvisibleRowSkips - victim reads at snapshot V1, breaker commits at V2, victim reads again
    Y_UNIT_TEST(InvisibleRowSkips) {
        TStringStream ss;
        TTliTestContext ctx(ss);
        ctx.CreateTable("/Root/Tenant1/TableSkips");
        ctx.SeedTable("/Root/Tenant1/TableSkips", {{1, "Initial"}});

        const TString victimRead1Text = "SELECT * FROM `/Root/Tenant1/TableSkips` WHERE Key = 1u /* victim-read1 */";
        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/TableSkips` (Key, Value) VALUES (1u, \"BreakerV2\")";
        const TString victimRead2Text = "SELECT * FROM `/Root/Tenant1/TableSkips` WHERE Key = 1u /* victim-read2 */";
        const TString victimCommitText = "UPSERT INTO `/Root/Tenant1/TableSkips` (Key, Value) VALUES (1u, \"VictimVal\")";

        // Victim reads key 1 at snapshot V1 - establishes lock
        auto victimTx = BeginReadTx(ctx.VictimSession, victimRead1Text);

        // Breaker writes to key 1 at V2 > V1, breaking victim's lock
        ctx.ExecuteQuery(breakerQueryText);

        // Victim reads key 1 AGAIN - triggers InvisibleRowSkips detection
        {
            auto result = ctx.VictimSession.ExecuteDataQuery(victimRead2Text, TTxControl::Tx(victimTx)).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
            victimTx = *result.GetTransaction();
        }

        // Victim tries to commit -> aborted because lock was broken
        auto [status, issues] = ExecuteVictimCommitWithIssues(ctx.VictimSession, victimTx, victimCommitText);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        // The breaker immediately breaks victim's lock (1 immediate entry)
        // AND the victim re-read detects InvisibleRowSkips (1 deferred entry) = 2 total
        VerifyTliIssueAndLogs(issues, ss, breakerQueryText, victimRead1Text,
            /* victimExtraQueryText */ std::nullopt,
            /* expectedBreakerSessionActorCount */ 2);
    }

    // Test: Victim snapshots on one key, breaker commits, victim reads and writes another key
    Y_UNIT_TEST(SnapshotThenReadWrite) {
        TStringStream ss;
        TTliTestContext ctx(ss);
        ctx.CreateTable("/Root/Tenant1/TableSnapshot");
        ctx.SeedTable("/Root/Tenant1/TableSnapshot", {{1, "V1"}, {2, "V2"}});

        const TString victimSnapshotText = "SELECT * FROM `/Root/Tenant1/TableSnapshot` WHERE Key = 2u /* snapshot */";
        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/TableSnapshot` (Key, Value) VALUES (1u, \"BreakerValue\")";
        const TString victimReadText = "SELECT * FROM `/Root/Tenant1/TableSnapshot` WHERE Key = 1u /* victim-read */";
        const TString victimWriteText = "UPSERT INTO `/Root/Tenant1/TableSnapshot` (Key, Value) VALUES (1u, \"VictimValue\")";

        // Victim: start tx and get snapshot on a different key
        auto victimTx = BeginReadTx(ctx.VictimSession, victimSnapshotText);

        // Breaker: write and commit key 1
        ctx.ExecuteQuery(breakerQueryText);

        // Victim: read the conflicting key after breaker commit
        {
            auto result = ctx.VictimSession.ExecuteDataQuery(victimReadText, TTxControl::Tx(victimTx)).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
            victimTx = *result.GetTransaction();
        }

        // Victim: write the key and try to commit -> should be aborted
        auto [status, issues] = ExecuteVictimCommitWithIssues(ctx.VictimSession, victimTx, victimWriteText);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerQueryText, victimReadText, victimSnapshotText);
    }

    // Test: Deferred lock detection with many queries in both breaker and victim transactions.
    // Like SnapshotThenReadWrite but with several UPSERTs in breaker (only the middle one conflicts)
    // and several SELECTs in victim (only the middle one detects InvisibleRowSkips).
    Y_UNIT_TEST(ManyUpsertsDeferredLock) {
        TStringStream ss;
        TTliTestContext ctx(ss);
        for (int i = 1; i <= 6; ++i) {
            ctx.CreateTable(Sprintf("/Root/Tenant1/Table%d", i));
            ctx.SeedTable(Sprintf("/Root/Tenant1/Table%d", i), {{1, Sprintf("Init%d", i)}, {2, Sprintf("Init%d_2", i)}});
        }

        // Victim queries
        const TString victimSnapshotTable1 = "SELECT * FROM `/Root/Tenant1/Table1` WHERE Key = 2u /* snapshot */";
        const TString victimSelectTable2 = "SELECT * FROM `/Root/Tenant1/Table2` WHERE Key = 1u";
        const TString victimSelectTable3 = "SELECT * FROM `/Root/Tenant1/Table3` WHERE Key = 1u /* victim-read */";
        const TString victimSelectTable4 = "SELECT * FROM `/Root/Tenant1/Table4` WHERE Key = 1u";
        const TString victimWriteTable3 = "UPSERT INTO `/Root/Tenant1/Table3` (Key, Value) VALUES (1u, \"VictimValue\")";

        // Breaker queries
        const TString breakerUpdateTable5 = "UPDATE `/Root/Tenant1/Table5` SET Value = \"BreakerUpdate5\" WHERE Key = 1u";
        const TString breakerUpdateTable3 = "UPDATE `/Root/Tenant1/Table3` SET Value = \"BreakerUpdate3\" WHERE Key = 1u";
        const TString breakerUpdateTable6 = "UPDATE `/Root/Tenant1/Table6` SET Value = \"BreakerUpdate6\" WHERE Key = 1u";

        // Step 1: Victim starts tx with snapshot on a safe key of Table1
        auto victimTx = BeginReadTx(ctx.VictimSession, victimSnapshotTable1);

        // Step 2: Breaker writes to tables 5, 3, 6 and commits (only Table3 key 1 conflicts)
        auto breakerTx = BeginTx(ctx.Session, breakerUpdateTable5);
        NKqp::AssertSuccessResult(ctx.Session.ExecuteDataQuery(breakerUpdateTable3, TTxControl::Tx(*breakerTx)).GetValueSync());
        NKqp::AssertSuccessResult(ctx.Session.ExecuteDataQuery(breakerUpdateTable6, TTxControl::Tx(*breakerTx).CommitTx()).GetValueSync());

        // Step 3: Victim reads tables 2, 3, 4 after breaker committed
        // Only SELECT Table3 key 1 triggers InvisibleRowSkips (deferred detection)
        NKqp::AssertSuccessResult(ctx.VictimSession.ExecuteDataQuery(victimSelectTable2, TTxControl::Tx(victimTx)).GetValueSync());
        {
            auto result = ctx.VictimSession.ExecuteDataQuery(victimSelectTable3, TTxControl::Tx(victimTx)).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
            victimTx = *result.GetTransaction();
        }
        NKqp::AssertSuccessResult(ctx.VictimSession.ExecuteDataQuery(victimSelectTable4, TTxControl::Tx(victimTx)).GetValueSync());

        // Step 4: Victim writes and tries to commit -> should be aborted
        auto [status, issues] = ExecuteVictimCommitWithIssues(ctx.VictimSession, victimTx, victimWriteTable3);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerUpdateTable3, victimSelectTable3, victimSnapshotTable1);
    }

    // Test: Concurrent UPSERT...SELECT transactions - replicates user's production scenario
    // Tests that BreakerQueryTraceId and VictimQueryTraceId linkage is maintained even with
    // OLTP sink + UPSERT...SELECT where locks may be created lazily (deferred lock creation).
    Y_UNIT_TEST(ConcurrentUpsertSelect) {
        TStringStream ss;
        TTliTestContext ctx(ss);
        ctx.CreateTable("/Root/Tenant1/ConcurrentTable");

        // Seed with initial data in the key range 1-10
        for (ui64 i = 1; i <= 10; ++i) {
            ctx.SeedTable("/Root/Tenant1/ConcurrentTable", {{i, Sprintf("Initial%lu", i)}});
        }

        // Victim transaction: UPSERT...SELECT that reads and writes keys 1-5
        const TString victimUpsertSelect = "UPSERT INTO `/Root/Tenant1/ConcurrentTable` (Key, Value) "
                                           "SELECT Key, \"VictimModified\" AS Value FROM `/Root/Tenant1/ConcurrentTable` "
                                           "WHERE Key >= 1u AND Key <= 5u";

        // Breaker transaction: simple UPSERT to key 3 (overlaps with victim's range)
        const TString breakerUpsert = "UPSERT INTO `/Root/Tenant1/ConcurrentTable` (Key, Value) VALUES (3u, \"BreakerValue\")";

        // Victim: start transaction with UPSERT...SELECT (reads keys 1-5, then writes them)
        // Note: with OLTP sink, the lock is NOT created immediately here (deferred lock creation)
        auto victimTx = BeginTx(ctx.VictimSession, victimUpsertSelect);

        // Breaker: write to key 3
        // At this point, victim's lock doesn't exist yet (deferred lock creation)
        // The breaker's write is tracked for later TLI linkage via RecentWritesForTli cache
        ctx.ExecuteQuery(breakerUpsert);

        // Victim: try to commit - should be aborted due to MVCC conflict detection
        auto [status, issues] = CommitTxWithIssues(*victimTx);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        // Verify issue and TLI logs using common verification function
        VerifyTliIssueAndLogs(issues, ss, breakerUpsert, victimUpsertSelect);
    }

    // ==================== 2-Node Tests ====================
    // These tests use a 2-node environment:
    // - Node 0: victim KQP session
    // - Node 1: breaker KQP session
    // This tests that TLI logging works correctly when breaker and victim sessions
    // are on different nodes.

    // Test: 2-node version of ManyUpsertsSeparateCommit
    Y_UNIT_TEST(ManyUpsertsSeparateCommit2Node) {
        TStringStream ss;
        TTli2NodeTestContext ctx(ss);
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
        auto victimTx = BeginReadTx(*ctx.VictimSession, victimSelectTable1);
        NKqp::AssertSuccessResult(ctx.VictimSession->ExecuteDataQuery(victimSelectTable2, TTxControl::Tx(victimTx)).GetValueSync());
        NKqp::AssertSuccessResult(ctx.VictimSession->ExecuteDataQuery(victimSelectTable3, TTxControl::Tx(victimTx)).GetValueSync());
        NKqp::AssertSuccessResult(ctx.VictimSession->ExecuteDataQuery(victimUpdateTable4, TTxControl::Tx(victimTx)).GetValueSync());

        // Breaker: update tables 5,2,6, then commit separately (breaks victim's lock on table 2)
        auto breakerTx = BeginTx(*ctx.BreakerSession, breakerUpdateTable5);
        NKqp::AssertSuccessResult(ctx.BreakerSession->ExecuteDataQuery(breakerUpdateTable2, TTxControl::Tx(*breakerTx)).GetValueSync());
        NKqp::AssertSuccessResult(ctx.BreakerSession->ExecuteDataQuery(breakerUpdateTable6, TTxControl::Tx(*breakerTx).CommitTx()).GetValueSync());

        auto [status, issues] = CommitTxWithIssues(victimTx);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerUpdateTable2, victimSelectTable2);
    }

    // Test: 2-node version of ManyUpsertsStandaloneCommit
    Y_UNIT_TEST(ManyUpsertsStandaloneCommit2Node) {
        TStringStream ss;
        TTli2NodeTestContext ctx(ss);
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
        auto victimTx = BeginReadTx(*ctx.VictimSession, victimSelectTable1);
        NKqp::AssertSuccessResult(ctx.VictimSession->ExecuteDataQuery(victimSelectTable2, TTxControl::Tx(victimTx)).GetValueSync());
        NKqp::AssertSuccessResult(ctx.VictimSession->ExecuteDataQuery(victimSelectTable3, TTxControl::Tx(victimTx)).GetValueSync());
        NKqp::AssertSuccessResult(ctx.VictimSession->ExecuteDataQuery(victimUpdateTable4, TTxControl::Tx(victimTx)).GetValueSync());

        // Breaker: update tables 5,2,6, then standalone COMMIT_TX (no query, just commit)
        auto breakerTx = BeginTx(*ctx.BreakerSession, breakerUpdateTable5);
        NKqp::AssertSuccessResult(ctx.BreakerSession->ExecuteDataQuery(breakerUpdateTable2, TTxControl::Tx(*breakerTx)).GetValueSync());
        NKqp::AssertSuccessResult(ctx.BreakerSession->ExecuteDataQuery(breakerUpdateTable6, TTxControl::Tx(*breakerTx)).GetValueSync());
        // Standalone COMMIT_TX (QUERY_ACTION_COMMIT_TX)
        NKqp::AssertSuccessResult(breakerTx->Commit().ExtractValueSync());

        auto [status, issues] = CommitTxWithIssues(victimTx);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerUpdateTable2, victimSelectTable2);
    }

    // Test: 2-node version of ConcurrentUpsertSelect
    Y_UNIT_TEST(ConcurrentUpsertSelect2Node) {
        TStringStream ss;
        TTli2NodeTestContext ctx(ss);
        ctx.CreateTable("/Root/Tenant1/ConcurrentTable");

        // Seed with initial data in the key range 1-10
        for (ui64 i = 1; i <= 10; ++i) {
            ctx.SeedTable("/Root/Tenant1/ConcurrentTable", {{i, Sprintf("Initial%lu", i)}});
        }

        // Victim transaction: UPSERT...SELECT that reads and writes keys 1-5
        const TString victimUpsertSelect = "UPSERT INTO `/Root/Tenant1/ConcurrentTable` (Key, Value) "
                                           "SELECT Key, \"VictimModified\" AS Value FROM `/Root/Tenant1/ConcurrentTable` "
                                           "WHERE Key >= 1u AND Key <= 5u";

        // Breaker transaction: simple UPSERT to key 3 (overlaps with victim's range)
        const TString breakerUpsert = "UPSERT INTO `/Root/Tenant1/ConcurrentTable` (Key, Value) VALUES (3u, \"BreakerValue\")";

        // Victim: start transaction with UPSERT...SELECT
        auto victimTx = BeginTx(*ctx.VictimSession, victimUpsertSelect);

        // Breaker: write to key 3
        NKqp::AssertSuccessResult(ctx.BreakerSession->ExecuteDataQuery(
            breakerUpsert, TTxControl::BeginTx().CommitTx()).GetValueSync());

        // Victim: try to commit - should be aborted
        auto [status, issues] = CommitTxWithIssues(*victimTx);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        // Verify issue and TLI logs
        VerifyTliIssueAndLogs(issues, ss, breakerUpsert, victimUpsertSelect);
    }
}

} // namespace NKqp
} // namespace NKikimr

