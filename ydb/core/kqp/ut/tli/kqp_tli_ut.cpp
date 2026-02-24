#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/test_tli.h>
#include <ydb/core/protos/data_integrity_trails.pb.h>
#include <ydb/library/actors/wilson/test_util/fake_wilson_uploader.h>

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
using namespace NYdb::NQuery;

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

    std::optional<ui64> ExtractCurrentQuerySpanId(const TString& logs, const TString& component, const TString& messagePattern) {
        for (const auto& record : ExtractTliRecords(logs)) {
            if (!record.Contains("Component: " + component) || !MatchesMessage(record, messagePattern)) {
                continue;
            }
            return ExtractNumericField(record, "CurrentQuerySpanId");
        }
        return std::nullopt;
    }

    std::optional<ui64> ExtractBreakerQuerySpanId(const TString& logs, const TString& component, const TString& messagePattern,
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
            return ExtractNumericField(record, "BreakerQuerySpanId");
        }
        return std::nullopt;
    }

    std::optional<ui64> ExtractVictimQuerySpanId(const TString& logs, const TString& component, const TString& messagePattern,
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
            return ExtractNumericField(record, "VictimQuerySpanId");
        }
        return std::nullopt;
    }

    std::optional<std::vector<ui64>> ExtractVictimQuerySpanIds(const TString& logs, const TString& component, const TString& messagePattern) {
        std::vector<ui64> result;
        bool foundField = false;
        for (const auto& record : ExtractTliRecords(logs)) {
            if (!record.Contains("Component: " + component) || !MatchesMessage(record, messagePattern)) {
                continue;
            }
            static constexpr TStringBuf victimIdsPrefix = "VictimQuerySpanIds: [";
            const size_t idsPos = record.find(victimIdsPrefix);
            if (idsPos == TString::npos) {
                continue;
            }
            foundField = true;
            const size_t listStart = idsPos + victimIdsPrefix.size();
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

    std::optional<std::vector<ui64>> ExtractVictimQuerySpanIdOccurrences(
        const TString& logs,
        const TString& component,
        const TString& messagePattern)
    {
        std::vector<ui64> result;
        for (const auto& record : ExtractTliRecords(logs)) {
            if (!record.Contains("Component: " + component) || !MatchesMessage(record, messagePattern)) {
                continue;
            }
            auto value = ExtractNumericField(record, "VictimQuerySpanId");
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
        std::optional<ui64> BreakerSessionBreakerQuerySpanId;
        std::optional<ui64> BreakerShardBreakerQuerySpanId;
        std::optional<std::vector<ui64>> BreakerShardVictimQuerySpanIds;

        std::optional<ui64> VictimSessionCurrentQuerySpanId;
        std::optional<ui64> VictimShardCurrentQuerySpanId;
        std::optional<ui64> VictimSessionVictimQuerySpanId;
        std::optional<ui64> VictimShardVictimQuerySpanId;
        std::optional<std::vector<ui64>> VictimSessionVictimQuerySpanIdOccurrences;

        bool FoundBreakerRecordInDatashard = false;
        std::optional<std::vector<ui64>> MatchingDsBreakerVictimQuerySpanIds;
        bool FoundVictimRecordInDatashard = false;
    };

    std::pair<bool, std::optional<std::vector<ui64>>> ExtractMatchingFromBreakerDatashard(
        const TString& logs,
        const TString& messagePattern,
        std::optional<ui64> breakerQuerySpanIdFromKQP)
    {
        if (!breakerQuerySpanIdFromKQP) {
            return {false, std::nullopt};
        }

        for (const auto& record : ExtractTliRecords(logs)) {
            if (!record.Contains("Component: DataShard") || !MatchesMessage(record, messagePattern)) {
                continue;
            }
            auto breakerQuerySpanId = ExtractNumericField(record, "BreakerQuerySpanId");
            if (breakerQuerySpanId && *breakerQuerySpanId == *breakerQuerySpanIdFromKQP) {
                std::optional<std::vector<ui64>> matchingVictimIds;
                static constexpr TStringBuf victimIdsPrefix = "VictimQuerySpanIds: [";
                const size_t idsPos = record.find(victimIdsPrefix);
                if (idsPos != TString::npos) {
                    const size_t listStart = idsPos + victimIdsPrefix.size();
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
        std::optional<ui64> victimQuerySpanIdFromKQP)
    {
        if (!victimQuerySpanIdFromKQP) {
            return false;
        }
        for (const auto& record : ExtractTliRecords(logs)) {
            if (!record.Contains("Component: DataShard") || !MatchesMessage(record, messagePattern)) {
                continue;
            }
            auto victimQuerySpanId = ExtractNumericField(record, "VictimQuerySpanId");
            if (victimQuerySpanId && *victimQuerySpanId == *victimQuerySpanIdFromKQP) {
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
        data.BreakerSessionBreakerQuerySpanId = ExtractBreakerQuerySpanId(logs, "SessionActor", patterns.BreakerSessionActorMessagePattern, expectedBreakerQueryText);
        data.BreakerShardBreakerQuerySpanId = ExtractBreakerQuerySpanId(logs, "DataShard", patterns.BreakerDatashardMessage);
        data.BreakerShardVictimQuerySpanIds = ExtractVictimQuerySpanIds(logs, "DataShard", patterns.BreakerDatashardMessage);
        data.VictimSessionCurrentQuerySpanId = ExtractCurrentQuerySpanId(logs, "SessionActor", patterns.VictimSessionActorMessagePattern);
        data.VictimShardCurrentQuerySpanId = ExtractCurrentQuerySpanId(logs, "DataShard", patterns.VictimDatashardMessage);
        data.VictimSessionVictimQuerySpanId = ExtractVictimQuerySpanId(logs, "SessionActor", patterns.VictimSessionActorMessagePattern, expectedVictimQueryText);
        data.VictimShardVictimQuerySpanId = ExtractVictimQuerySpanId(logs, "DataShard", patterns.VictimDatashardMessage);
        data.VictimSessionVictimQuerySpanIdOccurrences = ExtractVictimQuerySpanIdOccurrences(
            logs, "SessionActor", patterns.VictimSessionActorMessagePattern);

        auto [foundBreaker, matchingVictimIds] = ExtractMatchingFromBreakerDatashard(logs, patterns.BreakerDatashardMessage, data.BreakerSessionBreakerQuerySpanId);
        data.FoundBreakerRecordInDatashard = foundBreaker;
        data.MatchingDsBreakerVictimQuerySpanIds = matchingVictimIds;

        data.FoundVictimRecordInDatashard = CheckMatchingInVictimDatashard(logs, patterns.VictimDatashardMessage, data.VictimSessionVictimQuerySpanId);

        return data;
    }

    void AssertCommonTliAsserts(
        const TExtractedTliData& data,
        const TString& breakerQueryText,
        const TString& victimQueryText,
        const std::optional<TString>& victimExtraQueryText = std::nullopt)
    {
        // ==================== QuerySpanId Linkage Assertions ====================

        UNIT_ASSERT_C(data.BreakerSessionBreakerQuerySpanId, "breaker SessionActor BreakerQuerySpanId should be present");
        UNIT_ASSERT_C(data.VictimSessionVictimQuerySpanId, "victim SessionActor VictimQuerySpanId should be present");

        // 1. DS Breaker ↔ KQP Breaker: Find the DataShard breaker record matching the SessionActor's BreakerQuerySpanId
        UNIT_ASSERT_C(data.FoundBreakerRecordInDatashard,
            "SessionActor BreakerQuerySpanId should exist in some DataShard breaker record");

        // 2. DS Victim ↔ KQP Victim: Find the DataShard victim record matching the SessionActor's VictimQuerySpanId
        UNIT_ASSERT_C(data.FoundVictimRecordInDatashard,
            "SessionActor VictimQuerySpanId should exist in some DataShard victim record");

        // 3. DS Breaker ↔ DS Victim: The matching DataShard breaker record should contain the VictimQuerySpanId
        UNIT_ASSERT_C(data.MatchingDsBreakerVictimQuerySpanIds.has_value(), "matching DataShard breaker record should have VictimQuerySpanIds");
        bool victimInBreaker = std::find(data.MatchingDsBreakerVictimQuerySpanIds->begin(), data.MatchingDsBreakerVictimQuerySpanIds->end(),
            *data.VictimSessionVictimQuerySpanId) != data.MatchingDsBreakerVictimQuerySpanIds->end();
        UNIT_ASSERT_C(victimInBreaker,
            "victim VictimQuerySpanId should be in matching DataShard breaker's VictimQuerySpanIds");

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
        settings.LogStream = &ss;
        settings.SetWithSampleTables(false);
        return settings;
    }

    void ConfigureKikimrForTli(TKikimrRunner& kikimr, bool logEnabled = true) {
        if (logEnabled) {
            kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TLI, NLog::PRI_INFO);
        }
    }

    TString NumberedTablePath(int index) {
        return Sprintf("/Root/Tenant1/Table%d", index);
    }

    void CreateTableInSession(TSession& session, const TString& tableName) {
        NKqp::AssertSuccessResult(session.ExecuteQuery(
            Sprintf(R"(CREATE TABLE `%s` (Key Uint64, Value String, PRIMARY KEY (Key));)", tableName.c_str()),
            TTxControl::NoTx()
        ).GetValueSync());
    }

    void SeedTableInSession(TSession& session, const TString& tableName, const TVector<std::pair<ui64, TString>>& rows) {
        for (const auto& [key, value] : rows) {
            NKqp::AssertSuccessResult(session.ExecuteQuery(
                Sprintf("UPSERT INTO `%s` (Key, Value) VALUES (%luu, \"%s\")", tableName.c_str(), key, value.c_str()),
                TTxControl::BeginTx().CommitTx()
            ).GetValueSync());
        }
    }

    void CreateAndSeedTablesInSession(TSession& session, int count) {
        for (int i = 1; i <= count; ++i) {
            const TString tablePath = NumberedTablePath(i);
            CreateTableInSession(session, tablePath);
            SeedTableInSession(session, tablePath, {{1, Sprintf("Init%d", i)}});
        }
    }

    void CreateAndSeedTablesWithSecondKeyInSession(TSession& session, int count) {
        for (int i = 1; i <= count; ++i) {
            const TString tablePath = NumberedTablePath(i);
            CreateTableInSession(session, tablePath);
            SeedTableInSession(session, tablePath, {{1, Sprintf("Init%d", i)}, {2, Sprintf("Init%d_2", i)}});
        }
    }

    // NQuery-based test context (primary API for all tests)
    struct TTliTestContext {
        TKikimrRunner Kikimr;
        TQueryClient Client;
        TSession Session;
        TSession VictimSession;

        TTliTestContext(TStringStream& ss, bool logEnabled = true)
            : Kikimr(MakeKikimrSettings(ss))
            , Client(Kikimr.GetQueryClient())
            , Session(Client.GetSession().GetValueSync().GetSession())
            , VictimSession(Client.GetSession().GetValueSync().GetSession())
        {
            ConfigureKikimrForTli(Kikimr, logEnabled);
        }

        void CreateTable(const TString& tableName) {
            CreateTableInSession(Session, tableName);
        }

        void SeedTable(const TString& tableName, const TVector<std::pair<ui64, TString>>& rows) {
            SeedTableInSession(Session, tableName, rows);
        }

        void CreateAndSeedTables(int count) {
            CreateAndSeedTablesInSession(Session, count);
        }

        void CreateAndSeedTablesWithSecondKey(int count) {
            CreateAndSeedTablesWithSecondKeyInSession(Session, count);
        }

        void ExecuteQuery(const TString& query) {
            NKqp::AssertSuccessResult(Session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync());
        }
    };

    // 2-node test context: victim session on node 0, breaker session on node 1.
    // Logs from both nodes are captured via per-node log backends.
    struct TTli2NodeTestContext {
        TKikimrRunner Kikimr;
        std::unique_ptr<NYdb::TDriver> VictimDriver;
        std::unique_ptr<NYdb::TDriver> BreakerDriver;
        std::unique_ptr<TQueryClient> VictimClient;
        std::unique_ptr<TQueryClient> BreakerClient;
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

            VictimClient = std::make_unique<TQueryClient>(*VictimDriver);
            BreakerClient = std::make_unique<TQueryClient>(*BreakerDriver);

            VictimSession = VictimClient->GetSession().GetValueSync().GetSession();
            BreakerSession = BreakerClient->GetSession().GetValueSync().GetSession();
        }

        void CreateTable(const TString& tableName) {
            CreateTableInSession(*BreakerSession, tableName);
        }

        void SeedTable(const TString& tableName, const TVector<std::pair<ui64, TString>>& rows) {
            SeedTableInSession(*BreakerSession, tableName, rows);
        }

        void CreateAndSeedTables(int count) {
            CreateAndSeedTablesInSession(*BreakerSession, count);
        }

        void CreateAndSeedTablesWithSecondKey(int count) {
            CreateAndSeedTablesWithSecondKeyInSession(*BreakerSession, count);
        }
    };

    // ==================== NQuery transaction helpers ====================

    TTransaction BeginReadTx(TSession& session, const TString& queryText) {
        while (true) {
            auto result = session.ExecuteQuery(queryText, TTxControl::BeginTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            if (FormatResultSetYson(result.GetResultSet(0)) != "[]") {
                auto tx = result.GetTransaction();
                UNIT_ASSERT(tx);
                return *tx;
            }
        }
    }

    std::optional<TTransaction> BeginTx(TSession& session, const TString& query) {
        auto result = session.ExecuteQuery(query, TTxControl::BeginTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        return result.GetTransaction();
    }

    void ExecuteInTx(TSession& session, TTransaction& tx, const TString& query) {
        auto result = session.ExecuteQuery(query, TTxControl::Tx(tx)).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    void ExecuteAndCommitTx(TSession& session, TTransaction& tx, const TString& query) {
        auto result = session.ExecuteQuery(query, TTxControl::Tx(tx).CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    // Execute a query in tx and reassign the transaction from the result
    // (needed when the test must track the updated transaction object, e.g. InvisibleRowSkips)
    void ExecuteInTxReassign(TSession& session, TTransaction& tx, const TString& query) {
        auto result = session.ExecuteQuery(query, TTxControl::Tx(tx)).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        tx = *result.GetTransaction();
    }

    // Execute victim commit and return both status and issues for verification
    std::pair<EStatus, TString> ExecuteVictimCommitWithIssues(TSession& session, TTransaction& tx, const TString& query) {
        auto result = session.ExecuteQuery(query, TTxControl::Tx(tx).CommitTx()).GetValueSync();
        return {result.GetStatus(), result.GetIssues().ToString()};
    }

    // Execute a direct commit (no query) and return both status and issues
    std::pair<EStatus, TString> CommitTxWithIssues(TTransaction& tx) {
        auto result = tx.Commit().GetValueSync();
        return {result.GetStatus(), result.GetIssues().ToString()};
    }

    // Extract VictimQuerySpanId from issue message
    std::optional<ui64> ExtractVictimQuerySpanIdFromIssue(const TString& issues) {
        const TString prefix = "VictimQuerySpanId: ";
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
        UNIT_ASSERT_C(!issues.Contains("BreakerQuerySpanId:"),
            "Issue should NOT contain 'BreakerQuerySpanId:': " << issues);

        auto victimQuerySpanId = ExtractVictimQuerySpanIdFromIssue(issues);
        UNIT_ASSERT_C(victimQuerySpanId.has_value(),
            "Issue should contain 'VictimQuerySpanId:': " << issues);
        UNIT_ASSERT_C(*victimQuerySpanId != 0,
            "VictimQuerySpanId should not be 0: " << issues);
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

        auto victimQuerySpanId = ExtractVictimQuerySpanIdFromIssue(issues);
        UNIT_ASSERT_C(data.VictimSessionVictimQuerySpanIdOccurrences.has_value(),
            "victim SessionActor VictimQuerySpanId should be present");
        const auto& occurrences = *data.VictimSessionVictimQuerySpanIdOccurrences;
        UNIT_ASSERT_C(std::find(occurrences.begin(), occurrences.end(), *victimQuerySpanId) != occurrences.end(),
            "VictimQuerySpanId should match between issue and victim SessionActor log");

        AssertTliRecordCounts(ss.Str(), patterns, expectedBreakerCount, expectedVictimCount);
    }

    void VerifyTliIssueAndLogsWhenDisabled(
        const TString& issues,
        TStringStream& ss)
    {
        UNIT_ASSERT_C(issues.Contains("Transaction locks invalidated"),
            "Issue should contain 'Transaction locks invalidated': " << issues);

        // BreakerQuerySpanId should NOT be present in the issue
        UNIT_ASSERT_C(!issues.Contains("BreakerQuerySpanId:"),
            "Issue should NOT contain 'BreakerQuerySpanId:': " << issues);

        auto victimQuerySpanId = ExtractVictimQuerySpanIdFromIssue(issues);

        UNIT_ASSERT_C(!victimQuerySpanId.has_value(),
            "Issue should not contain 'VictimQuerySpanId:' when TLI logs are disabled: " << issues);

        UNIT_ASSERT_C(ss.Str().find("TLI INFO") == TString::npos,
            "no TLI INFO logs expected when TLI logs are disabled");
    }

    // ==================== DataQuery backward-compatibility helpers ====================

    namespace NDataQueryCompat {
        NYdb::NTable::TTransaction BeginReadTx(NYdb::NTable::TSession& session, const TString& queryText) {
            while (true) {
                auto result = session.ExecuteDataQuery(queryText, NYdb::NTable::TTxControl::BeginTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
                if (FormatResultSetYson(result.GetResultSet(0)) != "[]") {
                    auto tx = result.GetTransaction();
                    UNIT_ASSERT(tx);
                    return *tx;
                }
            }
        }

        std::optional<NYdb::NTable::TTransaction> BeginTx(NYdb::NTable::TSession& session, const TString& query) {
            auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
            return result.GetTransaction();
        }

        std::pair<EStatus, TString> ExecuteVictimCommitWithIssues(NYdb::NTable::TSession& session, NYdb::NTable::TTransaction& tx, const TString& query) {
            auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::Tx(tx).CommitTx()).ExtractValueSync();
            return {result.GetStatus(), result.GetIssues().ToString()};
        }
    } // namespace NDataQueryCompat

} // namespace

Y_UNIT_TEST_SUITE(KqpTli) {

    Y_UNIT_TEST(LogDisabled) {
        TStringStream ss;
        TTliTestContext ctx(ss, false);
        ctx.CreateAndSeedTables(1);

        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"BreakerValue\")";
        const TString victimQueryText = "SELECT * FROM `/Root/Tenant1/Table1` WHERE Key = 1u";
        const TString victimCommitText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"VictimValue\")";

        auto victimTx = BeginReadTx(ctx.VictimSession, victimQueryText);
        ctx.ExecuteQuery(breakerQueryText);
        auto [status, issues] = ExecuteVictimCommitWithIssues(ctx.VictimSession, victimTx, victimCommitText);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogsWhenDisabled(issues, ss);
    }

    Y_UNIT_TEST(Basic) {
        TStringStream ss;
        TTliTestContext ctx(ss);
        ctx.CreateAndSeedTables(1);

        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"BreakerValue\")";
        const TString victimQueryText = "SELECT * FROM `/Root/Tenant1/Table1` WHERE Key = 1u";
        const TString victimCommitText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"VictimValue\")";

        auto victimTx = BeginReadTx(ctx.VictimSession, victimQueryText);
        ctx.ExecuteQuery(breakerQueryText);
        auto [status, issues] = ExecuteVictimCommitWithIssues(ctx.VictimSession, victimTx, victimCommitText);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerQueryText, victimQueryText, victimCommitText);
    }

    Y_UNIT_TEST(SeparateCommit) {
        TStringStream ss;
        TTliTestContext ctx(ss);
        ctx.CreateAndSeedTables(1);

        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"BreakerValue\")";
        const TString breakerQueryText2 = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (2u, \"UsualValue\")";
        const TString victimQueryText = "SELECT * FROM `/Root/Tenant1/Table1` WHERE Key = 1u";
        const TString victimCommitText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"VictimValue\")";

        auto victimTx = BeginReadTx(ctx.VictimSession, victimQueryText);

        // Breaker: begin tx, write key 1, write key 2, then separate commit
        auto breakerTx = BeginTx(ctx.Session, breakerQueryText);
        ExecuteInTx(ctx.Session, *breakerTx, breakerQueryText2);
        NKqp::AssertSuccessResult(breakerTx->Commit().GetValueSync());

        auto [status, issues] = ExecuteVictimCommitWithIssues(ctx.VictimSession, victimTx, victimCommitText);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerQueryText, victimQueryText, victimCommitText);
    }

    // ALL writes go to the SAME table (same shard), and the
    // actual lock-breaking key (Key=1) is written by a query in the MIDDLE, surrounded
    // by non-conflicting writes to the same shard before AND after it.
    Y_UNIT_TEST(SeparateCommitBreakerInMiddleOfSameShard) {
        TStringStream ss;
        TTliTestContext ctx(ss);
        ctx.CreateAndSeedTables(1);

        const TString victimQueryText = "SELECT * FROM `/Root/Tenant1/Table1` WHERE Key = 1u";
        const TString victimCommitText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"VictimValue\")";

        // All writes go to the SAME table (same shard). The actual breaker (Key=1)
        // is in the middle of the sequence.
        const TString breakerBefore1 = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (10u, \"Before1\")";
        const TString breakerBefore2 = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (20u, \"Before2\")";
        const TString breakerBefore3 = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (30u, \"Before3\")";
        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"BreakerValue\")";
        const TString breakerAfter1 = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (40u, \"After1\")";
        const TString breakerAfter2 = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (50u, \"After2\")";
        const TString breakerAfter3 = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (60u, \"After3\")";

        // Victim: read Key=1 (creates a lock)
        auto victimTx = BeginReadTx(ctx.VictimSession, victimQueryText);

        // Breaker: 7 queries all to the same table, Key=1 (the breaker) is Q4.
        auto breakerTx = BeginTx(ctx.Session, breakerBefore1);                    // Q1: Key=10
        ExecuteInTx(ctx.Session, *breakerTx, breakerBefore2);                     // Q2: Key=20
        ExecuteInTx(ctx.Session, *breakerTx, breakerBefore3);                     // Q3: Key=30
        ExecuteInTx(ctx.Session, *breakerTx, breakerQueryText);                   // Q4: Key=1 (BREAKER)
        ExecuteInTx(ctx.Session, *breakerTx, breakerAfter1);                      // Q5: Key=40
        ExecuteInTx(ctx.Session, *breakerTx, breakerAfter2);                      // Q6: Key=50
        ExecuteInTx(ctx.Session, *breakerTx, breakerAfter3);                      // Q7: Key=60
        NKqp::AssertSuccessResult(breakerTx->Commit().GetValueSync());             // Standalone commit

        auto [status, issues] = ExecuteVictimCommitWithIssues(ctx.VictimSession, victimTx, victimCommitText);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerQueryText, victimQueryText, victimCommitText);
    }

    // Test: Many upserts in a single transaction, the breaker is the middle upsert
    Y_UNIT_TEST(ManyUpserts) {
        TStringStream ss;
        TTliTestContext ctx(ss);
        ctx.CreateAndSeedTables(6);

        const TString victimSelectTable1 = "SELECT * FROM `/Root/Tenant1/Table1` WHERE Key = 1u";
        const TString victimSelectTable2 = "SELECT * FROM `/Root/Tenant1/Table2` WHERE Key = 1u";
        const TString victimSelectTable3 = "SELECT * FROM `/Root/Tenant1/Table3` WHERE Key = 1u";
        const TString victimUpdateTable4 = "UPDATE `/Root/Tenant1/Table4` SET Value = \"VictimUpdate\" WHERE Key = 1u";
        const TString breakerUpdateTable2 = "UPDATE `/Root/Tenant1/Table2` SET Value = \"BreakerUpdate2\" WHERE Key = 1u";
        const TString breakerUpdateTable5 = "UPDATE `/Root/Tenant1/Table5` SET Value = \"BreakerUpdate5\" WHERE Key = 1u";
        const TString breakerUpdateTable6 = "UPDATE `/Root/Tenant1/Table6` SET Value = \"BreakerUpdate6\" WHERE Key = 1u";

        // Victim: read tables 1,2,3, then update table 4 (without commit)
        auto victimTx = BeginReadTx(ctx.VictimSession, victimSelectTable1);
        ExecuteInTx(ctx.VictimSession, victimTx, victimSelectTable2);
        ExecuteInTx(ctx.VictimSession, victimTx, victimSelectTable3);
        ExecuteInTx(ctx.VictimSession, victimTx, victimUpdateTable4);

        // Breaker: update tables 5,2,6, then commit (breaks victim's lock on table 2)
        auto breakerTx = BeginTx(ctx.Session, breakerUpdateTable5);
        ExecuteInTx(ctx.Session, *breakerTx, breakerUpdateTable2);
        ExecuteAndCommitTx(ctx.Session, *breakerTx, breakerUpdateTable6);

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
        ctx.CreateAndSeedTables(6);

        const TString victimSelectTable1 = "SELECT * FROM `/Root/Tenant1/Table1` WHERE Key = 1u";
        const TString victimSelectTable2 = "SELECT * FROM `/Root/Tenant1/Table2` WHERE Key = 1u";
        const TString victimSelectTable3 = "SELECT * FROM `/Root/Tenant1/Table3` WHERE Key = 1u";
        const TString victimUpdateTable4 = "UPDATE `/Root/Tenant1/Table4` SET Value = \"VictimUpdate\" WHERE Key = 1u";
        const TString breakerUpdateTable2 = "UPDATE `/Root/Tenant1/Table2` SET Value = \"BreakerUpdate2\" WHERE Key = 1u";
        const TString breakerUpdateTable5 = "UPDATE `/Root/Tenant1/Table5` SET Value = \"BreakerUpdate5\" WHERE Key = 1u";
        const TString breakerUpdateTable6 = "UPDATE `/Root/Tenant1/Table6` SET Value = \"BreakerUpdate6\" WHERE Key = 1u";

        // Victim: read tables 1,2,3, then update table 4 (without commit)
        auto victimTx = BeginReadTx(ctx.VictimSession, victimSelectTable1);
        ExecuteInTx(ctx.VictimSession, victimTx, victimSelectTable2);
        ExecuteInTx(ctx.VictimSession, victimTx, victimSelectTable3);
        ExecuteInTx(ctx.VictimSession, victimTx, victimUpdateTable4);

        // Breaker: update tables 5,2,6, then standalone COMMIT_TX (no query, just commit)
        auto breakerTx = BeginTx(ctx.Session, breakerUpdateTable5);
        ExecuteInTx(ctx.Session, *breakerTx, breakerUpdateTable2);
        ExecuteInTx(ctx.Session, *breakerTx, breakerUpdateTable6);
        // Standalone COMMIT_TX (QUERY_ACTION_COMMIT_TX, unlike CommitTx() which is QUERY_ACTION_EXECUTE with commit flag)
        NKqp::AssertSuccessResult(breakerTx->Commit().GetValueSync());

        auto [status, issues] = CommitTxWithIssues(victimTx);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerUpdateTable2, victimSelectTable2);
    }

    // Test: Victim reads key 1, breaker writes key 1, victim writes key 2
    Y_UNIT_TEST(DifferentKeys) {
        TStringStream ss;
        TTliTestContext ctx(ss);
        ctx.CreateAndSeedTablesWithSecondKey(1);

        const TString victimQueryText = "SELECT * FROM `/Root/Tenant1/Table1` WHERE Key = 1u";
        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"Breaker\")";
        const TString victimCommitText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (2u, \"VictimWrite\")";

        auto victimTx = BeginReadTx(ctx.VictimSession, victimQueryText);
        ctx.ExecuteQuery(breakerQueryText);
        auto [status, issues] = ExecuteVictimCommitWithIssues(ctx.VictimSession, victimTx, victimCommitText);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerQueryText, victimQueryText);
    }

    // Test: Victim reads and writes the same table before breaker commits.
    // Verifies that VictimQuerySpanId correctly identifies the read operation
    // (which established the lock), not the subsequent write within the same transaction.
    Y_UNIT_TEST(VictimReadThenWriteSameTable) {
        TStringStream ss;
        TTliTestContext ctx(ss);
        ctx.CreateAndSeedTablesWithSecondKey(1);

        const TString victimQueryText = "SELECT * FROM `/Root/Tenant1/Table1` WHERE Key = 1u";
        const TString victimWriteText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (2u, \"VictimWrite\")";
        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"BreakerValue\")";

        auto victimTx = BeginReadTx(ctx.VictimSession, victimQueryText);
        ExecuteInTx(ctx.VictimSession, victimTx, victimWriteText);

        ctx.ExecuteQuery(breakerQueryText);

        auto [status, issues] = CommitTxWithIssues(victimTx);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerQueryText, victimQueryText, victimWriteText);
    }

    // Test: Multi-table scenario where victim reads and writes the same table,
    // plus reads/writes other tables. Simulates TPCC-like workload where a transaction
    // SELECTs and then UPDATEs the same row (e.g., customer table).
    Y_UNIT_TEST(VictimReadThenWriteSameTableMultiTable) {
        TStringStream ss;
        TTliTestContext ctx(ss);
        ctx.CreateAndSeedTables(3);

        const TString victimSelectTable1 = "SELECT * FROM `/Root/Tenant1/Table1` WHERE Key = 1u";
        const TString victimSelectTable2 = "SELECT * FROM `/Root/Tenant1/Table2` WHERE Key = 1u";
        const TString victimUpdateTable1 = "UPDATE `/Root/Tenant1/Table1` SET Value = \"VictimUpdate\" WHERE Key = 1u";
        const TString victimUpdateTable3 = "UPDATE `/Root/Tenant1/Table3` SET Value = \"VictimUpdate3\" WHERE Key = 1u";
        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"BreakerValue\")";

        auto victimTx = BeginReadTx(ctx.VictimSession, victimSelectTable1);
        ExecuteInTx(ctx.VictimSession, victimTx, victimSelectTable2);
        ExecuteInTx(ctx.VictimSession, victimTx, victimUpdateTable1);
        ExecuteInTx(ctx.VictimSession, victimTx, victimUpdateTable3);

        ctx.ExecuteQuery(breakerQueryText);

        auto [status, issues] = CommitTxWithIssues(victimTx);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerQueryText, victimSelectTable1, victimUpdateTable1,
            /* expectedBreakerCount */ 2);
    }

    // Test: Victim reads multiple keys, breaker writes them all
    Y_UNIT_TEST(MultipleKeys) {
        TStringStream ss;
        TTliTestContext ctx(ss);
        ctx.CreateAndSeedTablesWithSecondKey(1);
        ctx.SeedTable("/Root/Tenant1/Table1", {{3, "V3"}});

        const TString victimQueryText = "SELECT * FROM `/Root/Tenant1/Table1` WHERE Key IN (1u, 2u, 3u)";
        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"B1\"), (2u, \"B2\"), (3u, \"B3\")";
        const TString victimCommitText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"Victim\")";

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
        ctx.CreateAndSeedTables(2);

        const TString victimQueryText = "SELECT * FROM `/Root/Tenant1/Table1` WHERE Key = 1u";
        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"Breaker\")";
        const TString victimCommitText = "UPSERT INTO `/Root/Tenant1/Table2` (Key, Value) VALUES (1u, \"DstVal\")";

        auto victimTx = BeginReadTx(ctx.VictimSession, victimQueryText);
        ctx.ExecuteQuery(breakerQueryText);
        auto [status, issues] = ExecuteVictimCommitWithIssues(ctx.VictimSession, victimTx, victimCommitText);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerQueryText, victimQueryText);
    }

    // Test: Two victims on two different tables, one breaker writes to both tables.
    // The breaker's SessionActor should emit two TLI log entries with different BreakerQuerySpanIds,
    // each matching the corresponding DataShard's BreakerQuerySpanId.
    Y_UNIT_TEST(TwoVictimsOneBreaker) {
        TStringStream ss;
        TTliTestContext ctx(ss);

        // Create two victim sessions
        TSession victim1Session = ctx.Client.GetSession().GetValueSync().GetSession();
        TSession victim2Session = ctx.Client.GetSession().GetValueSync().GetSession();

        ctx.CreateAndSeedTables(2);

        const TString victim1QueryText = "SELECT * FROM `/Root/Tenant1/Table1` WHERE Key = 1u";
        const TString victim2QueryText = "SELECT * FROM `/Root/Tenant1/Table2` WHERE Key = 1u";
        const TString breakerUpdate1 = "UPDATE `/Root/Tenant1/Table1` SET Value = \"BreakerA\" WHERE Key = 1u";
        const TString breakerUpdate2 = "UPDATE `/Root/Tenant1/Table2` SET Value = \"BreakerB\" WHERE Key = 1u";
        const TString victim1CommitText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"VictimA\")";
        const TString victim2CommitText = "UPSERT INTO `/Root/Tenant1/Table2` (Key, Value) VALUES (1u, \"VictimB\")";

        // Both victims read their respective tables
        auto victim1Tx = BeginReadTx(victim1Session, victim1QueryText);
        auto victim2Tx = BeginReadTx(victim2Session, victim2QueryText);

        // Breaker: write to both tables in a single transaction
        auto breakerTx = BeginTx(ctx.Session, breakerUpdate1);
        ExecuteAndCommitTx(ctx.Session, *breakerTx, breakerUpdate2);

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
        ctx.CreateAndSeedTables(1);

        const TString victimRead1Text = "SELECT * FROM `/Root/Tenant1/Table1` WHERE Key = 1u /* victim-read1 */";
        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"BreakerV2\")";
        const TString victimRead2Text = "SELECT * FROM `/Root/Tenant1/Table1` WHERE Key = 1u /* victim-read2 */";
        const TString victimCommitText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"VictimVal\")";

        // Victim reads key 1 at snapshot V1 - establishes lock
        auto victimTx = BeginReadTx(ctx.VictimSession, victimRead1Text);

        // Breaker writes to key 1 at V2 > V1, breaking victim's lock
        ctx.ExecuteQuery(breakerQueryText);

        // Victim reads key 1 AGAIN - triggers InvisibleRowSkips detection
        ExecuteInTxReassign(ctx.VictimSession, victimTx, victimRead2Text);

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
        ctx.CreateAndSeedTablesWithSecondKey(1);

        const TString victimSnapshotText = "SELECT * FROM `/Root/Tenant1/Table1` WHERE Key = 2u /* snapshot */";
        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"BreakerValue\")";
        const TString victimReadText = "SELECT * FROM `/Root/Tenant1/Table1` WHERE Key = 1u /* victim-read */";
        const TString victimWriteText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"VictimValue\")";

        // Victim: start tx and get snapshot on a different key
        auto victimTx = BeginReadTx(ctx.VictimSession, victimSnapshotText);

        // Breaker: write and commit key 1
        ctx.ExecuteQuery(breakerQueryText);

        // Victim: read the conflicting key after breaker commit
        ExecuteInTxReassign(ctx.VictimSession, victimTx, victimReadText);

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
        // Note: key 2 is needed for snapshot on Table1 in this scenario.
        ctx.CreateAndSeedTablesWithSecondKey(6);

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
        ExecuteInTx(ctx.Session, *breakerTx, breakerUpdateTable3);
        ExecuteAndCommitTx(ctx.Session, *breakerTx, breakerUpdateTable6);

        // Step 3: Victim reads tables 2, 3, 4 after breaker committed
        // Only SELECT Table3 key 1 triggers InvisibleRowSkips (deferred detection)
        ExecuteInTx(ctx.VictimSession, victimTx, victimSelectTable2);
        ExecuteInTxReassign(ctx.VictimSession, victimTx, victimSelectTable3);
        ExecuteInTx(ctx.VictimSession, victimTx, victimSelectTable4);

        // Step 4: Victim writes and tries to commit -> should be aborted
        auto [status, issues] = ExecuteVictimCommitWithIssues(ctx.VictimSession, victimTx, victimWriteTable3);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerUpdateTable3, victimSelectTable3, victimSnapshotTable1);
    }

    // Test: Concurrent UPSERT...SELECT transactions - replicates user's production scenario
    // Tests that BreakerQuerySpanId and VictimQuerySpanId linkage is maintained even with
    // OLTP sink + UPSERT...SELECT where locks may be created lazily (deferred lock creation).
    Y_UNIT_TEST(ConcurrentUpsertSelect) {
        TStringStream ss;
        TTliTestContext ctx(ss);
        ctx.CreateAndSeedTables(1);

        // Seed with initial data in the key range 1-10
        for (ui64 i = 2; i <= 10; ++i) {
            ctx.SeedTable("/Root/Tenant1/Table1", {{i, Sprintf("Initial%lu", i)}});
        }

        // Victim transaction: UPSERT...SELECT that reads and writes keys 1-5
        const TString victimUpsertSelect = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) "
                                           "SELECT Key, \"VictimModified\" AS Value FROM `/Root/Tenant1/Table1` "
                                           "WHERE Key >= 1u AND Key <= 5u";

        // Breaker transaction: simple UPSERT to key 3 (overlaps with victim's range)
        const TString breakerUpsert = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (3u, \"BreakerValue\")";

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

    // Test: 2-node version of ManyUpserts
    Y_UNIT_TEST(ManyUpserts2Node) {
        TStringStream ss;
        TTli2NodeTestContext ctx(ss);
        ctx.CreateAndSeedTables(6);

        const TString victimSelectTable1 = "SELECT * FROM `/Root/Tenant1/Table1` WHERE Key = 1u";
        const TString victimSelectTable2 = "SELECT * FROM `/Root/Tenant1/Table2` WHERE Key = 1u";
        const TString victimSelectTable3 = "SELECT * FROM `/Root/Tenant1/Table3` WHERE Key = 1u";
        const TString victimUpdateTable4 = "UPDATE `/Root/Tenant1/Table4` SET Value = \"VictimUpdate\" WHERE Key = 1u";
        const TString breakerUpdateTable2 = "UPDATE `/Root/Tenant1/Table2` SET Value = \"BreakerUpdate2\" WHERE Key = 1u";
        const TString breakerUpdateTable5 = "UPDATE `/Root/Tenant1/Table5` SET Value = \"BreakerUpdate5\" WHERE Key = 1u";
        const TString breakerUpdateTable6 = "UPDATE `/Root/Tenant1/Table6` SET Value = \"BreakerUpdate6\" WHERE Key = 1u";

        // Victim: read tables 1,2,3, then update table 4 (without commit)
        auto victimTx = BeginReadTx(*ctx.VictimSession, victimSelectTable1);
        ExecuteInTx(*ctx.VictimSession, victimTx, victimSelectTable2);
        ExecuteInTx(*ctx.VictimSession, victimTx, victimSelectTable3);
        ExecuteInTx(*ctx.VictimSession, victimTx, victimUpdateTable4);

        // Breaker: update tables 5,2,6, then commit separately (breaks victim's lock on table 2)
        auto breakerTx = BeginTx(*ctx.BreakerSession, breakerUpdateTable5);
        ExecuteInTx(*ctx.BreakerSession, *breakerTx, breakerUpdateTable2);
        ExecuteAndCommitTx(*ctx.BreakerSession, *breakerTx, breakerUpdateTable6);

        auto [status, issues] = CommitTxWithIssues(victimTx);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerUpdateTable2, victimSelectTable2);
    }

    // Test: 2-node version of ManyUpsertsStandaloneCommit
    Y_UNIT_TEST(ManyUpsertsStandaloneCommit2Node) {
        TStringStream ss;
        TTli2NodeTestContext ctx(ss);
        ctx.CreateAndSeedTables(6);

        const TString victimSelectTable1 = "SELECT * FROM `/Root/Tenant1/Table1` WHERE Key = 1u";
        const TString victimSelectTable2 = "SELECT * FROM `/Root/Tenant1/Table2` WHERE Key = 1u";
        const TString victimSelectTable3 = "SELECT * FROM `/Root/Tenant1/Table3` WHERE Key = 1u";
        const TString victimUpdateTable4 = "UPDATE `/Root/Tenant1/Table4` SET Value = \"VictimUpdate\" WHERE Key = 1u";
        const TString breakerUpdateTable2 = "UPDATE `/Root/Tenant1/Table2` SET Value = \"BreakerUpdate2\" WHERE Key = 1u";
        const TString breakerUpdateTable5 = "UPDATE `/Root/Tenant1/Table5` SET Value = \"BreakerUpdate5\" WHERE Key = 1u";
        const TString breakerUpdateTable6 = "UPDATE `/Root/Tenant1/Table6` SET Value = \"BreakerUpdate6\" WHERE Key = 1u";

        // Victim: read tables 1,2,3, then update table 4 (without commit)
        auto victimTx = BeginReadTx(*ctx.VictimSession, victimSelectTable1);
        ExecuteInTx(*ctx.VictimSession, victimTx, victimSelectTable2);
        ExecuteInTx(*ctx.VictimSession, victimTx, victimSelectTable3);
        ExecuteInTx(*ctx.VictimSession, victimTx, victimUpdateTable4);

        // Breaker: update tables 5,2,6, then standalone COMMIT_TX (no query, just commit)
        auto breakerTx = BeginTx(*ctx.BreakerSession, breakerUpdateTable5);
        ExecuteInTx(*ctx.BreakerSession, *breakerTx, breakerUpdateTable2);
        ExecuteInTx(*ctx.BreakerSession, *breakerTx, breakerUpdateTable6);
        // Standalone COMMIT_TX (QUERY_ACTION_COMMIT_TX)
        NKqp::AssertSuccessResult(breakerTx->Commit().GetValueSync());

        auto [status, issues] = CommitTxWithIssues(victimTx);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerUpdateTable2, victimSelectTable2);
    }

    // Test: 2-node version of ConcurrentUpsertSelect
    Y_UNIT_TEST(ConcurrentUpsertSelect2Node) {
        TStringStream ss;
        TTli2NodeTestContext ctx(ss);
        ctx.CreateAndSeedTables(1);

        // Seed with initial data in the key range 1-10
        for (ui64 i = 2; i <= 10; ++i) {
            ctx.SeedTable("/Root/Tenant1/Table1", {{i, Sprintf("Initial%lu", i)}});
        }

        // Victim transaction: UPSERT...SELECT that reads and writes keys 1-5
        const TString victimUpsertSelect = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) "
                                           "SELECT Key, \"VictimModified\" AS Value FROM `/Root/Tenant1/Table1` "
                                           "WHERE Key >= 1u AND Key <= 5u";

        // Breaker transaction: simple UPSERT to key 3 (overlaps with victim's range)
        const TString breakerUpsert = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (3u, \"BreakerValue\")";

        // Victim: start transaction with UPSERT...SELECT
        auto victimTx = BeginTx(*ctx.VictimSession, victimUpsertSelect);

        // Breaker: write to key 3
        NKqp::AssertSuccessResult(ctx.BreakerSession->ExecuteQuery(
            breakerUpsert, TTxControl::BeginTx().CommitTx()).GetValueSync());

        // Victim: try to commit - should be aborted
        auto [status, issues] = CommitTxWithIssues(*victimTx);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        // Verify issue and TLI logs
        VerifyTliIssueAndLogs(issues, ss, breakerUpsert, victimUpsertSelect);
    }

    // Test: Basic TLI flow with Wilson tracing enabled.
    // Verifies that QuerySpanId is derived from the Wilson trace's SpanId
    // instead of a random fallback, and that TLI logging works correctly.
    Y_UNIT_TEST(BasicWithWilsonTracing) {
        TStringStream ss;

        // Configure tracing: always sample all requests at max verbosity
        TKikimrSettings settings;
        settings.LogStream = &ss;
        settings.SetWithSampleTables(false);

        auto* tracingConfig = settings.AppConfig.MutableTracingConfig();
        auto* samplingRule = tracingConfig->AddSampling();
        samplingRule->SetFraction(1.0);
        samplingRule->SetLevel(15);
        samplingRule->SetMaxTracesPerMinute(1000000);
        samplingRule->SetMaxTracesBurst(1000000);

        TKikimrRunner kikimr(settings);
        auto* runtime = kikimr.GetTestServer().GetRuntime();

        // Register fake Wilson uploader so spans are collected
        auto* uploader = new NWilson::TFakeWilsonUploader();
        TActorId uploaderId = runtime->Register(uploader, 0);
        runtime->RegisterService(NWilson::MakeWilsonUploaderId(), uploaderId, 0);

        ConfigureKikimrForTli(kikimr);

        TQueryClient client(kikimr.GetQueryClient());
        TSession session = client.GetSession().GetValueSync().GetSession();
        TSession victimSession = client.GetSession().GetValueSync().GetSession();

        CreateAndSeedTablesInSession(session, 1);

        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"BreakerValue\")";
        const TString victimQueryText = "SELECT * FROM `/Root/Tenant1/Table1` WHERE Key = 1u";
        const TString victimCommitText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"VictimValue\")";

        auto victimTx = BeginReadTx(victimSession, victimQueryText);
        NKqp::AssertSuccessResult(session.ExecuteQuery(breakerQueryText, TTxControl::BeginTx().CommitTx()).GetValueSync());
        auto [status, issues] = ExecuteVictimCommitWithIssues(victimSession, victimTx, victimCommitText);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerQueryText, victimQueryText, victimCommitText);

        // When Wilson tracing is active, SessionActor TLI logs must include TraceId
        const TString logs = ss.Str();
        const auto patterns = MakeTliLogPatterns();
        bool foundTraceIdInBreaker = false;
        bool foundTraceIdInVictim = false;
        for (const auto& record : ExtractTliRecords(logs)) {
            if (!record.Contains("Component: SessionActor")) {
                continue;
            }
            if (MatchesMessage(record, patterns.BreakerSessionActorMessagePattern) && record.Contains("TraceId: ")) {
                foundTraceIdInBreaker = true;
            }
            if (MatchesMessage(record, patterns.VictimSessionActorMessagePattern) && record.Contains("TraceId: ")) {
                foundTraceIdInVictim = true;
            }
        }
        UNIT_ASSERT_C(foundTraceIdInBreaker, "breaker SessionActor TLI log should contain TraceId when Wilson tracing is active");
        UNIT_ASSERT_C(foundTraceIdInVictim, "victim SessionActor TLI log should contain TraceId when Wilson tracing is active");
    }

    // Test: Transaction that is both a breaker AND a victim during distributed commit.
    // When the victim-shard responds before the breaker-shard, the buffer write actor
    // must still collect and propagate breaker TLI stats.
    Y_UNIT_TEST(BreakerAndVictimInSameTransaction) {
        TStringStream ss;
        TTliTestContext ctx(ss);
        ctx.CreateAndSeedTables(3);

        const TString victimOfTSelect = "SELECT * FROM `/Root/Tenant1/Table1` WHERE Key = 1u";
        const TString victimOfTUpdate = "UPDATE `/Root/Tenant1/Table3` SET Value = \"VictimOfTWrite\" WHERE Key = 1u";
        const TString tSelectTable2  = "SELECT * FROM `/Root/Tenant1/Table2` WHERE Key = 1u";
        const TString tWriteTable1   = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"TWrite1\")";
        const TString externalBreakerWrite = "UPSERT INTO `/Root/Tenant1/Table2` (Key, Value) VALUES (1u, \"ExtBreaker\")";

        // VictimOfT: read table1 (lock on table1) + write table3 (non-read-only tx)
        auto victimOfTTx = BeginReadTx(ctx.VictimSession, victimOfTSelect);
        ExecuteInTx(ctx.VictimSession, victimOfTTx, victimOfTUpdate);

        // T: read table2, write table1 (uses TKqpBufferWriteActor path)
        auto tSession = ctx.Client.GetSession().GetValueSync().GetSession();
        auto tTx = BeginTx(tSession, tSelectTable2);
        UNIT_ASSERT(tTx);
        ExecuteInTx(tSession, *tTx, tWriteTable1);

        // ExternalBreaker: write to table2 -> breaks T's lock on table2
        ctx.ExecuteQuery(externalBreakerWrite);

        // T: standalone commit -> distributed commit via TKqpBufferWriteActor
        //   shard1 (table1): T's write breaks VictimOfT's lock -> reports BreakerQuerySpanId
        //   shard2 (table2): T's lock was broken -> STATUS_LOCKS_BROKEN
        //   Result: T is BOTH a breaker (of VictimOfT) and a victim (of ExternalBreaker)
        auto [tStatus, tIssues] = CommitTxWithIssues(*tTx);
        UNIT_ASSERT_VALUES_EQUAL_C(tStatus, EStatus::ABORTED, tIssues);

        // Verify the ExternalBreaker->T victim pair and record counts.
        // expectedBreakerCount=2: ExternalBreaker's session + T's session (T broke VictimOfT).
        // Without the fix, T's breaker log is missing (count would be 1 instead of 2).
        VerifyTliIssueAndLogs(tIssues, ss, externalBreakerWrite, tSelectTable2,
            /* victimExtraQueryText */ std::nullopt,
            /* expectedBreakerCount */ 2, /* expectedVictimCount */ 1);

        // Additionally verify T's breaker log content (T broke VictimOfT's lock on table1)
        const auto patterns = MakeTliLogPatterns();
        auto tBreakerQueryText = ExtractQueryText(ss.Str(), patterns.BreakerSessionActorMessagePattern, tWriteTable1);
        UNIT_ASSERT_C(tBreakerQueryText,
            "T should emit breaker TLI log for tWriteTable1 (T is both breaker and victim)");
    }

    // ==================== DataQuery backward-compatibility tests ====================
    // These tests verify that TLI logging works correctly with the legacy DataQuery API
    // (NYdb::NTable::TTableClient / ExecuteDataQuery).

    Y_UNIT_TEST(BasicDataQuery) {
        TStringStream ss;
        TTliTestContext ctx(ss);
        ctx.CreateAndSeedTables(1);

        // Create DataQuery sessions
        NYdb::NTable::TTableClient tableClient(ctx.Kikimr.GetTableClient());
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        auto victimSession = tableClient.CreateSession().GetValueSync().GetSession();

        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"BreakerValue\")";
        const TString victimQueryText = "SELECT * FROM `/Root/Tenant1/Table1` WHERE Key = 1u";
        const TString victimCommitText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"VictimValue\")";

        auto victimTx = NDataQueryCompat::BeginReadTx(victimSession, victimQueryText);
        NKqp::AssertSuccessResult(session.ExecuteDataQuery(breakerQueryText, NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync());
        auto [status, issues] = NDataQueryCompat::ExecuteVictimCommitWithIssues(victimSession, victimTx, victimCommitText);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerQueryText, victimQueryText, victimCommitText);
    }

    Y_UNIT_TEST(SeparateCommitDataQuery) {
        TStringStream ss;
        TTliTestContext ctx(ss);
        ctx.CreateAndSeedTables(1);

        // Create DataQuery sessions
        NYdb::NTable::TTableClient tableClient(ctx.Kikimr.GetTableClient());
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        auto victimSession = tableClient.CreateSession().GetValueSync().GetSession();

        const TString breakerQueryText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"BreakerValue\")";
        const TString breakerQueryText2 = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (2u, \"UsualValue\")";
        const TString victimQueryText = "SELECT * FROM `/Root/Tenant1/Table1` WHERE Key = 1u";
        const TString victimCommitText = "UPSERT INTO `/Root/Tenant1/Table1` (Key, Value) VALUES (1u, \"VictimValue\")";

        auto victimTx = NDataQueryCompat::BeginReadTx(victimSession, victimQueryText);

        // Breaker: begin tx, write key 1, write key 2, then separate commit
        auto breakerTx = NDataQueryCompat::BeginTx(session, breakerQueryText);
        NKqp::AssertSuccessResult(session.ExecuteDataQuery(
            breakerQueryText2, NYdb::NTable::TTxControl::Tx(*breakerTx)).GetValueSync());
        NKqp::AssertSuccessResult(breakerTx->Commit().ExtractValueSync());

        auto [status, issues] = NDataQueryCompat::ExecuteVictimCommitWithIssues(victimSession, victimTx, victimCommitText);
        UNIT_ASSERT_VALUES_EQUAL(status, EStatus::ABORTED);

        VerifyTliIssueAndLogs(issues, ss, breakerQueryText, victimQueryText, victimCommitText);
    }
}

} // namespace NKqp
} // namespace NKikimr
