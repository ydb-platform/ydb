#pragma once

#include "column_shard_log_writer.h"

#include <ydb/library/services/services.pb.h>

#include <set>

namespace NKikimr::NKqp::NEventLog {

class TTliEventLogFilter {
public:
    TTliEventLogFilter(int component, TString componentName, std::set<std::string> messages)
        : Component(component)
        , ComponentName(std::move(componentName))
        , Messages(std::move(messages))
    {
    }

    bool operator()(const NActors::NStructuredLog::TLogMessage& message) const {
        if (message.Component != Component) {
            return false;
        }

        TStringValueExtractor extractor;
        const auto extractedComponentName = extractor.ExtractValue(
            message.StructuredMessage, std::vector<TKeyName>{"component"});
        const auto messageText = extractor.ExtractValue(
            message.StructuredMessage, std::vector<TKeyName>{"message"});
        if (!messageText.has_value()) {
            return false;
        }

        return extractedComponentName.has_value() && extractedComponentName.value() == ComponentName
            && Messages.find(messageText.value()) != Messages.end();
    }

private:
    const int Component;
    const TString ComponentName;
    const std::set<std::string> Messages;
};

class TTliEventLogCommon {
public:
    static TVector<std::shared_ptr<TEventLogColumn>> GetColumns() {
        return TVector<std::shared_ptr<TEventLogColumn>>{
            std::make_shared<TDBLogMessageIdColumn>(1),
            std::make_shared<TDBLogMessageTimeColumn>(),
            std::make_shared<TDBLogMessagePrioColumn>(),
            std::make_shared<TDBLogMessageNodeIdColumn>(),
            std::make_shared<TDBLogMessageErrorColumn>()};
    }
};

class TDataShardTliEventLog {
public:
    static TVector<std::shared_ptr<TEventLogColumn>> GetColumns() {
        auto notNull = TEventLogColumn::TDatabaseSettings::NotNull();
        auto notNullDict = TEventLogColumn::TDatabaseSettings::NotNull().SetDictionary(true);

        auto result = TTliEventLogCommon::GetColumns();
        TVector<std::shared_ptr<TEventLogColumn>> ownColumns{
            std::make_shared<TDBLogColumnUint64>("tablet_id", std::vector<TKeyName>{"tabletId"}, notNull),
            std::make_shared<TDBLogMessageStringValueColumn>("message", std::vector<TKeyName>{"message"}, notNullDict),
            std::make_shared<TDBLogColumnUint64>("breaker_query_span_id", std::vector<TKeyName>{"breakerQuerySpanId"}),
            std::make_shared<TDBLogColumnUint64>("victim_query_span_id", std::vector<TKeyName>{"victimQuerySpanId"})
        };
        std::copy(ownColumns.begin(), ownColumns.end(), std::back_inserter(result));
        return result;
    }

    static TBaseEventLogWriter::TFilterFunction CreateFilter() {
        return TTliEventLogFilter(NKikimrServices::TLI, "DataShard", GetMessages());
    }

    static std::set<std::string> GetMessages() {
        return std::set<std::string>{
            "Write transaction broke other locks",
            "Write transaction aborted, broke other transaction locks during cleanup",
            "Write transaction broke other locks (deferred)",
            "Write transaction was a victim of broken locks",
            "Read transaction was a victim of broken locks",
            "Tablet split operation invalidated locks",
            "Schema change: table removed invalidated locks",
            "Replication apply broke locks on replicated rows"};
    }
};

class TSessionTliEventLog {
public:
    static TVector<std::shared_ptr<TEventLogColumn>> GetColumns() {
        auto notNullDict = TEventLogColumn::TDatabaseSettings::NotNull().SetDictionary(true);

        auto result = TTliEventLogCommon::GetColumns();
        TVector<std::shared_ptr<TEventLogColumn>> ownColumns{
            std::make_shared<TDBLogMessageStringValueColumn>("message", std::vector<TKeyName>{"message"}, notNullDict),
            std::make_shared<TDBLogMessageStringValueColumn>("trace_id", std::vector<TKeyName>{"traceId"}),
            std::make_shared<TDBLogColumnUint64>("breaker_tx_span_id", std::vector<TKeyName>{"breakerTxSpanId"}),
            std::make_shared<TDBLogColumnUint64>("victim_tx_span_id", std::vector<TKeyName>{"victimTxSpanId"}),
            std::make_shared<TDBLogColumnUint64>("query_span_id", std::vector<TKeyName>{"querySpanId"}),
            std::make_shared<TDBLogMessageStringValueColumn>("query_text", std::vector<TKeyName>{"queryText"})
        };
        std::copy(ownColumns.begin(), ownColumns.end(), std::back_inserter(result));
        return result;
    }

    static TBaseEventLogWriter::TFilterFunction CreateFilter() {
        return TTliEventLogFilter(NKikimrServices::TLI, "SessionActor", GetMessages());
    }

    static std::set<std::string> GetMessages() {
        return std::set<std::string>{
            "Commit had broken other locks",
            "Query had broken other locks",
            "Commit had broken other locks (deferred)",
            "Query had broken other locks (deferred)",
            "Commit was a victim of broken locks",
            "Query was a victim of broken locks"};
    }
};

namespace NToColumnShard {

using TDataShardTliEventLogWriter = TColumnShardLogWriter<TDataShardTliEventLog>;
using TSessionTliEventLogWriter = TColumnShardLogWriter<TSessionTliEventLog>;

} // namespace NToColumnShard

} // namespace NKikimr::NKqp::NEventLog
