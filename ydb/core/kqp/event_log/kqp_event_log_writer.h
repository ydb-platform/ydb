#pragma once

#include "column_shard_log_writer.h"

#include <ydb/library/services/services.pb.h>

namespace NKikimr::NKqp::NEventLog {

class TKqpEventLogWriter : public TColumnShardLogWriter {
public:
    TVector<std::shared_ptr<TSchematizedLogColumn>> GetColumns() {
        auto notNull = TSchematizedLogColumn::TDatabaseSettings::NotNull();
        return TVector<std::shared_ptr<TSchematizedLogColumn>>{
            std::make_shared<TDBLogMessageIdColumn>(1),
            std::make_shared<TDBLogMessageTimeColumn>(),
            std::make_shared<TDBLogMessagePrioColumn>(),
            std::make_shared<TDBLogMessageNodeIdColumn>(),
            std::make_shared<TDBLogMessageErrorColumn>(),
            std::make_shared<TDBLogMessageStringValueColumn>("req_id", std::vector<TKeyName>{"reqId"}, notNull),
            std::make_shared<TDBLogMessageStringValueColumn>("pool_id", std::vector<TKeyName>{"poolId"}, notNull),
            std::make_shared<TDBLogMessageStringValueColumn>("session_id", std::vector<TKeyName>{"sessionId"}, notNull),
            std::make_shared<TDBLogMessageStringValueColumn>("user_sid", std::vector<TKeyName>{"userSID"}, notNull),
            std::make_shared<TDBLogMessageStringValueColumn>("request", std::vector<TKeyName>{"request"}, notNull),
            std::make_shared<TDBLogMessageStringValueColumn>("issues", std::vector<TKeyName>{"issues"}, notNull),
            std::make_shared<TDBLogMessageStringValueColumn>("is_streaming_query", std::vector<TKeyName>{"isStreamingQuery"}, notNull),
            std::make_shared<TDBLogMessageStringValueColumn>("duration_us", std::vector<TKeyName>{"durationUs"}, notNull),
            std::make_shared<TDBLogMessageStringValueColumn>("database", std::vector<TKeyName>{"database"}),
            std::make_shared<TDBLogMessageStringValueColumn>("databaseId", std::vector<TKeyName>{"databaseId"}),
            std::make_shared<TDBLogMessageStringValueColumn>("traceId", std::vector<TKeyName>{"traceId"}),
            std::make_shared<TDBLogMessageStringValueColumn>("queryId", std::vector<TKeyName>{"queryId"}),
            std::make_shared<TDBLogMessageStringValueColumn>("action", std::vector<TKeyName>{"action"}),
            std::make_shared<TDBLogMessageStringValueColumn>("type", std::vector<TKeyName>{"type"}),
            std::make_shared<TDBLogMessageStringValueColumn>("started_at_us", std::vector<TKeyName>{"startedAtUs"}),
            std::make_shared<TDBLogMessageStringValueColumn>("status", std::vector<TKeyName>{"status"}),
            std::make_shared<TDBLogMessageStringValueColumn>("queued_time_us", std::vector<TKeyName>{"queuedTimeUs"}),
            std::make_shared<TDBLogMessageStringValueColumn>("compile_from_cache", std::vector<TKeyName>{"compileFromCache"}),
            std::make_shared<TDBLogMessageStringValueColumn>("compile_time_us", std::vector<TKeyName>{"compileTimeUs"})
        };
    }

    static TDatabaseSettings UpdateSettings(TDatabaseSettings settings) {
        settings.TableName = "kqp_requests";
        settings.StoreName = "kqp_requests";
        return settings;
    }

    TKqpEventLogWriter(const TDatabaseSettings& settings):
        TColumnShardLogWriter(
            [](const NActors::NStructuredLog::TLogMessage& message){
                // @todo текст сообщения в константу
                return (message.Component == NKikimrServices::KQP_REQUEST) &&
                       (message.TextMessage == "KQP request processed");
            },
            UpdateSettings(settings),
            GetColumns()) {}
};

}