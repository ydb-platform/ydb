#pragma once

#include <ydb/core/util/backoff.h>
#include <ydb/core/ymq/actor/cfg/defs.h>
#include <ydb/core/ymq/actor/events.h>
#include <ydb/core/ymq/actor/log.h>
#include <ydb/core/ymq/base/run_query.h>
#include <ydb/core/ymq/queues/common/db_queries_maker.h>
#include <ydb/core/ymq/queues/common/key_hashes.h>

#include <unordered_map>

namespace NKikimr::NSQS {

struct TDeduplicatorSettings {
    TString UserName;
    TString QueueName;
    ui64 QueueVersion;
    ui64 TablesFormat;
};

class TDeduplicatorActor : public TActorBootstrapped<TDeduplicatorActor> {
public:
    TDeduplicatorActor(TSqsEvents::TEvDeduplicateMessageBatch::TPtr&& ev, TDeduplicatorSettings&& settings)
        : Request_(std::move(ev))
        , Settings_(std::move(settings))
        , RequestId_(Request_->Get()->RequestId)
    {
    }

    ~TDeduplicatorActor() = default;

    void Bootstrap() {
        Become(&TDeduplicatorActor::StateFunc);

        const auto& messageIds = Request_->Get()->DeduplicationMessageIds;
        RLOG_SQS_DEBUG(TLogQueueName(Settings_.UserName, Settings_.QueueName, -1) << " Executing deduplicate batch. BatchId: "
            << RequestId_ << ". Size: " << messageIds.size());
    
        const auto& root = AppData()->SqsConfig.GetRoot();

        TDbQueriesMaker queryMaker(
            root,
            Settings_.UserName,
            Settings_.QueueName,
            Settings_.QueueVersion,
            true, // is fifo
            -1, // shard
            Settings_.TablesFormat,
            "", // dlq name
            -1, // dlq shard
            0, // dlq version
            0 // dlq tables format
        );

        TStringBuilder declareIds;
        TStringBuilder listIds;
        for (size_t i = 0; i < messageIds.size(); ++i) {
            declareIds << "DECLARE $DeduplicationId" << i << " as String;\n";

            if (i) {
                listIds << ", ";
            }
            listIds << "$DeduplicationId" << i;
        }
    
        TString query = Sprintf(R"__(
            DECLARE $QueueIdNumber as Uint64;
            DECLARE $QueueIdNumberHash as Uint64;
            %s
    
            SELECT
                DedupId,
                MessageId,
                Offset
            FROM `%s/Deduplication`
            WHERE QueueIdNumberHash = $QueueIdNumberHash
              AND QueueIdNumber = $QueueIdNumber
              AND DedupId IN (%s);
    
        )__", declareIds.c_str(), queryMaker.GetQueueTablesFolder().c_str(), listIds.c_str());

        RLOG_SQS_DEBUG(TLogQueueName(Settings_.UserName, Settings_.QueueName, -1) << " Query: " << query);
    
        auto builder = NYdb::TParamsBuilder();
        builder.AddParam("$QueueIdNumberHash")
                .Uint64(GetKeysHash(Settings_.QueueVersion))
                .Build();
        builder.AddParam("$QueueIdNumber")
                .Uint64(Settings_.QueueVersion)
                .Build();

        for (size_t i = 0; i < messageIds.size(); ++i) {
            builder.AddParam(TStringBuilder() << "$DeduplicationId" << i).String(messageIds[i]).Build();
        }
    
        auto params = builder.Build();

        RunYqlQuery(query, std::move(params), true, TDuration::Zero(), TActivationContext::AsActorContext());
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
            RLOG_SQS_DEBUG(TLogQueueName(Settings_.UserName, Settings_.QueueName, -1) << " YQL error: " << record.DebugString());
            if (Backoff_.HasMore()) {
                Schedule(Backoff_.Next(), new TEvents::TEvWakeup());
            } else {
                Send(Request_->Sender, new TSqsEvents::TEvDeduplicateMessageBatchResponse(record.GetYdbStatus()));
                PassAway();
            }
            return;
        }

        auto& response = record.GetResponse();

        Y_ABORT_UNLESS(response.YdbResultsSize() == 1);
        NYdb::TResultSetParser parser(response.GetYdbResults(0));

        std::unordered_map<TString, std::pair<TString, ui64>> blockedDeduplicationMessageIds;
        while (parser.TryNextRow()) {
            TString deduplicationId = parser.ColumnParser(0).GetOptionalString().value_or("");
            TString messageId = parser.ColumnParser(1).GetOptionalString().value_or("");
            ui64 offset = parser.ColumnParser(2).GetOptionalUint64().value_or(0);
            blockedDeduplicationMessageIds[deduplicationId] = {messageId, offset};
        }

        Send(Request_->Sender, new TSqsEvents::TEvDeduplicateMessageBatchResponse(std::move(blockedDeduplicationMessageIds)));
        PassAway();
    }

    void Handle(TEvents::TEvWakeup::TPtr&) {
        Bootstrap();
    }

    void Handle(TEvents::TEvPoison::TPtr&) {
        PassAway();
    }

private:
    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
            hFunc(TEvents::TEvPoison, Handle);
        }
    }

private:
    const TSqsEvents::TEvDeduplicateMessageBatch::TPtr Request_;
    const TDeduplicatorSettings Settings_;
    const TString RequestId_;

    TBackoff Backoff_ = TBackoff(10, TDuration::MilliSeconds(50), TDuration::Seconds(10));
};

} // namespace NKikimr::NSQS
