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

struct TGetMessageGroupsSettings {
    TString UserName;
    TString QueueName;
    ui64 QueueVersion;
    ui64 TablesFormat;
};

class TGetMessageGroupsActor : public TActorBootstrapped<TGetMessageGroupsActor> {
public:
    TGetMessageGroupsActor(TSqsEvents::TEvGetMessageGroups::TPtr&& ev, TGetMessageGroupsSettings&& settings)
        : Request_(std::move(ev))
        , Settings_(std::move(settings))
        , RequestId_(Request_->Get()->RequestId)
    {
    }

    ~TGetMessageGroupsActor() = default;

    void Bootstrap() {
        Become(&TGetMessageGroupsActor::StateFunc);

        RLOG_SQS_DEBUG(TLogQueueName(Settings_.UserName, Settings_.QueueName, -1) << " Executing get message groups. RequestId: "
            << RequestId_ );
    
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

        TString query = Sprintf(R"__(
            DECLARE $QueueIdNumber as Uint64;
            DECLARE $QueueIdNumberHash as Uint64;
    
            SELECT DISTINCT GroupId
            FROM `%s/Messages`
            WHERE QueueIdNumberHash = $QueueIdNumberHash
              AND QueueIdNumber = $QueueIdNumber
            LIMIT 101;
    
        )__", queryMaker.GetQueueTablesFolder().c_str());

        RLOG_SQS_DEBUG(TLogQueueName(Settings_.UserName, Settings_.QueueName, -1) << " Query: " << query);
    
        auto builder = NYdb::TParamsBuilder();
        builder.AddParam("$QueueIdNumberHash")
                .Uint64(GetKeysHash(Settings_.QueueVersion))
                .Build();
        builder.AddParam("$QueueIdNumber")
                .Uint64(Settings_.QueueVersion)
                .Build();

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
                Send(Request_->Sender, new TSqsEvents::TEvGetMessageGroupsResponse(record.GetYdbStatus()));
                PassAway();
            }
            return;
        }

        auto& response = record.GetResponse();

        Y_ABORT_UNLESS(response.YdbResultsSize() == 1);
        NYdb::TResultSetParser parser(response.GetYdbResults(0));

        std::vector<TString> blockedMessageGroups;
        while (parser.TryNextRow()) {
            TString messageGroupId = parser.ColumnParser(0).GetOptionalString().value_or("");
            blockedMessageGroups.push_back(messageGroupId);
        }

        if (blockedMessageGroups.size() > 100) {
            Send(Request_->Sender, new TSqsEvents::TEvGetMessageGroupsResponse(Ydb::StatusIds::OVERLOADED));
        } else {
            Send(Request_->Sender, new TSqsEvents::TEvGetMessageGroupsResponse(std::move(blockedMessageGroups)));
        }

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
    const TSqsEvents::TEvGetMessageGroups::TPtr Request_;
    const TGetMessageGroupsSettings Settings_;
    const TString RequestId_;

    TBackoff Backoff_ = TBackoff(10, TDuration::MilliSeconds(50), TDuration::Seconds(10));
};

} // namespace NKikimr::NSQS
