#include "controller_impl.h"
#include "logging.h"
#include "private_events.h"
#include "target_base.h"

#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <google/protobuf/util/time_util.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

namespace {

struct TTopicToDescribe {
    TString SrcPath;
    TString Consumer;
};

bool CanDescribeMirror(const TReplication& replication, const TReplication::ITarget& target) {
    return IsTopicTarget(target.GetKind())
        && replication.GetState() == TReplication::EState::Ready
        && replication.GetDesiredState() == TReplication::EState::Ready
        && target.GetDstState() == TReplication::EDstState::Ready
        && target.GetStreamState() == TReplication::EStreamState::Ready;
}

class TTargetDescriber: public TActorBootstrapped<TTargetDescriber> {
    void DescribeTarget(ui64 id) {
        Y_ABORT_UNLESS(Targets.contains(id));
        Send(YdbProxy, new TEvYdbProxy::TEvDescribeTableRequest(Targets.at(id), {}), 0, id);
    }

    void Reply() {
        Send(Parent, new TEvPrivate::TEvDescribeTargetsResult(Sender, ReplicationId, std::move(Result), std::move(TopicLags)));
        PassAway();
    }

    void Handle(TEvYdbProxy::TEvDescribeTableResponse::TPtr& ev) {
        YDB_LOG_TRACE("Handle",
            {"ev", ev->Get()->ToString()});

        if (!Targets.contains(ev->Cookie)) {
            YDB_LOG_WARN("Unknown describe response",
                {"cookie", ev->Cookie});
            return;
        }

        const auto id = ev->Cookie;
        const auto& path = Targets.at(id);

        if (Result.contains(id)) {
            YDB_LOG_WARN("Duplicate describe response",
                {"id", id},
                {"path", path});
            return;
        }

        auto& result = ev->Get()->Result;
        if (result.IsSuccess()) {
            YDB_LOG_DEBUG("Describe succeeded",
                {"id", id},
                {"path", path});
            Result.emplace(id, std::move(result));
        } else {
            YDB_LOG_ERROR("Describe failed",
                {"id", id},
                {"path", path},
                {"status", result.GetStatus()},
                {"issues", result.GetIssues().ToOneLineString()});
            Result.emplace(id, std::nullopt);
        }

        CheckDone();
    }

    void Handle(TEvYdbProxy::TEvDescribeTopicResponse::TPtr& ev) {
        const auto id = ev->Cookie;
        const auto it = Topics.find(id);
        if (it == Topics.end() || TopicLags.contains(id)) {
            return;
        }

        std::optional<ui64> lagMs;
        const auto& result = ev->Get()->Result;
        if (result.IsSuccess()) {
            const auto& topic = NYdb::TProtoAccessor::GetProto(result.GetTopicDescription());
            const auto* consumer = FindIfPtr(topic.consumers(), [&](const auto& consumer) {
                return consumer.name() == it->second.Consumer;
            });
            if (consumer && consumer->consumer_stats().has_max_committed_time_lag()) {
                lagMs = google::protobuf::util::TimeUtil::DurationToMilliseconds(
                    consumer->consumer_stats().max_committed_time_lag());
            }
        }

        TopicLags.emplace(id, lagMs);
        CheckDone();
    }

    void CheckDone() {
        if (Result.size() == Targets.size() && TopicLags.size() == Topics.size()) {
            Reply();
        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_CONTROLLER_TARGET_DESCRIBER;
    }

    explicit TTargetDescriber(
            const TActorId& sender,
            const TActorId& parent,
            ui64 rid,
            const TActorId& proxy,
            THashMap<ui64, TString>&& targets,
            THashMap<ui64, TTopicToDescribe>&& topics)
        : Sender(sender)
        , Parent(parent)
        , ReplicationId(rid)
        , YdbProxy(proxy)
        , Targets(std::move(targets))
        , Topics(std::move(topics))
        , LogPrefix(CreateActorLogPrefix("TargetDescriber", ReplicationId))
    {
    }

    void Bootstrap() {
        YDB_LOG_CREATE_CONTEXT(LogPrefix);
        if (!YdbProxy || (Targets.empty() && Topics.empty())) {
            return Reply();
        }

        // Source statistics are optional: even a live proxy may never receive
        // a response from the source. Return the available results on timeout.
        Schedule(TDuration::Seconds(5), new TEvents::TEvWakeup());
        for (const auto& [id, _] : Targets) {
            DescribeTarget(id);
        }

        for (const auto& [id, topic] : Topics) {
            Send(YdbProxy, new TEvYdbProxy::TEvDescribeTopicRequest(topic.SrcPath,
                NYdb::NTopic::TDescribeTopicSettings().IncludeStats(true)), 0, id);
        }
        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateWork"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvDescribeTableResponse, Handle);
            hFunc(TEvYdbProxy::TEvDescribeTopicResponse, Handle);
            sFunc(TEvents::TEvWakeup, Reply);
            sFunc(TEvents::TEvPoison, Reply);
        }
    }

private:
    const TActorId Sender;
    const TActorId Parent;
    const ui64 ReplicationId;
    const TActorId YdbProxy;
    const THashMap<ui64, TString> Targets;
    const THashMap<ui64, TTopicToDescribe> Topics;
    TEvPrivate::TEvDescribeTargetsResult::TTopicLags TopicLags;
    const NActors::NStructuredLog::TStructuredMessage LogPrefix;

    TEvPrivate::TEvDescribeTargetsResult::TResult Result;

}; // TTargetDescriber

} // anonymous namespace

class TController::TTxDescribeReplication: public TTxBase {
    const TActorId Sender;
    TEvController::TEvDescribeReplication::TPtr PubEv;
    TEvPrivate::TEvDescribeTargetsResult::TPtr PrivEv;
    TReplication::TPtr Replication;
    THolder<TEvController::TEvDescribeReplicationResult> Result;
    THashMap<ui64, TString> TargetsToDescribe;
    THashMap<ui64, TTopicToDescribe> TopicsToDescribe;

    static bool HasInitialScan(TReplication::ETargetKind kind) {
        return EqualToOneOf(kind, TReplication::ETargetKind::Table, TReplication::ETargetKind::IndexTable);
    }

public:
    explicit TTxDescribeReplication(TController* self, TEvController::TEvDescribeReplication::TPtr& ev)
        : TTxBase("TxDescribeReplication", self)
        , Sender(ev->Sender)
        , PubEv(ev)
    {
    }

    explicit TTxDescribeReplication(TController* self, TEvPrivate::TEvDescribeTargetsResult::TPtr& ev)
        : TTxBase("TxDescribeReplication", self)
        , Sender(ev->Get()->Sender)
        , PrivEv(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_DESCRIBE_REPLICATION;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        if (PubEv) {
            return ExecutePub(txc, ctx);
        } else if (PrivEv) {
            return ExecutePriv(txc, ctx);
        } else {
            Y_ABORT("unreachable");
        }
    }

    bool ExecutePub(TTransactionContext&, const TActorContext& ctx) {
        YDB_LOG_DEBUG_CTX(ctx, "Execute",
            {"ev", PubEv->Get()->ToString()});

        const auto& record = PubEv->Get()->Record;
        const auto pathId = TPathId::FromProto(record.GetPathId());

        Replication = Self->Find(pathId);
        if (!Replication) {
            Result = MakeHolder<TEvController::TEvDescribeReplicationResult>();
            Result->Record.SetStatus(NKikimrReplication::TEvDescribeReplicationResult::NOT_FOUND);
            return true;
        }

        if (record.GetIncludeStats()
            && !Replication->GetConfig().HasTransferSpecific()
            && Replication->GetState() != TReplication::EState::Done
            && Replication->GetDesiredState() != TReplication::EState::Done
            && Replication->GetYdbProxy())
        {
            for (ui64 tid = 0; tid < Replication->GetNextTargetId(); ++tid) {
                auto* target = Replication->FindTarget(tid);
                if (!target) {
                    continue;
                }

                if (HasInitialScan(target->GetKind())) {
                    TargetsToDescribe.emplace(tid, target->GetSrcPath());
                } else if (CanDescribeMirror(*Replication, *target)) {
                    TopicsToDescribe.emplace(tid, TTopicToDescribe{target->GetSrcPath(), target->GetStreamConsumerName()});
                }
            }

            if (TargetsToDescribe || TopicsToDescribe) {
                return true;
            }
        }

        return DescribeReplication(Replication, record.GetIncludeStats());
    }

    bool ExecutePriv(TTransactionContext&, const TActorContext& ctx) {
        YDB_LOG_DEBUG_CTX(ctx, "Execute",
            {"ev", PrivEv->Get()->ToString()});

        const auto rid = PrivEv->Get()->ReplicationId;

        Replication = Self->Find(rid);
        if (!Replication) {
            Result = MakeHolder<TEvController::TEvDescribeReplicationResult>();
            Result->Record.SetStatus(NKikimrReplication::TEvDescribeReplicationResult::NOT_FOUND);
            return true;
        }

        Replication->RemoveTargetDescriber(PrivEv->Sender);
        return DescribeReplication(Replication, false);
    }

    bool DescribeReplication(TReplication::TPtr replication, bool includeDetailedStats) {
        Result = MakeHolder<TEvController::TEvDescribeReplicationResult>();
        Result->Record.SetStatus(NKikimrReplication::TEvDescribeReplicationResult::SUCCESS);
        Result->Record.MutableConnectionParams()->CopyFrom(replication->GetConfig().GetSrcConnectionParams());
        Result->Record.MutableConsistencySettings()->CopyFrom(replication->GetConfig().GetConsistencySettings());
        if (replication->GetConfig().HasAvailabilityPeriod()) {
            Result->Record.MutableAvailabilityPeriod()->CopyFrom(replication->GetConfig().GetAvailabilityPeriod());
        } else if (!replication->GetConfig().HasTransferSpecific()) {
            Result->Record.MutableAvailabilityPeriod()->MutableDuration()->set_seconds(
                AppData()->ReplicationConfig.GetConsumerAvailabilityPeriodSeconds());
        }
        if (replication->GetConfig().GetConsistencySettings().HasGlobal() && Self->GetSingle() == replication) {
            auto* transactions = Result->Record.MutableTransactions();
            // CommittingTxId is already included in AssignedTxIds. Entries are
            // removed by TTxCommitChanges and restored from Schema::TxIds on restart.
            for (const auto& [version, txId] : Self->AssignedTxIds) {
                auto* item = transactions->add_assigned();
                item->mutable_upper_bound_barrier()->set_plan_step(version.Step);
                item->mutable_upper_bound_barrier()->set_tx_id(version.TxId);
                item->set_write_tx_id(txId);
            }
            for (const auto& [version, _] : Self->PendingTxId) {
                auto* item = transactions->add_pending();
                item->set_plan_step(version.Step);
                item->set_tx_id(version.TxId);
            }
        }

        using TInitialScanProgress = NYdb::NTable::TChangefeedDescription::TInitialScanProgress;
        std::optional<TInitialScanProgress> totalScanProgress;

        if (PrivEv && !PrivEv->Get()->Result.empty()) {
            totalScanProgress.emplace();
        }

        bool isTransfer = replication->GetConfig().HasTransferSpecific();
        if (isTransfer) {
            auto& specific = replication->GetConfig().GetTransferSpecific();

            auto& transferSpecific = *Result->Record.MutableTransferSpecific();
            transferSpecific.MutableTarget()->SetSrcPath(specific.GetTarget().GetSrcPath());
            transferSpecific.MutableTarget()->SetDstPath(specific.GetTarget().GetDstPath());
            transferSpecific.MutableTarget()->SetTransformLambda(specific.GetTarget().GetTransformLambda());
            transferSpecific.MutableBatching()->CopyFrom(specific.GetBatching());
        }

        for (ui64 tid = 0; tid < replication->GetNextTargetId(); ++tid) {
            auto* target = replication->FindTarget(tid);
            if (!target) {
                continue;
            }

            if (isTransfer) {
                // transfer always has one target
                auto& specific = replication->GetConfig().GetTransferSpecific();

                auto& transferSpecific = *Result->Record.MutableTransferSpecific();
                transferSpecific.MutableTarget()->SetConsumerName(target->GetStreamConsumerName()
                    ? target->GetStreamConsumerName()
                    : specific.GetTarget().GetConsumerName());

                if (auto* stats = target->GetStats()) {
                    stats->Serialize(Result->Record, includeDetailedStats);
                }
            }

            auto& item = *Result->Record.AddTargets();
            item.SetId(target->GetId());
            item.SetSrcPath(target->GetSrcPath());
            item.SetDstPath(target->GetDstPath());

            if (target->GetStreamName()) {
                item.SetSrcStreamName(target->GetStreamName());
            }
            if (const auto lag = target->GetLag()) {
                item.SetLagMilliSeconds(lag->MilliSeconds());
            }

            if (PrivEv && CanDescribeMirror(*replication, *target)) {
                const auto& lags = PrivEv->Get()->TopicLags;
                if (auto it = lags.find(tid); it != lags.end() && it->second) {
                    item.SetLagMilliSeconds(*it->second);
                }
            }

            if (PrivEv && HasInitialScan(target->GetKind())) {
                const auto& result = PrivEv->Get()->Result;

                auto it = result.find(tid);
                if (it == result.end() || !it->second) {
                    totalScanProgress.reset();
                    continue;
                }

                const auto& changefeeds = it->second->GetTableDescription().GetChangefeedDescriptions();
                auto* cfPtr = FindIfPtr(changefeeds, [target](const NYdb::NTable::TChangefeedDescription& cf) {
                    return cf.GetName() == target->GetStreamName();
                });

                if (!cfPtr || !cfPtr->GetInitialScanProgress()) {
                    totalScanProgress.reset();
                    continue;
                }

                item.SetInitialScanProgress(cfPtr->GetInitialScanProgress()->GetProgress());
                if (totalScanProgress) {
                    *totalScanProgress += *cfPtr->GetInitialScanProgress();
                }
            }
        }

        auto& state = *Result->Record.MutableState();
        switch (replication->GetState()) {
        case TReplication::EState::Ready:
        case TReplication::EState::Removing:
            state.MutableStandBy();
            if (const auto lag = replication->GetLag()) {
                state.MutableStandBy()->SetLagMilliSeconds(lag->MilliSeconds());
            }
            if (totalScanProgress) {
                state.MutableStandBy()->SetInitialScanProgress(totalScanProgress->GetProgress());
            }
            break;
        case TReplication::EState::Done:
            state.MutableDone();
            break;
        case TReplication::EState::Paused:
            state.MutablePaused();
            break;
        case TReplication::EState::Error:
            if (auto issue = state.MutableError()->AddIssues()) {
                issue->set_severity(static_cast<uint32_t>(NYdb::NIssue::ESeverity::Error));
                issue->set_message(replication->GetIssue());
            }
            break;
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG_CTX(ctx, "Complete");

        if (Result) {
            ctx.Send(Sender, Result.Release());
        } else if (TargetsToDescribe || TopicsToDescribe) {
            Y_ABORT_UNLESS(Replication);
            Replication->AddTargetDescriber(ctx.Register(new TTargetDescriber(
                Sender,
                ctx.SelfID,
                Replication->GetId(),
                Replication->GetYdbProxy(),
                std::move(TargetsToDescribe),
                std::move(TopicsToDescribe))));
        }
    }

}; // TTxDescribeReplication

void TController::RunTxDescribeReplication(TEvController::TEvDescribeReplication::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxDescribeReplication(this, ev), ctx);
}

void TController::RunTxDescribeReplication(TEvPrivate::TEvDescribeTargetsResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxDescribeReplication(this, ev), ctx);
}

}
