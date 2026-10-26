#include "controller_impl.h"
#include "logging.h"
#include "private_events.h"
#include "target_base.h"

#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/core/base/path.h>
#include <google/protobuf/util/time_util.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

class TTargetDescriber: public TActorBootstrapped<TTargetDescriber> {
    void DescribeTarget(ui64 id) {
        Y_ABORT_UNLESS(Targets.contains(id));
        Send(YdbProxy, new TEvYdbProxy::TEvDescribeTableRequest(Targets.at(id), {}), 0, id);
    }

    void Reply() {
        Send(Parent, new TEvPrivate::TEvDescribeTargetsResult(Sender, ReplicationId, std::move(Result), SampleTime));
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

        if (Result.size() == Targets.size()) {
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
            THashMap<ui64, TString>&& targets)
        : Sender(sender)
        , Parent(parent)
        , ReplicationId(rid)
        , YdbProxy(proxy)
        , Targets (std::move(targets))
        , LogPrefix(CreateActorLogPrefix("TargetDescriber", ReplicationId))
    {
    }

    void Bootstrap() {
        YDB_LOG_CREATE_CONTEXT(LogPrefix);
        if (!YdbProxy || Targets.empty()) {
            return Reply();
        }

        SampleTime = TActivationContext::Now();

        // Source statistics are optional: even a live proxy may never receive
        // a response from the source. Return the available results on timeout.
        Schedule(TDuration::Seconds(5), new TEvents::TEvWakeup());
        for (const auto& [id, _] : Targets) {
            DescribeTarget(id);
        }

        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        YDB_LOG_CREATE_CONTEXT(LogPrefix,
            {"actorState", "StateWork"});
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvDescribeTableResponse, Handle);
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
    const NActors::NStructuredLog::TStructuredMessage LogPrefix;

    TEvPrivate::TEvDescribeTargetsResult::TResult Result;
    TInstant SampleTime;

}; // TTargetDescriber

class TController::TTxDescribeReplication: public TTxBase {
    const TActorId Sender;
    TEvController::TEvDescribeReplication::TPtr PubEv;
    TEvPrivate::TEvDescribeTargetsResult::TPtr PrivEv;
    TReplication::TPtr Replication;
    THolder<TEvController::TEvDescribeReplicationResult> Result;
    THashMap<ui64, TString> TargetsToDescribe;

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
            && Replication->CanDescribeSource())
        {
            for (ui64 tid = 0; tid < Replication->GetNextTargetId(); ++tid) {
                auto* target = Replication->FindTarget(tid);
                if (!target) {
                    continue;
                }

                if (target->GetStreamName()) {
                    // Index streams belong to indexImplTable, not the logical
                    // index path exposed in the public replication item.
                    TargetsToDescribe.emplace(tid, TString(ExtractParent(target->GetStreamPath())));
                }
            }

            if (TargetsToDescribe) {
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

        if (!Replication->RemoveTargetDescriber(PrivEv->Sender)) {
            // A credential reset retires describers before poisoning them.
            // Their final replies must not repopulate source history.
            PrivEv->Get()->Result.clear();
        }
        return DescribeReplication(Replication, true);
    }

    const NYdb::NTable::TChangefeedDescription* FindSourceChangefeed(const TReplication::ITarget& target) const {
        if (!PrivEv) {
            return nullptr;
        }
        const auto& results = PrivEv->Get()->Result;
        const auto it = results.find(target.GetId());
        if (it == results.end() || !it->second) {
            return nullptr;
        }

        const auto& streams = it->second->GetTableDescription().GetChangefeedDescriptions();
        return FindIfPtr(streams, [&target](const auto& cf) {
            return cf.GetName() == target.GetStreamName();
        });
    }

    std::optional<TDuration> ObserveInitialScan(
            TReplication::ITarget& target, const NYdb::NTable::TChangefeedDescription* stream)
    {
        if (!stream) {
            return std::nullopt;
        }

        using EState = NYdb::NTable::EChangefeedState;
        const bool complete = stream->GetState() == EState::Enabled;
        const bool scanning = stream->GetState() == EState::InitialScan;
        const auto& progress = stream->GetInitialScanProgress();
        if (!complete && (!scanning || !progress)) {
            return std::nullopt;
        }
        return target.GetReplicationStats().ObserveScan(
            PrivEv->Get()->SampleTime,
            progress ? progress->GetPartsTotal() : 0,
            progress ? progress->GetPartsCompleted() : 0,
            complete);
    }

    bool DescribeReplication(TReplication::TPtr replication, bool includeDetailedStats) {
        Result = MakeHolder<TEvController::TEvDescribeReplicationResult>();
        Result->Record.SetStatus(NKikimrReplication::TEvDescribeReplicationResult::SUCCESS);
        Result->Record.MutableConnectionParams()->CopyFrom(replication->GetConfig().GetSrcConnectionParams());
        Result->Record.MutableConsistencySettings()->CopyFrom(replication->GetConfig().GetConsistencySettings());

        using TInitialScanProgress = NYdb::NTable::TChangefeedDescription::TInitialScanProgress;
        std::optional<TInitialScanProgress> totalScanProgress;

        if (PrivEv) {
            totalScanProgress = std::make_optional<TInitialScanProgress>();
        }

        TReplicationStatistics aggregate(replication->IsTargetDiscoveryComplete());
        const auto now = TActivationContext::Now();
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

            const auto* sourceChangefeed = FindSourceChangefeed(*target);
            if (includeDetailedStats && !isTransfer) {
                const auto speed = target->GetReplicationStats().GetWriteSpeed(now);
                const auto eta = ObserveInitialScan(*target, sourceChangefeed);
                aggregate.Add(speed, eta);
                if (speed) {
                    item.SetWriteSpeedBytesPerSecond(*speed);
                }
                if (eta) {
                    *item.MutableInitialScanEta() = google::protobuf::util::TimeUtil::MicrosecondsToDuration(
                        eta->MicroSeconds());
                }
            }

            if (PrivEv) {
                if (!sourceChangefeed || !sourceChangefeed->GetInitialScanProgress()) {
                    totalScanProgress.reset();
                    continue;
                }

                item.SetInitialScanProgress(sourceChangefeed->GetInitialScanProgress()->GetProgress());
                if (totalScanProgress) {
                    *totalScanProgress += *sourceChangefeed->GetInitialScanProgress();
                }
            }
        }

        auto& state = *Result->Record.MutableState();
        switch (replication->GetState()) {
        case TReplication::EState::Ready:
        case TReplication::EState::Removing:
            state.MutableStandBy();
            if (includeDetailedStats && !isTransfer) {
                if (aggregate.WriteSpeed) {
                    state.MutableStandBy()->SetWriteSpeedBytesPerSecond(*aggregate.WriteSpeed);
                }
                if (aggregate.ScanEta) {
                    *state.MutableStandBy()->MutableInitialScanEta() =
                        google::protobuf::util::TimeUtil::MicrosecondsToDuration(aggregate.ScanEta->MicroSeconds());
                }
            }
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
        } else if (TargetsToDescribe) {
            Y_ABORT_UNLESS(Replication);
            Replication->AddTargetDescriber(ctx.Register(new TTargetDescriber(
                Sender,
                ctx.SelfID,
                Replication->GetId(),
                Replication->GetYdbProxy(),
                std::move(TargetsToDescribe))));
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
