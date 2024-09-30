#include "controller_impl.h"
#include "logging.h"
#include "private_events.h"

#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h> 

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>

namespace NKikimr::NReplication::NController {

class TTargetDescriber: public TActorBootstrapped<TTargetDescriber> {
    void DescribeTarget(ui64 id) {
        Y_ABORT_UNLESS(Targets.contains(id));
        Send(YdbProxy, new TEvYdbProxy::TEvDescribeTableRequest(Targets.at(id), {}), 0, id);
    }

    void Handle(TEvYdbProxy::TEvDescribeTableResponse::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());

        if (!Targets.contains(ev->Cookie)) {
            LOG_W("Unknown describe response"
                << ": cookie# " << ev->Cookie);
            return;
        }

        const auto id = ev->Cookie;
        const auto& path = Targets.at(id);

        if (Result.contains(id)) {
            LOG_W("Duplicate describe response"
                << ": id# " << id
                << ", path# " << path);
            return;
        }

        auto& result = ev->Get()->Result;
        if (result.IsSuccess()) {
            LOG_D("Describe succeeded"
                << ": id# " << id
                << ", path# " << path);
            Result.emplace(id, std::move(result));
        } else {
            LOG_E("Describe failed"
                << ": id# " << id
                << ", path# " << path
                << ", status# " << result.GetStatus()
                << ", issues# " << result.GetIssues().ToOneLineString());
            Result.emplace(id, std::nullopt);
        }

        if (Result.size() == Targets.size()) {
            Send(Parent, new TEvPrivate::TEvDescribeTargetsResult(Sender, ReplicationId, std::move(Result)));
            PassAway();
        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_CONTROLLER_TARGET_DESCRIBER;
    }

    explicit TTargetDescriber(const TActorId& sender, const TActorId& parent, ui64 rid, const TActorId& proxy, THashMap<ui64, TString>&& targets)
        : Sender(sender)
        , Parent(parent)
        , ReplicationId(rid)
        , YdbProxy(proxy)
        , Targets (std::move(targets))
        , LogPrefix("TargetDescriber", ReplicationId)
    {
    }

    void Bootstrap() {
        for (const auto& [id, _] : Targets) {
            DescribeTarget(id);
        }

        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvDescribeTableResponse, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Sender;
    const TActorId Parent;
    const ui64 ReplicationId;
    const TActorId YdbProxy;
    const THashMap<ui64, TString> Targets;
    const TActorLogPrefix LogPrefix;

    TEvPrivate::TEvDescribeTargetsResult::TResult Result;

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
        if (PubEv) {
            return ExecutePub(txc, ctx);
        } else if (PrivEv) {
            return ExecutePriv(txc, ctx);
        } else {
            Y_ABORT("unreachable");
        }
    }

    bool ExecutePub(TTransactionContext&, const TActorContext& ctx) {
        CLOG_D(ctx, "Execute: " << PubEv->Get()->ToString());

        const auto& record = PubEv->Get()->Record;
        const auto pathId = PathIdFromPathId(record.GetPathId());

        Replication = Self->Find(pathId);
        if (!Replication) {
            Result = MakeHolder<TEvController::TEvDescribeReplicationResult>();
            Result->Record.SetStatus(NKikimrReplication::TEvDescribeReplicationResult::NOT_FOUND);
            return true;
        }

        if (record.GetIncludeStats()) {
            for (ui64 tid = 0; tid < Replication->GetNextTargetId(); ++tid) {
                auto* target = Replication->FindTarget(tid);
                if (!target) {
                    continue;
                }

                TargetsToDescribe.emplace(tid, target->GetSrcPath());
            }

            if (TargetsToDescribe) {
                return true;
            }
        }

        return DescribeReplication(Replication);
    }

    bool ExecutePriv(TTransactionContext&, const TActorContext& ctx) {
        CLOG_D(ctx, "Execute: " << PrivEv->Get()->ToString());

        const auto rid = PrivEv->Get()->ReplicationId;

        Replication = Self->Find(rid);
        if (!Replication) {
            Result = MakeHolder<TEvController::TEvDescribeReplicationResult>();
            Result->Record.SetStatus(NKikimrReplication::TEvDescribeReplicationResult::NOT_FOUND);
            return true;
        }

        return DescribeReplication(Replication);
    }

    bool DescribeReplication(TReplication::TPtr replication) {
        Result = MakeHolder<TEvController::TEvDescribeReplicationResult>();
        Result->Record.SetStatus(NKikimrReplication::TEvDescribeReplicationResult::SUCCESS);
        Result->Record.MutableConnectionParams()->CopyFrom(replication->GetConfig().GetSrcConnectionParams());

        using TInitialScanProgress = NYdb::NTable::TChangefeedDescription::TInitialScanProgress;
        std::optional<TInitialScanProgress> totalScanProgress;

        if (PrivEv) {
            totalScanProgress = std::make_optional<TInitialScanProgress>();
        }

        for (ui64 tid = 0; tid < replication->GetNextTargetId(); ++tid) {
            auto* target = replication->FindTarget(tid);
            if (!target) {
                continue;
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

            if (PrivEv) {
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
        case TReplication::EState::Error:
            if (auto issue = state.MutableError()->AddIssues()) {
                issue->set_severity(NYql::TSeverityIds::S_ERROR);
                issue->set_message(replication->GetIssue());
            }
            break;
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        CLOG_D(ctx, "Complete");

        if (Result) {
            ctx.Send(Sender, Result.Release());
        } else if (TargetsToDescribe) {
            Y_ABORT_UNLESS(Replication);
            ctx.Register(new TTargetDescriber(Sender, ctx.SelfID,
                Replication->GetId(), Replication->GetYdbProxy(), std::move(TargetsToDescribe)));
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
