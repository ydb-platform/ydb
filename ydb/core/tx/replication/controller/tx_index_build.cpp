#include "controller_impl.h"
#include "dst_schema_changer.h"

#include <ydb/core/base/path.h>

#include <algorithm>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

namespace {

using TBuild = NKikimrReplication::TIndexBuildState;

ui64 GetCommitInterval(const TReplication& replication) {
    return replication.GetConfig().GetConsistencySettings().GetGlobal().GetCommitIntervalMilliSeconds();
}

TRowVersion IntervalEnd(const TRowVersion& version, ui64 interval) {
    return TRowVersion((version.Step / interval + 1) * interval, 0);
}

bool IsValidCheckpoint(const NKikimrReplication::TIndexBuildProgress& progress,
        const NKikimrReplication::TIndexBuildProgress& previous)
{
    if (progress.GetOffset() < previous.GetOffset()) {
        return false;
    }

    const auto maxVersion = TRowVersion::FromProto(progress.GetMaxVersion());
    const auto previousMaxVersion = TRowVersion::FromProto(previous.GetMaxVersion());
    if (previous.HasSwitchOffset()) {
        if (!progress.HasSwitchOffset() || progress.GetSwitchOffset() != previous.GetSwitchOffset()) {
            return false;
        }

        if (maxVersion != previousMaxVersion) {
            return false;
        }
    }

    const auto heartbeat = TRowVersion::FromProto(progress.GetHeartbeat());
    const auto previousHeartbeat = TRowVersion::FromProto(previous.GetHeartbeat());
    if (previous.HasHeartbeat() && heartbeat < previousHeartbeat) {
        return false;
    }

    return maxVersion >= previousMaxVersion
        && (!progress.HasSwitchOffset() || progress.GetSwitchOffset() <= progress.GetOffset());
}

} // namespace

bool TController::IsCancelledIndexBuild(const TBuild& build) {
    return build.GetPhase() == TBuild::CANCELLING || build.GetPhase() == TBuild::CANCELLED;
}

bool TController::IsJoinedIndexBuild(const TBuild& build, const TRowVersion& version) {
    return !IsCancelledIndexBuild(build)
        && build.GetPhase() != TBuild::FILLING
        && version >= TRowVersion::FromProto(build.GetJoinVersion());
}

void TController::SaveIndexBuild(NIceDb::TNiceDb& db, const std::pair<ui64, ui64>& key, const TBuild& build) {
    db.Table<Schema::Targets>().Key(key.first, key.second).Update(
        NIceDb::TUpdate<Schema::Targets::IndexBuild>(build.SerializeAsString())
    );
}

TVector<TString> TController::GetCommitTablePaths(const TRowVersion& version) const {
    TVector<TString> paths;
    const auto replication = GetSingle();
    if (!replication) {
        return paths;
    }

    for (const auto* target : replication->GetTargets()) {
        if (target->GetDstState() == TReplication::EDstState::Removing) {
            continue;
        }

        const auto it = IndexBuilds.find({replication->GetId(), target->GetId()});
        if (it != IndexBuilds.end() && !IsJoinedIndexBuild(it->second, version)) {
            continue;
        }

        paths.push_back(target->GetDstPath());
    }

    return paths;
}

bool TController::CanCommitIndexBuilds(const TRowVersion& version) const {
    for (const auto& [key, build] : IndexBuilds) {
        const auto replication = Find(key.first);
        const auto* target = replication ? replication->FindTarget(key.second) : nullptr;
        if (!target || target->GetDstState() == TReplication::EDstState::Removing || !IsJoinedIndexBuild(build, version)) {
            continue;
        }

        if (build.AssignmentsSize() || !CompleteWorkerSets.contains(key)) {
            return false;
        }

        const auto workers = target->GetWorkers();
        if (workers.empty()) {
            return false;
        }

        for (const auto wid : workers) {
            const auto it = IndexBuildProgress.find(TWorkerId(key.first, key.second, wid));
            if (it == IndexBuildProgress.end() || !it->second.HasHeartbeat()) {
                return false;
            }

            if (TRowVersion::FromProto(it->second.GetHeartbeat()) < version) {
                return false;
            }
        }
    }

    return true;
}

class TController::TTxIndexBuild: public TTxBase {
    TVector<std::pair<ui64, THolder<TEvService::TEvIndexBuildProgressResult>>> Checkpoints;
    TVector<std::pair<ui64, THolder<TEvService::TEvTxIdResult>>> Assignments;
    TVector<std::pair<ui64, TVector<TString>>> Commits;

    ui64 Allocate(const TRowVersion& end, NIceDb::TNiceDb& db) {
        const ui64 txId = Self->AllocatedTxIds.front();
        Self->AllocatedTxIds.pop_front();
        db.Table<Schema::TxIds>().Key(end.Step, end.TxId).Update(
            NIceDb::TUpdate<Schema::TxIds::WriteTxId>(txId)
        );
        Self->AssignedTxIds.emplace(end, txId);
        return txId;
    }

    void Checkpoint(NIceDb::TNiceDb& db) {
        while (!Self->PendingIndexBuildProgress.empty()) {
            auto ev = std::move(Self->PendingIndexBuildProgress.front());
            Self->PendingIndexBuildProgress.pop_front();
            const auto& record = ev->Get()->Record;
            const auto id = TWorkerId::Parse(record.GetWorker());
            const auto* target = Self->FindTarget(id);
            if (!target || !target->IsIndexBuild() || !Self->IsValidWorker(id) || !record.HasProgress()) {
                continue;
            }

            if (!Self->HasWorkerSession(id, ev->Sender.NodeId())) {
                continue;
            }

            const auto& progress = record.GetProgress();
            auto& previous = Self->IndexBuildProgress[id];
            if (!IsValidCheckpoint(progress, previous)) {
                continue;
            }

            previous.CopyFrom(progress);
            db.Table<Schema::IndexBuildWorkers>().Key(id.ReplicationId(), id.TargetId(), id.WorkerId()).Update(
                NIceDb::TUpdate<Schema::IndexBuildWorkers::Progress>(progress.SerializeAsString())
            );
            auto reply = MakeHolder<TEvService::TEvIndexBuildProgressResult>();
            reply->Record.CopyFrom(record);
            reply->Record.MutableController()->SetTabletId(Self->TabletID());
            reply->Record.MutableController()->SetGeneration(Self->Executor()->Generation());
            Checkpoints.emplace_back(ev->Sender.NodeId(), std::move(reply));
        }
    }

    bool CanReuseLastGlobalAssignment(const TRowVersion& join) const {
        if (Self->AssignedTxIds.size() < MaxOpenTxIds) {
            return false;
        }

        const auto& last = *Self->AssignedTxIds.rbegin();
        return last.first > join
            && last.second != Self->CommittingTxId;
    }

    void Assign(NIceDb::TNiceDb& db) {
        for (auto it = Self->PendingIndexTxIds.begin(); it != Self->PendingIndexTxIds.end();) {
            const auto id = TWorkerId::Parse((*it)->Get()->Record.GetWorker());
            const auto key = std::make_pair(id.ReplicationId(), id.TargetId());
            auto buildIt = Self->IndexBuilds.find(key);
            auto* target = Self->FindTarget(id);
            const bool activeBuild = buildIt != Self->IndexBuilds.end() && !IsCancelledIndexBuild(buildIt->second);
            const bool activeTarget = target && target->GetDstState() != TReplication::EDstState::Removing;
            if (!activeBuild || !activeTarget || !Self->HasWorkerSession(id, (*it)->Sender.NodeId())) {
                it = Self->PendingIndexTxIds.erase(it);
                continue;
            }

            auto& build = buildIt->second;
            const auto replication = Self->Find(key.first);
            const auto interval = GetCommitInterval(*replication);
            auto reply = MakeHolder<TEvService::TEvTxIdResult>(Self->TabletID(), Self->Executor()->Generation());
            id.Serialize(*reply->Record.MutableWorker());
            bool blocked = false;
            for (const auto& v : (*it)->Get()->Record.GetVersions()) {
                const auto version = TRowVersion::FromProto(v);
                auto end = IntervalEnd(version, interval);
                const auto join = TRowVersion::FromProto(build.GetJoinVersion());
                const bool global = build.GetPhase() != TBuild::FILLING && version >= join;
                ui64 txId = 0;
                if (global) {
                    auto assigned = Self->AssignedTxIds.lower_bound(end);
                    if (assigned != Self->AssignedTxIds.end()) {
                        end = assigned->first;
                        txId = assigned->second;
                    } else if (CanReuseLastGlobalAssignment(join)) {
                        auto node = Self->AssignedTxIds.extract(Self->AssignedTxIds.rbegin()->first);
                        db.Table<Schema::TxIds>().Key(node.key().Step, node.key().TxId).Delete();
                        node.key() = end;
                        txId = node.mapped();
                        db.Table<Schema::TxIds>().Key(end.Step, end.TxId).Update(
                            NIceDb::TUpdate<Schema::TxIds::WriteTxId>(txId)
                        );
                        Self->AssignedTxIds.insert(std::move(node));
                    } else if (!Self->AllocatedTxIds.empty() && Self->AssignedTxIds.size() < MaxOpenTxIds + 1) {
                        txId = Allocate(end, db);
                    }
                } else {
                    if (build.GetPhase() != TBuild::FILLING) {
                        end = Min(end, join);
                    }

                    for (const auto& assigned : build.GetAssignments()) {
                        if (TRowVersion::FromProto(assigned.GetVersion()) >= end) {
                            txId = assigned.GetWriteTxId();
                            end = TRowVersion::FromProto(assigned.GetVersion());
                            break;
                        }
                    }

                    if (!txId && build.AssignmentsSize() >= MaxOpenTxIds) {
                        auto& last = *build.MutableAssignments(build.AssignmentsSize() - 1);
                        txId = last.GetWriteTxId();
                        // An in-flight commit cannot be expanded; wait for it
                        // and allocate its successor from the same scope.
                        if (Self->IndexCommits.contains(txId)) {
                            txId = 0;
                        } else {
                            end.ToProto(last.MutableVersion());
                            Self->SaveIndexBuild(db, key, build);
                        }
                    }

                    if (!txId && !Self->AllocatedTxIds.empty() && build.AssignmentsSize() < MaxOpenTxIds) {
                        txId = Self->AllocatedTxIds.front();
                        Self->AllocatedTxIds.pop_front();
                        auto& assigned = *build.AddAssignments();
                        end.ToProto(assigned.MutableVersion());
                        assigned.SetWriteTxId(txId);
                        std::sort(build.MutableAssignments()->begin(), build.MutableAssignments()->end(),
                            [](const auto& lhs, const auto& rhs) {
                                return TRowVersion::FromProto(lhs.GetVersion()) < TRowVersion::FromProto(rhs.GetVersion());
                            });
                        Self->SaveIndexBuild(db, key, build);
                    }
                }

                if (!txId) {
                    blocked = true;
                    continue;
                }

                auto& assigned = *reply->Record.AddVersionTxIds();
                end.ToProto(assigned.MutableVersion());
                assigned.SetTxId(txId);
                (global ? join : TRowVersion::Min()).ToProto(assigned.MutableBegin());
            }

            if (reply->Record.VersionTxIdsSize()) {
                Assignments.emplace_back((*it)->Sender.NodeId(), std::move(reply));
            }

            if (blocked) {
                ++it;
            } else {
                it = Self->PendingIndexTxIds.erase(it);
            }
        }
    }

    void Progress(NIceDb::TNiceDb& db) {
        for (auto& [key, build] : Self->IndexBuilds) {
            auto replication = Self->Find(key.first);
            auto* target = replication ? replication->FindTarget(key.second) : nullptr;
            if (!target || target->GetDstState() == TReplication::EDstState::Removing) {
                continue;
            }

            const bool unfinished = build.GetPhase() == TBuild::FILLING || build.GetPhase() == TBuild::JOINING;
            const bool finishing = replication->GetDesiredState() == TReplication::EState::Done;
            if (unfinished && finishing && !Self->HasActiveSchemaBarrier(key.first)) {
                // Stop every writer before dropping its table. Persist this
                // phase and its paused destination before starting cancellation.
                build.SetPhase(TBuild::CANCELLING);
                target->SetDstState(TReplication::EDstState::Paused);
                db.Table<Schema::Targets>().Key(key.first, key.second).Update(
                    NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState())
                );
                Self->SaveIndexBuild(db, key, build);
            }

            if (IsCancelledIndexBuild(build)) {
                continue;
            }

            auto& assignments = *build.MutableAssignments();
            for (auto it = assignments.begin(); it != assignments.end();) {
                if (Self->CompletedIndexCommits.erase(it->GetWriteTxId())) {
                    it = assignments.erase(it);
                    Self->SaveIndexBuild(db, key, build);
                } else {
                    ++it;
                }
            }
            if (!Self->CompleteWorkerSets.contains(key)) {
                continue;
            }

            const auto workers = target->GetWorkers();
            if (workers.empty()) {
                continue;
            }

            auto heartbeat = TRowVersion::Max();
            auto maxVersion = TRowVersion::Min();
            // Retired partitions may have applied scan rows newer than the
            // current heartbeat. Their persisted maximum still bounds C.
            for (const auto& [id, progress] : Self->IndexBuildProgress) {
                if (id.ReplicationId() == key.first && id.TargetId() == key.second) {
                    maxVersion = Max(maxVersion, TRowVersion::FromProto(progress.GetMaxVersion()));
                }
            }

            bool complete = true;
            for (const auto wid : workers) {
                const auto it = Self->IndexBuildProgress.find(TWorkerId(key.first, key.second, wid));
                if (it == Self->IndexBuildProgress.end() || !it->second.HasSwitchOffset() || !it->second.HasHeartbeat()) {
                    complete = false;
                    break;
                }

                heartbeat = Min(heartbeat, TRowVersion::FromProto(it->second.GetHeartbeat()));
                maxVersion = Max(maxVersion, TRowVersion::FromProto(it->second.GetMaxVersion()));
            }

            if (!complete) {
                continue;
            }

            if (build.GetPhase() == TBuild::FILLING && heartbeat > maxVersion && !Self->AllocatedTxIds.empty()) {
                const auto interval = GetCommitInterval(*replication);
                // Every already issued interval ends before or at C. Taking
                // the next boundary after the last global interval also
                // excludes an already proposed commit, including after reboot.
                auto join = Max(IntervalEnd(heartbeat, interval),
                    IntervalEnd(Self->CommittedVersions[key.first], interval));
                if (!Self->AssignedTxIds.empty()) {
                    join = Max(join, IntervalEnd(Self->AssignedTxIds.rbegin()->first, interval));
                }

                for (const auto& assigned : build.GetAssignments()) {
                    join = Max(join, TRowVersion::FromProto(assigned.GetVersion()));
                }
                join.ToProto(build.MutableJoinVersion());
                build.SetPhase(TBuild::JOINING);
                Allocate(join, db); // Force the first joint commit even with no further data.
                Self->SaveIndexBuild(db, key, build);
            }

            if (build.AssignmentsSize()) {
                const auto& assigned = build.GetAssignments(0);
                const auto version = TRowVersion::FromProto(assigned.GetVersion());
                if (heartbeat >= version && !Self->IndexCommits.contains(assigned.GetWriteTxId())) {
                    Self->IndexCommits.emplace(assigned.GetWriteTxId(), key);
                    Commits.emplace_back(assigned.GetWriteTxId(), TVector<TString>{target->GetDstPath()});
                }
            }
        }
    }

public:
    explicit TTxIndexBuild(TController* self)
        : TTxBase("TxIndexBuild", self)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_INDEX_BUILD;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        Checkpoint(db);
        Progress(db);
        Assign(db);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Self->IndexBuildTxInFlight = false;
        for (auto& [node, ev] : Checkpoints) {
            ctx.Send(MakeReplicationServiceId(node), std::move(ev));
        }
        for (auto& [node, ev] : Assignments) {
            ctx.Send(MakeReplicationServiceId(node), std::move(ev));
        }
        for (auto& [txId, paths] : Commits) {
            ctx.Send(MakeTxProxyID(), MakeCommitProposal(txId, paths).Release(), 0, txId);
        }
        for (const auto& [key, build] : Self->IndexBuilds) {
            if (build.GetPhase() == TBuild::CANCELLING) {
                auto replication = Self->Find(key.first);
                auto* target = replication ? replication->FindTarget(key.second) : nullptr;
                if (target) {
                    target->Shutdown(ctx);
                    const auto workers = target->GetWorkers();
                    for (const auto wid : workers) {
                        const TWorkerId id(key.first, key.second, wid);
                        if (!Self->RemoveQueue.contains(id)) {
                            ctx.Send(ctx.SelfID, new TEvPrivate::TEvRemoveWorker(key.first, key.second, wid));
                        }
                    }
                    if (workers.empty()) {
                        Self->StartIndexReady(key, ctx);
                    }
                }
            } else if (build.GetPhase() == TBuild::MAKING_READY) {
                Self->StartIndexReady(key, ctx);
            }
        }
        if (!Self->IndexBuilds.empty() || !Self->PendingIndexTxIds.empty()) {
            Self->AllocateTxIds(ctx);
        }

        Self->RunTxHeartbeat(ctx);
    }
};

void TController::RunTxIndexBuild(const TActorContext& ctx) {
    if (!IndexBuildTxInFlight && !IndexBuilds.empty()) {
        IndexBuildTxInFlight = true;
        Execute(new TTxIndexBuild(this), ctx);
    }
}

void TController::Handle(TEvService::TEvIndexBuildProgress::TPtr& ev, const TActorContext& ctx) {
    PendingIndexBuildProgress.push_back(std::move(ev));
    RunTxIndexBuild(ctx);
}

void TController::StartIndexReady(const std::pair<ui64, ui64>& key, const TActorContext& ctx) {
    if (IndexReadyActors.contains(key)) {
        return;
    }

    auto replication = Find(key.first);
    auto* target = replication ? replication->FindTarget(key.second) : nullptr;
    const auto* base = target ? replication->FindBaseTableTarget(*target) : nullptr;
    if (!base) {
        return;
    }

    const auto& build = IndexBuilds.at(key);
    const bool cancelling = build.GetPhase() == TBuild::CANCELLING;
    const bool finishing = replication->GetDesiredState() == TReplication::EState::Done;
    const auto state = replication->GetState();
    if (state != TReplication::EState::Ready && !(finishing && state == TReplication::EState::Paused)) {
        return;
    }

    const bool paused = target->GetDstState() == TReplication::EDstState::Paused;
    if (cancelling) {
        if (!paused || !target->GetWorkers().empty()) {
            return;
        }
    } else if (target->GetDstState() != TReplication::EDstState::Ready && !(finishing && paused)) {
        return;
    }

    const TSchemaChangeDstAlterSettings settings{
        .TxId = build.GetReadyTxId(),
        .GlobalConsistency = true,
        .IndexName = SplitPath(target->GetSrcPath()).back(),
        .SnapshotTxId = build.GetSnapshotTxId(),
        .CancelIndex = cancelling,
    };
    IndexReadyActors[key] = ctx.Register(CreateSchemaChangeDstAlterer(ctx.SelfID,
        replication->GetSchemeShardId(), key.first, key.second, TReplication::ETargetKind::Table,
        base->GetDstPathId(), {}, settings));
}

class TController::TTxIndexReady: public TTxBase {
    TEvPrivate::TEvSchemaChangeDstAlterTxId::TPtr TxIdEvent;
    TEvPrivate::TEvSchemaChangeDstAlterResult::TPtr ResultEvent;
    ui64 SavedTxId = 0;
    bool Finished = false;
    TReplication::TPtr Replication;

public:
    TTxIndexReady(TController* self, TEvPrivate::TEvSchemaChangeDstAlterTxId::TPtr& ev)
        : TTxBase("TxIndexReadyTxId", self)
        , TxIdEvent(ev)
    {}

    TTxIndexReady(TController* self, TEvPrivate::TEvSchemaChangeDstAlterResult::TPtr& ev)
        : TTxBase("TxIndexReadyResult", self)
        , ResultEvent(ev)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_SCHEMA_CHANGE_REPORT;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto key = TxIdEvent
            ? std::make_pair(TxIdEvent->Get()->ReplicationId, TxIdEvent->Get()->TargetId)
            : std::make_pair(ResultEvent->Get()->ReplicationId, ResultEvent->Get()->TargetId);
        const auto sender = TxIdEvent ? TxIdEvent->Sender : ResultEvent->Sender;
        const auto actor = Self->IndexReadyActors.find(key);
        const auto it = Self->IndexBuilds.find(key);
        Replication = Self->Find(key.first);
        auto* target = Replication ? Replication->FindTarget(key.second) : nullptr;
        if (actor == Self->IndexReadyActors.end() || actor->second != sender) {
            return true;
        }

        if (it == Self->IndexBuilds.end() || !target || target->GetDstState() == TReplication::EDstState::Removing) {
            return true;
        }

        auto& build = it->second;
        if (build.GetPhase() != TBuild::MAKING_READY && build.GetPhase() != TBuild::CANCELLING) {
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        if (TxIdEvent) {
            if (!build.GetReadyTxId()) {
                build.SetReadyTxId(TxIdEvent->Get()->TxId);
            }

            SavedTxId = build.GetReadyTxId();
        } else {
            if (ResultEvent->Get()->DstAlterTxId != build.GetReadyTxId()) {
                return true;
            }

            Finished = true;
            if (ResultEvent->Get()->IsSuccess()) {
                if (build.GetPhase() == TBuild::CANCELLING) {
                    build.SetPhase(TBuild::CANCELLED);
                    build.ClearAssignments();
                    // The implementation table was removed by CancelIndexBuild.
                    // The deferred DONE alter must not try to detach it.
                    target->SetDstPathId({});
                    db.Table<Schema::Targets>().Key(key.first, key.second).Update(
                        NIceDb::TUpdate<Schema::Targets::DstPathOwnerId>(InvalidOwnerId),
                        NIceDb::TUpdate<Schema::Targets::DstPathLocalId>(InvalidLocalPathId)
                    );
                    for (auto progress = Self->IndexBuildProgress.begin(); progress != Self->IndexBuildProgress.end();) {
                        const auto& id = progress->first;
                        if (id.ReplicationId() == key.first && id.TargetId() == key.second) {
                            db.Table<Schema::IndexBuildWorkers>().Key(key.first, key.second, id.WorkerId()).Delete();
                            Self->IndexBuildProgress.erase(progress++);
                        } else {
                            ++progress;
                        }
                    }
                } else {
                    build.SetPhase(TBuild::READY);
                }
            } else {
                target->SetDstState(TReplication::EDstState::Error);
                target->SetIssue(ResultEvent->Get()->Error);
                Replication->SetState(TReplication::EState::Error, ResultEvent->Get()->Error);
                db.Table<Schema::Targets>().Key(key.first, key.second).Update(
                    NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState()),
                    NIceDb::TUpdate<Schema::Targets::Issue>(target->GetIssue())
                );
                db.Table<Schema::Replications>().Key(key.first).Update(
                    NIceDb::TUpdate<Schema::Replications::State>(Replication->GetState()),
                    NIceDb::TUpdate<Schema::Replications::Issue>(Replication->GetIssue())
                );
            }
        }

        Self->SaveIndexBuild(db, key, build);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (SavedTxId) {
            ctx.Send(TxIdEvent->Sender, new TEvPrivate::TEvSchemaChangeDstAlterTxIdSaved(SavedTxId));
        }

        if (Finished) {
            Self->IndexReadyActors.erase(std::make_pair(ResultEvent->Get()->ReplicationId, ResultEvent->Get()->TargetId));
            Replication->Progress(ctx);
            if (Self->DeferredAlters.contains(Replication->GetId()) && !Self->HasPendingAlter(Replication->GetId())) {
                ctx.Send(ctx.SelfID, new TEvPrivate::TEvResumeDeferredAlter(Replication->GetId()));
            }
        }
    }
};

void TController::RunTxIndexReadyTxId(TEvPrivate::TEvSchemaChangeDstAlterTxId::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxIndexReady(this, ev), ctx);
}

void TController::RunTxIndexReadyResult(TEvPrivate::TEvSchemaChangeDstAlterResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxIndexReady(this, ev), ctx);
}

} // namespace NKikimr::NReplication::NController
