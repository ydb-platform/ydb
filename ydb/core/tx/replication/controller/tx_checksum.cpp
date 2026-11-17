#include "controller_impl.h"
#include "checksum_admission.h"
#include "checksum_source.h"

#include <ydb/core/base/appdata.h>
#include <google/protobuf/util/time_util.h>
#include <util/generic/algorithm.h>

namespace NKikimr::NReplication::NController {
namespace {
using TExecution = NKikimrReplication::TTableChecksumExecution;
using TTime = google::protobuf::util::TimeUtil;
}

class TController::TTxChecksum: public TTxBase {
public:
    TTxChecksum(TController* self, std::function<void(NIceDb::TNiceDb&)> execute,
        std::function<void(const TActorContext&)> complete)
        : TTxBase("TxChecksum", self)
        , ExecuteAction(std::move(execute))
        , CompleteAction(std::move(complete))
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CHECKSUM;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        ExecuteAction(db);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        CompleteAction(ctx);
    }

private:
    std::function<void(NIceDb::TNiceDb&)> ExecuteAction;
    std::function<void(const TActorContext&)> CompleteAction;
};

void TController::RunChecksumTx(std::function<void(NIceDb::TNiceDb&)> execute,
    std::function<void(const TActorContext&)> complete)
{
    Execute(new TTxChecksum(this, std::move(execute), std::move(complete)), ActorContext());
}

void TController::SaveChecksum(NIceDb::TNiceDb& db, TChecksumKey key) {
    const auto replication = Find(key.first);
    if (!replication || !replication->FindTarget(key.second)) {
        return;
    }
    db.Table<Schema::Targets>().Key(key.first, key.second).Update(
        NIceDb::TUpdate<Schema::Targets::ChecksumExecution>(ChecksumExecutions.at(key).SerializeAsString()));
    if (const auto it = ChecksumResults.find(key); it != ChecksumResults.end()) {
        db.Table<Schema::Targets>().Key(key.first, key.second).Update(
            NIceDb::TUpdate<Schema::Targets::ChecksumStatus>(it->second.SerializeAsString()));
    }
}

void TController::RebuildChecksumTargets() {
    ChecksumTargets.clear();
    for (const auto& [rid, replication] : Replications) {
        for (const auto* target : replication->GetTargets()) {
            if (target->GetKind() != TReplication::ETargetKind::Table || !target->GetDstPathId()) {
                continue;
            }
            const auto [it, inserted] = ChecksumTargets.emplace(target->GetDstPathId(), TChecksumKey{rid, target->GetId()});
            if (!inserted) {
                it->second = {}; // An ambiguous destination must never select an arbitrary source.
            }
        }
    }
}

void TController::Handle(TEvController::TEvCheckTable::TPtr& ev, const TActorContext& ctx) {
    const auto& request = ev->Get()->Record;
    auto reply = MakeHolder<TEvController::TEvCheckTableResult>();
    auto& response = reply->Record;
    *response.MutableDestinationPathId() = request.GetDestinationPathId();
    response.SetRequestId(request.GetRequestId());
    const auto target = ChecksumTargets.find(TPathId::FromProto(request.GetDestinationPathId()));
    if (request.GetRequestId().empty() || target == ChecksumTargets.end() || !target->second.first) {
        response.SetStatus(Ydb::StatusIds::SCHEME_ERROR);
        response.SetIssue("Expected a request ID and an unambiguous replicated base table");
    } else {
        const auto key = target->second;
        auto& state = ChecksumExecutions[key];
        if (state.GetRequestId() == request.GetRequestId()) {
            if (state.GetPhase() == TExecution::FINISHED) {
                response = state.GetResponse();
            } else {
                auto& waiters = ChecksumRuntime[key].Waiters;
                const auto waiter = std::make_pair(ev->Sender, ev->Cookie);
                if (std::find(waiters.begin(), waiters.end(), waiter) == waiters.end()) {
                    waiters.push_back(waiter);
                }
                return;
            }
        } else if (!AppData()->FeatureFlags.GetEnableAsyncReplicationChecksumCheck()) {
            response.SetStatus(Ydb::StatusIds::PRECONDITION_FAILED);
            response.SetIssue("Replication checksum checks are disabled");
        } else if (state.GetPhase() != TExecution::FINISHED || state.GetReleasePending()) {
            response.SetStatus(Ydb::StatusIds::OVERLOADED);
            response.SetDeferred(true);
            response.SetIssue("A checksum request is already outstanding for this table");
        } else {
            const auto replication = Find(key.first);
            const auto issue = ValidateChecksum(*replication, true);
            if (issue || GetSingle() != replication) {
                response.SetStatus(Ydb::StatusIds::PRECONDITION_FAILED);
                response.SetDeferred(true);
                response.SetIssue(issue ? issue : "Expected one GLOBAL replication");
            } else {
                auto& runtime = ChecksumRuntime[key];
                runtime.Waiters.emplace_back(ev->Sender, ev->Cookie);
                runtime.TransitionInFlight = true;
                const auto record = request;
                RunChecksumTx([this, key, record](NIceDb::TNiceDb& db) {
                    if (!ChecksumExecutions.contains(key)) {
                        return;
                    }
                    auto& execution = ChecksumExecutions[key];
                    execution.Clear();
                    execution.SetRequestId(record.GetRequestId());
                    *execution.MutableDestinationPathId() = record.GetDestinationPathId();
                    execution.SetPhase(TExecution::PREFLIGHT);
                    execution.SetDeadlineUs((TActivationContext::Now() + TDuration::Seconds(30)).MicroSeconds());
                    SaveChecksum(db, key);
                }, [this, key](const TActorContext& ctx) {
                    if (!ChecksumExecutions.contains(key)) {
                        return;
                    }
                    ChecksumRuntime[key].TransitionInFlight = false;
                    ProgressChecksums(ctx);
                });
                return;
            }
        }
    }
    ctx.Send(ev->Sender, reply.Release(), 0, ev->Cookie);
}

void TController::Handle(TEvController::TEvCancelCheckTable::TPtr& ev, const TActorContext& ctx) {
    for (const auto& [key, state] : ChecksumExecutions) {
        if (state.GetRequestId() == ev->Get()->Record.GetRequestId()
            && TPathId::FromProto(state.GetDestinationPathId()) == TPathId::FromProto(ev->Get()->Record.GetDestinationPathId()))
        {
            if (ChecksumRuntime[key].TransitionInFlight) {
                ChecksumRuntime[key].PendingEvents.emplace_back(ev.Release());
                return;
            }
            FinishChecksum(key, Ydb::StatusIds::CANCELLED, "Checksum cancelled", false, ctx);
            return;
        }
    }
}

bool TController::IsChecksumBoundaryTxId(ui64 writeTxId) const {
    return AnyOf(ChecksumExecutions, [writeTxId](const auto& item) {
        return item.second.GetPhase() != TExecution::FINISHED && item.second.GetBoundaryWriteTxId() == writeTxId;
    });
}

bool TController::ChecksumAssignmentPaused() const {
    return AnyOf(ChecksumExecutions, [](const auto& item) {
        return item.second.GetPhase() == TExecution::PREPARING;
    });
}

TRowVersion TController::GetChecksumBoundary() const {
    const auto replication = GetSingle();
    if (!replication) {
        return TRowVersion::Min();
    }
    const auto it = ChecksumBoundaries.find(replication->GetId());
    return it == ChecksumBoundaries.end() ? TRowVersion::Min() : it->second;
}
TString TController::ValidateChecksum(const TReplication& replication, bool requireSessions) const {
    if (!replication.GetConfig().GetConsistencySettings().HasGlobal() || replication.GetConfig().HasTransferSpecific()) {
        return "Checksum checks require GLOBAL table replication";
    }
    if (replication.GetState() != TReplication::EState::Ready || replication.GetDesiredState() != TReplication::EState::Ready || HasPendingAlter(replication.GetId())
        || HasActiveSchemaBarrier(replication.GetId())) {
        return "Replication must be running with stable schemas";
    }
    bool hasTables = false;
    for (auto* target : replication.GetTargets()) {
        if (target->GetDstState() != TReplication::EDstState::Ready) {
            return "Replication targets are not ready";
        }
        if (target->IsIndexBuild()) {
            const auto build = IndexBuilds.find({replication.GetId(), target->GetId()});
            if (build == IndexBuilds.end() || build->second.GetPhase() != NKikimrReplication::TIndexBuildState::READY) {
                return "Replication index build is not complete";
            }
        }
        hasTables |= target->GetKind() == TReplication::ETargetKind::Table;
        if (requireSessions && !CompleteWorkerSets.contains(std::make_pair(replication.GetId(), target->GetId()))) {
            return "Replication worker discovery is not complete";
        }
    }
    if (!hasTables) {
        return "Replication has no base tables";
    }
    for (const auto& [id, worker] : Workers) {
        if (id.ReplicationId() != replication.GetId()) {
            continue;
        }
        if (!worker.HasSession()) {
            if (requireSessions) {
                return "Replication workers are not connected";
            }
            continue;
        }
        const auto it = Sessions.find(worker.GetSession());
        if (it == Sessions.end() || !it->second.SupportsChecksumRanges) {
            return "Replication service does not support checksum transaction ranges";
        }
    }
    return {};
}

TRowVersion TController::AdjustTxIdVersion(TRowVersion version) const {
    const auto replication = GetSingle();
    Y_ABORT_UNLESS(replication);
    const auto interval = replication->GetConfig().GetConsistencySettings().GetGlobal().GetCommitIntervalMilliSeconds();
    auto adjusted = TRowVersion(interval * (version.Step / interval + 1), 0);
    const auto boundary = GetChecksumBoundary();
    if (version < boundary) {
        adjusted = Min(adjusted, boundary);
    }
    return adjusted;
}

void TController::RebuildPendingTxIds() {
    PendingTxId.clear();
    for (const auto& [version, nodes] : RawPendingTxId) {
        auto& pending = PendingTxId[AdjustTxIdVersion(version)];
        pending.insert(nodes.begin(), nodes.end());
    }
}

TRowVersion TController::GetTxIdBegin(TRowVersion end) const {
    const auto it = AssignedTxIds.lower_bound(end);
    if (it != AssignedTxIds.end()) {
        const auto begin = AssignedTxIdBegins.find(it->first);
        if (begin != AssignedTxIdBegins.end()) {
            return begin->second;
        }
    }
    return end > GetChecksumBoundary() ? GetChecksumBoundary() : TRowVersion::Min();
}


void TController::StopChecksum(TChecksumKey key, const TActorContext& ctx) {
    auto& runtime = ChecksumRuntime[key];
    if (runtime.Source) {
        ctx.Send(std::exchange(runtime.Source, {}), new TEvents::TEvPoison());
    }
    if (runtime.Destination) {
        ctx.Send(std::exchange(runtime.Destination, {}), new TEvents::TEvPoison());
    }
    runtime.SourceDone = false;
    runtime.DestinationDone = false;
    runtime.DestinationHold = {};
    runtime.Retained = false;
    runtime.Retaining = false;
    runtime.SourceSchema.clear();
    runtime.DestinationSchema.clear();
    EraseNodesIf(ChecksumRetainCookies, [key](const auto& item) {
        return item.second.Key == key;
    });
}

void TController::StartChecksumAdmission(TChecksumKey key, const TActorContext& ctx) {
    const auto& state = ChecksumExecutions.at(key);
    NKikimrReplication::TEvChecksumAdmission request;
    request.SetControllerId(TabletID());
    request.SetRequestId(state.GetRequestId());
    *request.MutableDestinationPathId() = state.GetDestinationPathId();
    request.SetRelease(state.GetReleasePending());
    ChecksumRuntime[key].Admission = ctx.Register(CreateChecksumAdmission(SelfId(), std::move(request)));
}

void TController::FinishChecksum(TChecksumKey key, Ydb::StatusIds::StatusCode status, TString issue,
    bool deferred, const TActorContext& ctx)
{
    auto& state = ChecksumExecutions.at(key);
    auto& runtime = ChecksumRuntime[key];
    if (state.GetPhase() == TExecution::FINISHED || runtime.TransitionInFlight) {
        return;
    }
    runtime.TransitionInFlight = true;
    StopChecksum(key, ctx);
    RunChecksumTx([this, key, status, issue = std::move(issue), deferred](NIceDb::TNiceDb& db) {
        if (!ChecksumExecutions.contains(key)) {
            return;
        }
        auto& execution = ChecksumExecutions.at(key);
        execution.SetReleasePending(execution.GetPhase() != TExecution::PREFLIGHT);
        execution.SetPhase(TExecution::FINISHED);
        auto& response = *execution.MutableResponse();
        response.SetRequestId(execution.GetRequestId());
        *response.MutableDestinationPathId() = execution.GetDestinationPathId();
        response.SetStatus(status);
        response.SetIssue(issue);
        response.SetDeferred(deferred);
        if (!deferred) {
            auto& result = ChecksumResults[key];
            result.set_status(status);
            result.clear_issues();
            if (issue) {
                result.add_issues()->set_message(issue);
            }
            *result.mutable_finished_at() = TTime::MicrosecondsToTimestamp(TActivationContext::Now().MicroSeconds());
            if (status == Ydb::StatusIds::SUCCESS) {
                auto& comparison = *result.mutable_last_result();
                *comparison.mutable_source() = execution.GetSource();
                *comparison.mutable_destination() = execution.GetDestination();
                comparison.set_matches(NChecksum::Equal(execution.GetSource(), execution.GetDestination()));
                *comparison.mutable_finished_at() = result.finished_at();
            }
            *response.MutableChecksumStatus() = result;
        }
        SaveChecksum(db, key);
    }, [this, key](const TActorContext& ctx) {
        if (!ChecksumExecutions.contains(key)) {
            return;
        }
        auto& runtime = ChecksumRuntime[key];
        runtime.TransitionInFlight = false;
        runtime.Recovery = false;
        const auto& state = ChecksumExecutions.at(key);
        if (state.GetReleasePending() && runtime.Admission) {
            ctx.Send(runtime.Admission, new TEvents::TEvPoison());
        }
        for (const auto& [sender, cookie] : std::exchange(runtime.Waiters, {})) {
            auto reply = MakeHolder<TEvController::TEvCheckTableResult>();
            reply->Record = state.GetResponse();
            ctx.Send(sender, reply.Release(), 0, cookie);
        }
        RunTxAssignTxId(ctx);
        RunTxIndexBuild(ctx);
        ProgressChecksums(ctx);
    });
}

void TController::StartChecksumSource(TChecksumKey key, bool probe, const TActorContext& ctx) {
    const auto replication = Find(key.first);
    const auto* target = replication->FindTarget(key.second);
    const auto& execution = ChecksumExecutions.at(key);
    Ydb::Replication::StreamChecksumTablesInternalRequest request;
    auto minimum = CommittedVersions[key.first];
    if (!AssignedTxIds.empty()) {
        minimum = Max(minimum, AssignedTxIds.rbegin()->first);
    }
    NChecksum::ToProto(minimum, request.mutable_minimum_snapshot());
    request.set_probe_only(probe);
    auto* table = request.add_tables();
    table->set_id(key.second);
    table->set_path(target->GetSrcPath());
    table->set_changefeed_name(target->GetStreamName());
    const auto deadline = TInstant::MicroSeconds(execution.GetDeadlineUs());
    auto& runtime = ChecksumRuntime[key];
    if (replication->GetConfig().GetSrcConnectionParams().GetEndpoint().empty()) {
        NChecksum::TSettings settings;
        settings.Database = replication->GetDatabase();
        settings.Request = std::move(request);
        settings.Deadline = deadline;
        runtime.Source = ctx.Register(NChecksum::CreateChecksumTables(SelfId(), std::move(settings)));
    } else {
        runtime.Source = ctx.Register(CreateChecksumSource(SelfId(), replication->GetYdbProxy(), std::move(request), deadline));
    }
}

void TController::ProgressChecksums(const TActorContext& ctx) {
    // A failed execution may still have an installed, uncommitted boundary.
    bool barrierBusy = AssignedTxIds.contains(GetChecksumBoundary()) || AnyOf(ChecksumExecutions, [this](const auto& item) {
        const auto phase = item.second.GetPhase();
        return phase == TExecution::PREPARING || phase == TExecution::SYNCHRONIZING
            || (phase == TExecution::CHECKSUMMING && !ChecksumRuntime[item.first].Destination);
    });
    bool active = false;
    TVector<TChecksumKey> removed;
    for (auto& [key, execution] : ChecksumExecutions) {
        auto& runtime = ChecksumRuntime[key];
        if (runtime.TransitionInFlight) {
            const auto replication = Find(key.first);
            if (execution.GetPhase() != TExecution::FINISHED && (!replication
                || replication->GetState() != TReplication::EState::Ready
                || replication->GetDesiredState() != TReplication::EState::Ready))
            {
                auto cancel = MakeHolder<TEvController::TEvCancelCheckTable>();
                cancel->Record.SetRequestId(execution.GetRequestId());
                *cancel->Record.MutableDestinationPathId() = execution.GetDestinationPathId();
                runtime.PendingEvents.emplace_back(new IEventHandle(SelfId(), SelfId(), cancel.Release()));
            }
            active = true;
            continue;
        }
        if (!runtime.PendingEvents.empty()) {
            for (auto& event : std::exchange(runtime.PendingEvents, {})) {
                ctx.Send(event.Release());
            }
            active = true;
            continue;
        }
        if (execution.GetReleasePending() && !runtime.Admission) {
            StartChecksumAdmission(key, ctx);
        }
        if (execution.GetPhase() == TExecution::FINISHED) {
            active |= execution.GetReleasePending();
            const auto replication = Find(key.first);
            if (!execution.GetReleasePending() && (!replication || !replication->FindTarget(key.second))) {
                removed.push_back(key);
            }
            continue;
        }
        active = true;
        const auto replication = Find(key.first);
        const auto* target = replication ? replication->FindTarget(key.second) : nullptr;
        if (runtime.Recovery) {
            FinishChecksum(key, Ydb::StatusIds::UNAVAILABLE, "Checksum interrupted by controller restart", false, ctx);
            continue;
        }
        if (!target || target->GetDstPathId() != TPathId::FromProto(execution.GetDestinationPathId())) {
            FinishChecksum(key, Ydb::StatusIds::SCHEME_ERROR, "Checksum target was removed or replaced", false, ctx);
            continue;
        }
        if (execution.GetDeadlineUs() && ctx.Now().MicroSeconds() >= execution.GetDeadlineUs()) {
            FinishChecksum(key, Ydb::StatusIds::TIMEOUT, "Checksum deadline expired", false, ctx);
            continue;
        }
        if (const auto issue = ValidateChecksum(*replication, false)) {
            FinishChecksum(key, Ydb::StatusIds::PRECONDITION_FAILED, issue, execution.GetPhase() == TExecution::PREFLIGHT, ctx);
            continue;
        }
        if (execution.GetPhase() == TExecution::PREFLIGHT
            && (!AppData()->FeatureFlags.GetEnableSnapshotsLocking() || !AppData()->FeatureFlags.GetEnableDataShardSnapshotLocking()))
        {
            FinishChecksum(key, Ydb::StatusIds::PRECONDITION_FAILED,
                "Checksum checks require EnableSnapshotsLocking and EnableDataShardSnapshotLocking", false, ctx);
            continue;
        }
        if (execution.GetPhase() == TExecution::QUEUED) {
            if (!runtime.Admission) {
                StartChecksumAdmission(key, ctx);
            }
            continue;
        }
        if (execution.GetPhase() == TExecution::ADMITTED) {
            if (barrierBusy) {
                continue;
            }
            barrierBusy = true;
            runtime.TransitionInFlight = true;
            RunChecksumTx([this, key](NIceDb::TNiceDb& db) {
                if (!ChecksumExecutions.contains(key)) {
                    return;
                }
                ChecksumExecutions.at(key).SetPhase(TExecution::PREPARING);
                SaveChecksum(db, key);
            }, [this, key](const TActorContext& ctx) {
                if (!ChecksumExecutions.contains(key)) {
                    return;
                }
                ChecksumRuntime[key].TransitionInFlight = false;
                ProgressChecksums(ctx);
            });
            continue;
        }
        if ((execution.GetPhase() == TExecution::PREFLIGHT || execution.GetPhase() == TExecution::PREPARING) && !runtime.Source) {
            if (replication->GetYdbProxy() && ValidateChecksum(*replication, true).empty()) {
                StartChecksumSource(key, execution.GetPhase() == TExecution::PREFLIGHT, ctx);
            }
        }
        if (execution.GetPhase() == TExecution::PREPARING && execution.HasSourceSnapshot()) {
            if (AllocatedTxIds.empty()) {
                AllocateTxIds(ctx);
                continue;
            }
            runtime.TransitionInFlight = true;
            RunChecksumTx([this, key](NIceDb::TNiceDb& db) {
                if (!ChecksumExecutions.contains(key)) {
                    return;
                }
                auto& execution = ChecksumExecutions.at(key);
                const auto boundary = NChecksum::FromProto(execution.GetSourceSnapshot()).Next();
                const auto begin = GetChecksumBoundary();
                Y_ABORT_UNLESS(AssignedTxIds.empty() || AssignedTxIds.rbegin()->first < boundary);
                const auto txId = AllocatedTxIds.front();
                AllocatedTxIds.pop_front();
                AssignedTxIds[boundary] = txId;
                AssignedTxIdBegins[boundary] = begin;
                db.Table<Schema::TxIds>().Key(boundary.Step, boundary.TxId).Update(
                    NIceDb::TUpdate<Schema::TxIds::WriteTxId>(txId),
                    NIceDb::TUpdate<Schema::TxIds::BeginStep>(begin.Step),
                    NIceDb::TUpdate<Schema::TxIds::BeginTxId>(begin.TxId));
                execution.SetBoundaryWriteTxId(txId);
                ChecksumBoundaries[key.first] = boundary;
                NKikimrProto::TRowVersion proto;
                boundary.ToProto(&proto);
                db.Table<Schema::Replications>().Key(key.first).Update(
                    NIceDb::TUpdate<Schema::Replications::ChecksumBoundary>(proto.SerializeAsString()));
                execution.SetPhase(TExecution::SYNCHRONIZING);
                RebuildPendingTxIds();
                SaveChecksum(db, key);
            }, [this, key](const TActorContext& ctx) {
                if (!ChecksumExecutions.contains(key)) {
                    return;
                }
                ChecksumRuntime[key].TransitionInFlight = false;
                RunTxAssignTxId(ctx);
                RunTxIndexBuild(ctx);
                RunTxHeartbeat(ctx);
                ProgressChecksums(ctx);
            });
            continue;
        }
        if (execution.HasDestinationSnapshot() && !runtime.Destination) {
            const auto version = NChecksum::FromProto(execution.GetDestinationSnapshot());
            if (!runtime.Retaining && !runtime.Retained) {
                OnChecksumCommitPlanned(execution.GetBoundaryWriteTxId(), version, ctx);
            }
            if (!runtime.Retained || runtime.DestinationVersion != version) {
                continue;
            }
            NChecksum::TSettings settings;
            settings.Database = replication->GetDatabase();
            settings.Deadline = TInstant::MicroSeconds(execution.GetDeadlineUs());
            settings.MaxShardsInFlight = execution.GetGrantedShards();
            settings.Snapshot = version;
            settings.SnapshotHandle = std::move(runtime.DestinationHold);
            settings.ExpectedTables.emplace_back(target->GetDstPathId());
            auto* table = settings.Request.add_tables();
            table->set_id(key.second);
            table->set_path(target->GetDstPath());
            runtime.Destination = ctx.Register(NChecksum::CreateChecksumTables(SelfId(), std::move(settings)));
        }
        if (runtime.SourceDone && runtime.DestinationDone) {
            if (!execution.HasSource() || !execution.HasDestination() || runtime.SourceSchema != runtime.DestinationSchema) {
                FinishChecksum(key, Ydb::StatusIds::PRECONDITION_FAILED, "Source and destination schemas differ or results are incomplete", false, ctx);
            } else {
                FinishChecksum(key, Ydb::StatusIds::SUCCESS, {}, false, ctx);
            }
        }
    }
    for (const auto key : removed) {
        ChecksumExecutions.erase(key);
        ChecksumRuntime.erase(key);
        ChecksumResults.erase(key);
    }
    TabletCounters->Simple()[COUNTER_CHECKSUM_MISMATCH] = AnyOf(ChecksumResults, [this](const auto& item) {
        const auto replication = Find(item.first.first);
        return replication && replication->FindTarget(item.first.second)
            && item.second.has_last_result() && !item.second.last_result().matches();
    });
    if (active && !ChecksumWakeupScheduled) {
        ChecksumWakeupScheduled = true;
        ctx.Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
    }
}

void TController::HandleChecksumWakeup(TEvents::TEvWakeup::TPtr&, const TActorContext& ctx) {
    ChecksumWakeupScheduled = false;
    ProgressChecksums(ctx);
}

void TController::Handle(NChecksum::TEvResult::TPtr& ev, const TActorContext& ctx) {
    for (auto& [key, runtime] : ChecksumRuntime) {
        const bool source = ev->Sender == runtime.Source;
        if (!source && ev->Sender != runtime.Destination) {
            continue;
        }
        const auto response = ev->Get()->Record;
        const auto& execution = ChecksumExecutions.at(key);
        if (execution.GetPhase() == TExecution::FINISHED) {
            return;
        }
        if (runtime.TransitionInFlight) {
            runtime.PendingEvents.emplace_back(ev.Release());
            return;
        }
        if (response.status() != Ydb::StatusIds::SUCCESS || response.deferred()) {
            FinishChecksum(key, response.deferred() ? Ydb::StatusIds::PRECONDITION_FAILED : response.status(),
                response.deferred() ? "Replication initial scan is not complete" : NYql::IssuesFromMessageAsString(response.issues()),
                response.deferred() && execution.GetPhase() == TExecution::PREFLIGHT, ctx);
            return;
        }
        if (execution.GetPhase() == TExecution::PREFLIGHT && (response.has_snapshot() || response.has_table())) {
            FinishChecksum(key, Ydb::StatusIds::PRECONDITION_FAILED,
                "Source does not support checksum readiness probes", false, ctx);
            return;
        }
        const auto requestId = execution.GetRequestId();
        // Responses from one source actor may arrive while the preceding transaction
        // commits. Serialize their persistence without dropping snapshot/table markers.
        runtime.TransitionInFlight = true;
        RunChecksumTx([this, key, requestId, source, response](NIceDb::TNiceDb& db) {
            if (!ChecksumExecutions.contains(key)) {
                return;
            }
            auto& execution = ChecksumExecutions.at(key);
            if (execution.GetRequestId() != requestId || execution.GetPhase() == TExecution::FINISHED) {
                return;
            }
            auto& runtime = ChecksumRuntime[key];
            if (execution.GetPhase() == TExecution::PREFLIGHT && response.complete()) {
                runtime.Source = {};
                execution.SetPhase(TExecution::QUEUED);
                execution.ClearDeadlineUs();
            } else {
                if (source && response.has_snapshot()) {
                    *execution.MutableSourceSnapshot() = response.snapshot();
                }
                if (response.has_table() && response.table().id() == key.second) {
                    *(source ? execution.MutableSource() : execution.MutableDestination()) = response.table().checksum();
                    (source ? runtime.SourceSchema : runtime.DestinationSchema) = response.table().schema();
                }
                if (response.complete()) {
                    (source ? runtime.SourceDone : runtime.DestinationDone) = true;
                }
            }
            SaveChecksum(db, key);
        }, [this, key](const TActorContext& ctx) {
            if (!ChecksumExecutions.contains(key)) {
                return;
            }
            ChecksumRuntime[key].TransitionInFlight = false;
            ProgressChecksums(ctx);
        });
        return;
    }
}

void TController::Handle(NSchemeShard::TEvSchemeShard::TEvChecksumAdmissionResult::TPtr& ev, const TActorContext& ctx) {
    const auto response = ev->Get()->Record;
    for (auto& [key, execution] : ChecksumExecutions) {
        auto& runtime = ChecksumRuntime[key];
        if (execution.GetRequestId() != response.GetRequestId() || runtime.Admission != ev->Sender) {
            continue;
        }
        if (response.GetReleased()) {
            if (runtime.TransitionInFlight) {
                runtime.PendingEvents.emplace_back(ev.Release());
                return;
            }
            runtime.TransitionInFlight = true;
            RunChecksumTx([this, key](NIceDb::TNiceDb& db) {
                if (!ChecksumExecutions.contains(key)) {
                    return;
                }
                ChecksumExecutions.at(key).SetReleasePending(false);
                SaveChecksum(db, key);
            }, [this, key](const TActorContext& ctx) {
                if (!ChecksumExecutions.contains(key)) {
                    return;
                }
                ChecksumRuntime[key].Admission = {};
                ChecksumRuntime[key].TransitionInFlight = false;
                ProgressChecksums(ctx);
            });
            return;
        }
        if (execution.GetPhase() != TExecution::QUEUED || runtime.TransitionInFlight) {
            return;
        }
        if (response.GetStatus() != Ydb::StatusIds::SUCCESS) {
            FinishChecksum(key, response.GetStatus(), response.GetIssue(), false, ctx);
            return;
        }
        runtime.TransitionInFlight = true;
        RunChecksumTx([this, key, response](NIceDb::TNiceDb& db) {
            if (!ChecksumExecutions.contains(key)) {
                return;
            }
            auto& execution = ChecksumExecutions.at(key);
            execution.SetPhase(TExecution::ADMITTED);
            execution.SetGrantedShards(response.GetGrantedShards());
            execution.SetDeadlineUs(response.GetDeadlineUs());
            SaveChecksum(db, key);
        }, [this, key](const TActorContext& ctx) {
            if (!ChecksumExecutions.contains(key)) {
                return;
            }
            ChecksumRuntime[key].TransitionInFlight = false;
            ProgressChecksums(ctx);
        });
        return;
    }
}

void TController::OnChecksumCommitPlanned(ui64 writeTxId, TRowVersion version, const TActorContext& ctx) {
    for (const auto& [key, execution] : ChecksumExecutions) {
        if (!writeTxId || execution.GetBoundaryWriteTxId() != writeTxId || execution.GetPhase() == TExecution::FINISHED) {
            continue;
        }
        auto& runtime = ChecksumRuntime[key];
        if (runtime.Destination || runtime.Retaining || runtime.Retained) {
            return;
        }
        runtime.DestinationVersion = version;
        runtime.Retaining = true;
        const auto cookie = ++NextChecksumCookie;
        ChecksumRetainCookies[cookie] = {key, execution.GetRequestId(), version};
        TVector<TTableId> tables{TTableId(TPathId::FromProto(execution.GetDestinationPathId()))};
        ctx.Send(NLongTxService::MakeLongTxServiceID(SelfId().NodeId()),
            new NLongTxService::TEvLongTxService::TEvAcquireReadSnapshot(Find(key.first)->GetDatabase(), std::move(tables), version), 0, cookie);
    }
}

void TController::OnChecksumCommitComplete(ui64 writeTxId, TRowVersion version, NIceDb::TNiceDb& db) {
    for (auto& [key, execution] : ChecksumExecutions) {
        if (execution.GetBoundaryWriteTxId() == writeTxId && execution.GetPhase() != TExecution::FINISHED) {
            NChecksum::ToProto(version, execution.MutableDestinationSnapshot());
            execution.SetPhase(TExecution::CHECKSUMMING);
            SaveChecksum(db, key);
        }
    }
}

void TController::Handle(NLongTxService::TEvLongTxService::TEvAcquireReadSnapshotResult::TPtr& ev, const TActorContext& ctx) {
    const auto it = ChecksumRetainCookies.find(ev->Cookie);
    if (it == ChecksumRetainCookies.end()) {
        return;
    }
    const auto [key, requestId, version] = it->second;
    ChecksumRetainCookies.erase(it);
    const auto state = ChecksumExecutions.find(key);
    if (state == ChecksumExecutions.end() || state->second.GetPhase() == TExecution::FINISHED || state->second.GetRequestId() != requestId) {
        return;
    }
    auto& runtime = ChecksumRuntime[key];
    if (runtime.TransitionInFlight) {
        ChecksumRetainCookies[ev->Cookie] = {key, requestId, version};
        runtime.PendingEvents.emplace_back(ev.Release());
        return;
    }
    runtime.Retaining = false;
    auto& result = *ev->Get();
    if (result.Status != Ydb::StatusIds::SUCCESS || result.Snapshot != version) {
        FinishChecksum(key, result.Status == Ydb::StatusIds::SUCCESS ? Ydb::StatusIds::INTERNAL_ERROR : result.Status,
            result.Issues.ToString(), false, ctx);
        return;
    }
    runtime.DestinationHold = std::move(result.SnapshotHandle);
    runtime.Retained = true;
    ProgressChecksums(ctx);
}

void TController::RemoveReplicationChecksums(ui64 rid, const TActorContext& ctx) {
    for (const auto& [key, execution] : ChecksumExecutions) {
        if (key.first != rid) {
            continue;
        }
        StopChecksum(key, ctx);
        auto& runtime = ChecksumRuntime[key];
        if (!runtime.Admission && execution.GetPhase() != TExecution::PREFLIGHT) {
            auto request = NKikimrReplication::TEvChecksumAdmission();
            request.SetControllerId(TabletID());
            request.SetRequestId(execution.GetRequestId());
            *request.MutableDestinationPathId() = execution.GetDestinationPathId();
            request.SetRelease(true);
            runtime.Admission = ctx.Register(CreateChecksumAdmission(SelfId(), std::move(request)));
        }
        if (runtime.Admission) {
            ctx.Send(runtime.Admission, new TEvents::TEvPoison());
        }
        for (const auto& [sender, cookie] : runtime.Waiters) {
            auto result = MakeHolder<TEvController::TEvCheckTableResult>();
            result->Record.SetRequestId(execution.GetRequestId());
            *result->Record.MutableDestinationPathId() = execution.GetDestinationPathId();
            result->Record.SetStatus(Ydb::StatusIds::CANCELLED);
            ctx.Send(sender, result.Release(), 0, cookie);
        }
    }
    EraseNodesIf(ChecksumExecutions, [rid](const auto& item) { return item.first.first == rid; });
    EraseNodesIf(ChecksumRuntime, [rid](const auto& item) { return item.first.first == rid; });
    EraseNodesIf(ChecksumResults, [rid](const auto& item) { return item.first.first == rid; });
    ChecksumBoundaries.erase(rid);
}

} // namespace NKikimr::NReplication::NController
