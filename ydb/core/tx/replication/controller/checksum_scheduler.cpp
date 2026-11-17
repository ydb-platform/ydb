#include "controller_impl.h"

#include <ydb/core/base/appdata.h>
#include <google/protobuf/util/time_util.h>
#include <util/generic/algorithm.h>
#include <util/generic/guid.h>

namespace NKikimr::NReplication::NController {

void TController::ProgressChecksumSchedule(const TActorContext& ctx) {
    EraseNodesIf(ChecksumSchedules, [this](const auto& item) {
        const auto replication = Find(item.first.first);
        return !replication || !replication->FindTarget(item.first.second);
    });
    const bool needed = AnyOf(Replications, [](const auto& item) {
        return item.second->GetConfig().GetChecksumIntervalMilliSeconds() != 0;
    }) || AnyOf(ChecksumSchedules, [](const auto& item) {
        return item.second.GetEnabled() || item.second.HasRequestId();
    });
    if (!needed) {
        return;
    }
    if (!ChecksumScheduleWakeupScheduled) {
        ChecksumScheduleWakeupScheduled = true;
        ctx.Schedule(TDuration::Seconds(1), new TEvPrivate::TEvChecksumSchedule());
    }
    if (ChecksumScheduleTxInFlight) {
        return;
    }
    struct TAction {
        TPathId PathId;
        TString RequestId;
        bool Cancel;
    };
    auto actions = std::make_shared<TVector<TAction>>();
    TMap<TChecksumKey, NKikimrReplication::TTableChecksumSchedule> updates;
    const auto now = ctx.Now().MicroSeconds();
    for (const auto& [pathId, key] : ChecksumTargets) {
        if (!key.first) {
            continue;
        }
        const auto replication = Find(key.first);
        const auto interval = replication->GetConfig().GetChecksumIntervalMilliSeconds();
        const bool enabled = interval && AppData()->FeatureFlags.GetEnableAsyncReplicationChecksumCheck()
            && replication->GetState() == TReplication::EState::Ready
            && replication->GetDesiredState() == TReplication::EState::Ready;
        auto schedule = ChecksumSchedules[key];
        const auto before = schedule.SerializeAsString();
        if (!enabled) {
            schedule.SetEnabled(false);
            // A feature-flag change allows admitted work to finish. Explicit
            // interval/state changes withdraw the periodic scheduler's request.
            if (schedule.HasRequestId() && (!interval || replication->GetDesiredState() != TReplication::EState::Ready)) {
                actions->push_back({pathId, schedule.GetRequestId(), true});
            }
        } else {
            const auto result = ChecksumResults.find(key);
            const ui64 finished = result == ChecksumResults.end() ? 0
                : google::protobuf::util::TimeUtil::TimestampToMicroseconds(result->second.finished_at());
            if (finished > schedule.GetLastFinishedUs()) {
                schedule.SetLastFinishedUs(finished);
                schedule.SetNextDueUs(finished + interval * 1000);
            }
            if (!schedule.GetEnabled()) {
                schedule.SetEnabled(true);
                schedule.SetNextDueUs(now);
            } else if (schedule.GetIntervalMs() != interval) {
                schedule.SetNextDueUs(finished ? finished + interval * 1000 : now);
            }
            schedule.SetIntervalMs(interval);
            if (!schedule.HasRequestId() && now >= schedule.GetNextDueUs()) {
                const auto requestId = CreateGuidAsString();
                schedule.SetRequestId(requestId);
                actions->push_back({pathId, requestId, false});
            } else if (schedule.HasRequestId()) {
                // Replay the same request after a lost reply or tablet restart.
                actions->push_back({pathId, schedule.GetRequestId(), false});
            }
        }
        if (before != schedule.SerializeAsString()) {
            updates.emplace(key, std::move(schedule));
        }
    }
    auto sendActions = [this, actions](const TActorContext& ctx) {
        for (const auto& action : *actions) {
            if (action.Cancel) {
                auto request = MakeHolder<TEvController::TEvCancelCheckTable>();
                action.PathId.ToProto(request->Record.MutableDestinationPathId());
                request->Record.SetRequestId(action.RequestId);
                ctx.Send(SelfId(), request.Release());
            } else {
                auto request = MakeHolder<TEvController::TEvCheckTable>();
                action.PathId.ToProto(request->Record.MutableDestinationPathId());
                request->Record.SetRequestId(action.RequestId);
                ctx.Send(SelfId(), request.Release());
            }
        }
    };
    if (updates.empty()) {
        sendActions(ctx);
        return;
    }
    ChecksumScheduleTxInFlight = true;
    RunChecksumTx([this, updates = std::move(updates)](NIceDb::TNiceDb& db) {
        for (const auto& [key, schedule] : updates) {
            const auto replication = Find(key.first);
            if (!replication || !replication->FindTarget(key.second)) {
                continue;
            }
            ChecksumSchedules[key] = schedule;
            db.Table<Schema::Targets>().Key(key.first, key.second).Update(
                NIceDb::TUpdate<Schema::Targets::ChecksumSchedule>(schedule.SerializeAsString()));
        }
    }, [this, sendActions](const TActorContext& ctx) {
        ChecksumScheduleTxInFlight = false;
        sendActions(ctx);
    });
}

void TController::Handle(TEvPrivate::TEvChecksumSchedule::TPtr&, const TActorContext& ctx) {
    ChecksumScheduleWakeupScheduled = false;
    ProgressChecksumSchedule(ctx);
}

void TController::Handle(TEvController::TEvCheckTableResult::TPtr& ev, const TActorContext& ctx) {
    if (ev->Sender != SelfId()) {
        return;
    }
    const auto response = ev->Get()->Record;
    const auto target = ChecksumTargets.find(TPathId::FromProto(response.GetDestinationPathId()));
    if (target == ChecksumTargets.end() || !target->second.first) {
        return;
    }
    const auto key = target->second;
    const auto schedule = ChecksumSchedules.find(key);
    if (schedule == ChecksumSchedules.end()) {
        return;
    }
    if (schedule->second.GetRequestId() != response.GetRequestId() && !response.HasChecksumStatus()) {
        return;
    }
    // Completion notifications also arrive for checks initiated by other callers.
    RunChecksumTx([this, key, response, now = ctx.Now()](NIceDb::TNiceDb& db) {
        const auto replication = Find(key.first);
        if (!replication || !replication->FindTarget(key.second)) {
            return;
        }
        auto& schedule = ChecksumSchedules[key];
        if (schedule.GetRequestId() == response.GetRequestId()) {
            schedule.ClearRequestId();
        }
        if (response.HasChecksumStatus()) {
            const ui64 finished = google::protobuf::util::TimeUtil::TimestampToMicroseconds(response.GetChecksumStatus().finished_at());
            if (finished > schedule.GetLastFinishedUs()) {
                schedule.SetLastFinishedUs(finished);
                schedule.SetNextDueUs(finished + replication->GetConfig().GetChecksumIntervalMilliSeconds() * 1000);
            }
        } else {
            schedule.SetNextDueUs((now + TDuration::Seconds(30)).MicroSeconds());
        }
        db.Table<Schema::Targets>().Key(key.first, key.second).Update(
            NIceDb::TUpdate<Schema::Targets::ChecksumSchedule>(schedule.SerializeAsString()));
    }, [](const TActorContext&) {});
}

} // namespace NKikimr::NReplication::NController
