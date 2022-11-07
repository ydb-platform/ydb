#pragma once
#include "activity.h"
#include "scheduler.h"
#include "state.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/events.h>
#include <ydb/core/tx/tiering/decoder.h>
#include <ydb/library/accessor/accessor.h>

#include <library/cpp/actors/core/events.h>
#include <library/cpp/object_factory/object_factory.h>
#include <util/generic/guid.h>

namespace NKikimr::NBackgroundTasks {

class TTask;

class TTask {
private:
    YDB_ACCESSOR(TString, Id, TGUID::CreateTimebased().AsGuidString());
    YDB_READONLY_DEF(TString, Class);
    YDB_READONLY_DEF(TString, ExecutorId);
    YDB_READONLY(TInstant, LastPing, TInstant::Zero());
    YDB_READONLY(TInstant, StartInstant, TInstant::Zero());
    YDB_READONLY(TInstant, ConstructInstant, TAppData::TimeProvider->Now());
    YDB_READONLY_DEF(TTaskActivityContainer, Activity);
    YDB_READONLY_DEF(TTaskSchedulerContainer, Scheduler);
    YDB_ACCESSOR_DEF(TTaskStateContainer, State);
    YDB_FLAG_ACCESSOR(Enabled, true);
public:
    TTask() = default;
    TTask(ITaskActivity::TPtr activity, ITaskScheduler::TPtr scheduler)
        : Activity(activity)
        , Scheduler(scheduler)
    {

    }

    void Execute(ITaskExecutorController::TPtr controller) const {
        if (!Activity) {
            controller->TaskFinished();
        } else {
            Activity.GetObjectPtr()->Execute(controller, State);
        }
    }

    void Finished() const {
        if (!!Activity) {
            Activity.GetObjectPtr()->Finished(State);
        }
    }

    class TDecoder: public NInternal::TDecoderBase {
    private:
        YDB_ACCESSOR(i32, IdIdx, -1);
        YDB_ACCESSOR(i32, ClassIdx, -1);
        YDB_ACCESSOR(i32, ExecutorIdIdx, -1);
        YDB_ACCESSOR(i32, LastPingIdx, -1);
        YDB_ACCESSOR(i32, StartInstantIdx, -1);
        YDB_ACCESSOR(i32, ConstructInstantIdx, -1);
        YDB_ACCESSOR(i32, ActivityIdx, -1);
        YDB_ACCESSOR(i32, SchedulerIdx, -1);
        YDB_ACCESSOR(i32, StateIdx, -1);
        YDB_ACCESSOR(i32, EnabledIdx, -1);
    public:
        static inline const TString Id = "id";
        static inline const TString Class = "class";
        static inline const TString ExecutorId = "executorId";
        static inline const TString LastPing = "lastPing";
        static inline const TString StartInstant = "startInstant";
        static inline const TString ConstructInstant = "constructInstant";
        static inline const TString Activity = "activity";
        static inline const TString Scheduler = "scheduler";
        static inline const TString State = "state";
        static inline const TString Enabled = "enabled";

        TDecoder(const Ydb::ResultSet& rawData) {
            IdIdx = GetFieldIndex(rawData, Id);
            ClassIdx = GetFieldIndex(rawData, Class);
            ExecutorIdIdx = GetFieldIndex(rawData, ExecutorId);
            LastPingIdx = GetFieldIndex(rawData, LastPing);
            StartInstantIdx = GetFieldIndex(rawData, StartInstant);
            ConstructInstantIdx = GetFieldIndex(rawData, ConstructInstant);
            ActivityIdx = GetFieldIndex(rawData, Activity);
            SchedulerIdx = GetFieldIndex(rawData, Scheduler);
            StateIdx = GetFieldIndex(rawData, State);
            EnabledIdx = GetFieldIndex(rawData, Enabled);
        }
    };

    bool DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawValue) {
        if (!decoder.Read(decoder.GetIdIdx(), Id, rawValue)) {
            return false;
        }
        if (!decoder.Read(decoder.GetClassIdx(), Class, rawValue)) {
            return false;
        }
        if (!decoder.Read(decoder.GetEnabledIdx(), EnabledFlag, rawValue)) {
            return false;
        }
        if (!decoder.Read(decoder.GetExecutorIdIdx(), ExecutorId, rawValue)) {
            return false;
        }
        if (!decoder.Read(decoder.GetLastPingIdx(), LastPing, rawValue)) {
            return false;
        }
        if (!decoder.Read(decoder.GetStartInstantIdx(), StartInstant, rawValue)) {
            return false;
        }
        if (!decoder.Read(decoder.GetConstructInstantIdx(), ConstructInstant, rawValue)) {
            return false;
        }
        {
            TString activityData;
            if (!decoder.Read(decoder.GetActivityIdx(), activityData, rawValue)) {
                return false;
            }
            if (!Activity.DeserializeFromString(activityData)) {
                return false;
            }
            if (!Activity) {
                ALS_ERROR(NKikimrServices::BG_TASKS) << "cannot parse task activity";
                return false;
            }
        }
        {
            TString schedulerData;
            if (!decoder.Read(decoder.GetSchedulerIdx(), schedulerData, rawValue)) {
                return false;
            }
            if (!Scheduler.DeserializeFromString(schedulerData)) {
                return false;
            }
        }
        {
            TString stateData;
            if (!decoder.Read(decoder.GetStateIdx(), stateData, rawValue)) {
                return false;
            }
            if (!State.DeserializeFromString(stateData)) {
                return false;
            }
        }
        return true;
    }
};

}
