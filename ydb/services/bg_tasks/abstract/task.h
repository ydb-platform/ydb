#pragma once
#include "activity.h"
#include "scheduler.h"
#include "state.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/events.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/services/metadata/abstract/decoder.h>

#include <library/cpp/time_provider/time_provider.h>
#include <ydb/library/actors/core/events.h>
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

    class TDecoder: public NMetadata::NInternal::TDecoderBase {
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

    bool DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawValue);
};

}
