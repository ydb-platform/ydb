#include "task.h"

namespace NKikimr::NBackgroundTasks {

bool TTask::DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawValue) {
    if (!decoder.Read(decoder.GetIdIdx(), Id, rawValue)) {
        ALS_ERROR(NKikimrServices::BG_TASKS) << "cannot parse task id";
        return false;
    }
    if (!decoder.Read(decoder.GetClassIdx(), Class, rawValue)) {
        ALS_ERROR(NKikimrServices::BG_TASKS) << "cannot parse task class";
        return false;
    }
    if (!decoder.Read(decoder.GetEnabledIdx(), EnabledFlag, rawValue)) {
        ALS_ERROR(NKikimrServices::BG_TASKS) << "cannot parse task enabled";
        return false;
    }
    if (!decoder.Read(decoder.GetExecutorIdIdx(), ExecutorId, rawValue)) {
        ALS_ERROR(NKikimrServices::BG_TASKS) << "cannot parse task executor id";
        return false;
    }
    if (!decoder.Read(decoder.GetLastPingIdx(), LastPing, rawValue)) {
        ALS_ERROR(NKikimrServices::BG_TASKS) << "cannot parse task last ping";
        return false;
    }
    if (!decoder.Read(decoder.GetStartInstantIdx(), StartInstant, rawValue)) {
        ALS_ERROR(NKikimrServices::BG_TASKS) << "cannot parse task start instant";
        return false;
    }
    if (!decoder.Read(decoder.GetConstructInstantIdx(), ConstructInstant, rawValue)) {
        ALS_ERROR(NKikimrServices::BG_TASKS) << "cannot parse task construct instant";
        return false;
    }
    {
        TString activityData;
        if (!decoder.Read(decoder.GetActivityIdx(), activityData, rawValue)) {
            ALS_ERROR(NKikimrServices::BG_TASKS) << "cannot read activity";
            return false;
        }
        if (!Activity.DeserializeFromString(activityData)) {
            ALS_ERROR(NKikimrServices::BG_TASKS) << "cannot deserialize activity";
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
            ALS_ERROR(NKikimrServices::BG_TASKS) << "cannot read scheduler";
            return false;
        }
        if (!Scheduler.DeserializeFromString(schedulerData)) {
            ALS_ERROR(NKikimrServices::BG_TASKS) << "cannot deserialize scheduler";
            return false;
        }
    }
    {
        TString stateData;
        if (!decoder.Read(decoder.GetStateIdx(), stateData, rawValue)) {
            ALS_ERROR(NKikimrServices::BG_TASKS) << "cannot read state";
            return false;
        }
        if (!State.DeserializeFromString(stateData)) {
            ALS_ERROR(NKikimrServices::BG_TASKS) << "cannot deserialize state";
            return false;
        }
    }
    return true;
}

}
