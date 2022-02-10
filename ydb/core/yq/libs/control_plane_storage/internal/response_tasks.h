#pragma once
#include <util/system/mutex.h>

#include <ydb/core/yq/libs/control_plane_storage/events/events.h>

namespace NYq {

class TResponseTasks {
public:
    void AddTaskNonBlocking(const TString& key, const TEvControlPlaneStorage::TTask& task);

    void AddTaskBlocking(const TString& key, const TEvControlPlaneStorage::TTask& task);

    void SafeEraseTaskNonBlocking(const TString& key);

    void SafeEraseTaskBlocking(const TString& key);

    bool EmptyNonBlocking();

    bool EmptyBlocking();

    const THashMap<TString, TEvControlPlaneStorage::TTask>& GetTasksNonBlocking();

    const THashMap<TString, TEvControlPlaneStorage::TTask>& GetTasksBlocking();

private:
    TMutex Mutex;
    THashMap<TString, TEvControlPlaneStorage::TTask> Tasks;
};

} //NYq
