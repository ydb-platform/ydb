#pragma once
#include <util/system/mutex.h>

#include <ydb/core/fq/libs/control_plane_storage/events/events.h>

namespace NFq {

struct TTask {
    TString Scope;
    TString QueryId;
    ui32 RetryCount;
    FederatedQuery::Query Query;
    FederatedQuery::Internal::QueryInternal Internal;
    ui64 Generation = 0;
    TInstant Deadline;
};

class TResponseTasks {
public:
    void AddTaskNonBlocking(const TString& key, const TTask& task);

    void AddTaskBlocking(const TString& key, const TTask& task);

    void SafeEraseTaskNonBlocking(const TString& key);

    void SafeEraseTaskBlocking(const TString& key);

    bool EmptyNonBlocking();

    bool EmptyBlocking();

    const THashMap<TString, TTask>& GetTasksNonBlocking();

    const THashMap<TString, TTask>& GetTasksBlocking();

private:
    TMutex Mutex;
    THashMap<TString, TTask> Tasks;
};

} //NFq
