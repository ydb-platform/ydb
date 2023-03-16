#include "response_tasks.h"

namespace NFq {

void TResponseTasks::AddTaskNonBlocking(const TString& key, const TTask& task) {
    Tasks[key] = task;
}

void TResponseTasks::AddTaskBlocking(const TString& key, const TTask& task) {
    with_lock (Mutex) {
        Tasks[key] = task;
    }
}

void TResponseTasks::SafeEraseTaskNonBlocking(const TString& key) {
    if (auto it = Tasks.find(key); it != Tasks.end())
        Tasks.erase(it);
}

void TResponseTasks::SafeEraseTaskBlocking(const TString& key) {
    with_lock (Mutex) {
        if (auto it = Tasks.find(key); it != Tasks.end())
            Tasks.erase(it);
    }
}

bool TResponseTasks::EmptyNonBlocking() {
    return Tasks.empty();
}

bool TResponseTasks::EmptyBlocking() {
    with_lock (Mutex) {
        return Tasks.empty();
    }
}

const THashMap<TString, TTask>& TResponseTasks::GetTasksNonBlocking() {
    return Tasks;
}

const THashMap<TString, TTask>& TResponseTasks::GetTasksBlocking() {
    with_lock (Mutex) {
        return Tasks;
    }
}

} //NFq