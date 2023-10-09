#include "misc.h"

#include <util/system/yassert.h>

using namespace NRainCheck;

void TSpawnNopTasksCoroTask::Run() {
    Y_ABORT_UNLESS(Count <= Completion.size());
    for (unsigned i = 0; i < Count; ++i) {
        SpawnSubtask<TNopCoroTask>(Env, &Completion[i], "");
    }

    WaitForSubtasks();
}

TContinueFunc TSpawnNopTasksSimpleTask::Start() {
    Y_ABORT_UNLESS(Count <= Completion.size());
    for (unsigned i = 0; i < Count; ++i) {
        SpawnSubtask<TNopSimpleTask>(Env, &Completion[i], "");
    }

    return &TSpawnNopTasksSimpleTask::Join;
}

TContinueFunc TSpawnNopTasksSimpleTask::Join() {
    return nullptr;
}
