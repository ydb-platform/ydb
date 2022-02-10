#include "spawn.h"

void NRainCheck::NPrivate::SpawnTaskImpl(TTaskRunnerBase* task) {
    task->Schedule();
}
