#pragma once

#include <util/generic/yexception.h>

namespace NActors {
    class TActorSystem;
}

namespace NKikimr {

enum class EInjectedFailureType : ui64 {
    BackupCollectionNotFound = 1,
    BackupChildrenEmpty = 2,
    PathSplitFailure = 3,
    IncrementalBackupPathNotResolved = 4,
    CreateChangePathStateFailed = 5,
    LateBackupCollectionNotFound = 6,
    DisableIncrementalRestoreAutoSwitchingToReadyStateForTests = 7,
};

class TTestFailureInjector {
public:
    static void InjectFailure(NActors::TActorSystem* actorSystem, EInjectedFailureType failureType);
    static void RemoveFailure(NActors::TActorSystem* actorSystem, EInjectedFailureType failureType);
    static void ClearAllFailures(NActors::TActorSystem* actorSystem);
    static bool HasInjectedFailure(NActors::TActorSystem* actorSystem, EInjectedFailureType failureType);
};

} // namespace NKikimr
