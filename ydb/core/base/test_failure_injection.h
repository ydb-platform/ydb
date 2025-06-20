#pragma once

#include <util/generic/yexception.h>

namespace NActors {
    class TActorSystem;
}

namespace NKikimr {

// Enum for different types of test failures that can be injected
enum class EInjectedFailureType : ui64 {
    BackupCollectionNotFound = 1,
    BackupChildrenEmpty = 2,
    PathSplitFailure = 3,
    IncrementalBackupPathNotResolved = 4,
    CreateChangePathStateFailed = 5,
};

// Helper functions for easier test failure injection
class TTestFailureInjector {
public:
    static void InjectFailure(NActors::TActorSystem* actorSystem, EInjectedFailureType failureType);
    static void RemoveFailure(NActors::TActorSystem* actorSystem, EInjectedFailureType failureType);
    static void ClearAllFailures(NActors::TActorSystem* actorSystem);
    static bool HasInjectedFailure(NActors::TActorSystem* actorSystem, EInjectedFailureType failureType);
};

} // namespace NKikimr
