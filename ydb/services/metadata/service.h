#pragma once
#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/abstract/manager.h>
#include <library/cpp/actors/core/event_local.h>

namespace NKikimr::NMetadataProvider {

class TEvObjectsOperation: public NActors::TEventLocal<TEvObjectsOperation, EEvSubscribe::EvAlterObjects> {
private:
    YDB_READONLY_DEF(NMetadata::IAlterCommand::TPtr, Command);
public:
    TEvObjectsOperation(NMetadata::IAlterCommand::TPtr command)
        : Command(command) {

    }
};

class TEvPrepareManager: public NActors::TEventLocal<TEvPrepareManager, EEvSubscribe::EvPrepareManager> {
private:
    YDB_READONLY_DEF(NMetadata::IOperationsManager::TPtr, Manager);
public:
    TEvPrepareManager(NMetadata::IOperationsManager::TPtr manager)
        : Manager(manager) {
        Y_VERIFY(!!Manager);
    }
};

class TEvManagerPrepared: public NActors::TEventLocal<TEvManagerPrepared, EEvSubscribe::EvManagerPrepared> {
private:
    YDB_READONLY_DEF(NMetadata::IOperationsManager::TPtr, Manager);
public:
    TEvManagerPrepared(NMetadata::IOperationsManager::TPtr manager)
        : Manager(manager) {
        Y_VERIFY(!!Manager);
    }
};

class TEvAskSnapshot: public NActors::TEventLocal<TEvAskSnapshot, EEvSubscribe::EvAskExternal> {
private:
    YDB_READONLY_DEF(ISnapshotsFetcher::TPtr, Fetcher);
public:
    TEvAskSnapshot(ISnapshotsFetcher::TPtr fetcher)
        : Fetcher(fetcher) {
        Y_VERIFY(!!Fetcher);
    }
};

class TEvSubscribeExternal: public NActors::TEventLocal<TEvSubscribeExternal, EEvSubscribe::EvSubscribeExternal> {
private:
    YDB_READONLY_DEF(ISnapshotsFetcher::TPtr, Fetcher);
public:
    TEvSubscribeExternal(ISnapshotsFetcher::TPtr fetcher)
        : Fetcher(fetcher)
    {
        Y_VERIFY(!!Fetcher);
    }
};

class TEvUnsubscribeExternal: public NActors::TEventLocal<TEvUnsubscribeExternal, EEvSubscribe::EvUnsubscribeExternal> {
private:
    YDB_READONLY_DEF(ISnapshotsFetcher::TPtr, Fetcher);
public:
    TEvUnsubscribeExternal(ISnapshotsFetcher::TPtr fetcher)
        : Fetcher(fetcher) {
        Y_VERIFY(!!Fetcher);
    }
};

NActors::TActorId MakeServiceId(const ui32 node);

class TConfig;

class TServiceOperator {
private:
    friend class TService;
    bool EnabledFlag = false;
    TString Path = ".metadata";
    static void Register(const TConfig& config);
public:
    static bool IsEnabled();
    static const TString& GetPath();
};

}
