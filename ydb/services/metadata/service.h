#pragma once
#include <ydb/services/metadata/abstract/common.h>
#include <ydb/library/actors/core/event_local.h>
#include <shared_mutex>

namespace NKikimr::NMetadata::NProvider {

class TEvObjectsOperation: public NActors::TEventLocal<TEvObjectsOperation, EEvents::EvAlterObjects> {
private:
    YDB_READONLY_DEF(NModifications::IObjectModificationCommand::TPtr, Command);
public:
    TEvObjectsOperation(NModifications::IObjectModificationCommand::TPtr command)
        : Command(command) {

    }
};

class TEvPrepareManager: public NActors::TEventLocal<TEvPrepareManager, EEvents::EvPrepareManager> {
private:
    YDB_READONLY_DEF(IClassBehaviour::TPtr, Manager);
public:
    TEvPrepareManager(IClassBehaviour::TPtr manager)
        : Manager(manager) {
        Y_ABORT_UNLESS(!!Manager);
    }
};

class TEvManagerPrepared: public NActors::TEventLocal<TEvManagerPrepared, EEvents::EvManagerPrepared> {
private:
    YDB_READONLY_DEF(IClassBehaviour::TPtr, Manager);
public:
    TEvManagerPrepared(IClassBehaviour::TPtr manager)
        : Manager(manager) {
        Y_ABORT_UNLESS(!!Manager);
    }
};

class TEvAskSnapshot: public NActors::TEventLocal<TEvAskSnapshot, EEvents::EvAskExternal> {
private:
    YDB_READONLY_DEF(NFetcher::ISnapshotsFetcher::TPtr, Fetcher);
public:
    TEvAskSnapshot(NFetcher::ISnapshotsFetcher::TPtr fetcher)
        : Fetcher(fetcher) {
        Y_ABORT_UNLESS(!!Fetcher);
    }
};

class TEvSubscribeExternal: public NActors::TEventLocal<TEvSubscribeExternal, EEvents::EvSubscribeExternal> {
private:
    YDB_READONLY_DEF(NFetcher::ISnapshotsFetcher::TPtr, Fetcher);
public:
    TEvSubscribeExternal(NFetcher::ISnapshotsFetcher::TPtr fetcher)
        : Fetcher(fetcher)
    {
        Y_ABORT_UNLESS(!!Fetcher);
    }
};

class TEvUnsubscribeExternal: public NActors::TEventLocal<TEvUnsubscribeExternal, EEvents::EvUnsubscribeExternal> {
private:
    YDB_READONLY_DEF(NFetcher::ISnapshotsFetcher::TPtr, Fetcher);
public:
    TEvUnsubscribeExternal(NFetcher::ISnapshotsFetcher::TPtr fetcher)
        : Fetcher(fetcher) {
        Y_ABORT_UNLESS(!!Fetcher);
    }
};

NActors::TActorId MakeServiceId(const ui32 node);

class TConfig;

class TServiceOperator {
private:
    friend class TService;
    std::shared_mutex Lock;
    bool EnabledFlag = false;
    TString Path = ".metadata";
    static void Register(const TConfig& config);
public:
    static bool IsEnabled();
    static TString GetPath();
};

}
