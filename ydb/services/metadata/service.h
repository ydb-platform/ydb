#pragma once
#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/abstract/manager.h>
#include <library/cpp/actors/core/event_local.h>

namespace NKikimr::NMetadataProvider {

class TEvAlterObjects: public NActors::TEventLocal<TEvAlterObjects, EEvSubscribe::EvAlterObjects> {
private:
    YDB_READONLY_DEF(NMetadata::IAlterCommand::TPtr, Command);
public:
    TEvAlterObjects(NMetadata::IAlterCommand::TPtr command)
        : Command(command) {

    }
};

class TEvSubscribeExternal: public NActors::TEventLocal<TEvSubscribeExternal, EEvSubscribe::EvSubscribeExternal> {
private:
    YDB_READONLY_DEF(ISnapshotParser::TPtr, Fetcher);
public:
    TEvSubscribeExternal(ISnapshotParser::TPtr fetcher)
        : Fetcher(fetcher)
    {
        Y_VERIFY(!!Fetcher);
    }
};

class TEvUnsubscribeExternal: public NActors::TEventLocal<TEvUnsubscribeExternal, EEvSubscribe::EvUnsubscribeExternal> {
private:
    YDB_READONLY_DEF(ISnapshotParser::TPtr, Fetcher);
public:
    TEvUnsubscribeExternal(ISnapshotParser::TPtr fetcher)
        : Fetcher(fetcher) {
        Y_VERIFY(!!Fetcher);
    }
};

NActors::TActorId MakeServiceId(const ui32 node);

class TServiceOperator {
private:
    friend class TService;
    bool EnabledFlag = false;
    static void Register();
public:
    static bool IsEnabled();
};

}
