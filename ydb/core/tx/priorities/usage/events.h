#pragma once
#include "abstract.h"

#include <ydb/core/base/events.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/conclusion/result.h>

namespace NKikimr::NPrioritiesQueue {

struct TEvExecution {
    enum EEv {
        EvRegisterClient = EventSpaceBegin(TKikimrEvents::ES_PRIORITY_QUEUE),
        EvUnregisterClient,
        EvAsk,
        EvAskMax,
        EvFree,
        EvAllocated,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIORITY_QUEUE), "expected EvEnd < EventSpaceEnd");

    class TEvRegisterClient: public NActors::TEventLocal<TEvRegisterClient, EvRegisterClient> {
    private:
        YDB_READONLY(ui64, ClientId, 0);

    public:
        TEvRegisterClient(const ui64 clientId)
            : ClientId(clientId) {
        }
    };

    class TEvUnregisterClient: public NActors::TEventLocal<TEvUnregisterClient, EvUnregisterClient> {
    private:
        YDB_READONLY(ui64, ClientId, 0);

    public:
        TEvUnregisterClient(const ui64 clientId)
            : ClientId(clientId) {
        }
    };

    class TEvAsk: public NActors::TEventLocal<TEvAsk, EvAsk> {
    private:
        YDB_READONLY(ui64, ClientId, 0);
        YDB_READONLY(ui32, Count, 0);
        YDB_READONLY_DEF(std::shared_ptr<IRequest>, Request);
        YDB_READONLY(ui64, Priority, 0);

    public:
        TEvAsk(const ui64 clientId, const ui32 count, const std::shared_ptr<IRequest>& request, const ui64 priority);
    };

    class TEvAskMax: public NActors::TEventLocal<TEvAskMax, EvAskMax> {
    private:
        YDB_READONLY(ui64, ClientId, 0);
        YDB_READONLY(ui32, Count, 0);
        YDB_READONLY_DEF(std::shared_ptr<IRequest>, Request);
        YDB_READONLY(ui64, Priority, 0);

    public:
        TEvAskMax(const ui64 clientId, const ui32 count, const std::shared_ptr<IRequest>& request, const ui64 priority);
    };

    class TEvFree: public NActors::TEventLocal<TEvFree, EvFree> {
    private:
        YDB_READONLY(ui64, ClientId, 0);
        YDB_READONLY(ui32, Count, 0);

    public:
        TEvFree(const ui64 clientId, const ui32 count)
            : ClientId(clientId)
            , Count(count) {
        }
    };

    class TEvAllocated: public NActors::TEventLocal<TEvAllocated, EvAllocated> {
    private:
        YDB_READONLY(ui64, RequestId, 0);
        YDB_READONLY_DEF(std::shared_ptr<TAllocationGuard>, Guard);

    public:
        TEvAllocated(const ui32 requestId, const std::shared_ptr<TAllocationGuard>& guard)
            : RequestId(requestId)
            , Guard(guard) {
        }
    };
};

}   // namespace NKikimr::NPrioritiesQueue
