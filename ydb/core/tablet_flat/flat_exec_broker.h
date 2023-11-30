#pragma once

#include "util_basics.h"
#include "flat_broker.h"
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

    using TTaskId = NTable::TTaskId;
    using TResourceParams = NTable::TResourceParams;
    using EResourceStatus = NTable::EResourceStatus;
    using IResourceBroker = NTable::IResourceBroker;

    enum class EWakeTag : ui64 {
        Default = 0,    /* Shouldn't be ever used   */
        Memory  = 1,    /* TMemory GC scheduler     */
    };

    struct TIdEmitter : public TSimpleRefCount<TIdEmitter> {
        ui64 Do() noexcept
        {
            return ++Serial;
        }

    protected:
        ui64 Serial = 0;
    };

    struct TResource : public TThrRefBase {
        enum class ESource {
            Seat,
            Scan,
        };

        TResource(ESource source)
            : Source(source)
        { }

        const ESource Source;
    };

    class TBroker final : public IResourceBroker {
        using IOps = NActors::IActorOps;

    public:
        TBroker(IOps* ops, TIntrusivePtr<TIdEmitter> emitter);

        // API implementation
        TTaskId SubmitTask(TString name, TResourceParams params, TResourceConsumer consumer) override;
        void UpdateTask(TTaskId taskId, TResourceParams params) override;
        void FinishTask(TTaskId taskId, EResourceStatus status = EResourceStatus::Finished) override;
        bool CancelTask(TTaskId) override;

        // Called by executor when resource is allocated
        void OnResourceAllocated(TTaskId taskId);

    private:
        void SendToBroker(TAutoPtr<IEventBase> event);

    private:
        IOps* const Ops;
        TIntrusivePtr<TIdEmitter> const Emitter;
        THashMap<TTaskId, TResourceConsumer> Submitted;
    };

}
}
