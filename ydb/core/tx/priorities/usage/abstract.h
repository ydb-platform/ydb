#pragma once
#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NPrioritiesQueue {

class TAllocationGuard {
private:
    const NActors::TActorId ServiceActorId;
    const ui64 ClientId;
    const ui32 Count;
    bool Released = false;

public:
    TAllocationGuard(const NActors::TActorId& serviceActorId, const ui64 clientId, const ui32 count)
        : ServiceActorId(serviceActorId)
        , ClientId(clientId)
        , Count(count) {
    }

    ~TAllocationGuard();

    void Release();
};

class IRequest {
protected:
    virtual void DoOnAllocated(const std::shared_ptr<TAllocationGuard>& guard) = 0;

public:
    virtual ~IRequest() = default;

    void OnAllocated(const std::shared_ptr<TAllocationGuard>& guard) {
        return DoOnAllocated(guard);
    }
};

}   // namespace NKikimr::NPrioritiesQueue
