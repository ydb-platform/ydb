#pragma once
#include <memory>
#include <ydb/library/signals/owner.h>

namespace NKikimr::NLimiter {
class IResourceRequest {
private:
    YDB_READONLY(ui64, Volume, 0);
    virtual void DoOnResourceAllocated() = 0;
public:
    void OnResourceAllocated() {
        return DoOnResourceAllocated();
    }

    virtual ~IResourceRequest() = default;

    IResourceRequest(const ui64 volume)
        : Volume(volume)
    {

    }
};

}
