#pragma once

namespace NKikimr::NPrioritiesQueue {

class IRequest {
protected:
    virtual void DoOnAllocated() = 0;

public:
    void OnAllocated() {
        return DoOnAllocated();
    }
};

}   // namespace NKikimr::NPrioritiesQueue
