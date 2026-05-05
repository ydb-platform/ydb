#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NKikimr::NKqp {

///
/// Interface for updating the execution state of a KQP session within the Workload Manager (WM).
/// Used to track request progression through WM queues (pending, delayed) and execution start.
///   
struct IWmSessionUpdater {
    enum EWmState : ui32 {
        NONE = 0,      // Request is not in workload manager queue
        PENDING = 1,   // Request is in local pending queue, waiting to be delayed or started
        DELAYED = 2,   // Request is in delayed_requests table, waiting for available slot
        EXITED = 3     // Request has exited from WM
    };

    virtual ~IWmSessionUpdater() = default;

    virtual void SetRequestState(EWmState state, TInstant timestamp) = 0;
    virtual void SetPoolId(TString poolId) = 0;
};

}
