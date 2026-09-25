#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <ydb/core/protos/kqp.pb.h>

namespace NKikimr::NWorkloadManager {

///
/// Interface for updating the execution state of a KQP session within the Workload Manager (WM).
/// Used to track request progression through WM queues (pending, delayed) and execution start.
///
class ISessionUpdater {
public:
    enum EState : ui32 {
        NONE = 0,      // Request is not in workload manager queue
        PENDING = 1,   // Request is in local pending queue, waiting to be delayed or started
        DELAYED = 2,   // Request is in delayed_requests table, waiting for available slot
        EXITED = 3     // Request has exited from WM
    };

    virtual ~ISessionUpdater() = default;

    virtual void SetRequestState(EState state, TInstant timestamp) = 0;
    virtual void SetPoolContext(TString poolId, TString classifiedBy) = 0;

    virtual EState GetState() const = 0;
    virtual TString GetClassifiedBy() const = 0;
};

inline NKikimrKqp::EWmState WmStateToProto(ISessionUpdater::EState state) {
    switch (state) {
        case ISessionUpdater::EState::PENDING:
        case ISessionUpdater::EState::DELAYED:
            return NKikimrKqp::WM_STATE_QUEUED;
        case ISessionUpdater::EState::EXITED:
            return NKikimrKqp::WM_STATE_EXECUTING;
        case ISessionUpdater::EState::NONE:
        default:
            return NKikimrKqp::WM_STATE_NONE;
    }
}

///
/// Convert the protobuf EWmState enum to a human-readable status string.
/// Returns "QUEUED" for WM_STATE_QUEUED, "EXECUTING" for WM_STATE_EXECUTING,
/// and an empty string for WM_STATE_NONE (not managed by the workload manager).
///
inline TString WmStateToStatus(NKikimrKqp::EWmState state) {
    switch (state) {
        case NKikimrKqp::WM_STATE_QUEUED:
            return "QUEUED";
        case NKikimrKqp::WM_STATE_EXECUTING:
            return "EXECUTING";
        case NKikimrKqp::WM_STATE_NONE:
        default:
            return "";
    }
}

inline TString WmStateToStatus(ISessionUpdater::EState state) {
    return WmStateToStatus(WmStateToProto(state));
}

///
/// Check if the WM session state indicates the request is in the WM queue
/// (either PENDING or DELAYED).
///
inline bool IsWmStateQueued(ISessionUpdater::EState state) {
    return state == ISessionUpdater::EState::PENDING ||
           state == ISessionUpdater::EState::DELAYED;
}

} // namespace NKikimr::NWorkloadManager
