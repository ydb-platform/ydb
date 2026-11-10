#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/size_literals.h>
#include <util/generic/algorithm.h>
#include <util/generic/ylimits.h>
#include <util/system/guard.h>
#include <util/system/spinlock.h>

#include <memory>

namespace NKikimr {

// One instance per AppData/node, shared by every DataShard. The lease owns the
// accounting state, so its destruction does not depend on the tablet's lifetime.
class TScanAdmissionControl {
    struct TState {
        TSpinLock Lock;
        ui64 Count = 0;
        ui64 Bytes = 0;
        ui64 TotalLimit = 512_MB;
        ::NMonitoring::TDynamicCounters::TCounterPtr CountCounter;
        ::NMonitoring::TDynamicCounters::TCounterPtr BytesCounter;
        ::NMonitoring::TDynamicCounters::TCounterPtr RejectedCounter;

        void UpdateCounters() {
            if (CountCounter) {
                CountCounter->Set(Count);
                BytesCounter->Set(Bytes);
            }
        }
    };

public:
    struct TUsage {
        ui64 Count;
        ui64 Bytes;
    };

    class TLease {
    public:
        ~TLease() {
            if (Armed) {
                with_lock(State->Lock) {
                    Y_ABORT_UNLESS(State->Count && State->Bytes >= Bytes);
                    --State->Count;
                    State->Bytes -= Bytes;
                    State->UpdateCounters();
                }
            }
        }

        TLease(const TLease&) = delete;
        TLease& operator=(const TLease&) = delete;

    private:
        friend class TScanAdmissionControl;
        TLease(std::shared_ptr<TState> state, ui64 bytes)
            : State(std::move(state))
            , Bytes(bytes)
        {}

        const std::shared_ptr<TState> State;
        const ui64 Bytes;
        bool Armed = false;
    };

    std::shared_ptr<TLease> TryAcquire(ui64 bytes, ui64 countLimit, ui64 bytesLimit,
        const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters = {})
    {
        // Allocate before charging: allocation failure cannot leak a reservation.
        auto lease = std::shared_ptr<TLease>(new TLease(State, bytes));
        with_lock(State->Lock) {
            if (counters && !State->CountCounter) {
                State->CountCounter = counters->GetCounter("PendingScans");
                State->BytesCounter = counters->GetCounter("PendingBytes");
                State->RejectedCounter = counters->GetCounter("RejectedScans", true);
            }
            // Keep pending descriptors inside the same overall index-build
            // budget as the active broker queue, including on small nodes.
            bytesLimit = Min(bytesLimit, State->TotalLimit / 8);
            if (State->Count >= countLimit || State->Bytes > bytesLimit || bytes > bytesLimit - State->Bytes) {
                if (State->RejectedCounter) {
                    State->RejectedCounter->Inc();
                }
                return {};
            }
            ++State->Count;
            State->Bytes += bytes;
            lease->Armed = true;
            State->UpdateCounters();
        }
        return lease;
    }

    ui64 GetByteLimit(ui64 configuredLimit) const {
        with_lock(State->Lock) {
            return Min(configuredLimit, State->TotalLimit / 8);
        }
    }

    void SetTotalLimit(ui64 bytes) {
        with_lock(State->Lock) {
            State->TotalLimit = bytes;
        }
    }

    TUsage GetUsage() const {
        with_lock(State->Lock) {
            return {State->Count, State->Bytes};
        }
    }

private:
    const std::shared_ptr<TState> State = std::make_shared<TState>();
};

} // namespace NKikimr
