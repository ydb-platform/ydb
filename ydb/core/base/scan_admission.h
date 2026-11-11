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
        ui64 ActiveLimit = Max<ui64>();
        ui64 BrokerLimit = Max<ui64>();
        ui64 BrokerLimitGeneration = 0;
        bool BrokerConfigurationPending = false;
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
    static ui64 CalculatePendingByteLimit(ui64 totalLimit, ui64 configuredLimit) {
        // A quarter leaves room for supported centroid requests on small
        // nodes, while preserving an independent bound on active scans.
        return Min(configuredLimit, totalLimit / 4);
    }

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
            if (!CanAcquireLocked(bytes, countLimit, bytesLimit)) {
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
            return CalculatePendingByteLimit(State->TotalLimit, configuredLimit);
        }
    }

    ui64 GetActiveByteLimit(ui64 configuredPendingLimit) const {
        with_lock(State->Lock) {
            return Min(Min(State->ActiveLimit, State->BrokerLimit),
                State->TotalLimit - CalculatePendingByteLimit(State->TotalLimit, configuredPendingLimit));
        }
    }

    bool CanAcquire(ui64 bytes, ui64 countLimit, ui64 bytesLimit) const {
        with_lock(State->Lock) {
            return CanAcquireLocked(bytes, countLimit, bytesLimit);
        }
    }

    ui64 GetBrokerLimitGeneration() const {
        with_lock(State->Lock) {
            return State->BrokerLimitGeneration;
        }
    }

    void RestrictBrokerLimit(ui64 bytes, ui64 generation) {
        with_lock(State->Lock) {
            // A queued scan may receive its rejection after a newer broker
            // configuration has already been acknowledged.
            if (generation == State->BrokerLimitGeneration) {
                State->BrokerLimit = Min(State->BrokerLimit, bytes);
            }
        }
    }

    void ConfirmBrokerLimit(ui64 bytes) {
        with_lock(State->Lock) {
            State->BrokerLimit = bytes;
            ++State->BrokerLimitGeneration;
            State->BrokerConfigurationPending = false;
        }
    }

    bool IsBrokerConfigurationPending() const {
        with_lock(State->Lock) {
            return State->BrokerConfigurationPending;
        }
    }

    void SetTotalLimit(ui64 bytes, ui64 activeLimit = Max<ui64>(), bool configurationPending = false) {
        with_lock(State->Lock) {
            State->TotalLimit = bytes;
            if (State->ActiveLimit != activeLimit) {
                State->BrokerLimit = Max<ui64>();
            }
            State->ActiveLimit = activeLimit;
            State->BrokerConfigurationPending |= configurationPending;
        }
    }

    TUsage GetUsage() const {
        with_lock(State->Lock) {
            return {State->Count, State->Bytes};
        }
    }

private:
    bool CanAcquireLocked(ui64 bytes, ui64 countLimit, ui64 bytesLimit) const {
        bytesLimit = CalculatePendingByteLimit(State->TotalLimit, bytesLimit);
        return State->Count < countLimit && State->Bytes <= bytesLimit && bytes <= bytesLimit - State->Bytes;
    }

    const std::shared_ptr<TState> State = std::make_shared<TState>();
};

} // namespace NKikimr
