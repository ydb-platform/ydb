#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/timer.h>

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/ptr.h>

namespace NYdb::NBS {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

enum EState
{
    IDLE,
    BUSY,
    MAX
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
concept TBusyIdleTimeStorage = requires(T t) {
    {
        t.IncrementState(std::declval<ui64>(), std::declval<EState>())
    } -> std::same_as<void>;
} && std::is_default_constructible<T>::value;

////////////////////////////////////////////////////////////////////////////////

class TDynamicCountersStorage
{
    TIntrusivePtr<TCounterForPtr> BusyTime;
    TIntrusivePtr<TCounterForPtr> IdleTime;

public:
    void Register(TDynamicCountersPtr counters)
    {
        IdleTime = counters->GetCounter("IdleTime", true);
        BusyTime = counters->GetCounter("BusyTime", true);
    }

    void IncrementState(ui64 value, EState state)
    {
        switch (state) {
            case IDLE:
                if (Y_LIKELY(IdleTime)) {
                    IdleTime->Add(value);
                }
                break;
            case BUSY:
                if (Y_LIKELY(BusyTime)) {
                    BusyTime->Add(value);
                }
                break;
            case MAX:
                break;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAtomicsStorage
{
    std::atomic<i64>* BusyTime = nullptr;
    std::atomic<i64>* IdleTime = nullptr;

public:
    void Register(std::atomic<i64>* busyTime, std::atomic<i64>* idleTime)
    {
        BusyTime = busyTime;
        IdleTime = idleTime;
    }

    void IncrementState(ui64 value, EState state)
    {
        switch (state) {
            case IDLE:
                if (Y_LIKELY(IdleTime)) {
                    IdleTime->fetch_add(value, std::memory_order_relaxed);
                }
                break;
            case BUSY:
                if (Y_LIKELY(BusyTime)) {
                    BusyTime->fetch_add(value, std::memory_order_relaxed);
                }
                break;
            case MAX:
                break;
        }
    }
};
}   // namespace

////////////////////////////////////////////////////////////////////////////////

template <TBusyIdleTimeStorage T>
class TBusyIdleTimeCalculator
{
    struct TFields
    {
        ui32 Gen = 0;
        ui32 Inflight = 0;
        ui64 Started = 0;
    };

    static_assert(sizeof(TFields) == 16);
    static_assert(std::is_trivially_copyable_v<TFields>);
    static_assert(std::atomic<TFields>::is_always_lock_free);

    std::atomic<TFields> Fields;

    T Storage;
    ITimerPtr Timer;

public:
    explicit TBusyIdleTimeCalculator(ITimerPtr timer = CreateWallClockTimer())
        : Timer(std::move(timer))
    {
        auto fields = Fields.load();
        fields.Started = Timer->Now().MicroSeconds();
        Fields.store(fields);
    }

    template <typename... Args>
    void Register(Args&&... args)
    {
        Storage.Register(std::forward<Args>(args)...);
    }

    void OnRequestStarted()
    {
        auto fields = Fields.load(std::memory_order_acquire);

        for (;;) {
            auto newFields = fields;
            ++newFields.Inflight;
            ++newFields.Gen;
            ui64 val = 0;
            if (fields.Inflight == 0) {
                ui64 now = Timer->Now().MicroSeconds();
                val = now - fields.Started;
                newFields.Started = now;
            }

            const bool success = Fields.compare_exchange_weak(
                fields,
                newFields,
                std::memory_order_release,
                std::memory_order_acquire);
            if (success) {
                if (val) {
                    Storage.IncrementState(val, EState::IDLE);
                }

                break;
            }
        }
    }

    void OnRequestCompleted()
    {
        auto fields = Fields.load(std::memory_order_acquire);

        for (;;) {
            auto newFields = fields;
            --newFields.Inflight;
            ++newFields.Gen;
            ui64 val = 0;
            if (newFields.Inflight == 0) {
                ui64 now = Timer->Now().MicroSeconds();
                val = now - fields.Started;
                newFields.Started = now;
            }

            const bool success = Fields.compare_exchange_weak(
                fields,
                newFields,
                std::memory_order_release,
                std::memory_order_acquire);
            if (success) {
                if (val) {
                    Storage.IncrementState(val, EState::BUSY);
                }

                break;
            }
        }
    }

    void OnUpdateStats()
    {
        UpdateProgress(IDLE);
        UpdateProgress(BUSY);
    }

private:
    void UpdateProgress(EState state)
    {
        auto fields = Fields.load(std::memory_order_acquire);

        for (;;) {
            auto newFields = fields;
            ui64 value = 0;
            switch (state) {
                case EState::BUSY: {
                    if (fields.Inflight == 0) {
                        return;
                    }

                    break;
                }

                case EState::IDLE: {
                    if (fields.Inflight != 0) {
                        return;
                    }

                    break;
                }

                case EState::MAX: {
                    Y_DEBUG_ABORT_UNLESS(false);
                    return;
                }
            }

            newFields.Started = Timer->Now().MicroSeconds();
            value = newFields.Started - fields.Started;

            const bool success = Fields.compare_exchange_weak(
                fields,
                newFields,
                std::memory_order_release,
                std::memory_order_acquire);
            if (success) {
                Storage.IncrementState(value, state);
                return;
            }
        }
    }
};

using TBusyIdleTimeCalculatorDynamicCounters =
    TBusyIdleTimeCalculator<TDynamicCountersStorage>;

using TBusyIdleTimeCalculatorAtomics = TBusyIdleTimeCalculator<TAtomicsStorage>;

}   // namespace NYdb::NBS
