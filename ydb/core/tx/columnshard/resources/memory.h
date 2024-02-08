#pragma once
#include <ydb/core/tx/columnshard/counters/common/object_counter.h>
#include <ydb/core/tx/columnshard/counters/common/owner.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/core/log.h>
#include <util/system/mutex.h>

namespace NKikimr::NOlap {

class TMemoryAggregation: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr DeriviativeAddInFlightBytes;
    NMonitoring::TDynamicCounters::TCounterPtr DeriviativeRemoveInFlightBytes;
    std::shared_ptr<NColumnShard::TValueAggregationClient> InFlightBytes;
public:
    TMemoryAggregation(const TString& moduleId, const TString& signalId)
        : TBase(moduleId) {
        DeriviativeAddInFlightBytes = TBase::GetDeriviative(signalId + "/Add/Bytes");
        DeriviativeRemoveInFlightBytes = TBase::GetDeriviative(signalId + "/Remove/Bytes");
        InFlightBytes = TBase::GetValueAutoAggregationsClient(signalId + "/Bytes");
    }

    void AddBytes(const ui64 size) {
        DeriviativeAddInFlightBytes->Add(size);
        InFlightBytes->Add(size);
    }

    void RemoveBytes(const ui64 size) {
        DeriviativeRemoveInFlightBytes->Add(size);
        InFlightBytes->Remove(size);
    }
};

class TScanMemoryCounter: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr MinimalMemory;
    NMonitoring::TDynamicCounters::TCounterPtr AvailableMemory;
    NMonitoring::TDynamicCounters::TCounterPtr DeriviativeTake;
    NMonitoring::TDynamicCounters::TCounterPtr DeriviativeFree;

    NMonitoring::TDynamicCounters::TCounterPtr DeriviativeWaitingStart;
    NMonitoring::TDynamicCounters::TCounterPtr DeriviativeWaitingFinish;
    NMonitoring::TDynamicCounters::TCounterPtr CurrentInWaiting;
public:
    TScanMemoryCounter(const TString& limitName, const ui64 memoryLimit);

    void Take(const ui64 size) const {
        DeriviativeTake->Add(size);
        AvailableMemory->Sub(size);
    }

    void OnMinimal(const ui64 size) const {
        MinimalMemory->Set(size);
    }

    void WaitStart() const {
        DeriviativeWaitingStart->Add(1);
        CurrentInWaiting->Add(1);
    }

    void WaitFinish() const {
        DeriviativeWaitingFinish->Add(1);
        CurrentInWaiting->Sub(1);
    }

    void Free(const ui64 size) const {
        DeriviativeFree->Add(size);
        AvailableMemory->Add(size);
    }
};

class TScanMemoryLimiter: TNonCopyable {
public:
    const TString LimiterName;
    const i64 AvailableMemoryLimit;
    class IMemoryAccessor;
    class TGuard: TNonCopyable {
    private:
        TAtomicCounter Value = 0;
        std::shared_ptr<IMemoryAccessor> MemoryAccessor;
        std::shared_ptr<TMemoryAggregation> MemorySignals;
    public:
        TGuard(std::shared_ptr<IMemoryAccessor> accesor, std::shared_ptr<TMemoryAggregation> memorySignals = nullptr)
            : MemoryAccessor(accesor)
            , MemorySignals(memorySignals) {
        }
        ~TGuard() {
            FreeAll();
        }
        i64 GetValue() const {
            return Value.Val();
        }
        void FreeAll();
        void Free(const ui64 size);
        void Take(const ui64 size);
        std::shared_ptr<TGuard> MakeSame() const {
            return std::make_shared<TGuard>(MemoryAccessor, MemorySignals);
        }
    };

    class IMemoryAccessor: public NColumnShard::TMonitoringObjectsCounter<IMemoryAccessor> {
    private:
        TAtomicCounter InWaitingFlag = 0;
        std::shared_ptr<TScanMemoryLimiter> Owner;
        friend class TScanMemoryLimiter;
        void StartWaiting() {
            Y_ABORT_UNLESS(InWaitingFlag.Val() == 0);
            InWaitingFlag = 1;
        }
    protected:
        virtual void DoOnBufferReady() = 0;
    public:
        using TPtr = std::shared_ptr<IMemoryAccessor>;
        IMemoryAccessor(std::shared_ptr<TScanMemoryLimiter> owner)
            : Owner(owner)
        {

        }

        bool HasBuffer() {
            return Owner->HasBufferOrSubscribe(nullptr);
        }

        TScanMemoryLimiter& GetLimiter() const {
            return *Owner;
        }

        virtual ~IMemoryAccessor() = default;
        void Take(const ui64 size) const {
            Owner->Take(size);
        }

        void Free(const ui64 size) const {
            Owner->Free(size);
        }

        void OnBufferReady() {
            Y_ABORT_UNLESS(InWaitingFlag.Val() == 1);
            InWaitingFlag = 0;
            DoOnBufferReady();
        }

        bool InWaiting() const {
            return InWaitingFlag.Val();
        }
    };

private:
    TAtomicCounter AvailableMemory = 0;
    TAtomicCounter WaitingCounter = 0;
    TScanMemoryCounter Counters;
    std::deque<std::shared_ptr<IMemoryAccessor>> InWaiting;

    TMutex Mutex;
    i64 MinMemory = 0;

    void Free(const ui64 size);
    void Take(const ui64 size);
public:
    TScanMemoryLimiter(const TString& limiterName, const ui64 memoryLimit)
        : LimiterName(limiterName)
        , AvailableMemoryLimit(memoryLimit)
        , AvailableMemory(memoryLimit)
        , Counters("MemoryLimiters/" + limiterName, memoryLimit)
    {

    }
    bool HasBufferOrSubscribe(std::shared_ptr<IMemoryAccessor> accessor);
};

class TMemoryLimitersController {
private:
    TRWMutex Mutex;
    // limiter by limit name
    THashMap<TString, std::shared_ptr<TScanMemoryLimiter>> Limiters;
    static const inline ui64 StandartLimit = (ui64)1 * 1024 * 1024 * 1024;
    std::shared_ptr<TScanMemoryLimiter> GetLimiterImpl(const TString& name) {
        TReadGuard rg(Mutex);
        auto it = Limiters.find(name);
        if (it == Limiters.end()) {
            rg.Release();
            TWriteGuard wg(Mutex);
            it = Limiters.find(name);
            if (it != Limiters.end()) {
                return it->second;
            } else {
                return Limiters.emplace(name, std::make_shared<TScanMemoryLimiter>(name, StandartLimit)).first->second;
            }
        } else {
            return it->second;
        }
    }
public:
    static std::shared_ptr<TScanMemoryLimiter> GetLimiter(const TString& name) {
        return Singleton<TMemoryLimitersController>()->GetLimiterImpl(name);
    }

};

}
