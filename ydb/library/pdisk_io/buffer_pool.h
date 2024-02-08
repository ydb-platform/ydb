#pragma once

#include <ydb/library/actors/util/queue_oneone_inplace.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/threading/queue/mpsc_read_as_filled.h>
#include <library/cpp/threading/queue/mpsc_vinfarr_obstructive.h>

#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/generic/vector.h>
#include <util/generic/list.h>

namespace NKikimr::NPDisk {

template<typename TObject, ui32 Size>
class TPool {
    TVector<TObject> Objects;
    NThreading::TObstructiveConsumerAuxQueue<TObject> InPoolObjects;
    ::NMonitoring::TDynamicCounters::TCounterPtr TotalAllocatedObjects;
    TAtomic FreeObjects;
    ::NMonitoring::TDynamicCounters::TCounterPtr FreeObjectsMin;

    public:
    TPool()
        : Objects(Size)
        , FreeObjects(Size)
    {
        for (auto it = Objects.begin(); it != Objects.end(); ++it) {
            InPoolObjects.Push(&(*it));
        }
    }

    TObject *Pop() {
        TObject *obj = InPoolObjects.Pop();
        if (!obj) {
            obj = new TObject();
            if (TotalAllocatedObjects) {
                *TotalAllocatedObjects += 1;
            }
        }
        TAtomicBase currentFree = AtomicDecrement(FreeObjects);
        if (FreeObjectsMin) {
            *FreeObjectsMin = Min(static_cast<TAtomicBase>(*FreeObjectsMin), currentFree);
        }
        return obj;
    }

    bool IsFromPool(TObject *obj) {
        return &Objects.front() <= obj && obj <= &Objects.back();
    }

    void Push(TObject *obj) {
        AtomicIncrement(FreeObjects);
        if (IsFromPool(obj)) {
            InPoolObjects.Push(obj);
        } else {
            delete obj;
        }
    }

    void InitializeMonitoring(::NMonitoring::TDynamicCounters::TCounterPtr totalAllocatedObjects,
            ::NMonitoring::TDynamicCounters::TCounterPtr freeObjectsMin) {
        TotalAllocatedObjects = totalAllocatedObjects;
        FreeObjectsMin = freeObjectsMin;
        *FreeObjectsMin = AtomicGet(FreeObjects);
    }

    ~TPool() {
        while(InPoolObjects.Pop());
    }
};

} // NKikimr::NPDisk
