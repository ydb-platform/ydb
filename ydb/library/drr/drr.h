#pragma once

#include <util/system/yassert.h>
#include <util/system/types.h>
#include <util/generic/ptr.h>
#include <util/generic/hash.h>
#include <util/generic/typetraits.h>
#include <util/generic/string.h>
#include <ydb/library/planner/base/defs.h>
#include "probes.h"

namespace NScheduling {

class TDRRSchedulerBase;
template <class TQueueType, class TId = TString> class TDRRScheduler;

class TDRRQueue {
private:
    TDRRSchedulerBase* Scheduler = nullptr;
    TWeight Weight;
    TUCost DeficitCounter;
    TUCost QuantumFraction = 0;
    TUCost MaxBurst;
    // Active list (cyclic double-linked list of active queues for round-robin to cycle through)
    TDRRQueue* ActNext = nullptr;
    TDRRQueue* ActPrev = nullptr;
    TString Name;
public:
    TDRRQueue(TWeight w = 1, TUCost maxBurst = 0, const TString& name = TString())
        : Weight(w)
        , DeficitCounter(maxBurst)
        , MaxBurst(maxBurst)
        , Name(name)
    {}

    TDRRSchedulerBase* GetScheduler() { return Scheduler; }
    TWeight GetWeight() const { return Weight; }
    TUCost GetDeficitCounter() const { return DeficitCounter; }
    TUCost GetQuantumFraction() const { return QuantumFraction; }
    const TString& GetName() const { return Name; }
    bool IsActive() const { return ActNext != nullptr; }
protected:
    void OnSchedulerAttach() {} // To be overriden in derived class
    void OnSchedulerDetach() {} // To be overriden in derived class
private: // For TDRRScheduler
    friend class TDRRSchedulerBase;
    template <class Q, class I> friend class TDRRScheduler;

    void SetScheduler(TDRRSchedulerBase* scheduler) { Scheduler = scheduler; }
    void SetWeight(TWeight w) { Y_ABORT_UNLESS(w != 0, "zero weight in queue '%s'", Name.data()); Weight = w; }
    void SetDeficitCounter(TUCost c) { DeficitCounter = c; }
    void SetQuantumFraction(TUCost c) { QuantumFraction = c; }

    TDRRQueue* GetActNext() { return ActNext; }
    void SetActNext(TDRRQueue* v) { ActNext = v; }
    TDRRQueue* GetActPrev() { return ActPrev; }
    void SetActPrev(TDRRQueue* v) { ActPrev = v; }

    void AddCredit(TUCost c)
    {
        DeficitCounter += c;
    }

    void UseCredit(TUCost c)
    {
        if (DeficitCounter > c) {
            DeficitCounter -= c;
        } else {
            DeficitCounter = 0;
        }
    }

    bool CreditOverflow() const
    {
        return DeficitCounter >= MaxBurst;
    }

    void FixCreditOverflow()
    {
        DeficitCounter = Min(MaxBurst, DeficitCounter);
    }
};

class TDRRSchedulerBase: public TDRRQueue {
protected:
    TUCost Quantum;
    TWeight AllWeight = 0;
    TWeight ActWeight = 0;
    TDRRQueue* CurQueue = nullptr;
    TDRRQueue* LastQueue = nullptr;
    void* Peek = nullptr;
public:
    TDRRSchedulerBase(TUCost quantum, TWeight w = 1, TUCost maxBurst = 0, const TString& name = TString())
        : TDRRQueue(w, maxBurst, name)
        , Quantum(quantum)
    {}

    // Must be called when queue becomes backlogged
    void ActivateQueue(TDRRQueue* queue)
    {
        if (queue->IsActive()) {
            return;
        }
        DRR_PROBE(ActivateQueue, GetName(), queue->GetName());
        ActWeight += queue->GetWeight();
        if (CurQueue == nullptr) {
            // First active queue
            queue->SetActNext(queue);
            queue->SetActPrev(queue);
            CurQueue = queue;
            if (GetScheduler()) {
                GetScheduler()->ActivateQueue(this);
            }
        } else {
            // Add queue to the tail of active list to avoid unfairness due to
            // otherwise possible multiple reorderings in a single round
            TDRRQueue* after = CurQueue->GetActPrev();
            queue->SetActNext(CurQueue);
            queue->SetActPrev(after);
            CurQueue->SetActPrev(queue);
            after->SetActNext(queue);
        }
    }

    // Must be called after each push to queue front
    void DropCache(TDRRQueue* queue)
    {
        DRR_PROBE(DropCache, GetName(), queue->GetName());
        if (CurQueue == queue && Peek) {
            DRR_PROBE(CacheDropped, GetName(), queue->GetName());
            Peek = nullptr;
            if (GetScheduler()) {
                GetScheduler()->DropCache(this);
            }
        }
    }
};

// Deficit Weighted Round Robin scheduling algorithm
// http://en.wikipedia.org/wiki/Deficit_round_robin
// TODO(serxa): add TSharingPolicy (as template parameter that is inherited) to control Quantum splitting
// TODO(serxa): add support for non-zero DeficitCounter while idle
template <class TQueueType, class TId>
class TDRRScheduler: public TDRRSchedulerBase {
    static_assert(std::is_base_of<TDRRQueue, TQueueType>::value, "TQueueType must inherit TDRRQueue");
private:
    typedef std::shared_ptr<TQueueType> TQueuePtr;
    typedef THashMap<TId, TQueuePtr> TAllQueues;
    TAllQueues AllQueues;
public:
    typedef typename TQueueType::TTask TTask;
    explicit TDRRScheduler(TUCost quantum, TWeight w = 1, TUCost maxBurst = 0, const TString& name = TString())
        : TDRRSchedulerBase(quantum, w, maxBurst, name)
    {}

    template <class Func>
    void ForEachQueue(Func func) const {
        for (auto kv : AllQueues) {
            func(kv.first, kv.second.Get());
        }
    }

    // Add an empty queue
    // NOTE: if queue is not empty ActivateQueue() must be called after Add()
    bool AddQueue(typename TTypeTraits<TId>::TFuncParam id, TQueuePtr queue)
    {
        if (AllQueues.contains(id)) {
            return false;
        }
        queue->SetScheduler(this);
        AllQueues.insert(std::make_pair(id, queue));
        AllWeight += queue->GetWeight();
        UpdateQuantums();
        queue->OnSchedulerAttach();
        return true;
    }

    // Delete queue by Id
    void DeleteQueue(typename TTypeTraits<TId>::TFuncParam id)
    {
        typename TAllQueues::iterator i = AllQueues.find(id);
        if (i != AllQueues.end()) {
            TQueueType* queue = i->second.get();
            queue->OnSchedulerDetach();
            if (queue->IsActive()) {
                DeactivateQueue(queue);
            }
            queue->SetScheduler(nullptr);
            AllWeight -= queue->GetWeight();
            AllQueues.erase(id);
        }
    }

    // Get queue by Id
    TQueuePtr GetQueue(typename TTypeTraits<TId>::TFuncParam id)
    {
        typename TAllQueues::iterator i = AllQueues.find(id);
        if (i != AllQueues.end()) {
            return i->second;
        } else {
            return TQueuePtr();
        }
    }

    // Get queue by Id
    TQueueType* GetQueuePtr(typename TTypeTraits<TId>::TFuncParam id)
    {
        typename TAllQueues::iterator i = AllQueues.find(id);
        if (i != AllQueues.end()) {
            return i->second.Get();
        } else {
            return nullptr;
        }
    }

    // Update queue weight
    void UpdateQueueWeight(TQueueType* queue, TWeight w)
    {
        if (w) {
            AllWeight -= queue->GetWeight();
            if (queue->IsActive()) {
                ActWeight -= queue->GetWeight();
            }
            queue->SetWeight(w);
            if (queue->IsActive()) {
                ActWeight += w;
            }
            AllWeight += w;
            UpdateQuantums();
        }
    }

    TQueueType* GetCurQueue()
    {
        return static_cast<TQueueType*>(CurQueue);
    }

    TTask* GetPeek()
    {
        return static_cast<TTask*>(Peek);
    }

    // Runs scheduling algorithm and returns task to be served next or nullptr if empty
    TTask* PeekTask()
    {
        if (!Peek) {
            if (!CurQueue) {
                LastQueue = nullptr;
                return nullptr;
            }

            if (!LastQueue) {
                GiveCredit(); // For first round at start of backlogged period
            }
            while (GetCurQueue()->Empty() || GetCurQueue()->PeekTask()->GetCost() > CurQueue->GetDeficitCounter()) {
                if (!NextNonEmptyQueue()) {
                    // If all queue was deactivated due to credit overflow
                    LastQueue = nullptr;
                    return nullptr;
                }
            }
            Peek = GetCurQueue()->PeekTask();
        }
        return GetPeek();
    }

    // Pops peek task from queue
    void PopTask()
    {
        if (!PeekTask()) {
            return; // No more tasks
        }

        // Pop peek task from current queue and use credit
        CurQueue->UseCredit(GetPeek()->GetCost());
        Peek = nullptr;
//        Cerr << "pop\t" << (ui64)CurQueue << "\tDC:" << CurQueue->DeficitCounter << Endl;
        GetCurQueue()->PopTask();

        LastQueue = CurQueue;
    }

    bool Empty()
    {
        PeekTask();
        return !CurQueue;
    }

private:
    void GiveCredit()
    {
        // Maintain given Quantum to be split to exactly one round of active queues
        CurQueue->AddCredit(CurQueue->GetQuantumFraction()*AllWeight/ActWeight);
//        Cerr << "credit\t" << (ui64)CurQueue << "\tplus:" << CurQueue->GetQuantumFraction()*AllWeight/ActWeight
//             << "\tDC:" << CurQueue->DeficitCounter << Endl;
    }

    bool NextNonEmptyQueue(TDRRQueue* next = nullptr)
    {
        while (true) {
            CurQueue = next? next: CurQueue->GetActNext();
            next = nullptr;
            GiveCredit();
            if (GetCurQueue()->Empty()) {
                if (CurQueue->CreditOverflow()) {
                    next = DeactivateQueueImpl(CurQueue);
                    if (!CurQueue) {
                        return false; // Last empty queue was deactivated due to credit overflow
                    }
                }
                continue;
            }
            break;
        }
        return true;
    }

    void DeactivateQueue(TDRRQueue* queue)
    {
        if (TDRRQueue* next = DeactivateQueueImpl(queue)) {
            NextNonEmptyQueue(next);
        }
    }

    TDRRQueue* DeactivateQueueImpl(TDRRQueue* queue)
    {
//        Cerr << "deactivate\t" << (ui64)queue << Endl;
        TDRRQueue* ret = nullptr;
        ActWeight -= queue->GetWeight();
        Y_ASSERT(queue->IsActive());
        TDRRQueue* before = queue->GetActPrev();
        TDRRQueue* after = queue->GetActNext();
        bool lastActive = (before == queue);
        if (lastActive) {
            CurQueue = nullptr;
        } else {
            if (queue == CurQueue) {
                ret = CurQueue->GetActNext();
            }
            before->SetActNext(after);
            after->SetActPrev(before);
        }
        queue->SetActNext(nullptr);
        queue->SetActPrev(nullptr);

        // Keep deficit counter of non-backlogged queues limited by MaxBurst
        // to exclude accumulation of resource while being idle
        queue->FixCreditOverflow();
        return ret;
    }

    void UpdateQuantums()
    {
        TUCost qsum = Quantum;
        TWeight wsum = AllWeight;
        for (typename TAllQueues::iterator i = AllQueues.begin(), e = AllQueues.end(); i != e; ++i) {
            TQueueType* qi = i->second.get();
            qi->SetQuantumFraction(WCut(qi->GetWeight(), wsum, qsum));
        }
    }
};

}
