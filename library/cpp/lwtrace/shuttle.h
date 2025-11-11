#pragma once

#include "event.h"

#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/generic/ptr.h>
#include <util/system/spinlock.h>

#include <algorithm>
#include <type_traits>

namespace NLWTrace {
    struct TProbe;

    class IShuttle;
    using TShuttlePtr = TIntrusivePtr<IShuttle>;

    // Represents interface of tracing context passing by application between probes
    class alignas(8) IShuttle: public TThrRefBase {
    private:
        ui64 TraceIdx;
        ui64 SpanId;
        ui64 ParentSpanId = 0;
        TAtomic Status = 0;
        static constexpr ui64 DeadFlag = 0x1ull;
        TShuttlePtr Next;

    public:
        explicit IShuttle(ui64 traceIdx, ui64 spanId)
            : TraceIdx(traceIdx)
            , SpanId(spanId)
        {
        }

        virtual ~IShuttle() {
        }

        ui64 GetTraceIdx() const {
            return TraceIdx;
        }

        ui64 GetSpanId() const {
            return SpanId;
        }

        ui64 GetParentSpanId() const {
            return ParentSpanId;
        }

        void SetParentSpanId(ui64 parentSpanId) {
            ParentSpanId = parentSpanId;
        }

        template <class F, class R>
        R UnlessDead(F func, R dead) {
            while (true) {
                ui64 status = AtomicGet(Status);
                if (status & DeadFlag) {
                    return dead;
                }
                if (AtomicCas(&Status, status + 2, status)) {
                    R result = func();
                    ATOMIC_COMPILER_BARRIER();
                    AtomicSub(Status, 2);
                    return result;
                }
            }
        }

        template <class F>
        void UnlessDead(F func) {
            while (true) {
                ui64 status = AtomicGet(Status);
                if (status & DeadFlag) {
                    return;
                }
                if (AtomicCas(&Status, status + 2, status)) {
                    func();
                    ATOMIC_COMPILER_BARRIER();
                    AtomicSub(Status, 2);
                    return;
                }
            }
        }

        void Serialize(TShuttleTrace& msg) {
            UnlessDead([&] {
                DoSerialize(msg);
            });
        }

        // Returns false iff shuttle should be destroyed
        bool AddProbe(TProbe* probe, const TParams& params, ui64 timestamp = 0) {
            return UnlessDead([&] {
                return DoAddProbe(probe, params, timestamp);
            }, false);
        }

        // Track object was destroyed
        void EndOfTrack() {
            if (Next) {
                Next->EndOfTrack();
                Next.Reset();
            }
            UnlessDead([&] {
                DoEndOfTrack();
            });
        }

        // Shuttle was dropped from orbit
        TShuttlePtr Drop() {
            UnlessDead([&] {
                DoDrop();
            });
            return Detach(); // Detached from orbit on Drop
        }

        TShuttlePtr Detach() {
            TShuttlePtr result;
            result.Swap(Next);
            return result;
        }

        // Trace session was destroyed
        void Kill() {
            AtomicAdd(Status, 1); // Sets DeadFlag
            while (AtomicGet(Status) != 1) { // Wait until any vcall is over
                SpinLockPause();
            }
            // After this point all virtual calls on shuttle are disallowed
            ATOMIC_COMPILER_BARRIER();
        }

        const TShuttlePtr& GetNext() const {
            return Next;
        }

        TShuttlePtr& GetNext() {
            return Next;
        }

        void SetNext(const TShuttlePtr& next) {
            Next = next;
        }

        bool Fork(TShuttlePtr& child) {
            return UnlessDead([&] {
                return DoFork(child);
            }, true);
        }

        bool Join(TShuttlePtr& child) {
            return UnlessDead([&] {
                return DoJoin(child);
            }, false);
        }

        bool IsDead() {
            return AtomicGet(Status) & DeadFlag;
        }

    protected:
        virtual bool DoAddProbe(TProbe* probe, const TParams& params, ui64 timestamp) = 0;
        virtual void DoEndOfTrack() = 0;
        virtual void DoDrop() = 0;
        virtual void DoSerialize(TShuttleTrace& msg) = 0;
        virtual bool DoFork(TShuttlePtr& child) = 0;
        virtual bool DoJoin(const TShuttlePtr& child) = 0;
    };

    // Not thread-safe orbit
    // Orbit should not be used concurrenty, lock is used
    // to ensure this is the case and avoid race condition if not.
    class TOrbit {
    private:
        TShuttlePtr HeadNoLock;
    public:
        TOrbit() = default;
        TOrbit(const TOrbit&) = delete;
        TOrbit(TOrbit&&) = default;

        TOrbit& operator=(const TOrbit&) = delete;
        TOrbit& operator=(TOrbit&&) = default;

        ~TOrbit() {
            Reset();
        }

        void Reset() {
            NotConcurrent([] (TShuttlePtr& head) {
                if (head) {
                    head->EndOfTrack();
                    head.Reset();
                }
            });
        }

        // Checks if there is at least one shuttle in orbit
        // NOTE: used by every LWTRACK macro check, so keep it optimized - do not lock
        bool HasShuttles() const {
            return HeadNoLock.Get();
        }

        void AddShuttle(const TShuttlePtr& shuttle) {
            NotConcurrent([&] (TShuttlePtr& head) {
                Y_ABORT_UNLESS(!shuttle->GetNext());
                shuttle->SetNext(head);
                head = shuttle;
            });
        }

        // Moves every shuttle from `orbit' into this
        void Take(TOrbit& orbit) {
            NotConcurrent([&] (TShuttlePtr& head) {
                orbit.NotConcurrent([&] (TShuttlePtr& oHead) {
                    TShuttlePtr* ref = &oHead;
                    if (ref->Get()) {
                        while (TShuttlePtr& next = (*ref)->GetNext()) {
                            ref = &next;
                        }
                        (*ref)->SetNext(head);
                        head.Swap(oHead);
                        oHead.Reset();
                    }
                });
            });
        }

        void AddProbe(TProbe* probe, const TParams& params, ui64 timestamp = 0) {
            NotConcurrent([&] (TShuttlePtr& head) {
                TShuttlePtr* ref = &head;
                while (IShuttle* s = ref->Get()) {
                    if (!s->AddProbe(probe, params, timestamp)) { // Shuttle self-destructed
                        *ref = s->Drop(); // May destroy shuttle
                    } else {
                        ref = &s->GetNext(); // Keep shuttle
                    }
                }
            });
        }

        template <class TFunc>
        void ForEachShuttle(ui64 traceIdx, TFunc&& func) {
            NotConcurrent([&] (TShuttlePtr& head) {
                TShuttlePtr* ref = &head;
                while (IShuttle* s = ref->Get()) {
                    if (s->GetTraceIdx() == traceIdx && !func(s)) { // Shuttle self-destructed
                        *ref = s->Drop(); // May destroy shuttle
                    } else {
                        ref = &s->GetNext(); // Keep shuttle
                    }
                }
            });
        }

        void Serialize(ui64 traceIdx, TShuttleTrace& msg) {
            ForEachShuttle(traceIdx, [&] (NLWTrace::IShuttle* shuttle) {
                shuttle->Serialize(msg);
                return false;
            });
        }

        bool HasShuttle(ui64 traceIdx) {
            return NotConcurrent([=] (TShuttlePtr& head) {
                TShuttlePtr ref = head;
                while (IShuttle* s = ref.Get()) {
                    if (s->GetTraceIdx() == traceIdx) {
                        return true;
                    } else {
                        ref = s->GetNext();
                    }
                }
                return false;
            });
        }

        bool Fork(TOrbit& child) {
            return NotConcurrent([&] (TShuttlePtr& head) {
                return child.NotConcurrent([&] (TShuttlePtr& cHead) {
                    bool result = true;
                    TShuttlePtr* ref = &head;
                    while (IShuttle* shuttle = ref->Get()) {
                        if (shuttle->IsDead()) {
                            *ref = shuttle->Drop();
                        } else {
                            result = result && shuttle->Fork(cHead);
                            ref = &shuttle->GetNext();
                        }
                    }
                    return result;
                });
            });
        }

        void Join(TOrbit& child) {
            NotConcurrent([&] (TShuttlePtr& head) {
                child.NotConcurrent([&] (TShuttlePtr& cHead) {
                    TShuttlePtr* ref = &head;
                    while (IShuttle* shuttle = ref->Get()) {
                        if (shuttle->IsDead()) {
                            *ref = shuttle->Drop();
                        } else {
                            child.Join(cHead, shuttle);
                            ref = &shuttle->GetNext();
                        }
                    }
                });
            });
        }

    private:
        static void Join(TShuttlePtr& head, IShuttle* parent) {
            TShuttlePtr* ref = &head;
            while (IShuttle* child = ref->Get()) {
                if (parent->GetTraceIdx() == child->GetTraceIdx() && parent->GetSpanId() == child->GetParentSpanId()) {
                    TShuttlePtr next = child->Detach();
                    parent->Join(*ref);
                    *ref = next;
                } else {
                    ref = &child->GetNext();
                }
            }
        }

        template <class TFunc>
        typename std::invoke_result<TFunc, TShuttlePtr&>::type NotConcurrent(TFunc func) {
            // `HeadNoLock` is binary-copied into local `headCopy` and written with special `locked` value
            // during not concurrent operations. Not concurrent operations should not work
            // with `HeadNoLock` directly. Instead `headCopy` is passed into `func` by reference and
            // after `func()` it is binary-copied back into `HeadNoLock`.
            static_assert(sizeof(HeadNoLock) == sizeof(TAtomic));
            TAtomic* headPtr = reinterpret_cast<TAtomic*>(&HeadNoLock);
            TAtomicBase headCopy = AtomicGet(*headPtr);
            static constexpr TAtomicBase locked = 0x1ull;
            if (headCopy != locked && AtomicCas(headPtr, locked, headCopy)) {
                struct TUnlocker { // to avoid specialization for R=void
                    TAtomic* HeadPtr;
                    TAtomicBase* HeadCopy;
                    ~TUnlocker() {
                        ATOMIC_COMPILER_BARRIER();
                        AtomicSet(*HeadPtr, *HeadCopy);
                    }
                } scoped{headPtr, &headCopy};
                return func(*reinterpret_cast<TShuttlePtr*>(&headCopy));
            } else {
                LockFailed();
                return typename std::invoke_result<TFunc, TShuttlePtr&>::type();
            }
        }

        void LockFailed();
    };

    inline size_t HasShuttles(const TOrbit& orbit) {
        return orbit.HasShuttles();
    }
}
