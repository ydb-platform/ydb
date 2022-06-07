#pragma once

#include "event.h"
#include "preprocessor.h"
#include "rwspinlock.h"
#include "shuttle.h"

#include <util/datetime/cputimer.h>
#include <util/generic/hide_ptr.h>
#include <util/generic/scope.h>
#include <library/cpp/deprecated/atomic/atomic.h>

namespace NLWTrace {
    // Represents a chain (linked list) of steps for execution of a trace query block
    // NOTE: different executor objects are used on different probes (even for the same query block)
    class IExecutor {
    private:
        IExecutor* Next;

    public:
        IExecutor()
            : Next(nullptr)
        {
        }

        virtual ~IExecutor() {
            if (Next != nullptr) {
                delete Next;
            }
        }

        void Execute(TOrbit& orbit, const TParams& params) {
            if (DoExecute(orbit, params) && Next != nullptr) {
                Next->Execute(orbit, params);
            }
        }

        void SetNext(IExecutor* next) {
            Next = next;
        }

        IExecutor* GetNext() {
            return Next;
        }

        const IExecutor* GetNext() const {
            return Next;
        }

    protected:
        virtual bool DoExecute(TOrbit& orbit, const TParams& params) = 0;
    };

    // Common class for all probes
    struct TProbe {
        // Const configuration
        TEvent Event;

        // State that don't need any locking
        TAtomic ExecutorsCount;

        // State that must be accessed under lock
        TRWSpinLock Lock;
        IExecutor* Executors[LWTRACE_MAX_ACTIONS];
        IExecutor** Front; // Invalid if ExecutorsCount == 0
        IExecutor** Back;  // Invalid if ExecutorsCount == 0

        void Init() {
            ExecutorsCount = 0;
            Lock.Init();
            Zero(Executors);
            Front = nullptr;
            Back = nullptr;
        }

        IExecutor** First() {
            return Executors;
        }

        IExecutor** Last() {
            return Executors + LWTRACE_MAX_ACTIONS;
        }

        void Inc(IExecutor**& it) {
            it++;
            if (it == Last()) {
                it = First();
            }
        }

        intptr_t GetExecutorsCount() const {
            return AtomicGet(ExecutorsCount);
        }

        bool Attach(IExecutor* exec) {
            TWriteSpinLockGuard g(Lock);
            if (ExecutorsCount > 0) {
                for (IExecutor** it = Front;; Inc(it)) {
                    if (*it == nullptr) {
                        *it = exec;
                        AtomicIncrement(ExecutorsCount);
                        return true; // Inserted into free slot in [First; Last]
                    }
                    if (it == Back) {
                        break;
                    }
                }
                IExecutor** newBack = Back;
                Inc(newBack);
                if (newBack == Front) {
                    return false; // Buffer is full
                } else {
                    Back = newBack;
                    *Back = exec;
                    AtomicIncrement(ExecutorsCount);
                    return true; // Inserted after Last
                }
            } else {
                Front = Back = First();
                *Front = exec;
                AtomicIncrement(ExecutorsCount);
                return true; // Inserted as a first element
            }
        }

        bool Detach(IExecutor* exec) {
            TWriteSpinLockGuard g(Lock);
            for (IExecutor** it = First(); it != Last(); it++) {
                if ((*it) == exec) {
                    *it = nullptr;
                    AtomicDecrement(ExecutorsCount);
                    if (ExecutorsCount > 0) {
                        for (;; Inc(Front)) {
                            if (*Front != nullptr) {
                                break;
                            }
                            if (Front == Back) {
                                break;
                            }
                        }
                    }
                    return true;
                }
            }
            return false;
        }

        void RunExecutors(TOrbit& orbit, const TParams& params) {
            // Read lock is implied
            if (ExecutorsCount > 0) {
                for (IExecutor** it = Front;; Inc(it)) {
                    IExecutor* exec = *it;
                    if (exec) {
                        exec->Execute(orbit, params);
                    }
                    if (it == Back) {
                        break;
                    }
                }
            }
        }

        void RunShuttles(TOrbit& orbit, const TParams& params) {
            orbit.AddProbe(this, params);
        }
    };

#ifndef LWTRACE_DISABLE

    template <class T>
    inline void PreparePtr(const T& ref, const T*& ptr) {
        ptr = &ref;
    }

    template <>
    inline void PreparePtr<TNil>(const TNil&, const TNil*&) {
    }

#define LWTRACE_SCOPED_FUNCTION_PARAMS_I(i) (1) typename ::NLWTrace::TParamTraits<TP##i>::TFuncParam p##i = ERROR_not_enough_parameters() LWTRACE_COMMA
#define LWTRACE_SCOPED_FUNCTION_PARAMS LWTRACE_EXPAND(LWTRACE_EAT FOREACH_PARAMNUM(LWTRACE_SCOPED_FUNCTION_PARAMS_I)(0))
#define LWTRACE_SCOPED_FUNCTION_PARAMS_BY_REF_I(i) (1) typename ::NLWTrace::TParamTraits<TP##i>::TStoreType& p##i = *(ERROR_not_enough_parameters*)(HidePointerOrigin(nullptr))LWTRACE_COMMA
#define LWTRACE_SCOPED_FUNCTION_PARAMS_BY_REF LWTRACE_EXPAND(LWTRACE_EAT FOREACH_PARAMNUM(LWTRACE_SCOPED_FUNCTION_PARAMS_BY_REF_I)(0))
#define LWTRACE_SCOPED_PREPARE_PTRS_I(i) PreparePtr(p##i, P##i);
#define LWTRACE_SCOPED_PREPARE_PTRS()                   \
    do {                                                \
        FOREACH_PARAMNUM(LWTRACE_SCOPED_PREPARE_PTRS_I) \
    } while (false)
#define LWTRACE_SCOPED_PREPARE_PARAMS_I(i, params) params.Param[i].CopyConstruct<typename ::NLWTrace::TParamTraits<TP##i>::TStoreType>(*P##i);
#define LWTRACE_SCOPED_PREPARE_PARAMS(params)                     \
    do {                                                          \
        FOREACH_PARAMNUM(LWTRACE_SCOPED_PREPARE_PARAMS_I, params) \
    } while (false)

    template <LWTRACE_TEMPLATE_PARAMS>
    struct TUserProbe;

    template <LWTRACE_TEMPLATE_PARAMS>
    class TScopedDurationImpl {
    private:
        TUserProbe<LWTRACE_TEMPLATE_ARGS>* Probe;
        ui64 Started;
        TParams Params;

    public:
        explicit TScopedDurationImpl(TUserProbe<LWTRACE_TEMPLATE_ARGS>& probe, LWTRACE_SCOPED_FUNCTION_PARAMS) {
            if (probe.Probe.GetExecutorsCount() > 0) {
                Probe = &probe;
                LWTRACE_PREPARE_PARAMS(Params);
                Started = GetCycleCount();
            } else {
                Probe = nullptr;
            }
        }
        ~TScopedDurationImpl() {
            if (Probe) {
                if (Probe->Probe.GetExecutorsCount() > 0) {
                    TReadSpinLockGuard g(Probe->Probe.Lock);
                    if (Probe->Probe.GetExecutorsCount() > 0) {
                        ui64 duration = CyclesToDuration(GetCycleCount() - Started).MicroSeconds();
                        Params.Param[0].template CopyConstruct<typename TParamTraits<TP0>::TStoreType>(duration);
                        TOrbit orbit;
                        Probe->Probe.RunExecutors(orbit, Params);
                    }
                }
                TUserSignature<LWTRACE_TEMPLATE_ARGS>::DestroyParams(Params);
            }
        }
    };

    // Class representing a specific probe
    template <LWTRACE_TEMPLATE_PARAMS_NODEF>
    struct TUserProbe {
        TProbe Probe;

        inline void operator()(LWTRACE_FUNCTION_PARAMS) {
            TParams params;
            LWTRACE_PREPARE_PARAMS(params);
            Y_DEFER { TUserSignature<LWTRACE_TEMPLATE_ARGS>::DestroyParams(params); };

            TOrbit orbit;
            Probe.RunExecutors(orbit, params);
        }

        inline void Run(TOrbit& orbit, LWTRACE_FUNCTION_PARAMS) {
            TParams params;
            LWTRACE_PREPARE_PARAMS(params);
            Y_DEFER { TUserSignature<LWTRACE_TEMPLATE_ARGS>::DestroyParams(params); };

            Probe.RunExecutors(orbit, params);
            Probe.RunShuttles(orbit, params); // Executors can create shuttles
        }

        // Required to avoid running executors w/o lock
        inline void RunShuttles(TOrbit& orbit, LWTRACE_FUNCTION_PARAMS) {
            TParams params;
            LWTRACE_PREPARE_PARAMS(params);
            Probe.RunShuttles(orbit, params);
            TUserSignature<LWTRACE_TEMPLATE_ARGS>::DestroyParams(params);
        }

        typedef TScopedDurationImpl<LWTRACE_TEMPLATE_ARGS> TScopedDuration;
    };

#endif

}
