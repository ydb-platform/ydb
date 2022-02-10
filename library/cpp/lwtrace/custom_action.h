#pragma once

#include "probe.h"

#include <library/cpp/lwtrace/protos/lwtrace.pb.h>

#include <util/generic/hash.h>

#include <functional>

namespace NLWTrace {
    class TSession;

    // Custom action can save any stuff (derived from IResource) in TSession object
    // IMPORTANT: Derived class will be used from multiple threads! (see example3)
    class IResource: public TAtomicRefCount<IResource> {
    public:
        virtual ~IResource() {
        }
    };
    using TResourcePtr = TIntrusivePtr<IResource>;

    // Trace resources that is used to hold/get/create any stuff
    class TTraceResources: public THashMap<TString, TResourcePtr> {
    public:
        template <class T>
        T& Get(const TString& name) {
            auto iter = find(name);
            if (iter == end()) {
                iter = insert(value_type(name, TResourcePtr(new T()))).first;
            }
            return *static_cast<T*>(iter->second.Get());
        }

        template <class T>
        const T* GetOrNull(const TString& name) const {
            auto iter = find(name);
            if (iter == end()) {
                return nullptr;
            }
            return *iter->second;
        }
    };

    // Base class of all custom actions
    class TCustomActionExecutor: public IExecutor {
    protected:
        TProbe* const Probe;
        bool Destructive;

    public:
        TCustomActionExecutor(TProbe* probe, bool destructive)
            : IExecutor()
            , Probe(probe)
            , Destructive(destructive)
        {
        }

        bool IsDestructive() {
            return Destructive;
        }
    };

    // Factory to produce custom action executors
    class TCustomActionFactory {
    public:
        using TCallback = std::function<TCustomActionExecutor*(TProbe* probe, const TCustomAction& action, TSession* trace)>;
        TCustomActionExecutor* Create(TProbe* probe, const TCustomAction& action, TSession* trace) const;
        void Register(const TString& name, const TCallback& callback);

    private:
        THashMap<TString, TCallback> Callbacks;
    };

}
