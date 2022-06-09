#pragma once

#include "custom_action.h"
#include "event.h"
#include "log.h"
#include "log_shuttle.h"
#include "probe.h"

#include <library/cpp/lwtrace/protos/lwtrace.pb.h>

#include <google/protobuf/repeated_field.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/noncopyable.h>
#include <util/generic/ptr.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>

namespace NLWTrace {

    using TProbeMap = THashMap<std::pair<TString, TString>, TProbe*>;

    // Interface for probe ownership management
    class IBox: public virtual TThrRefBase {
    private:
        bool Owns;

    public:
        explicit IBox(bool ownsProbe = false)
            : Owns(ownsProbe)
        {
        }

        bool OwnsProbe() {
            return Owns;
        }

        virtual TProbe* GetProbe() = 0;
    };

    using TBoxPtr = TIntrusivePtr<IBox>;

    // Simple not-owning box, that just holds pointer to static/global probe (e.g. created by LWTRACE_DEFINE_PROVIDER)
    class TStaticBox: public IBox {
    private:
        TProbe* Probe;

    public:
        explicit TStaticBox(TProbe* probe)
            : IBox(false)
            , Probe(probe)
        {
        }

        TProbe* GetProbe() override {
            return Probe;
        }
    };

    // Just a set of unique probes
    // TODO[serxa]: get rid of different ProbeRegistries, use unique one (singleton) with auto registration
    class TProbeRegistry: public TNonCopyable {
    private:
        TMutex Mutex;

        // Probe* pointer uniquely identifies a probe and boxptr actually owns TProbe object (if required)
        using TProbes = THashMap<TProbe*, TBoxPtr>;
        TProbes Probes;

        // Probe provider-name pairs must be unique, keep track of them
        using TIds = TSet<std::pair<TString, TString>>;
        TIds Ids;

    public:
        // Add probes from null-terminated array of probe pointers. Probe can be added multiple times. Thread-safe.
        // Implies probes you pass will live forever (e.g. created by LWTRACE_DEFINE_PROVIDER)
        void AddProbesList(TProbe** reg);

        // Manage probes that are created/destructed dynamically
        void AddProbe(const TBoxPtr& box);
        void RemoveProbe(TProbe* probe);

        // Helper class to make thread-safe iteration over probes
        class TProbesAccessor {
        private:
            TGuard<TMutex> Guard;
            TProbes& Probes;
        public:
            explicit TProbesAccessor(TProbeRegistry* registry)
                : Guard(registry->Mutex)
                , Probes(registry->Probes)
            {}

            explicit TProbesAccessor(TProbeRegistry& registry)
                : TProbesAccessor(&registry)
            {}

            auto begin() { return Probes.begin(); }
            auto end() { return Probes.end(); }
        };

        friend class TProbesAccessor;

    private:
        void AddProbeNoLock(const TBoxPtr& box);
        void RemoveProbeNoLock(TProbe* probe);
    };

    // Represents a compiled trace query, holds executors attached to probes
    class TSession: public TNonCopyable {
    public:
        typedef THashMap<TString, TAtomicBase> TTraceVariables;

    private:
        const TInstant StartTime;
        const ui64 TraceIdx;
        TProbeRegistry& Registry;
        TDuration StoreDuration;
        TDuration ReadDuration;
        TCyclicLog CyclicLog;
        TDurationLog DurationLog;
        TCyclicDepot CyclicDepot;
        TDurationDepot DurationDepot;
        TAtomic LastTrackId;
        TAtomic LastSpanId;
        typedef TVector<std::pair<TProbe*, IExecutor*>> TProbes;
        TProbes Probes;
        bool Attached;
        NLWTrace::TQuery Query;
        TTraceVariables TraceVariables;
        TTraceResources TraceResources;
        void InsertExecutor(TTraceVariables& traceVariables,
                            size_t bi, const NLWTrace::TPredicate* pred,
                            const google::protobuf::RepeatedPtrField<NLWTrace::TAction>& actions,
                            TProbe* probe, const bool destructiveActionsAllowed,
                            const TCustomActionFactory& customActionFactory);

    private:
        void Destroy();

    public:
        TSession(ui64 traceIdx,
               TProbeRegistry& registry,
               const NLWTrace::TQuery& query,
               const bool destructiveActionsAllowed,
               const TCustomActionFactory& customActionFactory);
        ~TSession();
        void Detach();
        size_t GetEventsCount() const;
        size_t GetThreadsCount() const;
        const NLWTrace::TQuery& GetQuery() const {
            return Query;
        }
        TInstant GetStartTime() const {
            return StartTime;
        }
        ui64 GetTraceIdx() const {
            return TraceIdx;
        }
        TTraceResources& Resources() {
            return TraceResources;
        }
        const TTraceResources& Resources() const {
            return TraceResources;
        }

        template <class TReader>
        void ReadThreads(TReader& r) const {
            CyclicLog.ReadThreads(r);
            DurationLog.ReadThreads(r);
        }

        template <class TReader>
        void ReadItems(TReader& r) const {
            CyclicLog.ReadItems(r);
            DurationLog.ReadItems(GetCycleCount(), DurationToCycles(ReadDuration), r);
        }

        template <class TReader>
        void ReadItems(ui64 now, ui64 duration, TReader& r) const {
            CyclicLog.ReadItems(r);
            DurationLog.ReadItems(now, duration, r);
        }

        template <class TReader>
        void ReadDepotThreads(TReader& r) const {
            CyclicDepot.ReadThreads(r);
            DurationDepot.ReadThreads(r);
        }

        template <class TReader>
        void ReadDepotItems(TReader& r) const {
            CyclicDepot.ReadItems(r);
            DurationDepot.ReadItems(GetCycleCount(), DurationToCycles(ReadDuration), r);
        }

        template <class TReader>
        void ReadDepotItems(ui64 now, ui64 duration, TReader& r) const {
            CyclicDepot.ReadItems(r);
            DurationDepot.ReadItems(now, duration, r);
        }

        template <class TReader>
        void ExtractItemsFromCyclicDepot(TReader& r) const {
            CyclicDepot.ExtractItems(r);
        }

        void ToProtobuf(TLogPb& pb) const;
    };

    // Deserialization result.
    // Either IsSuccess is true or FailedEventNames contains event names
    // we were not able to deserialize.
    struct TTraceDeserializeStatus
    {
        bool IsSuccess = true;
        TVector<TString> FailedEventNames;

        void AddFailedEventName(const TString& name)
        {
            IsSuccess = false;
            FailedEventNames.emplace_back(name);
        }
    };

    // Just a registry of all active trace queries
    // Facade for all interactions with probes/traces
    class TManager: public TNonCopyable {
    private:
        TProbeRegistry& Registry;
        TMutex Mtx;
        ui64 LastTraceIdx = 1;
        typedef THashMap<TString, TSession*> TTraces; // traceId -> TSession
        TTraces Traces;
        bool DestructiveActionsAllowed;
        TCustomActionFactory CustomActionFactory;
        THolder<TRunLogShuttleActionExecutor<TCyclicDepot>> SerializingExecutor;

    public:
        static constexpr ui64 RemoteTraceIdx = 0;

    public:
        TManager(TProbeRegistry& registry, bool allowDestructiveActions);
        ~TManager();
        bool HasTrace(const TString& id) const;
        const TSession* GetTrace(const TString& id) const;
        void New(const TString& id, const NLWTrace::TQuery& query);
        void Delete(const TString& id);
        void Stop(const TString& id);

        template <class TReader>
        void ReadProbes(TReader& reader) const {
            TProbeRegistry::TProbesAccessor probes(Registry);
            for (auto& kv : probes) {
                TProbe* probe = kv.first;
                reader.Push(probe);
            }
        }

        template <class TReader>
        void ReadTraces(TReader& reader) const {
            TGuard<TMutex> g(Mtx);
            for (const auto& Trace : Traces) {
                const TString& id = Trace.first;
                const TSession* trace = Trace.second;
                reader.Push(id, trace);
            }
        }

        template <class TReader>
        void ReadLog(const TString& id, TReader& reader) {
            TGuard<TMutex> g(Mtx);
            TTraces::iterator it = Traces.find(id);
            if (it == Traces.end()) {
                ythrow yexception() << "trace id '" << id << "' is not used";
            } else {
                TSession* trace = it->second;
                trace->ReadItems(reader);
            }
        }

        template <class TReader>
        void ReadLog(const TString& id, ui64 now, TReader& reader) {
            TGuard<TMutex> g(Mtx);
            TTraces::iterator it = Traces.find(id);
            if (it == Traces.end()) {
                ythrow yexception() << "trace id '" << id << "' is not used";
            } else {
                TSession* trace = it->second;
                trace->ReadItems(now, reader);
            }
        }

        template <class TReader>
        void ReadDepot(const TString& id, TReader& reader) {
            TGuard<TMutex> g(Mtx);
            TTraces::iterator it = Traces.find(id);
            if (it == Traces.end()) {
                ythrow yexception() << "trace id '" << id << "' is not used";
            } else {
                TSession* trace = it->second;
                trace->ReadDepotItems(reader);
            }
        }

        template <class TReader>
        void ReadDepot(const TString& id, ui64 now, TReader& reader) {
            TGuard<TMutex> g(Mtx);
            TTraces::iterator it = Traces.find(id);
            if (it == Traces.end()) {
                ythrow yexception() << "trace id '" << id << "' is not used";
            } else {
                TSession* trace = it->second;
                trace->ReadDepotItems(now, reader);
            }
        }

        template <class TReader>
        void ExtractItemsFromCyclicDepot(const TString& id, TReader& reader) {
            TGuard<TMutex> g(Mtx);
            TTraces::iterator it = Traces.find(id);
            if (it == Traces.end()) {
                ythrow yexception() << "trace id '" << id << "' is not used";
            } else {
                TSession* trace = it->second;
                trace->ExtractItemsFromCyclicDepot(reader);
            }
        }

        bool GetDestructiveActionsAllowed() {
            return DestructiveActionsAllowed;
        }

        void RegisterCustomAction(const TString& name, const TCustomActionFactory::TCallback& callback) {
            CustomActionFactory.Register(name, callback);
        }

        template <class T>
        void RegisterCustomAction() {
            CustomActionFactory.Register(T::GetActionName(), [=](TProbe* probe, const TCustomAction& action, TSession* trace) {
                return new T(probe, action, trace);
            });
        }

        TProbeMap GetProbesMap();

        void CreateTraceRequest(TTraceRequest& msg, TOrbit& orbit);

        bool HandleTraceRequest(
            const TTraceRequest& msg,
            TOrbit& orbit);

        TTraceDeserializeStatus HandleTraceResponse(
            const TTraceResponse& msg,
            const TProbeMap& probesMap,
            TOrbit& orbit,
            i64 timeOffset = 0,
            double timeScale = 1);

        void CreateTraceResponse(
            TTraceResponse& msg,
            TOrbit& orbit);

        bool IsTraced(TOrbit& orbit) {
            return orbit.HasShuttle(TManager::RemoteTraceIdx);
        }
    };
}
