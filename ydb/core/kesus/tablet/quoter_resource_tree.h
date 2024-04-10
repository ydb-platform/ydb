#pragma once

#include "rate_accounting.h"

#include <ydb/core/protos/kesus.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/hash_multi_map.h>
#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>

#include <queue>

namespace NKikimr {

namespace NKesus {

using TQuoterSessionId = std::pair<NActors::TActorId, ui64>; // client id, resource id
using TTickProcessorId = std::pair<NActors::TActorId, ui64>; // == TQuoterSessionId for sessions. == ResourceId for resources (with empty actor id).
class TQuoterResourceTree;

TString CanonizeQuoterResourcePath(const TVector<TString>& path);
TString CanonizeQuoterResourcePath(const TString& path);

// Member of tick processor queue.
// Contains ids that are sufficient to find processor,
// and fields for proper ordering in priority queue (time and level).
struct TTickProcessorTask {
    TTickProcessorId Processor;
    size_t ProcessorLevel;
    TInstant Time;

    bool operator<(const TTickProcessorTask& task) const {
        return std::tie(Time, ProcessorLevel) < std::tie(task.Time, task.ProcessorLevel);
    }

    bool operator<=(const TTickProcessorTask& task) const {
        return !task.operator<(*this);
    }
};

// Queue for scheduling ticks for resource filling.
// This queue has interface of priority queue,
// but it performs better than priority queue
// in our load profile.
// KIKIMR-7381
class TTickProcessorQueue {
public:
    TTickProcessorQueue() = default;

    void Push(const TTickProcessorTask& task);
    void Pop();
    const TTickProcessorTask& Top() const;
    bool Empty() const;
    void Merge(TTickProcessorQueue&& from);

private:
    void Sort();

private:
    std::vector<TTickProcessorTask> Tasks;
    size_t TopIndex = 0;
    bool Sorted = true;
    size_t FirstIndex = 0;
};

// Parent interface for tick processor.
// Can be resource or sessions connected to resource.
class TTickProcessor {
public:
    virtual ~TTickProcessor() = default;

    virtual size_t GetLevel() const = 0; // Level in the tree for proper ordering.
    virtual TTickProcessorId GetTickProcessorId() const = 0;

    void Schedule(TTickProcessorQueue& queue, TInstant time) {
        if (!Scheduled) {
            Scheduled = true;
            queue.Push({GetTickProcessorId(), GetLevel(), time});
        }
    }

    void Process(TTickProcessorQueue& queue, TInstant now) {
        Scheduled = false;
        DoProcess(queue, now);
    }

private:
    virtual void DoProcess(TTickProcessorQueue& queue, TInstant now) = 0;

private:
    bool Scheduled = false;
};

// Resource sink - encapsulates send method (to tablet pipe in fact).
// When session is reconnected, this object is changed to new one
// with different actor to send resource to.
class IResourceSink : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IResourceSink>;

    // Successful resource allocation notification.
    virtual void Send(ui64 resourceId, double amount, const NKikimrKesus::TStreamingQuoterResource* props) = 0;

    virtual void Sync(ui64 resourceId, ui32 lastReportId, double available) { Y_UNUSED(resourceId, lastReportId, available); }

    // Notification about resource allocation error. For example, when resource was deleted during usage.
    // This notification means, that Kesus will not process resource with given id.
    // Other resources from this client will be continued processing as usual.
    virtual void CloseSession(ui64 resourceId, Ydb::StatusIds::StatusCode status, const TString& reason) = 0;
};

// Common interface for session to resource.
class TQuoterSession : public TTickProcessor {
public:
    TQuoterSession(const NActors::TActorId& clientId, ui32 clientVersion, TQuoterResourceTree* resource);

    virtual ~TQuoterSession() = default;

    TQuoterResourceTree* GetResource() {
        return Resource;
    }

    const TQuoterResourceTree* GetResource() const {
        return Resource;
    }

    // Client actor id (remote client: quoter proxy).
    const NActors::TActorId& GetClientId() const {
        return ClientId;
    }

    // Set new sink or change in case of reconnection.
    void SetResourceSink(const IResourceSink::TPtr& sink) {
        ResourceSink = sink;
    }

    virtual void Send(double spent);

    virtual void Sync(double available);

    // Reaction for quoter runtime events: TEvSubscribeOnResources and TEvUpdateConsumptionState.
    virtual void UpdateConsumptionState(bool consume, double amount, TTickProcessorQueue& queue, TInstant now) = 0;

    // Reaction for quoter runtime event TEvAccountResources.
    virtual TInstant Account(TInstant start, TDuration interval, const double* values, size_t size, TTickProcessorQueue& queue, TInstant now) = 0;

    virtual double ReportConsumed(ui32 reportId, double consumed, TTickProcessorQueue& queue, TInstant now);

    // Close session when resource is deleted.
    virtual void CloseSession(Ydb::StatusIds::StatusCode status, const TString& reason);

    // Properties for viewer
    virtual bool IsActive() const {
        return Active;
    }

    virtual double GetTotalSent() const {
        return TotalSent;
    }

    virtual double GetTotalConsumed() const {
        return TotalConsumed;
    }

    virtual void ResetTotalConsumed() {
        TotalConsumed = 0.0;
    }

    virtual double GetAmountRequested() const {
        return AmountRequested;
    }

    virtual void OnPropsChanged() {
        NeedSendChangedProps = true;
        Send(0); // Update props immediately // KIKIMR-8563
    }

    NActors::TActorId SetPipeServerId(const NActors::TActorId& pipeServerId) {
        const NActors::TActorId prevId = PipeServerId;
        PipeServerId = pipeServerId;
        return prevId;
    }

    NActors::TActorId GetPipeServerId() const {
        return PipeServerId;
    }

    ui32 GetClientVersion() const {
        return ClientVersion;
    }

protected:
    void AddAllocatedCounter(double spent);

protected:
    TQuoterResourceTree* Resource = nullptr;
    NActors::TActorId ClientId;
    NActors::TActorId PipeServerId;
    double AmountRequested = 0.0;
    double TotalSent = 0.0; // Only for session statistics. Accuracy of this variable will degrade in time.
    double TotalConsumed = 0.0;
    ui32 LastReportId = 0;
    bool Active = false;
    bool NeedSendChangedProps = false;
    IResourceSink::TPtr ResourceSink;
    ui32 ClientVersion = 0;
};

// Common interface for hierarchical quoter resource.
class TQuoterResourceTree : public TTickProcessor {
public:
    TQuoterResourceTree(ui64 resourceId, ui64 parentId, NActors::TActorId kesus, const IBillSink::TPtr& billSink, const NKikimrKesus::TStreamingQuoterResource& props);
    TQuoterResourceTree(TQuoterResourceTree&&) = delete;
    TQuoterResourceTree(const TQuoterResourceTree&) = delete;

    virtual ~TQuoterResourceTree() = default;

    virtual void SetResourceCounters(TIntrusivePtr<::NMonitoring::TDynamicCounters> resourceCounters);

    virtual void FillSubscribeResult(NKikimrKesus::TEvSubscribeOnResourcesResult::TResourceSubscribeResult& result) const {
        result.SetResourceId(GetResourceId());
    }

    virtual void HtmlDebugInfo(IOutputStream& out) const { Y_UNUSED(out); }

    ui64 GetResourceId() const {
        return ResourceId;
    }

    ui64 GetParentId() const {
        return ParentId;
    }

    TQuoterResourceTree* GetParent() {
        return Parent;
    }

    const TQuoterResourceTree* GetParent() const {
        return Parent;
    }

    const NKikimrKesus::TStreamingQuoterResource& GetProps() const {
        return Props;
    }

    const NKikimrKesus::TStreamingQuoterResource& GetEffectiveProps() const {
        return EffectiveProps;
    }

    const TString& GetPath() const {
        return GetProps().GetResourcePath();
    }

    const THashSet<TQuoterResourceTree*>& GetChildren() const {
        return Children;
    }

    virtual void ReportConsumed(double consumed, TTickProcessorQueue& queue, TInstant now) = 0;

    // Static children manipulation.
    virtual void AddChild(TQuoterResourceTree* child);
    virtual void RemoveChild(TQuoterResourceTree* child);

    virtual bool Update(const NKikimrKesus::TStreamingQuoterResource& props, TString& errorMessage);

    virtual bool ValidateProps(const NKikimrKesus::TStreamingQuoterResource& props, TString& errorMessage);

    // Runtime algorithm entry points.
    virtual void CalcParameters(); // Recursively calculates all parameters for runtime algorithm.

    virtual THolder<TQuoterSession> DoCreateSession(const NActors::TActorId& clientId, ui32 clientVersion) = 0;

    THolder<TQuoterSession> CreateSession(const NActors::TActorId& clientId, ui32 clientVersion) {
        THolder<TQuoterSession> session = DoCreateSession(clientId, clientVersion);
        if (session) {
            Sessions[clientId] = session.Get(); // it is safe - we store it outside
            if (Counters.Sessions) {
                Counters.Sessions->Inc();
            }
        }
        return session;
    }

    THashMap<NActors::TActorId, TQuoterSession*>& GetSessions() {
        return Sessions;
    }

    const THashMap<NActors::TActorId, TQuoterSession*>& GetSessions() const {
        return Sessions;
    }

    void OnSessionDisconnected(const NActors::TActorId& clientId) {
        Sessions.erase(clientId);
    }

    // TTickProcessor interface implementation.
    size_t GetLevel() const override {
        return ResourceLevel;
    }

    TTickProcessorId GetTickProcessorId() const override {
        return {NActors::TActorId(), ResourceId};
    }

    class TCounters {
    public:
        TIntrusivePtr<::NMonitoring::TDynamicCounters> ResourceCounters;
        ::NMonitoring::TDynamicCounters::TCounterPtr Sessions;
        ::NMonitoring::TDynamicCounters::TCounterPtr ActiveSessions;
        ::NMonitoring::TDynamicCounters::TCounterPtr Limit; // Current limit according to settings. If resource has no explicit limit, the counter is nullptr.
        ::NMonitoring::TDynamicCounters::TCounterPtr ElapsedMicrosecWhenResourceActive;

        void AddAllocated(double allocated);
        ui64 GetAllocated() const {
            return Allocated ? Allocated->Val() : 0;
        }
        void SetResourceCounters(TIntrusivePtr<::NMonitoring::TDynamicCounters> resourceCounters);
        void SetLimit(TMaybe<double> limit);

    private:
        ::NMonitoring::TDynamicCounters::TCounterPtr Allocated;
        double AllocatedRemainder = 0.0;
    };

    TCounters& GetCounters() {
        return Counters;
    }

    void UpdateActiveTime(TInstant now);
    void StopActiveTime(TInstant now);

    void SetQuoterPath(const TString& quoterPath) {
        QuoterPath = quoterPath;
    }

    const TString& GetQuoterPath() const {
        return QuoterPath;
    }

protected:
    const ui64 ResourceId;
    const ui64 ParentId;
    NActors::TActorId Kesus;
    IBillSink::TPtr BillSink;
    TString QuoterPath;
    size_t ResourceLevel = 0;
    TQuoterResourceTree* Parent = nullptr;
    THashSet<TQuoterResourceTree*> Children;
    THashMap<NActors::TActorId, TQuoterSession*> Sessions;
    NKikimrKesus::TStreamingQuoterResource Props;
    NKikimrKesus::TStreamingQuoterResource EffectiveProps; // Props with actual values taken from Props or from parent's Props or from defaults.
    TCounters Counters;
    TInstant StartActiveTime = TInstant::Zero();
};

// All Kesus tablet resources container.
class TQuoterResources {
public:
    struct TCounters {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> QuoterCounters;
        bool DetailedCountersMode = false;

        ::NMonitoring::TDynamicCounters::TCounterPtr ResourceSubscriptions = MakeIntrusive<NMonitoring::TCounterForPtr>(true);
        ::NMonitoring::TDynamicCounters::TCounterPtr UnknownResourceSubscriptions = MakeIntrusive<NMonitoring::TCounterForPtr>(true);
        ::NMonitoring::TDynamicCounters::TCounterPtr ResourceConsumptionStarts = MakeIntrusive<NMonitoring::TCounterForPtr>(true);
        ::NMonitoring::TDynamicCounters::TCounterPtr ResourceConsumptionStops = MakeIntrusive<NMonitoring::TCounterForPtr>(true);
        ::NMonitoring::TDynamicCounters::TCounterPtr ElapsedMicrosecOnResourceAllocation = MakeIntrusive<NMonitoring::TCounterForPtr>(true);
        ::NMonitoring::TDynamicCounters::TCounterPtr TickProcessorTasksProcessed = MakeIntrusive<NMonitoring::TCounterForPtr>(true);
    };

public:
    TQuoterResources() = default;

    TQuoterResourceTree* FindPath(const TString& resourcePath);
    TQuoterResourceTree* FindId(ui64 resourceId);
    const THashMap<TString, TQuoterResourceTree*>& GetAllResources() const {
        return ResourcesByPath;
    }
    bool Exists(ui64 resourceId) const;
    TQuoterResourceTree* LoadResource(ui64 resourceId, ui64 parentId, const NKikimrKesus::TStreamingQuoterResource& props); // initialization
    TQuoterResourceTree* AddResource(ui64 resourceId, const NKikimrKesus::TStreamingQuoterResource& props, TString& errorMessage);
    bool DeleteResource(TQuoterResourceTree* resource, TString& errorMessage);

    void SetQuoterCounters(TIntrusivePtr<::NMonitoring::TDynamicCounters> quoterCounters);
    void EnableDetailedCountersMode(bool enable = true);
    void FillCounters(NKikimrKesus::TEvGetQuoterResourceCountersResult& counters);

    void SetupBilling(NActors::TActorId kesus, const IBillSink::TPtr& billSink);
    void ConstructTrees(); // Constructs all trees during initialization.

    size_t GetResourcesCount() const {
        return ResourcesById.size();
    }

    // Checks whether resource path consists of only valid characters.
    static bool IsResourcePathValid(const TString& path);

    void ProcessTick(const TTickProcessorTask& task, TTickProcessorQueue& queue);

    TQuoterSession* GetOrCreateSession(const NActors::TActorId& clientId, ui32 clientVersion, TQuoterResourceTree* resource);
    TQuoterSession* FindSession(const NActors::TActorId& clientId, ui64 resourceId);
    const TQuoterSession* FindSession(const NActors::TActorId& clientId, ui64 resourceId) const;
    void DisconnectSession(const NActors::TActorId& pipeServerId);
    void SetPipeServerId(TQuoterSessionId sessionId, const NActors::TActorId& prevId, const NActors::TActorId& id);

    void OnUpdateResourceProps(TQuoterResourceTree* rootResource);

    const TCounters& GetCounters() {
        return Counters;
    }

    void SetQuoterPath(const TString& quoterPath);


private:
    TQuoterResourceTree* FindPathImpl(const TString& resourcePath); // doesn't canonize path

    void SetResourceCounters(TQuoterResourceTree* res);
    void ReinitResourceCounters();

private:
    TString QuoterPath;
    NActors::TActorId Kesus;
    IBillSink::TPtr BillSink;

    THashMap<ui64, THolder<TQuoterResourceTree>> ResourcesById;
    THashMap<TString, TQuoterResourceTree*> ResourcesByPath;
    THashMap<TQuoterSessionId, THolder<TQuoterSession>> Sessions;
    THashMultiMap<NActors::TActorId, TQuoterSessionId> PipeServerIdToSession;

    TCounters Counters;
};

}
}
