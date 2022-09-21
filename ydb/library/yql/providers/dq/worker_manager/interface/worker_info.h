#pragma once

#include <utility>
#include <util/generic/hash.h>
#include <util/generic/guid.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <ydb/library/yql/providers/common/metrics/sensors_group.h>
#include <ydb/library/yql/providers/dq/api/grpc/api.grpc.pb.h>


namespace NYql::NDqs {

class TInflightLimiter: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TInflightLimiter>;

    TInflightLimiter(i32 inflightLimit);
    bool CanDownload(const TString& id);
    void MarkDownloadStarted(const TString& id);
    void MarkDownloadFinished(const TString& id);

private:
    THashMap<TString, i32> InflightResources;
    i32 InflightLimit;
};

struct TGlobalResources {
    TGlobalResources(TSensorsGroupPtr metrics)
        : Metrics(metrics)
        , Capacity(0)
        , RunningRequests(0)
    { }

    void AddCapacity(int value) {
        Capacity += value;
    }

    void AddRunning(int value) {
        RunningRequests += value;
    }

    i64 GetCapacity() const {
        return Capacity;
    }

    i64 GetRunningRequests() const {
        return RunningRequests;
    }

    TSensorsGroupPtr GetMetrics() {
        return Metrics;
    }

private:
    TSensorsGroupPtr Metrics;
    i64 Capacity;
    i64 RunningRequests;

    TGlobalResources();
    TGlobalResources(const TGlobalResources&);
};

struct TClusterResources {
public:
    TClusterResources(TGlobalResources& resources, const TString& clusterName)
        : Resources(resources)
        , Capacity(Resources.GetMetrics()
                   ->GetSubgroup("component", "lists")
                   ->GetSubgroup("ClusterName", clusterName)
                   ->GetCounter("Capacity"))
        , RunningRequests(Resources.GetMetrics()
                   ->GetSubgroup("component", "lists")
                   ->GetSubgroup("ClusterName", clusterName)
                   ->GetCounter("RunningRequests"))
    { }

    void Reset() { }

    void AddCapacity(int value) {
        Resources.AddCapacity(value);
        *Capacity += value;
    }

    void AddRunning(int value) {
        Resources.AddRunning(value);
        *RunningRequests += value;
    }

private:
    TGlobalResources& Resources;
    ::NMonitoring::TDynamicCounters::TCounterPtr Capacity;
    ::NMonitoring::TDynamicCounters::TCounterPtr RunningRequests;
};

struct TWorkerInfo: public TThrRefBase {
    using TPtr = TIntrusivePtr<TWorkerInfo>;
    using TFileResource = Yql::DqsProto::TFile;

    const ui32 NodeId;
    const TGUID WorkerId;
    TInstant LastPingTime;
    const TString Revision;
    const TString ClusterName;
    const TString Address;
    const TString StartTime;
    const ui32 Port;
    ui32 Epoch;
    THashMap<TString, TString> Attributes;
    THashSet<TString> Operations;

    ui64 UseCount = 0;
    TInstant RequestStartTime;
    TDuration UseTime;
    bool IsDead = false;
    bool Stopping = false;

    const ui32 Capabilities = 0;

    i64 FreeDiskSize = 0;
    i64 UsedDiskSize = 0;
    const int Capacity;
    const int CpuCores = 1; // unused yet

    TDuration Stime;
    TDuration Utime;
    i64 CpuSystem = 0;
    i64 CpuUser = 0;
    i64 CpuTotal = 0;
    i64 MajorPageFaults = 0;

    int RunningRequests = 0;
    int RunningWorkerActors = 0;

    TWorkerInfo(
        const TString& startTime,
        ui32 nodeId,
        TGUID workerId,
        const Yql::DqsProto::RegisterNodeRequest& request,
        NYql::TSensorsGroupPtr metrics,
        const TInflightLimiter::TPtr& inflightLimiter,
        TGlobalResources& globalResources
    );

    ~TWorkerInfo();

    bool Update(const Yql::DqsProto::RegisterNodeRequest& request);

    void RemoveFromDownloadList(const TString& objectId);

    void AddToDownloadList(const THashMap<TString, TFileResource>& downloadList);

    bool AddToDownloadList(const TString& key, const TFileResource& value);

    const THashMap<TString, TFileResource>& GetDownloadList();

    THashMap<TString, TFileResource> GetResourcesForDownloading();

    const THashSet<TString>& GetResources();

    const auto& GetResourcesOrdered() {
        return ResourcesOrdered;
    }

    const auto& GetActiveDownloads() {
        return ActiveDownloads;
    }

    void OnDead();

    bool Acquire();

    bool Release();

private:
    THashSet<TString> Resources;
    google::protobuf::RepeatedPtrField<::Yql::DqsProto::RegisterNodeRequest_LocalFile> ResourcesOrdered;
    THashMap<TString, TFileResource> DownloadList;
    TSensorsGroupPtr Metrics;
    TInflightLimiter::TPtr InflightLimiter;
    TClusterResources ClusterResources;
    THashMap<TString, TFileResource> ActiveDownloads;

    TSensorCounterPtr CurrentDownloadsSum;
    TSensorCounterPtr CurrentDownloadsMax;
    TSensorCounterPtr CurrentDownloadsArgMax;

    TSensorCounterPtr ActiveDownloadsSum;

    TSensorCounterPtr DeadWorkers;

    TSensorCounterPtr FilesCountSum;
    TSensorCounterPtr FilesCountMax;
    TSensorCounterPtr FilesCountArgMax;

    TSensorCounterPtr FreeDiskSizeSum;
    TSensorCounterPtr FreeDiskSizeMin;
    TSensorCounterPtr FreeDiskSizeArgMin;

    TSensorCounterPtr UsedDiskSizeSum;
    TSensorCounterPtr UsedDiskSizeMax;
    TSensorCounterPtr UsedDiskSizeArgMax;

    TSensorCounterPtr CpuTotalSum;
    TSensorCounterPtr MajorPageFaultsSum;

    TSensorCounterPtr FileRemoveCounter;
    TSensorCounterPtr FileAddCounter;

    TSensorCounterPtr RunningActors;
};

struct TWorkerInfoPtrComparator {
    bool operator()(const TWorkerInfo::TPtr& a, const TWorkerInfo::TPtr& b) const {
        auto scoreA = (a->CpuTotal+1) * (a->RunningRequests + a->RunningWorkerActors + 1);
        auto scoreB = (b->CpuTotal+1) * (b->RunningRequests + b->RunningWorkerActors + 1);
        if (scoreA < scoreB) {
            return true;
        } else if (scoreA > scoreB) {
            return false;
        } else {
            return a.Get() < b.Get();
        }
    }
};

} // namespace NYql::NDqs
