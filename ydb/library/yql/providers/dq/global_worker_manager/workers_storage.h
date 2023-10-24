#pragma once

#include "worker_filter.h"

#include <ydb/library/yql/providers/dq/worker_manager/interface/worker_info.h>

#include <ydb/library/yql/providers/dq/scheduler/dq_scheduler.h>

namespace NYql {

class TWorkersStorage {
public:
    TWorkersStorage(ui32 nodeId, TSensorsGroupPtr metrics, TSensorsGroupPtr workerMetrics);

    void Clear();

    TList<NDqs::TWorkerInfo::TPtr> GetList();

    size_t Capacity() const { return GlobalResources.GetCapacity(); }
    size_t FreeSlots() const { return Max<i64>(GlobalResources.GetCapacity() - GlobalResources.GetRunningRequests(), 0); }

    std::tuple<NDqs::TWorkerInfo::TPtr, bool> CreateOrUpdate(ui32 nodeId, TGUID workerId, Yql::DqsProto::RegisterNodeRequest& request);

    void CheckZombie(ui32 nodeId, TGUID workerId, Yql::DqsProto::RegisterNodeRequest& request);

    void CleanUp(TInstant now, TDuration duration);

    void DropWorker(TInstant now, TGUID workerId);

    void FreeWorker(TInstant now, const NDqs::TWorkerInfo::TPtr& worker);

    TVector<NDqs::TWorkerInfo::TPtr> TryAllocate(const NDq::IScheduler::TWaitInfo& waitInfo) noexcept;

    void IsReady(const TVector<NDqs::TWorkerInfo::TFileResource>& resources, THashMap<TString, std::pair<ui32, ui32>>& clusterMap);

    void ClusterStatus(Yql::DqsProto::ClusterStatusResponse* r) const;

    void UpdateResourceUseTime(TDuration duration, const THashSet<TString>& ids);

    bool HasResource(const TString& id) const;

    void AddResource(const TString& id, Yql::DqsProto::TFile::EFileType type, const TString& name, i64 size);

    void UpdateMetrics();

    void Visit(const std::function<void(const NDqs::TWorkerInfo::TPtr& workerInfo)>& f);

private:

    const i32 MaxDownloadsPerFile = 20;
    ui32 NodeId;
    THashMap<TString, NDqs::TInflightLimiter::TPtr> InflightLimiters;
    THashMap<TGUID, NDqs::TWorkerInfo::TPtr> Workers;

    using TFreeList = std::set<NDqs::TWorkerInfo::TPtr, NDqs::TWorkerInfoPtrComparator>;
    TFreeList FreeList;

    TSensorsGroupPtr Metrics;
    TSensorsGroupPtr WorkerMetrics;
    TSensorsGroupPtr ResourceCounters;
    TSensorsGroupPtr ResourceDownloadCounters;
    NMonitoring::THistogramPtr ResourceWaitTime;
    NMonitoring::TDynamicCounters::TCounterPtr WorkersSize;
    NMonitoring::TDynamicCounters::TCounterPtr FreeListSize;
    TString StartTime = ToString(TInstant::Now());
    TVector<std::pair<TGUID, TString>> NodeIds;

    THashMap<TString, TResourceStat> Uploaded;

    NDqs::TGlobalResources GlobalResources;

    TFreeList::iterator ProcessMatched(
        TWorkersStorage::TFreeList::iterator it,
        TVector<NDqs::TWorkerInfo::TPtr>& result,
        TFreeList* freeList,
        THashMap<NDqs::TWorkerInfo::TPtr, int> tasksPerWorker,
        THashMap<i32, TWorkerFilter>& tasksToAllocate,
        int taskId);

    void SearchFreeList(
        TVector<NDqs::TWorkerInfo::TPtr>& result,
        TFreeList* freeList,
        const i32 maxTasksPerWorker,
        THashMap<NDqs::TWorkerInfo::TPtr, int>& tasksPerWorker,
        THashMap<i32, TWorkerFilter>& tasksToAllocate,
        THashMap<TString, THashSet<int>>& waitingResources,
        TWorkerFilter::EMatchStatus matchMode);
};

} // namespace NYql
