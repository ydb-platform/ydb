#include "workers_storage.h"

#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/utils/log/log.h>

#include <util/generic/scope.h>

namespace NYql {

using namespace NDqs;
using namespace NMonitoring;

using EFileType = Yql::DqsProto::TFile::EFileType;
using TFileResource = Yql::DqsProto::TFile;

TWorkersStorage::TWorkersStorage(ui32 nodeId, TSensorsGroupPtr metrics, TSensorsGroupPtr workerMetrics)
    : NodeId(nodeId)
    , Metrics(std::move(metrics))
    , WorkerMetrics(std::move(workerMetrics))
    , ResourceCounters(Metrics->GetSubgroup("component", "ResourceCounters"))
    , ResourceDownloadCounters(Metrics->GetSubgroup("component", "ResourceDownloadCounters"))
    , ResourceWaitTime(Metrics
        ->GetSubgroup("component", "Resource")
        ->GetHistogram("WaitTime", ExponentialHistogram(10, 2, 1000)))
    , WorkersSize(nullptr)
    , FreeListSize(nullptr)
    , GlobalResources(Metrics)
{
    NodeIds.resize(65536);
}

void TWorkersStorage::Clear()
{
    Workers.clear();
    FreeList.clear();
    WorkersSize = nullptr;
    FreeListSize = nullptr;
    Metrics->RemoveSubgroup("component", "lists");
}

TList<TWorkerInfo::TPtr> TWorkersStorage::GetList() {
    TList<TWorkerInfo::TPtr> workers;

    for (const auto& [_, workerInfo] : Workers) {
        workers.push_back(workerInfo);
    }

    return workers;
}

std::tuple<TWorkerInfo::TPtr, bool> TWorkersStorage::CreateOrUpdate(ui32 nodeId, TGUID workerId, Yql::DqsProto::RegisterNodeRequest& request) {
    auto& knownWorkers = request.GetKnownNodes();
    bool needResume = false;
    auto maybeWorker = Workers.find(workerId);
    TWorkerInfo::TPtr workerInfo;

    CheckZombie(nodeId, workerId, request);

    if (maybeWorker == Workers.end()) {
        for (auto node : knownWorkers) {
            if (node == NodeId) {
                YQL_CLOG(DEBUG, ProviderDq)
                    << "Resume " << GetGuidAsString(workerId)
                    << " " << request.GetRevision()
                    << " " << request.GetRunningWorkers()
                    << " " << request.GetEpoch();

                TInflightLimiter::TPtr limiter = InflightLimiters[request.GetClusterName()];
                if (!limiter) {
                    limiter = new NDqs::TInflightLimiter(MaxDownloadsPerFile);
                    InflightLimiters[request.GetClusterName()] = limiter;
                }

                workerInfo = MakeIntrusive<TWorkerInfo>(StartTime, nodeId, workerId, request, WorkerMetrics, limiter, GlobalResources);
                Workers[workerId] = workerInfo;
                FreeList.insert(workerInfo);
                needResume = true;
                break;
            }
        }
    } else {
        workerInfo = maybeWorker->second;
        FreeList.erase(workerInfo);
        needResume = workerInfo->Update(request);
        if (workerInfo->RunningRequests < workerInfo->Capacity) {
            FreeList.insert(workerInfo);
        }
    }

    return std::make_tuple(workerInfo, needResume);
}

void TWorkersStorage::CheckZombie(ui32 nodeId, TGUID workerId, Yql::DqsProto::RegisterNodeRequest& request) {
    Y_ABORT_UNLESS(nodeId < NodeIds.size());

    if (request.GetStartTime().empty()) {
        request.SetStartTime(StartTime);
    }

    if (!NodeIds[nodeId].first) {
        NodeIds[nodeId] = std::make_pair(workerId, request.GetStartTime());
        return;
    }
    if (NodeIds[nodeId].first != workerId) {
        auto curStartTime = TInstant::ParseIso8601(request.GetStartTime());
        auto prevStartTime = TInstant::ParseIso8601(NodeIds[nodeId].second);

        if (prevStartTime < curStartTime) {
            // Ping from new node: replace old node, mark old node as dead
            YQL_CLOG(DEBUG, ProviderDq) << "Zombie worker " << GetGuidAsString(NodeIds[nodeId].first) << " " << nodeId;
            NodeIds[nodeId] = std::make_pair(workerId, request.GetStartTime());
            auto maybeWorker = Workers.find(NodeIds[nodeId].first);
            if (maybeWorker != Workers.end()) {
                maybeWorker->second->OnDead();
            }
        } else {
            // Ping from old node
            YQL_CLOG(DEBUG, ProviderDq) << "Zombie worker " << GetGuidAsString(workerId) << " " << nodeId;
            request.SetZombie(true);
        }
    }
}

void TWorkersStorage::CleanUp(TInstant now, TDuration duration) {
    TVector<TGUID> deadWorkers;
    THashMap<TString, TWorkerInfo::TFileResource> downloadList;
    for (const auto& [k, v] : Workers) {
        if (now - v->LastPingTime > duration) {
            FreeList.erase(v);

            deadWorkers.push_back(k);
            downloadList.insert(v->GetDownloadList().begin(), v->GetDownloadList().end());
            v->OnDead();
        }
    }

    for (auto workerId : deadWorkers) {
        YQL_CLOG(WARN, ProviderDq) << "Remove dead worker " << GetGuidAsString(workerId);
        Workers.erase(workerId);
    }

    // pass dead download list to live workers
    auto it = Workers.begin();
    for (ui32 i = 0; i < deadWorkers.size() && it != Workers.end(); ++i, ++it) {
        it->second->AddToDownloadList(downloadList);
    }

    THashSet<TString> dropOldUploaded;
    for (const auto& [k, v] : Uploaded) {
        if (now - v.LastSeenTime > TDuration::Hours(1)) {
            dropOldUploaded.insert(k);
        }
    }
    for (const auto& k : dropOldUploaded) {
        Uploaded.erase(k);
    }
}

void TWorkersStorage::DropWorker(TInstant now, TGUID workerId) {
    Y_UNUSED(now);
    auto it = Workers.find(workerId);
    if (it != Workers.end()) {
        FreeList.erase(it->second);
        it->second->OnDead();
        Workers.erase(it);
    }
}

void TWorkersStorage::FreeWorker(TInstant now, const TWorkerInfo::TPtr& worker) {
    Y_UNUSED(now);

    FreeList.erase(worker);
    if (worker->Release()) {
        FreeList.insert(worker);
    }
}

TWorkersStorage::TFreeList::iterator TWorkersStorage::ProcessMatched(
    TWorkersStorage::TFreeList::iterator it,
    TVector<NDqs::TWorkerInfo::TPtr>& result,
    TFreeList* freeList,
    THashMap<TWorkerInfo::TPtr, int> tasksPerWorker,
    THashMap<i32, TWorkerFilter>& tasksToAllocate,
    int taskId)
{
    auto workerInfo = *it;

    it = freeList->erase(it);
    if (!workerInfo->Acquire()) {
        freeList->insert(workerInfo);
        it = freeList->begin();
    }

    tasksPerWorker[workerInfo] ++;

    Y_ASSERT(result[taskId] == nullptr);
    result[taskId] = workerInfo;
    tasksToAllocate.erase(taskId);

    return it;
}

void TWorkersStorage::SearchFreeList(
    TVector<TWorkerInfo::TPtr>& result,
    TFreeList* freeList,
    const i32 maxTasksPerWorker,
    THashMap<TWorkerInfo::TPtr, int>& tasksPerWorker,
    THashMap<i32, TWorkerFilter>& tasksToAllocate,
    THashMap<TString, THashSet<int>>& waitingResources,
    TWorkerFilter::EMatchStatus matchMode)
{
    TWorkerFilter::TStats stats;
    stats.WaitingResources = &waitingResources;
    stats.Uploaded = &Uploaded;

    // fresh nodes must be first
    for (auto it = freeList->begin(); it != freeList->end(); ) {
        // must exists
        auto workerInfo = *it;
        if (workerInfo->IsDead) {
            YQL_CLOG(DEBUG, ProviderDq) << "Remove dead worker from free list "
                           << GetGuidAsString(workerInfo->WorkerId);
            it = freeList->erase(it);
            continue;
        }
        if (workerInfo->Capacity <= workerInfo->RunningRequests) {
            it = freeList->erase(it);
            continue;
        }

        if (maxTasksPerWorker > 0 && tasksPerWorker[workerInfo] >= maxTasksPerWorker) {
            ++it;
            continue;
        }

        if (workerInfo->Stopping) {
            ++it;
            continue;
        }

        i32 taskId;
        bool ok = false;
        for (const auto& [taskId_, filter] : tasksToAllocate) {
            taskId = taskId_;
            if ((matchMode == filter.Match(workerInfo, taskId, &stats))) {
                ok = true;
                break;
            }
        }

        if (ok) {
            it = ProcessMatched(
                it, result, freeList, tasksPerWorker, tasksToAllocate, taskId);
        } else {
            ++it;
        }

        if (tasksToAllocate.empty()) {
            break;
        }
    }
}

TVector<TWorkerInfo::TPtr> TWorkersStorage::TryAllocate(const NDq::IScheduler::TWaitInfo& waitInfo) noexcept
{
    auto& request = waitInfo.Request;
    auto count = request.GetCount();
    TVector<TWorkerInfo::TPtr> result(count, nullptr);
    auto& filterPerTask = request.GetWorkerFilterPerTask();
    THashMap<i32, TWorkerFilter> tasksToAllocate;

    YQL_LOG_CTX_ROOT_SESSION_SCOPE(waitInfo.Request.GetTraceId());
    YQL_CLOG(DEBUG, ProviderDq) << "TryAllocate, workers count: " << count << " files per task: " << filterPerTask.size();

    if (filterPerTask.empty()) {
        // COMPAT demand all files on all workers
        Yql::DqsProto::TWorkerFilter commonFilter;
        for (const auto& file : request.GetFiles()) {
            *commonFilter.AddFile() = file;
        }
        for (ui32 i = 0; i < count; ++i) {
            tasksToAllocate.emplace(i, commonFilter);
        }
    } else {
        for (i32 i = 0; i < filterPerTask.size(); ++i) {
            tasksToAllocate.emplace(i, filterPerTask.Get(i));
        }
    }

    Y_ASSERT(tasksToAllocate.size() == count);

    auto now = TInstant::Now();

    for (const auto& file : request.GetFiles()) {
        auto id = file.GetObjectId();
        auto maybeUploaded = Uploaded.find(id);
        if (maybeUploaded == Uploaded.end()) {
            Uploaded.emplace(id, TResourceStat{id, file.GetObjectType(), file.GetName(), file.GetSize()});
        } else {
            if (maybeUploaded->second.Name.empty()) {
                maybeUploaded->second = TResourceStat{id, file.GetObjectType(), file.GetName(), file.GetSize()};
            } else {
                maybeUploaded->second.LastSeenTime = now;
            }
        }
    }

    THashMap<TString, THashSet<int>> waitingResources; // resourceId -> taskId

    TFreeList boundedList;
    auto* freeList = &FreeList;

    Y_SCOPE_EXIT(&) {
        for (const auto& workerInfo : boundedList) {
            FreeList.insert(workerInfo);
        }
    };

    i32 maxTasksPerWorker = 0;
    if (request.GetWorkersCount()) {
        YQL_CLOG(DEBUG, ProviderDq) << "Bounded mode";
        maxTasksPerWorker = (count + request.GetWorkersCount() - 1) / request.GetWorkersCount();
        freeList = &boundedList;
        auto slots = count; slots = 0;

        for (auto it = FreeList.begin(); it != FreeList.end() && freeList->size() < request.GetWorkersCount(); ) {
            auto workerInfo = *it;
            if (workerInfo->IsDead || workerInfo->Capacity <= workerInfo->RunningRequests) {
                it = FreeList.erase(it);
                continue;
            }
            auto freeSlots = workerInfo->Capacity - workerInfo->RunningRequests;
            if (freeSlots >= maxTasksPerWorker) {
                freeList->insert(workerInfo);
                slots += freeSlots;
                it = FreeList.erase(it);
            } else {
                ++it;
            }
        }

        if (slots < count) {
            YQL_CLOG(DEBUG, ProviderDq) << "Not enough slots";
            return { };
        }
    }

    THashMap<TWorkerInfo::TPtr, int> tasksPerWorker;
    SearchFreeList(result, freeList, maxTasksPerWorker, tasksPerWorker, tasksToAllocate, waitingResources, TWorkerFilter::EOK);
    if (!tasksToAllocate.empty()) {
        SearchFreeList(result, freeList, maxTasksPerWorker, tasksPerWorker, tasksToAllocate, waitingResources, TWorkerFilter::EPARTIAL);
    }

    for (const auto& file : request.GetFiles()) {
        const auto& id = file.GetObjectId();

        if (waitingResources.contains(id)) {
            waitInfo.Stat.StartCounter(id, now);
        } else {
            waitInfo.Stat.FlushCounter(id);
        }
    }

    waitInfo.ResLeft.clear();
    if (tasksToAllocate.empty()) {
        auto now = TInstant::Now();
        for (const auto& workerInfo : result) {
            Y_ASSERT(workerInfo != nullptr);
            workerInfo->UseCount ++;
            workerInfo->RequestStartTime = now;
        }
        for (const auto& file : request.GetFiles()) {
            auto id = file.GetObjectId();
            auto it = Uploaded.find(id);
            auto delta = waitInfo.Stat.Get().contains(id)
                ? TDuration::MilliSeconds(waitInfo.Stat.Get().find(id)->second.Sum)
                : TDuration();

            waitInfo.Stat.Clear();

            it->second.UseCount += count;
            it->second.WaitTime += delta;

            TString resourceName;

            switch (file.GetObjectType()) {
            case Yql::DqsProto::TFile::EEXE_FILE:
                resourceName = "ResourceExe";
                waitInfo.Stat.AddCounter(resourceName, delta);
                break;
            case Yql::DqsProto::TFile::EUDF_FILE:
                if (!file.GetName().empty()) {
                    resourceName = "ResourceUdf" + file.GetName();
                    waitInfo.Stat.AddCounter(resourceName, delta);
                }
                waitInfo.Stat.AddCounter("ResourceUdf", delta);
                break;
            default:
                resourceName = "ResourceFile";
                waitInfo.Stat.AddCounter(resourceName, delta);
                break;
            }

            ResourceWaitTime->Collect(delta.MilliSeconds());
            *ResourceCounters->GetCounter(resourceName, /*derivative=*/ true) += count;
        }

        return result;
    } else {
        YQL_CLOG(DEBUG, ProviderDq) << "Tasks left " << tasksToAllocate.size();
        // schedule downloads
        for (const auto& workerInfo : result) {
            if (workerInfo == nullptr) {
                continue;
            }
            freeList->erase(workerInfo);
            if (workerInfo->Release()) {
                freeList->insert(workerInfo);
            }
        }
        for (const auto& [_, filter] : tasksToAllocate) {
            filter.Visit([&](const auto& file) {
                waitInfo.ResLeft[file.GetObjectId()]++;
            });
        }

        auto it = freeList->begin();
        for (const auto& [_, filter] : tasksToAllocate) {
            if (it == freeList->end()) {
                it = freeList->begin();
            }
            if (it == freeList->end()) {
                // begin == end => no free workers
                break;
            }
            const auto workerInfo = *it++;
            if (workerInfo->Stopping) {
                continue;
            }
            filter.Visit([&](const auto& file) {
                if (workerInfo->AddToDownloadList(file.GetObjectId(), file)) {
                    YQL_CLOG(TRACE, ProviderDq) << "Added " << file.GetName() << "|" << file.GetObjectId() << " to worker's " << GetGuidAsString(workerInfo->WorkerId) << " download list" ;
                    TStringBuilder resourceDownloadName;
                    resourceDownloadName << "ResourceDownload";
                    switch (file.GetObjectType()) {
                        case Yql::DqsProto::TFile::EEXE_FILE:
                            resourceDownloadName << "Exe";
                            break;
                        case Yql::DqsProto::TFile::EUDF_FILE:
                            resourceDownloadName << "Udf" << file.GetName();
                            break;
                        default:
                            resourceDownloadName << "File";
                            break;
                    }
                    *ResourceDownloadCounters->GetCounter(resourceDownloadName, /*derivative=*/ true) += 1;
                }
            });
        }

        return { };
    }
}

void TWorkersStorage::IsReady(const TVector<TFileResource>& resources, THashMap<TString, std::pair<ui32, ui32>>& clusterMap) {
    for (const auto& [_,workerInfo] : Workers) {
        auto count = 0;
        for (const auto& r : resources){
            if (workerInfo->GetResources().contains(r.GetObjectId())) {
                count++;
            }
        }
        YQL_CLOG(TRACE, ProviderDq) << workerInfo->ClusterName << " worker " << GetGuidAsString(workerInfo->WorkerId) << " has: " << count << "/" << int(resources.size()) << " resources";
        clusterMap[workerInfo->ClusterName].second += count == int(resources.size());
    }
};


void TWorkersStorage::ClusterStatus(Yql::DqsProto::ClusterStatusResponse* r) const {
    for (const auto& [_, workerInfo] : Workers) {
        auto* node = r->AddWorker();
        node->SetGuid(GetGuidAsString(workerInfo->WorkerId));
        node->SetNodeId(workerInfo->NodeId);
        node->SetLastPingTime(workerInfo->LastPingTime.ToString());
        node->SetRevision(workerInfo->Revision);
        node->SetClusterName(workerInfo->ClusterName);
        node->SetAddress(workerInfo->Address);
        node->SetPort(workerInfo->Port);
        node->SetEpoch(workerInfo->Epoch);
        node->SetStopping(workerInfo->Stopping);
        node->SetStartTime(workerInfo->StartTime);

        for (const auto& f : workerInfo->GetResourcesOrdered()) {
            *node->AddResource() = f.GetObjectId();
        }

        for (const auto& [k, _] : workerInfo->GetDownloadList()) {
            node->AddDownloadList()->SetObjectId(k);
        }

        for (const auto& [k, _] : workerInfo->GetActiveDownloads()) {
            node->AddActiveDownload()->SetObjectId(k);
        }
        for (const auto& op : workerInfo->Operations){
            node->AddOperation(op);
        }
        node->SetFreeDiskSize(workerInfo->FreeDiskSize);
        node->SetUsedDiskSize(workerInfo->UsedDiskSize);
        node->SetCapacity(workerInfo->Capacity);
        node->SetRunningWorkerActors(workerInfo->RunningWorkerActors);
        node->SetRunningRequests(workerInfo->RunningRequests);
        node->SetDead(workerInfo->IsDead);

        node->MutableRusage()->SetStime(workerInfo->Stime.MicroSeconds());
        node->MutableRusage()->SetUtime(workerInfo->Utime.MicroSeconds());
        node->MutableRusage()->SetMajorPageFaults(workerInfo->MajorPageFaults);
        node->MutableRusage()->SetCpuSystem(workerInfo->CpuSystem);
        node->MutableRusage()->SetCpuUser(workerInfo->CpuUser);
        node->MutableRusage()->SetCpuTotal(workerInfo->CpuTotal);

        for (const auto& [k, v] : workerInfo->Attributes) {
            auto* attr = node->AddAttribute();
            attr->SetKey(k);
            attr->SetValue(v);
        }

        node->SetUseCount(workerInfo->UseCount);
        node->SetUseTime(workerInfo->UseTime.MilliSeconds());
    }

    for (const auto& [_, i] : Uploaded) {
        auto* rr = r->AddResource();
        rr->SetId(i.Id);
        rr->SetName(i.Name);
        rr->SetUseTime(i.UseTime.MilliSeconds());
        rr->SetWaitTime(i.WaitTime.MilliSeconds());
        rr->SetUseCount(i.UseCount);
        rr->SetTryCount(i.TryCount);
        rr->SetSize(i.Size);
    }

    r->SetFreeListSize(FreeList.size());
    r->SetCapacity(GlobalResources.GetCapacity());
    r->SetRunningRequests(GlobalResources.GetRunningRequests());
}

void TWorkersStorage::UpdateResourceUseTime(TDuration duration, const THashSet<TString>& ids) {
    for (const auto& id : ids) {
        auto it = Uploaded.find(id);
        if (it != Uploaded.end()) {
            it->second.UseTime += duration;
        }
    }
}

bool TWorkersStorage::HasResource(const TString& id) const {
    auto it = Uploaded.find(id);
    return (it != Uploaded.end()) && it->second.Uploaded;
}

void TWorkersStorage::AddResource(const TString& id, EFileType type, const TString& name, i64 size) {
    if (!Uploaded.contains(id)) {
        Uploaded.emplace(id, TResourceStat{id, type, name, size});
    }
    Uploaded.find(id)->second.Uploaded = true;
}

void TWorkersStorage::UpdateMetrics() {
    if (!WorkersSize) {
        WorkersSize = Metrics->GetSubgroup("component", "lists")->GetCounter("WorkersSize");
    }
    if (!FreeListSize) {
        FreeListSize = Metrics->GetSubgroup("component", "lists")->GetCounter("FreeListSize");
    }
    *WorkersSize = Workers.size();
    *FreeListSize = FreeList.size();
}

void TWorkersStorage::Visit(const std::function<void(const TWorkerInfo::TPtr& workerInfo)>& f) {
    for (const auto& [_, workerInfo] : Workers) {
        f(workerInfo);
    }
}

} // namespace NYql
