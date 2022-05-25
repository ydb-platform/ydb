#include <ydb/library/yql/providers/dq/api/protos/dqs.pb.h>
#include "worker_info.h"
#include <ydb/library/yql/utils/log/log.h>

namespace NYql::NDqs {

    TInflightLimiter::TInflightLimiter(i32 inflightLimit)
        : InflightLimit(inflightLimit) {
    }

    bool TInflightLimiter::CanDownload(const TString& id) {
        return !InflightResources.contains(id) || InflightResources.find(id)->second < InflightLimit;
    }

    void TInflightLimiter::MarkDownloadStarted(const TString& id) {
        if (!InflightResources.contains(id)) {
            InflightResources.emplace(id, 0);
        }
        InflightResources.find(id)->second++;
    }

    void TInflightLimiter::MarkDownloadFinished(const TString& id) {
        auto it = InflightResources.find(id);
        if (it != InflightResources.end() && it->second > 0) {
            it->second--;
            if (it->second == 0) {
                InflightResources.erase(id);
            }
        }
    }

    TWorkerInfo::TWorkerInfo(
        const TString& startTime,
        ui32 nodeId,
        TGUID workerId,
        const Yql::DqsProto::RegisterNodeRequest& request,
        NYql::TSensorsGroupPtr  metrics,
        const TInflightLimiter::TPtr& inflightLimiter,
        TGlobalResources& globalResources)
        : NodeId(nodeId)
        , WorkerId(workerId)
        , LastPingTime(TInstant::Now())
        , Revision(request.GetRevision())
        , ClusterName(request.GetClusterName())
        , Address(request.GetAddress())
        , StartTime(!request.GetStartTime().empty()
            ? request.GetStartTime()
            : startTime)
        , Port(request.GetPort())
        , Epoch(request.GetEpoch())
        , Capabilities(request.GetCapabilities())
        , Capacity(request.GetCapacity()?request.GetCapacity():1)
        , Metrics(std::move(metrics))
        , InflightLimiter(inflightLimiter)
        , ClusterResources(TClusterResources(globalResources, ClusterName))
    {
#define ADD_METRIC(name)                        \
        name = Metrics->GetCounter(#name)
#define ADD_METRIC_DERIV(name)                  \
        name = Metrics->GetCounter(#name, /*derivative=*/ true)

        ADD_METRIC(CurrentDownloadsSum);
        ADD_METRIC(CurrentDownloadsMax);
        ADD_METRIC(CurrentDownloadsArgMax);

        ADD_METRIC(FilesCountSum);
        ADD_METRIC(FilesCountMax);
        ADD_METRIC(FilesCountArgMax);


        ADD_METRIC(FreeDiskSizeSum);
        ADD_METRIC(FreeDiskSizeMin);
        ADD_METRIC(FreeDiskSizeArgMin);

        ADD_METRIC(UsedDiskSizeSum);
        ADD_METRIC(UsedDiskSizeMax);
        ADD_METRIC(UsedDiskSizeArgMax);

        ADD_METRIC(ActiveDownloadsSum);
        ADD_METRIC(DeadWorkers);

        ADD_METRIC(CpuTotalSum);
        ADD_METRIC(MajorPageFaultsSum);

        ADD_METRIC_DERIV(FileAddCounter);
        ADD_METRIC_DERIV(FileRemoveCounter);

        ADD_METRIC(RunningActors);

#undef ADD_METRIC
#undef ADD_METRIC_DERIV

        for (const auto& attr : request.GetAttribute()) {
            Attributes[attr.GetKey()] = attr.GetValue();
        }

        ClusterResources.AddCapacity(Capacity);

        Update(request);
    }

    bool TWorkerInfo::Update(const Yql::DqsProto::RegisterNodeRequest& request)
    {
        bool needResume = false;
        auto now = TInstant::Now();
        TDuration interval = now-LastPingTime;
        LastPingTime = now;

        if (request.GetZombie()) {
            OnDead();
            return false;
        }

        auto curStime = TDuration::MicroSeconds(request.GetRusage().GetStime());
        auto curUtime = TDuration::MicroSeconds(request.GetRusage().GetUtime());

        i64 cpuTotalCur = 0;
        i64 cpuSystemCur = 0;
        i64 cpuUserCur = 0;

        if (interval > TDuration::Seconds(1)) {
            cpuTotalCur = 100*(curStime+curUtime-Stime-Utime).MicroSeconds();
            cpuTotalCur /= interval.MicroSeconds();

            cpuSystemCur = 100*(curStime-Stime).MicroSeconds();
            cpuSystemCur /= interval.MicroSeconds();

            cpuUserCur = 100*(curUtime-Utime).MicroSeconds();
            cpuUserCur /= interval.MicroSeconds();
        }

        Stime = curStime;
        Utime = curUtime;

        auto cpuTotalDelta = (cpuTotalCur - CpuTotal);
        CpuTotal = cpuTotalCur;
        *CpuTotalSum += cpuTotalDelta;

        CpuSystem = cpuSystemCur;
        CpuUser = cpuUserCur;

        auto pageFaultsDelta = (request.GetRusage().GetMajorPageFaults() - MajorPageFaults);
        MajorPageFaults = request.GetRusage().GetMajorPageFaults();
        *MajorPageFaultsSum += pageFaultsDelta;

        for (auto file : request.GetFilesOnNode()) {
            if (Resources.insert(file.GetObjectId()).second) {
                needResume = true;
                *FilesCountSum += 1;
                *FileAddCounter += 1;
            }
        }
        Operations.clear();
        for (const auto& op: request.GetRunningOperation()) {
            Operations.insert(op);
        }

        int actorsDelta = request.GetRunningWorkers()-RunningWorkerActors;
        *RunningActors += actorsDelta;

        RunningWorkerActors = request.GetRunningWorkers();

        if (static_cast<int>(Resources.size()) > static_cast<int>(request.GetFilesOnNode().size())) {
            needResume = true;
            // drop files
            THashSet<TString> filesOnNode;
            for (const auto& file : request.GetFilesOnNode()) {
                filesOnNode.insert(file.GetObjectId());
            }
            THashSet<TString> toDrop;
            for (const auto& k : Resources) {
                if (!filesOnNode.contains(k)) {
                    toDrop.insert(k);
                }
            }
            for (const auto& k : toDrop) {
                YQL_CLOG(DEBUG, ProviderDq) << "Remove resource " << k << " from worker " << GetGuidAsString(WorkerId);
                Resources.erase(k);
            }
            *FileRemoveCounter += toDrop.size();
            *FilesCountSum -= toDrop.size();
        }

        // ordered as in LRU cache
        ResourcesOrdered = std::move(request.GetFilesOnNode());

        if (*FreeDiskSizeArgMin == 0 || *FreeDiskSizeMin > request.GetFreeDiskSize()) {
            *FreeDiskSizeMin = request.GetFreeDiskSize();
            *FreeDiskSizeArgMin = NodeId;
        }

        if (*FreeDiskSizeArgMin == NodeId && *FreeDiskSizeMin != request.GetFreeDiskSize()) {
            *FreeDiskSizeMin = request.GetFreeDiskSize();
        }

        *FreeDiskSizeSum += (request.GetFreeDiskSize() - FreeDiskSize);
        FreeDiskSize = request.GetFreeDiskSize();

        if (*UsedDiskSizeMax < request.GetUsedDiskSize()) {
            *UsedDiskSizeMax = request.GetUsedDiskSize();
            *UsedDiskSizeArgMax = NodeId;
        }

        *UsedDiskSizeSum += (request.GetUsedDiskSize() - UsedDiskSize);
        UsedDiskSize = request.GetUsedDiskSize();

        for (auto md5: Resources) {
            RemoveFromDownloadList(md5);
        }

        if (*FilesCountMax < static_cast<i64>(Resources.size())) {
            *FilesCountMax = Resources.size();
            *FilesCountArgMax = NodeId;
        }

        if (*CurrentDownloadsMax < static_cast<i64>(DownloadList.size())) {
            *CurrentDownloadsMax = DownloadList.size();
            *CurrentDownloadsArgMax = NodeId;
        }

        return needResume;
    }

    void TWorkerInfo::RemoveFromDownloadList(const TString& objectId) {
        auto delta = DownloadList.erase(objectId);
        if (delta) {
            InflightLimiter->MarkDownloadFinished(objectId);
            *ActiveDownloadsSum -= ActiveDownloads.erase(objectId);
        }
        *CurrentDownloadsSum -= delta;
    }

    void TWorkerInfo::AddToDownloadList(const THashMap<TString, TFileResource>& downloadList) {
        for (const auto& [k, v] : downloadList) {
            AddToDownloadList(k, v);
        }
    }

    bool TWorkerInfo::AddToDownloadList(const TString& key, const TFileResource& value) {
        if (!Resources.contains(key) && DownloadList.insert({key, value}).second) {
            *CurrentDownloadsSum += 1;
            return true;
        }
        return false;
    }

    const THashMap<TString, TWorkerInfo::TFileResource>& TWorkerInfo::GetDownloadList() {
        return DownloadList;
    }

    THashMap<TString, TWorkerInfo::TFileResource> TWorkerInfo::GetResourcesForDownloading() {
        for (const auto& it : DownloadList) {
            TString id = it.first;
            if (InflightLimiter->CanDownload(id) && !ActiveDownloads.contains(id)) {
                ActiveDownloads.emplace(id, it.second);
                *ActiveDownloadsSum += 1;
                InflightLimiter->MarkDownloadStarted(id);
            }
        }
        return ActiveDownloads;
    }

    const THashSet<TString>& TWorkerInfo::GetResources() {
        return Resources;
    }

    void TWorkerInfo::OnDead() {
        if (IsDead) {
            return;
        }
        IsDead = true;
        for (const auto& [id, _] : ActiveDownloads) {
            InflightLimiter->MarkDownloadFinished(id);
        }
        *ActiveDownloadsSum -= ActiveDownloads.size();
        ActiveDownloads.clear();

        *CurrentDownloadsSum -= DownloadList.size();
        *FilesCountSum -= Resources.size();

        *UsedDiskSizeSum += - UsedDiskSize;
        *FreeDiskSizeSum += - FreeDiskSize;

        *DeadWorkers += 1;

        ClusterResources.AddCapacity(-Capacity);
        ClusterResources.AddRunning(-RunningRequests);
        RunningRequests = 0;

        *CpuTotalSum -= CpuTotal;
        *MajorPageFaultsSum -= MajorPageFaults;

        *RunningActors -= RunningWorkerActors;
        Operations.clear();
    }

    TWorkerInfo::~TWorkerInfo() {
        OnDead();
    }

    bool TWorkerInfo::Acquire() {
        if (IsDead) {
            return true;
        }
        ClusterResources.AddRunning(1);
        return ++RunningRequests >= Capacity;
    }

    bool TWorkerInfo::Release() {
        if (IsDead) {
            return false;
        }
        UseTime += TInstant::Now() - RequestStartTime;
        ClusterResources.AddRunning(-1);
        return --RunningRequests < Capacity;
    }

} // namespace NYql::NDqs
