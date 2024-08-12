#include "global_worker_manager.h"
#include "workers_storage.h"

#include <ydb/library/yql/providers/dq/actors/events/events.h>
#include <ydb/library/yql/providers/dq/common/attrs.h>
#include <ydb/library/yql/providers/dq/scheduler/dq_scheduler.h>

#include <ydb/library/yql/providers/dq/worker_manager/worker_manager_common.h>
#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>
#include <ydb/library/yql/providers/dq/actors/execution_helpers.h>
#include <ydb/library/yql/providers/dq/api/grpc/api.grpc.pb.h>
#include <ydb/library/yql/providers/dq/counters/counters.h>

#include <ydb/library/yql/utils/failure_injector/failure_injector.h>

#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/utils/log/log.h>

#include <ydb/library/grpc/client/grpc_client_low.h>
#include <library/cpp/protobuf/util/pb_io.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/svnversion/svnversion.h>

#include <util/generic/vector.h>
#include <util/generic/guid.h>
#include <util/system/fs.h>
#include <util/system/hostname.h>
#include <util/system/getpid.h>
#include <util/generic/scope.h>
#include <util/random/shuffle.h>

namespace NYql {

using namespace NActors;
using namespace NDqs;
using namespace NThreading;
using namespace NMonitoring;
using EFileType = Yql::DqsProto::TFile::EFileType;
using TFileResource = Yql::DqsProto::TFile;

union TDqResourceId {
    struct {
        ui32 Counter;
        ui32 Epoch;
    };
    ui64 Data;
};

static_assert(sizeof(TDqResourceId) == 8);

class TWorkerStopFilter {
public:
    TWorkerStopFilter(const Yql::DqsProto::JobStopRequest& request)
        : Revision(request.GetRevision())
        , ClusterName(request.GetClusterName())
        , WorkerId(GetGuid(request.GetWorkerId()))
        , NegativeRevision(request.GetNegativeRevision())
        , LastUpdate(TInstant::Now())
    {
        for (const auto& attr : request.GetAttribute()) {
            Attributes.emplace(attr.GetKey(), attr.GetValue());
        }
    }

    bool Match(const TWorkerInfo& workerInfo) const {
        return MatchInternal(workerInfo);
    }

    TInstant GetLastUpdate() const {
        return LastUpdate;
    }

private:
    bool MatchInternal(const TWorkerInfo& workerInfo) const {
        if (!Revision.empty() && NegativeRevision == (Revision == workerInfo.Revision)) {
            return false;
        }
        if (!ClusterName.empty() && ClusterName != workerInfo.ClusterName) {
            return false;
        }
        if (!WorkerId.IsEmpty() && WorkerId != workerInfo.WorkerId) {
            return false;
        }

        for (const auto& [k, v] : Attributes) {
            auto i = workerInfo.Attributes.find(k);
            if (i == workerInfo.Attributes.end()) {
                return false;
            }
            if (i->second != v) {
                return false;
            }
        }

        LastUpdate = TInstant::Now();

        return true;
    }

    const TString Revision;
    const TString ClusterName;
    const TGUID WorkerId;
    const bool NegativeRevision;
    THashMap<TString, TString> Attributes;

    mutable TInstant LastUpdate;
};

// used for fast state recovery on restarted jobs
class TLocalCriticalFiles: public IFileCache {
public:
// used in IsReady
// exe unsupported
    void AddFile(const TString& path, const TString& objectId) override {
        Y_UNUSED(path);
        TGuard<TMutex> guard(Mutex);
        ObjectIds.insert(objectId);
    }

    void Clear() override {
        TGuard<TMutex> guard(Mutex);
        ObjectIds.clear();
    }

    TMaybe<TString> FindFile(const TString& objectId) override {
        Y_UNUSED(objectId);
        return { };
    }

    TMaybe<TString> AcquireFile(const TString& objectId) override {
        Y_UNUSED(objectId);
        return { };
    }

    void ReleaseFile(const TString& objectId) override {
        Y_UNUSED(objectId);
        return;
    }

    bool Contains(const TString& objectId) override {
        Y_UNUSED(objectId);
        return false;
    }

    void Walk(const std::function<void(const TString& objectId)>& f) override {
        TGuard<TMutex> guard(Mutex);
        for (const auto& id : ObjectIds) {
            f(id);
        }
    }

    i64 FreeDiskSize() override {
        return 0;
    }

    ui64 UsedDiskSize() override {
        return 0;
    }

    TString GetDir() override {
        return ""; // unused
    }

private:
    TMutex Mutex;
    THashSet<TString> ObjectIds;
};


// used in gwm master
class TAllCriticalFiles {
public:
    template<typename VectorLike>
    void Add(
        const TGUID& serviceNodeId,
        ui32 nodeId,
        const TString& revision,
        const TString& address,
        ui32 pid,
        const VectorLike& files)
    {
        auto now = TInstant::Now();
        auto& t = PerServiceNode[serviceNodeId];
        t.LastUpdate = now;
        t.NodeId = nodeId;
        t.Revision = revision;
        t.Pid = pid;
        t.Address = address;
        bool needUpdate = false;
        for (const auto& f : files) {
            needUpdate |= t.Files.insert(f.GetObjectId()).second;
        }

        if (static_cast<int>(t.Files.size()) > static_cast<int>(files.size())) {
            needUpdate = true;
            THashSet<TString> hash;
            for (const auto& f : files) {
                hash.insert(f.GetObjectId());
            }
            THashSet<TString> toDrop;
            for (const auto& f : t.Files) {
                if (!hash.contains(f)) {
                    toDrop.insert(f);
                }
            }
            for (const auto& f : toDrop) {
                t.Files.erase(f);
            }
        }

        if (needUpdate) {
            t.OrderedFiles.clear();
            for (const auto& f : files) {
                t.OrderedFiles.push_back(f.GetObjectId());
            }
        }

        needUpdate |= ClearOld(now);

        if (needUpdate) {
            Rebuild();
        }
    }

    const TVector<TWorkerInfo::TFileResource>& GetResources() const {
        return Resources;
    }

    struct TServiceNodeFiles {
        ui32 NodeId;
        THashSet<TString> Files;
        TVector<TString> OrderedFiles;
        TInstant LastUpdate = TInstant::Now();
        TString Revision;
        ui32 Pid;
        TString Address;
    };

    const THashMap<TGUID, TServiceNodeFiles>& GetNodes() const {
        return PerServiceNode;
    }

private:
    bool ClearOld(TInstant now) {
        THashSet<TGUID> toDrop;
        for (const auto& [k, v] : PerServiceNode) {
            if (k.IsEmpty()) { // local node
                continue;
            }

            if (now - v.LastUpdate > TDuration::Seconds(10)) {
                toDrop.insert(k);
            }
        }

        for (const auto& k : toDrop) {
            PerServiceNode.erase(k);
        }

        return !toDrop.empty();
    }

    void Rebuild() {
        Resources.clear();
        THashSet<TString> uniq;
        THashSet<TString> uniqExe;
        for (const auto& [_, v] : PerServiceNode) {
            for (int i = 0; i < static_cast<int>(v.OrderedFiles.size()) - 1; ++i) {
                const auto& f = v.OrderedFiles[i];
                uniq.insert(f);
            }
            if (!v.Files.empty()) {
                uniqExe.insert(v.OrderedFiles.back());
            }
        }
        for (const auto& f : uniq) {
            TWorkerInfo::TFileResource resource;
            resource.SetObjectType(TWorkerInfo::TFileResource::EUDF_FILE);
            resource.SetObjectId(f);
            Resources.push_back(resource);
        }
        //exe upload controlled via DqControl
        //for (const auto& f : uniqExe) {
        //    TWorkerInfo::TFileResource resource;
        //    resource.SetName("dq_vanilla_job.lite");
        //    resource.SetObjectType(TWorkerInfo::TFileResource::EEXE_FILE);
        //    resource.SetObjectId(f);
        //    Resources.push_back(resource);
        //}
    }

    THashMap<TGUID, TServiceNodeFiles> PerServiceNode; // serviceNodeId -> List
    TVector<TWorkerInfo::TFileResource> Resources;
};

class TGlobalWorkerManager: public TWorkerManagerCommon<TGlobalWorkerManager> {
public:

    static constexpr char ActorName[] = "GWM";

    explicit TGlobalWorkerManager(
           const ICoordinationHelper::TPtr& coordinator,
           const TVector<TResourceManagerOptions>& resourceUploaderOptions,
           IMetricsRegistryPtr metricsRegistry,
           const NProto::TDqConfig::TScheduler& schedulerConfig)
       : TWorkerManagerCommon<TGlobalWorkerManager>(&TGlobalWorkerManager::Initialization)
        , Coordinator(coordinator)
        , LeaderResolver(std::make_shared<TSingleNodeResolver>())
        , Metrics(metricsRegistry->GetSensors()->GetSubgroup("counters", "gwm"))
        , UploaderMetrics(metricsRegistry->GetSensors()->GetSubgroup("counters", "uploader"))
        , LatencyHistogram(Metrics->GetHistogram("LeaderLatency", ExponentialHistogram(10, 2, 1)))
        , Workers(Coordinator->GetNodeId(), Metrics, metricsRegistry->GetSensors()->GetSubgroup("counters", "workers"))
        , Scheduler(NDq::IScheduler::Make(schedulerConfig, metricsRegistry))
        , Revision(ToString(GetProgramCommitId()))
        , ResourceUploaderOptions(resourceUploaderOptions)
        , WaitListSize(nullptr)
    { }

private:

#define HHFunc(TEvType, HandleFunc)                                                 \
    case TEvType::EventType: {                                                      \
        Y_SCOPE_EXIT(&) { UpdateMetrics(); };                                       \
        typename TEvType::TPtr* x = reinterpret_cast<typename TEvType::TPtr*>(&ev); \
        TString name(#TEvType);                                                     \
        name = name.substr(name.find_last_of(':')+1);                               \
        *Metrics->GetSubgroup("component", "requests")->GetCounter(name, true) += 1;\
        TInstant t = TInstant::Now();                                               \
        HandleFunc(*x, TActivationContext::ActorContextFor(this->SelfId()));        \
        if (CurrentStateFunc() == &TGlobalWorkerManager::Leader) {                  \
            LatencyHistogram->Collect((TInstant::Now() - t).MilliSeconds());        \
        }                                                                           \
        break;                                                                      \
    }

    STRICT_STFUNC(Initialization, {
        HHFunc(TEvBecomeFollower, StartFollower)
        HHFunc(TEvBecomeLeader, StartLeader)
        CFunc(TEvents::TEvBootstrap::EventType, Bootstrap)

        HHFunc(TEvAllocateWorkersRequest, OnAllocateWorkersRequestStub)
        HHFunc(TEvFreeWorkersNotify, OnFreeWorkersStub)
        HHFunc(TEvRegisterNode, OnRegisterNodeStub)
        HHFunc(TEvClusterStatus, OnClusterStatusStub)
        HHFunc(TEvQueryStatus, OnQueryStatusStub)
        HHFunc(TEvIsReady, OnIsReadyStub)
        HHFunc(TEvGetMasterRequest, OnGetMasterStub)
        HHFunc(TEvConfigureFailureInjectorRequest, OnConfigureFailureInjectorStub)
        HHFunc(TEvOperationStop, OnOperationStopStub)

        cFunc(TEvents::TEvPoison::EventType, PassAway)
        IgnoreFunc(TEvJobStop)
        HHFunc(TEvRoutesRequest, this->OnRoutesRequest)
    })

    STRICT_STFUNC(Leader, {
        HHFunc(TEvAllocateWorkersRequest, OnAllocateWorkersRequest)
        HHFunc(TEvFreeWorkersNotify, OnFreeWorkers)
        HHFunc(TEvRegisterNode, OnRegisterNode)

        HHFunc(TEvBecomeLeader, StartLeader)
        HHFunc(TEvBecomeFollower, StartFollower)

        IgnoreFunc(TEvInterconnect::TEvNodeConnected)

        HHFunc(TEvents::TEvUndelivered, OnUndelivered)
        HHFunc(TEvInterconnect::TEvNodeDisconnected, OnExecuterDisconnected)

        cFunc(TEvents::TEvPoison::EventType, PassAway)

        HHFunc(TEvUploadComplete, OnFileUploaded)
        HHFunc(TEvClusterStatus, OnClusterStatus)
        HHFunc(TEvQueryStatus, OnQueryStatus)
        HHFunc(TEvIsReady, OnIsReady)
        HHFunc(TEvJobStop, OnJobStop)
        HHFunc(TEvGetMasterRequest, OnGetMaster)
        HHFunc(TEvConfigureFailureInjectorRequest, OnConfigureFailureInjector)
        cFunc(TEvTick::EventType, OnTick)
        HHFunc(TEvRoutesRequest, OnRoutesRequest)
        HHFunc(TEvOperationStop, OnOperationStop)
    })

#define FORWARD(T) \
    case T::EventType: { \
        /*YQL_CLOG(TRACE, ProviderDq) << "Forward event " << etype << " to " << LeaderId;*/ \
        typename T::TPtr& x = *(reinterpret_cast<typename T::TPtr*>(&ev)); \
        if (!x->Get()->Record.GetIsForwarded()) { \
            x->Get()->Record.SetIsForwarded(true); \
            Send(x->Forward(MakeWorkerManagerActorID(LeaderId))); \
        } \
        break; \
    }

    STRICT_STFUNC(Follower, {
        HHFunc(TEvBecomeLeader, StartLeader)
        HHFunc(TEvBecomeFollower, StartFollower)
        HHFunc(TEvAllocateWorkersRequest, StartUploadAndForward)
        FORWARD(TEvFreeWorkersNotify)
        FORWARD(TEvRegisterNode)
        FORWARD(TEvJobStop)
        FORWARD(TEvClusterStatus)
        FORWARD(TEvQueryStatus)
        FORWARD(TEvOperationStop)
        HHFunc(TEvIsReady, OnIsReadyForward)
        HHFunc(TEvGetMasterRequest, OnGetMaster)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
        HHFunc(TEvUploadComplete, OnFileUploaded)
        IgnoreFunc(TEvTick)
        HHFunc(TEvRoutesRequest, this->OnRoutesRequest)
    })

#undef FORWARD
#undef HHFunc

    void Tick() {
        Schedule(ScheduleInterval, new TEvTick);
    }

    void OnTick() {
        auto now = TInstant::Now();
        if (now - LastCleanTime > CleanInterval) {
            Y_SCOPE_EXIT(&) { UpdateMetrics(); };
            CleanUp(now);
            LastCleanTime = now;
        }
        TryResume();
        Tick();
    }

    void DoPassAway() override {
        Y_SCOPE_EXIT(&) { UpdateMetrics(); };
        for (const auto& sender: Scheduler->Cleanup()) {
            Send(sender, new TEvAllocateWorkersResponse("Shutdown in progress", NYql::NDqProto::StatusIds::UNAVAILABLE));
        }
    }

    std::pair<bool, TString> CheckFiles(const TVector<TFileResource>& files) {
        for (const auto& file : files) {
            if (file.GetObjectType() == Yql::DqsProto::TFile::EEXE_FILE) {
                if (file.GetName().empty()) {
                    return std::make_pair(false, "Unnamed exe file " + file.SerializeAsString());
                }
            } else {
                if (!NFs::Exists(file.GetLocalPath())) {
                    return std::make_pair(false, "Unknown file " + file.SerializeAsString());
                }
            }

            if (file.GetObjectId().empty()) {
                return std::make_pair(false, "Empty objectId (md5, revision) for " + file.SerializeAsString());
            }
        }

        return std::make_pair(true, "");
    }

    std::pair<bool, TString> MaybeUpload(bool isForwarded, const TVector<TFileResource>& files, bool useCache = false) {
        if (isForwarded) {
            return std::make_pair(false, "");
        }

        auto [hasExeFile,error] = CheckFiles(files);
        if (!error.empty()) {
            return std::make_pair(hasExeFile,error);
        }

        bool flag = false;

        TVector<TResourceFile> preparedFiles;
        TVector<TString> preparedFilesIds;
        TVector<TResourceFile> preparedExeFiles;
        auto now = TInstant::Now();
        auto cacheTimeout = TDuration::Minutes(5);

        if (useCache) {
            THashSet<TString> toremove;
            for (const auto& [objectId, ts] : LastUploadCache) {
                if (now - ts > cacheTimeout) {
                    toremove.insert(objectId);
                }
            }
            for (const auto& objectId : toremove) {
                LastUploadCache.erase(objectId);
            }
        }

        for (const auto& file : files) {
            flag |= file.GetObjectType() == Yql::DqsProto::TFile::EEXE_FILE;
            if (Uploading.contains(file.GetObjectId())) {
                continue;
            }

            // exe files can be without local path
            if (file.GetLocalPath().empty()) {
                continue;
            }
            auto fileName = file.GetName();
            auto objectId = file.GetObjectId();
            auto objectType = file.GetObjectType();
            auto localPath = file.GetLocalPath();
            auto size = file.GetSize();

            if (useCache || objectType == Yql::DqsProto::TFile_EFileType_EEXE_FILE) {
                if (LastUploadCache.contains(objectId) && (now - LastUploadCache[objectId] < cacheTimeout)) {
                    continue;
                }
                LastUploadCache[objectId] = now;
            }

            TResourceFile preparedFile;
            preparedFile.ObjectId = objectId;
            preparedFile.LocalFileName = fileName;
            preparedFile.Attributes["file_name"] = preparedFile.GetRemoteFileName();
            preparedFile.File = ::TFile(localPath, RdOnly | OpenExisting);
            preparedFile.RemoteFileName = objectId;
            preparedFiles.push_back(preparedFile);
            preparedFilesIds.push_back(objectId);

            YQL_CLOG(DEBUG, ProviderDq) << "Start upload: " << fileName << "," << objectId << "," << size;

            int uploadProcesses = static_cast<int>(ResourceUploaderOptions.size());

            if (objectType == Yql::DqsProto::TFile_EFileType_EEXE_FILE) {
                // COMPAT (aozeritsky)
                preparedFile.RemoteFileName = fileName;
                uploadProcesses *= 2;

                for (auto options : ResourceUploaderOptions) {
                    options.Counters = UploaderMetrics;
                    options.LockName = objectId + "." + options.YtBackend.GetClusterName();
                    options.Files = { preparedFile };
                    options.UploadPrefix = options.UploadPrefix + "/bin/" + objectId;
                    auto actor = CreateResourceUploader(options, Coordinator);
                    UploadProcesses[RegisterChild(actor)].push_back(objectId);
                }
            }

            Uploading.emplace(objectId, TUploadingInfo {objectType, fileName, uploadProcesses, size});
        }

        Shuffle(preparedFiles.begin(), preparedFiles.end(), TReallyFastRng32(TInstant::Now().NanoSeconds()));

        for (auto options : ResourceUploaderOptions) {
            options.Counters = UploaderMetrics;
            options.LockName.clear();

            if (!preparedFiles.empty()) {
                options.Files = preparedFiles;
                options.UploadPrefix = options.UploadPrefix + "/udfs";
                auto actor = CreateResourceUploader(options, Coordinator);
                UploadProcesses.emplace(RegisterChild(actor), preparedFilesIds);
            }
        }

        return std::make_pair(flag, "");
    }

    void StartUploadAndForward(TEvAllocateWorkersRequest::TPtr& ev, const TActorContext& ctx)
    {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(ev->Get()->Record.GetTraceId());
        auto [hasExeFile, err]  = MaybeUpload(ev->Get()->Record.GetIsForwarded(), TVector<TFileResource>(ev->Get()->Record.GetFiles().begin(), ev->Get()->Record.GetFiles().end()));
        if (!err.empty()) {
            Send(ev->Sender, new TEvAllocateWorkersResponse(err, NYql::NDqProto::StatusIds::EXTERNAL_ERROR));
        }

        if (!hasExeFile && LeaderRevision != Revision) {
            Send(ev->Sender, new TEvAllocateWorkersResponse(
                Sprintf("Wrong revision %s!=%s", LeaderRevision.c_str(), Revision.c_str()), NYql::NDqProto::StatusIds::BAD_REQUEST));
        } else {
            ev->Get()->Record.SetIsForwarded(true);
            ctx.Send(ev->Forward(MakeWorkerManagerActorID(LeaderId)));
        }
    }

    void OnFileUploaded(TEvUploadComplete::TPtr& ev, const TActorContext& ctx)
    {
        Y_UNUSED(ctx);
        auto sender = ev->Sender;
        auto it = UploadProcesses.find(sender);
        if (it == UploadProcesses.end()) {
            return;
        }
        UnregisterChild(sender);

        for (const auto& objectId : it->second) {
            auto jt = Uploading.find(objectId);
            Y_ABORT_UNLESS(jt != Uploading.end());
            Workers.AddResource(objectId, jt->second.ObjectType, jt->second.ObjectName, jt->second.Size);
            if (--jt->second.Clusters <= 0) {
                Uploading.erase(jt);
            }
        }
        UploadProcesses.erase(it);
    }

    void StartLeader(TEvBecomeLeader::TPtr& ev, const TActorContext& ctx)
    {
        Y_UNUSED(ctx);
        if (FollowingMode) {
            YQL_CLOG(INFO, ProviderDq) << "Skip Leader request in following mode";
            Send(SelfId(), new TEvBecomeFollower(), /*flag=*/0, /*cookie=*/1);
            return;
        }
        LeaderEpoch = CurrentResourceId.Epoch = ev->Get()->LeaderEpoch;
        CurrentResourceId.Counter = 0;

        // update leader info (used for leader)
        auto attributes = NYT::NodeFromYsonString(ev->Get()->Attributes).AsMap();
        UpdateLeaderInfo(attributes);

        YQL_CLOG(INFO, ProviderDq) << "Become leader, epoch=" << CurrentResourceId.Epoch;
        YQL_CLOG(INFO, ProviderDq) << "Leader attributes leader=" << ev->Get()->Attributes;
        YQL_CLOG(INFO, ProviderDq) << "Leader resolver=" << LeaderHost << ":" << LeaderPort;

        if (LeaderPinger) {
            UnregisterChild(LeaderPinger);
            LeaderPinger = TActorId();
        }
        Become(&TGlobalWorkerManager::Leader);
        Tick();
    }

    void StartFollower(TEvBecomeFollower::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        if (ev->Cookie == 1 && LockId) {
            // kill from main
            YQL_CLOG(INFO, ProviderDq) << "Kill from main";
            ctx.Send(ev->Forward(LockId));
            FollowingMode = true;
            LocalCriticalFiles->Clear(); // disable wormup for old dq process
            return;
        }
        auto attributes = NYT::NodeFromYsonString(ev->Get()->Attributes).AsMap();
        if (ev->Cookie == 1 && LockId) {
            // kill from main
            YQL_CLOG(INFO, ProviderDq) << "Kill from main";
            Send(LockId, new NActors::TEvents::TEvPoison);
            LockId = TActorId();
        }
        LeaderId = attributes.at(NCommonAttrs::ACTOR_NODEID_ATTR).AsUint64();
        LeaderResolver->SetLeaderHostPort(
                attributes.at(NCommonAttrs::HOSTNAME_ATTR).AsString() + ":" + attributes.at(NCommonAttrs::GRPCPORT_ATTR).AsString());
        if (!LeaderPinger) {
            TResourceManagerOptions rmOptions;
            rmOptions.FileCache = LocalCriticalFiles;
            LeaderPinger = RegisterChild(Coordinator->CreateServiceNodePinger(LeaderResolver, rmOptions));
        }
        if (attributes.contains(NCommonAttrs::REVISION_ATTR)) {
            LeaderRevision = attributes.at(NCommonAttrs::REVISION_ATTR).AsString();
        }

        UpdateLeaderInfo(attributes);

        YQL_CLOG(TRACE, ProviderDq) << " Following leader: " << LeaderId;
        YQL_CLOG(INFO, ProviderDq) << "Leader resolver=" << LeaderHost << ":" << LeaderPort;

        WaitListSize = nullptr;
        Workers.Clear();
        for (const auto& [key, value] : AllocatedResources) {
            Send(value.ActorId, new TEvents::TEvPoison());
        }
        for (const auto sender : Scheduler->Cleanup()) {
            Send(sender, new TEvAllocateWorkersResponse("StartFollower", NYql::NDqProto::StatusIds::UNSPECIFIED));
        }
        AllocatedResources.clear();
        for (auto& [k, v] : LiteralQueries) {
            *v -= *v;
        }

        Become(&TGlobalWorkerManager::Follower);
    }

    void UpdateLeaderInfo(THashMap<TString, NYT::TNode>& attributes) {
        LeaderHost = attributes.at(NCommonAttrs::HOSTNAME_ATTR).AsString();
        LeaderPort = std::stoi(attributes.at(NCommonAttrs::GRPCPORT_ATTR).AsString().Data());
    }

    void Bootstrap(const TActorContext& ctx) {
        Y_UNUSED(ctx);
        LockId = RegisterChild(Coordinator->CreateLock("gwm", false));
    }

    TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parentId) override {
        return new IEventHandle(self, parentId, new TEvents::TEvBootstrap(), 0);
    }

    void OnRegisterNodeStub(TEvRegisterNode::TPtr& ev, const NActors::TActorContext&) {
        Send(ev->Sender, new TEvRegisterNodeResponse());
    }

    void OnRegisterNode(TEvRegisterNode::TPtr& ev, const NActors::TActorContext& ctx) {
        auto* mutableRequest = ev->Get()->Record.MutableRequest();
        auto& request = ev->Get()->Record.GetRequest();
        auto nodeId = request.GetNodeId();
        auto workerId = NYql::NDqs::NExecutionHelpers::GuidFromProto(request.GetGuid());
        auto role = request.GetRole();

        Y_SCOPE_EXIT(&) {
            ctx.Send(ev->Forward(NActors::GetNameserviceActorId()));
        };

        if (role == "worker_node" && Revision == request.GetRevision()) {
            mutableRequest->SetEpoch(LeaderEpoch);
        }

        if (role == "service_node") {
            AllCriticalFiles.Add(
                workerId,
                nodeId,
                request.GetRevision(),
                request.GetAddress(),
                request.GetPid(),
                request.GetFilesOnNode());
        }

        if (role != "worker_node") {
            mutableRequest->SetEpoch(LeaderEpoch);
            return;
        }

        int capacity = request.GetCapacity();
        if (capacity == 0) {
            capacity = 1;
        }

        {
            TWorkerInfo::TPtr workerInfo;
            bool needResume;

            auto newWorkerId = workerId;

            std::tie(workerInfo, needResume) = Workers.CreateOrUpdate(nodeId, newWorkerId, *mutableRequest);

            for (auto i = StopFilters.begin(); i != StopFilters.end(); ) {
                if (workerInfo && !workerInfo->IsDead && i->Match(*workerInfo)) {
                    workerInfo->Stopping = true;
                    if (request.GetRunningWorkers() == 0) {
                        YQL_CLOG(DEBUG, ProviderDq) << "Stop worker on user request " << GetGuidAsString(workerId);
                        Send(MakeWorkerManagerActorID(nodeId), new TEvents::TEvPoison);
                        return;
                    }
                }

                if ((TInstant::Now() - i->GetLastUpdate()) > TDuration::Seconds(600)) {
                    i = StopFilters.erase(i);
                } else {
                    ++i;
                }
            }

            if (needResume) {
                MarkDirty();
            }

            if (workerInfo && !workerInfo->IsDead) {
                for (auto& r : AllCriticalFiles.GetResources()) {
                    workerInfo->AddToDownloadList(r.GetObjectId(), r);
                }

                for (const auto& [_, file] : workerInfo->GetResourcesForDownloading()) {
                    *mutableRequest->AddDownloadList() = file;
                }
            }
        }
    }

    void CleanUp(TInstant now) {
        Workers.CleanUp(now, TDuration::Seconds(15));
    }

    void OnAllocateWorkersRequestStub(TEvAllocateWorkersRequest::TPtr& ev, const NActors::TActorContext&) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(ev->Get()->Record.GetTraceId());
        YQL_CLOG(DEBUG, ProviderDq) << "TGlobalWorkerManager::TEvAllocateWorkersRequest initialization";
        Send(ev->Sender, new TEvAllocateWorkersResponse("GWM: Waiting for initialization", NYql::NDqProto::StatusIds::UNAVAILABLE));
    }

    void MarkDirty(size_t count = 0U) {
        ScheduleWaitCount = Min(ScheduleWaitCount, count);
    }

    void TryResume() {
        if (Workers.FreeSlots() >= ScheduleWaitCount) {
            Scheduler->Process(Workers.Capacity(), Workers.FreeSlots(), [&] (const auto& item) {
                auto maybeDead = DeadOperations.find(item.Request.GetResourceId());
                if (maybeDead != DeadOperations.end()) {
                    DeadOperations.erase(maybeDead);
                    return true; // remove from WaitList
                }
                auto candidates = Workers.TryAllocate(item);
                if (!candidates.empty()) {
                    DoAllocate(item, candidates);
                }
                return !candidates.empty();
            });
            ScheduleWaitCount = std::numeric_limits<size_t>::max();
            DeadOperations.clear();
        }
    }

    void OnAllocateWorkersRequest(TEvAllocateWorkersRequest::TPtr& ev, const NActors::TActorContext& ctx) {
        Y_UNUSED(ctx);
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(ev->Get()->Record.GetTraceId());
        YQL_CLOG(DEBUG, ProviderDq) << "TGlobalWorkerManager::TEvAllocateWorkersRequest";

        TFailureInjector::Reach("allocate_workers_failure", [] { ::_exit(1); });

        const auto count = ev->Get()->Record.GetCount();
        Y_ASSERT(count != 0);

        if (!Scheduler->Suspend(NDq::IScheduler::TWaitInfo(ev->Get()->Record, ev->Sender))) {
            Send(ev->Sender, new TEvAllocateWorkersResponse("Too many dq operations", NYql::NDqProto::StatusIds::OVERLOADED));
            return;
        }

        auto [_, error]  = MaybeUpload(ev->Get()->Record.GetIsForwarded(), TVector<TFileResource>(ev->Get()->Record.GetFiles().begin(),ev->Get()->Record.GetFiles().end()));
        if (!error.empty()) {
            Send(ev->Sender, new TEvAllocateWorkersResponse(error, NYql::NDqProto::StatusIds::EXTERNAL_ERROR));
            return;
        }

        MarkDirty(count);
    }

    void DecrLiteralQueries(const TString& clusterName) {
        *LiteralQueries[clusterName] += -1;
    }

    void IncrLiteralQueries(const TString& clusterName) {
        auto& counter = LiteralQueries[clusterName];
        if (!counter) {
            counter = Metrics
                ->GetSubgroup("counters", "gwm")
                ->GetSubgroup("component", "lists")
                ->GetSubgroup("ClusterName", clusterName)
                ->GetCounter("LiteralQueries");
        }
        *counter += 1;
    }

    void DoAllocate(const NDq::IScheduler::TWaitInfo& waitInfo, const TVector<TWorkerInfo::TPtr>& allocated) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(waitInfo.Request.GetTraceId());
        Y_ABORT_UNLESS(allocated.size() == waitInfo.Request.GetCount());

        THashSet<TString> clusters;

        for (const auto& workerInfo : allocated) {
            YQL_CLOG(DEBUG, ProviderDq) << "Allocating worker " << GetGuidAsString(workerInfo->WorkerId);
            YQL_CLOG(DEBUG, ProviderDq) << " Address : " << workerInfo->Address;
            for (const auto& [k, v] : workerInfo->Attributes) {
                YQL_CLOG(DEBUG, ProviderDq) << " " << k << " : " << v;
            }
            clusters.insert(workerInfo->ClusterName);
        }

        THashSet<TString> usedFiles;
        for (const auto& file : waitInfo.Request.GetFiles()) {
            usedFiles.insert(file.GetObjectId());
        }

        auto resourceId = CurrentResourceId.Data;
        CurrentResourceId.Counter ++;

        auto resourceAllocator = waitInfo.Sender;

        auto response = MakeHolder<TEvAllocateWorkersResponse>(resourceId, allocated);
        waitInfo.Stat.FlushCounters(response->Record);
        Send(
            resourceAllocator,
            response.Release(),
            IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);

        Subscribe(resourceAllocator.NodeId());

        AllocatedResources[resourceId] = {
            resourceAllocator,
            allocated,
            usedFiles,
            clusters,
            TInstant::Now()
        };

        if (allocated.size() == 1) {
            Y_ABORT_UNLESS(clusters.size() == 1);
            IncrLiteralQueries(*clusters.begin());
        }
    }

    void DropActorOrNode(const std::function<bool(TActorId actorId)>& check, const NActors::TActorContext&) {
        TVector<ui64> freeList;
        for (const auto& [k, v] : AllocatedResources) {
            if (check(v.ActorId)) {
                freeList.push_back(k);
            }
        }

        for (auto resourceId : freeList) {
            FreeResource(resourceId, {});
        }

        Scheduler->ProcessAll([&] (const auto& item) {
            return check(item.Sender);
        });

        MarkDirty();
    }

    void OnUndelivered(TEvents::TEvUndelivered::TPtr& ev, const NActors::TActorContext& ctx)
    {
        auto deadActor = ev->Sender;
        DropActorOrNode([&](TActorId actorId) {
            return actorId == deadActor;
        }, ctx);
    }

    void OnExecuterDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev, const NActors::TActorContext& ctx)
    {
        YQL_CLOG(DEBUG, ProviderDq) << "OnExecuterDisconnected " << ev->Get()->NodeId;

        Unsubscribe(ev->Get()->NodeId);

        auto deadNode = ev->Get()->NodeId;

        DropActorOrNode([&](TActorId actorId) {
            return actorId.NodeId() == deadNode;
        }, ctx);
    }

    void OnFreeWorkersStub(TEvFreeWorkersNotify::TPtr& ev, const NActors::TActorContext&) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(ev->Get()->Record.GetTraceId());
        YQL_CLOG(DEBUG, ProviderDq) << "TGlobalWorkerManager::OnFreeWorkersStub " << ev->Sender.NodeId();
    }

    void FreeResource(ui64 resourceId, const THashSet<TGUID>& failedWorkers) {
        if (!AllocatedResources.contains(resourceId)) {
            return;
        }

        auto& resource = AllocatedResources[resourceId];

        auto now = TInstant::Now();

        for (const auto& worker : resource.Workers) {
            YQL_CLOG(DEBUG, ProviderDq) << "Free worker " << GetGuidAsString(worker->WorkerId);

            if (failedWorkers.contains(worker->WorkerId)) {
                Workers.DropWorker(now, worker->WorkerId);
            } else {
                Workers.FreeWorker(now, worker);
            }
        }

        if (resource.Workers.size() == 1) {
            DecrLiteralQueries(*resource.Clusters.begin());
        }

        Workers.UpdateResourceUseTime(now - resource.StartTime, resource.UsedFiles);
        AllocatedResources.erase(resourceId);
    }

    void OnFreeWorkers(TEvFreeWorkersNotify::TPtr& ev, const TActorContext&) {
        auto resourceId = ev->Get()->Record.GetResourceId();
        if (AllocatedResources.contains(resourceId)) {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(ev->Get()->Record.GetTraceId());
            YQL_CLOG(DEBUG, ProviderDq) << "TEvFreeWorkersNotify " << resourceId;
            THashSet<TGUID> failedWorkers;
            for (const auto& workerInfo : ev->Get()->Record.GetFailedWorkerGuid()) {
                auto guid = GetGuid(workerInfo);
                YQL_CLOG(DEBUG, ProviderDq) << "Failed worker: " << GetGuidAsString(guid);
                failedWorkers.insert(guid);
            }
            FreeResource(resourceId, failedWorkers);
        } else {
            DeadOperations.insert(resourceId);
        }

        MarkDirty();
    }

    void OnJobStop(TEvJobStop::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);

        auto request = ev->Get()->Record.GetRequest();
        TString requestStr;
        TStringOutput output1(requestStr);
        SerializeToTextFormat(request, output1);

        YQL_CLOG(DEBUG, ProviderDq) << "JobStop " << requestStr;

        if (request.GetForce()) {
            TWorkerStopFilter filter(request);
            Workers.Visit([&](const TWorkerInfo::TPtr& workerInfo) {
                if (!workerInfo->IsDead && filter.Match(*workerInfo)) {
                    workerInfo->Stopping = true;
                    YQL_CLOG(DEBUG, ProviderDq) << "Force stop worker on user request " << GetGuidAsString(workerInfo->WorkerId);
                    Send(MakeWorkerManagerActorID(workerInfo->NodeId), new TEvents::TEvPoison);
                }
            });
        } else {
            StopFilters.emplace_back(request);
        }
    }

    void OnClusterStatusStub(TEvClusterStatus::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        auto response = MakeHolder<TEvClusterStatusResponse>();
        Send(ev->Sender, response.Release());
    }

    void OnClusterStatus(TEvClusterStatus::TPtr& ev, const TActorContext& ctx) {
        YQL_CLOG(DEBUG, ProviderDq) << "TEvClusterStatus ";

        Y_UNUSED(ctx);

        auto response = MakeHolder<TEvClusterStatusResponse>();

        auto* r = response->Record.MutableResponse();

        r->SetRevision(Revision);

        Workers.ClusterStatus(r);

        Scheduler->ForEach([&] (const auto& item) {
            auto* info = r->AddWaitList();
            info->SetCount(item.Request.GetCount());
            info->SetOperationId(item.Request.GetTraceId());
            info->SetUserName(item.Request.GetUser());

            THashMap<TString, Yql::DqsProto::ClusterStatusResponse::File> files;

            for (const auto& rec : item.Request.GetFiles()) {
                auto& file = files[rec.GetObjectId()];
                file.SetObjectId(rec.GetObjectId());
                file.SetName(rec.GetName());
                if (item.ResLeft.contains(rec.GetObjectId())) {
                    file.SetLeft(item.ResLeft[rec.GetObjectId()]);
                }
                file.SetSize(rec.GetSize());
            }

            for (const auto& it : item.Request.GetWorkerFilterPerTask()) {
                for (const auto& rec : it.GetFile()) {
                    auto& file = files[rec.GetObjectId()];
                    file.SetObjectId(rec.GetObjectId());
                    file.SetName(rec.GetName());
                    file.SetCount(file.GetCount()+1);
                    file.SetSize(rec.GetSize());
                }
            }

            for (const auto& [_, v] : files) {
                *info->AddResources() = v;
            }
        });

        for (const auto& i : Uploading) {
            *r->AddUploading() = i.first;
        }

        for (const auto& [guid, node] : AllCriticalFiles.GetNodes()) {
            auto* nodeInfo = r->AddServiceNode();
            nodeInfo->SetRevision(node.Revision);
            nodeInfo->SetNodeId(node.NodeId);
            nodeInfo->SetPid(node.Pid);
            nodeInfo->SetAddress(node.Address);
            nodeInfo->SetGuid(GetGuidAsString(guid));
            for (const auto& file : node.OrderedFiles) {
                *nodeInfo->AddFile() = file;
            }
        }

        Send(ev->Sender, response.Release());
    }

    void OnQueryStatusStub(TEvQueryStatus::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        auto response = MakeHolder<TEvQueryStatusResponse>();
        Send(ev->Sender, response.Release());
    }

    void OnQueryStatus(TEvQueryStatus::TPtr& ev, const TActorContext& ctx) {
        // TODO: remove me YQL-18071. Executer actor is responsible for this function
        Y_UNUSED(ctx);

        auto sessionId = ev->Get()->Record.request().GetSession();

        bool queued = false;
        ui32 workersCount;

        Scheduler->ForEach([&queued, &sessionId, &workersCount] (const NDq::IScheduler::TWaitInfo& item) {
            if (sessionId == item.Request.GetTraceId()) {
                queued = true;
                workersCount = item.Request.GetCount();
            }
        });

        auto response = MakeHolder<TEvQueryStatusResponse>();

        auto* r = response->Record.MutableResponse();

        if (queued) {
            if (workersCount > Workers.FreeSlots()) {
                r->SetStatus("Awaiting");
            } else {
                r->SetStatus("Uploading artifacts");
            }
        } else {
            r->SetStatus("Executing");
        }

        Send(ev->Sender, response.Release());
    }

    void OnIsReadyStub(TEvIsReady::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);

        auto response = MakeHolder<TEvIsReadyResponse>();
        Send(ev->Sender, response.Release());
    }

    void OnOperationStopStub(TEvOperationStop::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);

        auto response = MakeHolder<TEvOperationStopResponse>();
        Send(ev->Sender, response.Release());
    }

    void OnOperationStop(TEvOperationStop::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);

        Scheduler->Process(Workers.Capacity(), Workers.FreeSlots(), [&] (const auto& item) {
            if (item.Request.GetTraceId() == ev->Get()->Record.GetRequest().GetOperationId()) {
                Send(item.Sender, new TEvDqFailure(NYql::NDqProto::StatusIds::ABORTED, TIssue("Operation stopped by scheduler").SetCode(TIssuesIds::DQ_GATEWAY_ERROR, TSeverityIds::S_ERROR)));
                return true;
            }
            return false;
        });

        auto response = MakeHolder<TEvOperationStopResponse>();
        Send(ev->Sender, response.Release());
    }

    void FillCriticalFiles(TEvIsReady::TPtr& ev) {
        if (FollowingMode) {
            return;
        }
        if (ev->Get()->Record.GetIsForwarded()) {
            return;
        }
        LocalCriticalFiles->Clear();
        for (auto& f : ev->Get()->Record.GetRequest().GetFiles()) {
            //TODO: Add support types in LocalCriticalFiles
            //Don't uncomment this
            //if (f.GetObjectType() == TWorkerInfo::TFileResource::EEXE_FILE) {
                // LocalCriticalFiles supports only udfs
            //    continue;
            //}
            LocalCriticalFiles->AddFile("unused", f.GetObjectId());
        }
        TVector<TFileResource> files;
        LocalCriticalFiles->Walk([&](const TString& objectId) {
            TFileResource r;
            r.SetObjectId(objectId);
            files.push_back(r);
        });
        TFileResource r;
        r.SetObjectId(Revision);
        files.push_back(r);

        AllCriticalFiles.Add(TGUID(), SelfId().NodeId(), Revision, Address, Pid, files);
    }

    void OnIsReadyForward(TEvIsReady::TPtr& ev, const TActorContext& ctx) {
        auto [_, error]  = MaybeUpload(ev->Get()->Record.GetIsForwarded(), TVector<TFileResource>(ev->Get()->Record.GetRequest().GetFiles().begin(),ev->Get()->Record.GetRequest().GetFiles().end()), true);
        if (!error.empty()) {
            YQL_CLOG(WARN, ProviderDq) << "TEvIsReady error on upload: " << error;
        }

        if (FollowingMode) {
            auto response = MakeHolder<TEvIsReadyResponse>();
            response->Record.SetIsReady(true);
            Send(ev->Sender, response.Release());
        } else {
            FillCriticalFiles(ev);
            ev->Get()->Record.SetIsForwarded(true);
            ctx.Send(ev->Forward(MakeWorkerManagerActorID(LeaderId)));
        }
    }

    void OnIsReady(TEvIsReady::TPtr& ev, const TActorContext& ctx) {
        YQL_CLOG(DEBUG, ProviderDq) << "TEvIsReady ";
        Y_UNUSED(ctx);

        FillCriticalFiles(ev);

        auto [_, error]  = MaybeUpload(ev->Get()->Record.GetIsForwarded(), TVector<TFileResource>(ev->Get()->Record.GetRequest().GetFiles().begin(),ev->Get()->Record.GetRequest().GetFiles().end()), true);
        if (!error.empty()) {
            YQL_CLOG(WARN, ProviderDq) << "TEvIsReady error on upload: " << error;
        }
        THashMap<TString , std::pair<ui32, ui32>> clusterMap;
        for (const auto& options : ResourceUploaderOptions) {
            clusterMap.emplace(options.YtBackend.GetClusterName(), std::make_pair(options.YtBackend.GetMaxJobs(), 0));
        }

        auto resources = ev->Get()->Record.GetRequest().GetFiles();
        Workers.IsReady(TVector<TFileResource>(resources.begin(), resources.end()), clusterMap);

        // any cluster has at least 50% of workers with actual vanilla job ready
        bool isReady = false;
        for (const auto& [cluster,pair] : clusterMap) {
            YQL_CLOG(DEBUG, ProviderDq) << cluster << " ready: " << pair.second << "/" << pair.first << " workers ready";
            isReady |= pair.second * 2 >= pair.first;
        }

        YQL_CLOG(DEBUG, ProviderDq) << "IsReady status : " << isReady;

        auto response = MakeHolder<TEvIsReadyResponse>();
        response->Record.SetIsReady(isReady);

        Send(ev->Sender, response.Release());
    }

    void OnGetMasterStub(TEvGetMasterRequest::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        auto response = MakeHolder<TEvGetMasterResponse>();
        Send(ev->Sender, response.Release());
    }

    void OnGetMaster(TEvGetMasterRequest::TPtr& ev, const TActorContext& ctx) {
        YQL_CLOG(DEBUG, ProviderDq) << "TEvGetMasterRequest ";

        Y_UNUSED(ctx);

        auto response = MakeHolder<TEvGetMasterResponse>();

        auto* r = response->Record.MutableResponse();

        r->SetHost(LeaderHost);
        r->SetPort(LeaderPort);

        Send(ev->Sender, response.Release());
    }

    void OnConfigureFailureInjectorStub(TEvConfigureFailureInjectorRequest::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        auto response = MakeHolder<TEvConfigureFailureInjectorResponse>();
        Send(ev->Sender, response.Release());
    }

    void OnConfigureFailureInjector(TEvConfigureFailureInjectorRequest::TPtr& ev, const TActorContext& ctx) {
        YQL_CLOG(DEBUG, ProviderDq) << "TEvConfigureFailureInjectorRequest ";

        Y_UNUSED(ctx);

        // TODO: support resending the event to all workers and gathering the results?
        auto& request = ev->Get()->Record.GetRequest();
        // just forward the event to a worker node
        if (request.GetNodeId() == SelfId().NodeId()) {
            TFailureInjector::Set(request.GetName(), request.GetSkip(), request.GetCountOfFails());

            auto response = MakeHolder<TEvConfigureFailureInjectorResponse>();
            auto* r = response->Record.MutableResponse();
            r->Setsuccess(true);
            Send(ev->Sender, response.Release());
        } else {
            ctx.Send(ev->Forward(MakeWorkerManagerActorID(request.GetNodeId())));
        }
    }

    void UpdateMetrics() {
        if (!WaitListSize) {
            WaitListSize = Metrics->GetSubgroup("component", "lists")->GetCounter("WaitListSize");
        }
        *WaitListSize = Scheduler->UpdateMetrics();
        Workers.UpdateMetrics();
    }

private:
    const ICoordinationHelper::TPtr Coordinator;
    const std::shared_ptr<TSingleNodeResolver> LeaderResolver;
    TActorId LeaderPinger;
    TActorId LockId;

    TSensorsGroupPtr Metrics;
    TSensorsGroupPtr UploaderMetrics;
    THistogramPtr LatencyHistogram;
    THashMap<TString,NMonitoring::TDynamicCounters::TCounterPtr> LiteralQueries;
    TWorkersStorage Workers;

    struct TResourceInfo {
        TActorId ActorId;
        TVector<TWorkerInfo::TPtr> Workers;
        THashSet<TString> UsedFiles;
        THashSet<TString> Clusters;
        TInstant StartTime;
    };
    THashMap<ui64, TResourceInfo> AllocatedResources;
    THashSet<ui64> DeadOperations;

    ui32 LeaderId = static_cast<ui32>(-1);
    TString LeaderRevision = "";
    TString LeaderHost = "";
    ui32 LeaderPort = 0u;

    ui32 LeaderEpoch;
    TDqResourceId CurrentResourceId;

    const NDq::IScheduler::TPtr Scheduler;
    const TString Revision;

    THashMap<TActorId, TVector<TString>> UploadProcesses; // actorId -> objects

    struct TUploadingInfo {
        EFileType ObjectType;
        TString ObjectName;
        int Clusters;
        i64 Size;
    };
    THashMap<TString, TUploadingInfo> Uploading; // objectId -> objectName

    TVector<TResourceManagerOptions> ResourceUploaderOptions;

    TList<TWorkerStopFilter> StopFilters;

    TDynamicCounters::TCounterPtr WaitListSize;

    // filled in IsReady
    IFileCache::TPtr LocalCriticalFiles = MakeIntrusive<TLocalCriticalFiles>();
    // filled in IsReady for local files
    // filled in OnRegister for other files
    TAllCriticalFiles AllCriticalFiles;

    THashMap<TString, TInstant> LastUploadCache;

    // Don't reschedule too frequently to avoid GWM hanging
    TDuration ScheduleInterval = TDuration::MilliSeconds(100);
    size_t ScheduleWaitCount = std::numeric_limits<size_t>::max(); // max - no, 0 - any, > 0 count
    TInstant LastCleanTime;
    TDuration CleanInterval = TDuration::Seconds(2);
    bool FollowingMode = false;
    const TString Address = HostName();
    const ui32 Pid = GetPID();
};

NActors::IActor* CreateGlobalWorkerManager(
        const ICoordinationHelper::TPtr& coordinator,
        const TVector<TResourceManagerOptions>& resourceUploaderOptions,
        IMetricsRegistryPtr metricsRegistry,
        const NProto::TDqConfig::TScheduler& schedulerConfig) {
    return new TGlobalWorkerManager(coordinator, resourceUploaderOptions, std::move(metricsRegistry), schedulerConfig);
}

} // namespace NYql
