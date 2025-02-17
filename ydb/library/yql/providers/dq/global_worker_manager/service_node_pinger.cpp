#include "service_node_pinger.h"
#include <ydb/library/yql/providers/dq/worker_manager/interface/events.h>

#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/utils/log/log.h>

#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/guid.h>
#include <util/system/getpid.h>
#include <util/system/fs.h>

#include <ydb/library/yql/providers/dq/api/grpc/api.grpc.pb.h>
#include <ydb/library/yql/providers/dq/runtime/runtime_data.h>

#include <ydb/library/yql/providers/dq/actors/events/events.h>
#include <ydb/library/yql/providers/dq/actors/yt/resource_manager.h>
#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>
#include <ydb/library/yql/providers/dq/actors/execution_helpers.h>
#include <ydb/library/yql/providers/dq/task_runner/file_cache.h>

namespace NYql {

using namespace NActors;

struct TEvRegisterNodeResponse
    : NActors::TEventLocal<TEvRegisterNodeResponse, TDqEvents::ES_OTHER1> {
    TEvRegisterNodeResponse()
        : Error(true)
    { }

    TEvRegisterNodeResponse(const Yql::DqsProto::RegisterNodeResponse& res)
        : Error(false)
        , Response(res)
    { }

    bool Error;
    const Yql::DqsProto::RegisterNodeResponse Response;
};

class TServiceNodePinger: public TActor<TServiceNodePinger> {
public:
    static constexpr char ActorName[] = "PINGER";

    TServiceNodePinger(
        ui32 nodeId,
        const TString& address,
        ui16 port,
        const TString& role,
        const THashMap<TString, TString>& attributes,
        const IServiceNodeResolver::TPtr& resolver,
        const ICoordinationHelper::TPtr& coordinator,
        const TResourceManagerOptions& options)
        : TActor<TServiceNodePinger>(&TServiceNodePinger::Handler)
        , NodeId(nodeId)
        , Address(address)
        , Port(port)
        , Role(role)
        , Attributes(attributes)
        , Resolver(resolver)
        , Revision(coordinator->GetRevision())
        , Options(options)
        , RuntimeData(coordinator->GetRuntimeData())
        , Coordinator(coordinator)
    {
        CreateGuid(&Guid);

        RuntimeData->WorkerId = Guid;
        if (Options.AnnounceClusterName) {
            RuntimeData->ClusterName = *Options.AnnounceClusterName;
        } else {
            RuntimeData->ClusterName = Options.YtBackend.GetClusterName();
        }

        if (coordinator->GetConfig().HasHeartbeatPeriodMs()) {
            HeartbeatPeriod = TDuration::MilliSeconds(coordinator->GetConfig().GetHeartbeatPeriodMs());
        }

        YQL_CLOG(DEBUG, ProviderDq) << "Node started nodeId|role|guid " << NodeId << "|" << Role << "|" << GetGuidAsString(Guid);
    }

    ~TServiceNodePinger() {
        Resolver->Stop();
    }

    STRICT_STFUNC(Handler, {
        CFunc(TEvents::TEvBootstrap::EventType, Ping)
        HFunc(TEvInterconnect::TEvNodesInfo, OnNodesInfo)
        HFunc(TEvDownloadComplete, OnDownloadComplete)
        HFunc(TEvRegisterNodeResponse, OnRegisterNodeResponse)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
    });

private:
    TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parentId) override {
        return new IEventHandle(self, parentId, new TEvents::TEvBootstrap, 0);
    }

    void OnDownloadComplete(TEvDownloadComplete::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);

        auto id = ev->Sender;
        auto it = DownloadProcess.find(id);
        if (it == DownloadProcess.end()) {
            return;
        }

        for (auto file : it->second) {
            Downloading.erase(/*objectId = */ file.LocalFileName);
        }

        DownloadProcess.erase(it);
    }

    void OnNodesInfo(TEvInterconnect::TEvNodesInfo::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        KnownNodes.clear();
        KnownNodes.reserve(ev->Get()->Nodes.size());
        for (const auto& node: ev->Get()->Nodes) {
            KnownNodes.push_back(node.NodeId);
        }
    }

    void SchedulePing() {
        auto now = TInstant::Now();
        auto delta = now - LastPingTime;
        if (delta > HeartbeatPeriod) {
//            YQL_CLOG(DEBUG, ProviderDq) << "Ping Now";
            Send(SelfId(), new TEvents::TEvBootstrap);
        } else {
            //         YQL_CLOG(DEBUG, ProviderDq) << "Ping After " << (HeartbeatPeriod - delta).MilliSeconds();
            Schedule(HeartbeatPeriod - delta, new TEvents::TEvBootstrap);
        }
    }

    void OnRegisterNodeResponse(TEvRegisterNodeResponse::TPtr& ev, const TActorContext& ctx) {
//        YQL_CLOG(DEBUG, ProviderDq) << "Pong";

        if (ev->Get()->Error) {
            Errors += 1;
            Oks = 0;
            if (Options.ExitOnPingFail && Errors > 3) {
                YQL_CLOG(DEBUG, ProviderDq) << "ExitOnPingFail";
                _exit(-1);
            }
            SchedulePing();
            return;
        }

        Oks += 1;
        Errors = 0;

        auto& resp = ev->Get()->Response;

        if (resp.GetEpoch()) {
            RuntimeData->Epoch = resp.GetEpoch();
        }

        auto nodes = MakeIntrusive<TIntrusiveVector<TEvInterconnect::TNodeInfo>>();
        nodes->reserve(resp.GetNodes().size());
        for (auto& node : resp.GetNodes()) {
            nodes->emplace_back(
                node.GetNodeId(),
                node.GetAddress(),
                node.GetAddress(),
                node.GetAddress(),
                node.GetPort(),
                NActors::TNodeLocation());
        }
        THolder<TEvInterconnect::TEvNodesInfo> reply(new TEvInterconnect::TEvNodesInfo(nodes));

        TVector<TResourceFile> downloadList;
        for (auto& file : resp.GetDownloadList()) {
            if (Downloading.contains(file.GetObjectId()) || (Options.FileCache && Options.FileCache->Contains(file.GetObjectId())))
            {
                continue;
            }
            TResourceFile resource;

            switch (file.GetObjectType()) {
                case Yql::DqsProto::TFile::EEXE_FILE:
                    resource.RemoteFileName = "bin/" + file.GetObjectId() + "/" + file.GetName();
                    break;
                default:
                    resource.RemoteFileName = "udfs/" + file.GetObjectId();
                    break;
            }

            resource.LocalFileName = file.GetObjectId();

            downloadList.push_back(resource);

            YQL_CLOG(DEBUG, ProviderDq) << "Start downloading: " << file.GetObjectId();

            Downloading.insert(file.GetObjectId());
        }

        if (!downloadList.empty() && Options.FileCache) {
            auto rmOptions = Options;
            rmOptions.Files = downloadList;
            rmOptions.UploadPrefix = rmOptions.YtBackend.GetUploadPrefix();

            YQL_CLOG(DEBUG, ProviderDq) << "Start downloading from " << rmOptions.YtBackend.GetClusterName();
            DownloadProcess.emplace(ctx.Register(CreateResourceDownloader(rmOptions, Coordinator)), downloadList);
        }

        Send(NActors::GetNameserviceActorId(), reply.Release());
        Send(NActors::GetNameserviceActorId(), new TEvInterconnect::TEvListNodes);
        SchedulePing();
    }

    void Ping(const TActorContext& ctx) {
        Yql::DqsProto::RegisterNodeRequest req;
        req.SetCapabilities(Yql::DqsProto::RegisterNodeRequest::ECAP_RUNEXE | Options.Capabilities);
        req.SetNodeId(NodeId);
        req.SetPort(Port);
        req.SetRole(Role);
        req.SetAddress(Address);
        req.SetRevision(Revision);
        req.SetPid(Pid);
        auto runningWorkers = RuntimeData->GetRunningWorkers();
        int sum = 0;
        for (const auto& [id, count] : runningWorkers) {
            req.AddRunningOperation(id);
            sum += count;
        }
        req.SetRunningWorkers(sum);
        req.SetClusterName(RuntimeData->ClusterName);
        req.SetStartTime(StartTime);
        if (RuntimeData->Epoch) {
            req.SetEpoch(RuntimeData->Epoch);
        }
        auto rusageFull = RuntimeData->GetRusage();
        req.MutableRusage()->SetStime(rusageFull.Stime.MicroSeconds());
        req.MutableRusage()->SetUtime(rusageFull.Utime.MicroSeconds());
        req.MutableRusage()->SetMajorPageFaults(rusageFull.MajorPageFaults);

        //if (Options.MetricsRegistry) {
        //    auto* metrics = req.MutableMetricsRegistry();
        //    metrics->SetDontIncrement(true); // don't invalidate metrics
        //    Options.MetricsRegistry->TakeSnapshot(metrics);
        //}

        if (Options.FileCache) {
            req.SetUsedDiskSize(Options.FileCache->UsedDiskSize());
            req.SetFreeDiskSize(Options.FileCache->FreeDiskSize());
        }
        for (const auto& [k, v] : Attributes) {
            auto* attr = req.AddAttribute();
            attr->SetKey(k);
            attr->SetValue(v);
        }

        NDqs::NExecutionHelpers::GuidToProto(*req.MutableGuid(), Guid);
        req.SetCapacity(Options.YtBackend.GetWorkerCapacity());

        if (Options.FileCache) {
            Options.FileCache->Walk([&] (const TString& objectId) {
                req.AddFilesOnNode()->SetObjectId(objectId);
            });
        }

        // self exe must be at the end of list
        req.AddFilesOnNode()->SetObjectId(Revision);

        for (auto node : KnownNodes) {
            req.AddKnownNodes(node);
        }

        auto* actorSystem = ctx.ExecutorThread.ActorSystem;
        auto selfId = SelfId();

        Resolver->GetConnection()
            .Apply([actorSystem, selfId, req, maybeResolver=std::weak_ptr<IServiceNodeResolver>(Resolver), timeout=HeartbeatPeriod, lastPingTime=LastPingTime, errors=Errors, oks=Oks] (const NThreading::TFuture<IServiceNodeResolver::TConnectionResult>& resultFuture) {
                const auto& result = resultFuture.GetValueSync();
                if (!result.Success()) {
                    YQL_CLOG(DEBUG, ProviderDq) << "Cannot resolve service node";
                    actorSystem->Send(selfId, new TEvRegisterNodeResponse());
                    return;
                }

                if (!maybeResolver.lock()) {
                    return;
                }

                if (!lastPingTime || errors > 1 || oks < 2 || !result.NodeId) {
                    // GRPC ping
                    NYdbGrpc::TCallMeta meta;
                    meta.Timeout = timeout;
                    result.Connection->DoRequest<Yql::DqsProto::RegisterNodeRequest, Yql::DqsProto::RegisterNodeResponse>(
                        req, [=] (NYdbGrpc::TGrpcStatus&& status, Yql::DqsProto::RegisterNodeResponse&& resp) {
                            if (!status.Ok()) {
                                YQL_CLOG(DEBUG, ProviderDq) << "Error on service node ping " << status.Msg;
                                if (auto resolver = maybeResolver.lock()) {
                                    resolver->InvalidateCache();
                                }
                                Y_ABORT_UNLESS(status.GRpcStatusCode != grpc::INVALID_ARGUMENT);
                                actorSystem->Send(selfId, new TEvRegisterNodeResponse());
                                return;
                            }

                            actorSystem->Send(selfId, new TEvRegisterNodeResponse(resp));
                        }, &Yql::DqsProto::DqService::Stub::AsyncRegisterNode, meta, result.GRpcContext.get());
                } else {
                    // IC Ping
                    YQL_CLOG(DEBUG, ProviderDq) << "IC Ping " << result.NodeId;
                    auto ev = MakeHolder<NDqs::TEvRegisterNode>(req);
                    using ResultEv = NDqs::TEvRegisterNodeResponse;

                    // Handle errors and timeouts
                    auto callback = MakeHolder<TRichActorFutureCallback<ResultEv>>(
                        [actorSystem, selfId] (TAutoPtr<TEventHandle<ResultEv>>& event) mutable {
                            actorSystem->Send(selfId, new TEvRegisterNodeResponse(event->Get()->Record.GetResponse()));
                        },
                        [actorSystem, selfId, maybeResolver] () mutable {
                            YQL_CLOG(DEBUG, ProviderDq) << "Error on service node ping";
                            if (auto resolver = maybeResolver.lock()) {
                                resolver->InvalidateCache();
                            }
                            actorSystem->Send(selfId, new TEvRegisterNodeResponse());
                        },
                        timeout);

                    TActorId callbackId = actorSystem->Register(callback.Release());

                    actorSystem->Send(new IEventHandle(
                        NDqs::MakeWorkerManagerActorID(result.NodeId),
                        callbackId,
                        ev.Release(),
                        IEventHandle::FlagTrackDelivery));
                }
            });

        LastPingTime = TInstant::Now();

        CheckFs();
    }

    void CheckFs() {
        if (Options.DieOnFileAbsence.empty()) {
            return;
        }

        auto now = TInstant::Now();

        if (now - LastCheckTime < TDuration::Seconds(10)) {
            return;
        }

        Y_ABORT_UNLESS(NFs::Exists(Options.DieOnFileAbsence));

        LastCheckTime = now;
    }

private:
    const ui32 NodeId;
    const TString Address;
    const ui16 Port;
    const TString Role;
    const THashMap<TString, TString> Attributes;
    const IServiceNodeResolver::TPtr Resolver;
    TVector<ui32> KnownNodes;
    const TString Revision;
    const TString StartTime = ToString(TInstant::Now());
    const TProcessId Pid = GetPID();

    TResourceManagerOptions Options;
    TWorkerRuntimeData* RuntimeData;
    const ICoordinationHelper::TPtr Coordinator;

    THashSet<TString> Downloading;
    THashMap<TActorId, TVector<TResourceFile>> DownloadProcess;

    TDuration HeartbeatPeriod = TDuration::Seconds(5);

    TGUID Guid;
    TInstant LastPingTime;
    TInstant LastCheckTime;
    int Errors = 0;
    int Oks = 0;
};

IActor* CreateServiceNodePinger(
    ui32 nodeId,
    const TString& address,
    ui16 port,
    const TString& role,
    const THashMap<TString, TString>& attributes,
    const IServiceNodeResolver::TPtr& ptr,
    const ICoordinationHelper::TPtr& coordinator,
    const TResourceManagerOptions& rmOptions)
{
    return new TServiceNodePinger(nodeId, address, port, role, attributes, ptr, coordinator, rmOptions);
}

} // namespace NYql
