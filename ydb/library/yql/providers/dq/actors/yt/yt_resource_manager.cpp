#include "yt_wrapper.h"

#include <util/thread/pool.h>
#include <util/generic/size_literals.h>
#include <util/string/strip.h>
#include <util/system/env.h>

#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/utils/log/log.h>

#include <ydb/library/yql/providers/dq/common/attrs.h>
#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>
#include <ydb/library/yql/providers/dq/actors/events/events.h>
#include <ydb/library/yql/providers/dq/actors/yt/resource_manager.h>
#include <ydb/library/yql/providers/dq/global_worker_manager/coordination_helper.h>

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/yson/node/node_io.h>

#include <yt/cpp/mapreduce/interface/fluent.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NYql {

#define RM_LOG(A) YQL_CLOG(A, ProviderDq) << ClusterName << ": "

    namespace NCommonJobVars {
        const TString ACTOR_PORT("ACTOR_PORT");
        const TString ACTOR_NODE_ID("ACTOR_NODE_ID");
        const TString UDFS_PATH("UDFS_PATH");
        const TString OPERATION_SIZE("OPERATION_SIZE");
        const TString YT_COORDINATOR("YT_COORDINATOR");
        const TString YT_BACKEND("YT_BACKEND");
        const TString YT_FORCE_IPV4("YT_FORCE_IPV4");
    }

    using namespace NActors;

    struct TEvDropOperation
        : NActors::TEventLocal<TEvDropOperation, TDqEvents::ES_OTHER1> {
        TEvDropOperation() = default;
        TEvDropOperation(const TString& operationId, const TString& mutationId)
            : OperationId(operationId)
            , MutationId(mutationId)
        { }

        TString OperationId;
        TString MutationId;
    };

    class TYtVanillaOperation: public TActor<TYtVanillaOperation> {
    public:
        static constexpr char ActorName[] = "YT_OPERATION";

        TYtVanillaOperation(const TString& clusterName, TActorId ytWrapper, TActorId parentId, TString operationId, TString mutationId, TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
            : TActor<TYtVanillaOperation>(&TYtVanillaOperation::Handler)
            , ClusterName(clusterName)
            , YtWrapper(ytWrapper)
            , ParentId(parentId)
            , OperationId(NYT::NScheduler::TOperationId(NYT::TGuid::FromString(operationId)))
            , MutationId(mutationId)
            , Counters(counters)
        { }

        ~TYtVanillaOperation()
        {
            auto counters = Counters->GetSubgroup("operation", "brief_progress");
            for (const auto& [k, v] : Status) {
                *counters->GetCounter(k) += -v;
            }
        }

    private:
        STRICT_STFUNC(Handler, {
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            HFunc(TEvGetOperationResponse, OnGetOperationResponse);
        })

        TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& /*parentId*/) override {
            return new IEventHandle(YtWrapper, self, new TEvGetOperation(OperationId, NYT::NApi::TGetOperationOptions()), 0);
        }

        void OnGetOperationResponse(TEvGetOperationResponse::TPtr& ev, const NActors::TActorContext& ) {
            auto result = std::get<0>(*ev->Get());
            bool stopWatcher = false;
            if (!result.IsOK() && result.FindMatching(NYT::NYTree::EErrorCode::ResolveError)) {
                stopWatcher = true;
            }

            if (result.IsOK()) {
                auto attributesMap = NYT::NodeFromYsonString(result.Value()).AsMap();

                try {
                    if (attributesMap.contains("result")) {
                        RM_LOG(DEBUG) << "Result " << NYT::NodeToYsonString(attributesMap["result"]);
                        stopWatcher = true;
                    }

                    if (attributesMap.contains("brief_progress")) {
                        auto statusMap = attributesMap["brief_progress"].AsMap()["jobs"].AsMap();

                        auto counters = Counters->GetSubgroup("operation", "brief_progress");
                        for (const auto& [k, v] : statusMap) {
                            auto& oldStatus = Status[k];
                            auto newStatus = v.AsInt64();
                            *counters->GetCounter(k) += newStatus - oldStatus;
                            oldStatus = newStatus;
                        }
                    }

                } catch (...) {
                    RM_LOG(DEBUG) << CurrentExceptionMessage();
                }
            }

            if (stopWatcher) {
                RM_LOG(DEBUG) << "Stop watching operation (1) " << ToString(OperationId) << " " << ToString(result);
                Send(YtWrapper, new TEvPrintJobStderr(OperationId));
                Send(ParentId, new TEvDropOperation(ToString(OperationId), MutationId));
                PassAway();
            } else {
                TimerCookieHolder.Reset(NActors::ISchedulerCookie::Make2Way());
                TActivationContext::Schedule(TDuration::Seconds(5),
                    new IEventHandle(YtWrapper, SelfId(), new TEvGetOperation(OperationId, NYT::NApi::TGetOperationOptions()), 0),
                    TimerCookieHolder.Get());
            }
        }

        const TString ClusterName;
        const TActorId YtWrapper;
        const TActorId ParentId;
        const NYT::NScheduler::TOperationId OperationId;
        const TString MutationId;
        NActors::TSchedulerCookieHolder TimerCookieHolder;
        TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
        THashMap<TString, i64> Status;
    };

    class TNodeIdAllocator {
    public:
        TNodeIdAllocator(ui32 minNodeId, ui32 maxNodeId)
            : MinNodeId(minNodeId)
            , MaxNodeId(maxNodeId)
        { }

        void Allocate(const TVector<ui32>& res) {
            // TODO: check duplicates?
            for (ui32 id : res) {
                Allocated.insert(id);
            }
        }

        void Allocate(TVector<ui32>* res, int count) {
            res->reserve(count);
            for (ui32 id = MinNodeId; id < MaxNodeId && static_cast<int>(res->size()) < count; id = id + 1) {
                if (!Allocated.contains(id)) {
                    res->push_back(id);
                    Allocated.insert(id);
                }
            }
        }

        void Deallocate(const TVector<ui32>& nodes) {
            for (auto id : nodes) {
                Allocated.erase(id);
            }
        }

        void Clear() {
            Allocated.clear();
        }

    private:
        THashSet<ui32> Allocated;
        ui32 MinNodeId;
        ui32 MaxNodeId;
    };

    class TYtResourceManager: public TRichActor<TYtResourceManager> {
    public:
        static constexpr char ActorName[] = "YTRM";

        TYtResourceManager(
            const TResourceManagerOptions& options,
            const ICoordinationHelper::TPtr& coordinator)
            : TRichActor<TYtResourceManager>(&TYtResourceManager::Follower)
            , Options(options)
            , Counters(Options.Counters)
            , ClusterName(Options.YtBackend.GetClusterName())
            , Coordinator(coordinator)
            , CoordinatorConfig(Coordinator->GetConfig())
            , CoordinatorWrapper(Coordinator->GetWrapper())
            , NodeIdAllocator(Options.YtBackend.GetMinNodeId(), Options.YtBackend.GetMaxNodeId())
        {
        }

    private:
        // States: Follower <-> (ListOperations -> Leader)

        void StartFollower(TEvBecomeFollower::TPtr& ev, const TActorContext& ctx) {
            Y_UNUSED(ctx);

            auto leaderAttributes = NYT::NodeFromYsonString(ev->Get()->Attributes).AsMap();
            LeaderTransactionId = NYT::NObjectClient::TTransactionId();

            RM_LOG(TRACE) << " Following leader: " << leaderAttributes.at(NCommonAttrs::ACTOR_NODEID_ATTR).AsUint64();
            for (const auto& [k, v] : RunningOperations) {
                UnregisterChild(v.ActorId);
            }
            RunningOperations.clear();
            NodeIdAllocator.Clear();
            Become(&TYtResourceManager::Follower);
        }

        void StartLeader(TEvBecomeLeader::TPtr& ev, const TActorContext& ctx) {
            Y_UNUSED(ctx);
            RM_LOG(INFO) << "Become leader, epoch=" << ev->Get()->LeaderEpoch;

            LeaderTransactionId = NYT::NObjectClient::TTransactionId::FromString(ev->Get()->LeaderTransaction);

            ListOperations();
            Become(&TYtResourceManager::ListOperationsState);
        }

        STRICT_STFUNC(Follower, {
            HFunc(TEvBecomeFollower, StartFollower)
            HFunc(TEvBecomeLeader, StartLeader)
            cFunc(TEvents::TEvPoison::EventType, PassAway)
            CFunc(TEvents::TEvBootstrap::EventType, Bootstrap)

            IgnoreFunc(TEvTick)
            IgnoreFunc(TEvDropOperation)
            IgnoreFunc(TEvStartOperationResponse)
            IgnoreFunc(TEvCreateNodeResponse)
            IgnoreFunc(TEvSetNodeResponse)
        })

        STRICT_STFUNC(ListOperationsState, {
            HFunc(TEvBecomeFollower, StartFollower)
            HFunc(TEvDropOperation, OnDropOperation)
            HFunc(TEvListNodeResponse, OnListOperations)
            HFunc(TEvStartOperationResponse, OnStartOperationResponse)
            cFunc(TEvTick::EventType, ListOperations)
            cFunc(TEvents::TEvPoison::EventType, PassAway)
            IgnoreFunc(TEvCreateNodeResponse)
            IgnoreFunc(TEvSetNodeResponse)
        })

        STRICT_STFUNC(Leader, {
            HFunc(TEvBecomeFollower, StartFollower)
            HFunc(TEvDropOperation, OnDropOperation)
            HFunc(TEvListNodeResponse, OnListResponse)
            HFunc(TEvStartOperationResponse, OnStartOperationResponse)
            cFunc(TEvents::TEvPoison::EventType, PassAway)
            cFunc(TEvTick::EventType, [this]() {
                ListWorkers();
                Tick();
            })
            HFunc(TEvCreateNodeResponse, OnCreateNode)
            IgnoreFunc(TEvRemoveNodeResponse)
            IgnoreFunc(TEvSetNodeResponse)
        })

        TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parentId) override {
            return new IEventHandle(self, parentId, new TEvents::TEvBootstrap, 0);
        }

        void Bootstrap(const NActors::TActorContext& ctx) {
            YtWrapper = Coordinator->GetWrapper(
                ctx.ActorSystem(),
                Options.YtBackend.GetClusterName(),
                Options.YtBackend.GetUser(),
                Options.YtBackend.GetToken());
            RegisterChild(Coordinator->CreateLockOnCluster(YtWrapper, Options.YtBackend.GetPrefix(), Options.LockName, false));
        }

        void Tick() {
            TimerCookieHolder.Reset(NActors::ISchedulerCookie::Make2Way());
            Schedule(TDuration::Seconds(5), new TEvTick(), TimerCookieHolder.Get());
        }

        void CreateCoreTable(ui32 tableNumber)
        {
            NYT::NApi::TCreateNodeOptions options;
            options.Recursive = true;
            options.IgnoreExisting = true;

            YQL_CLOG(DEBUG, ProviderDq) << "Creating core table: " << Options.UploadPrefix + "/CoreTable-" + ToString(tableNumber);

            Send(YtWrapper, new TEvCreateNode(
                static_cast<ui64>(-1),
                Options.UploadPrefix + "/CoreTable-" + ToString(tableNumber),
                NYT::NObjectClient::EObjectType::Table,
                options));

            YQL_CLOG(DEBUG, ProviderDq) << "Creating stderr table: " << Options.UploadPrefix + "/StderrTable-" + ToString(tableNumber);

            Send(YtWrapper, new TEvCreateNode(
                static_cast<ui64>(-1),
                Options.UploadPrefix + "/StderrTable-" + ToString(tableNumber),
                NYT::NObjectClient::EObjectType::Table,
                options));
        }

        void StartOperationWatcher(const TString& operationId, const TString& mutationId, const NActors::TActorContext& ctx)
        {
            Y_UNUSED(ctx);
            RM_LOG(DEBUG) << "StartOperationWatcher " << operationId << "|" << mutationId;
            auto operation = RunningOperations.find(mutationId);
            Y_ABORT_UNLESS(operation != RunningOperations.end());
            auto actorId = RegisterChild(new TYtVanillaOperation(ClusterName, YtWrapper, SelfId(), operationId, mutationId, Counters));
            operation->second.ActorId = actorId;
        }

        void MaybeStartOperations(const NActors::TActorContext& ctx)
        {
            // to avoid races do nothing if there is PendingStartOperationRequests
            if (!PendingStartOperationRequests.empty()) {
                RM_LOG(DEBUG) << "PendingStartOperationRequests contains " << PendingStartOperationRequests.size() << " requests ";
                for (const auto& [k, _]: PendingStartOperationRequests) {
                    RM_LOG(DEBUG) << "RequestId " << k;
                }
                return;
            }

            int totalJobs = 0;
            for (const auto& [k, v] : RunningOperations) {
                totalJobs += v.Nodes.size();
                RM_LOG(DEBUG) << "Operation: " << k << " " << v.Nodes.size() << " ";
            }

            RM_LOG(DEBUG) << "Running/Max jobs: " << totalJobs << "/" << Options.YtBackend.GetMaxJobs();

            int needToStart = Options.YtBackend.GetMaxJobs() - totalJobs;
            RM_LOG(DEBUG) << "Need to start: " << needToStart;
            if (needToStart > 0) {
                StartOperations(needToStart, ctx);
            }
        }

        void DropRunningOperation(const TString& mutationId, const TVector<ui32>& preserve = {}) {
            if (RunningOperations.contains(mutationId)) {
                NodeIdAllocator.Deallocate(RunningOperations[mutationId].Nodes);
            }

            RunningOperations.erase(mutationId);

            if (!preserve.empty()) {
                RM_LOG(DEBUG) << "Operation in unknown state, preserve mutation " << mutationId;
                MutationsCache[mutationId] = preserve;
                NodeIdAllocator.Allocate(preserve);
            } else {
                auto removePath = Options.YtBackend.GetPrefix() + "/operations/" + ClusterName + "/" + mutationId;
                RM_LOG(DEBUG) << "Removing node " << removePath;
                NYT::NApi::TRemoveNodeOptions removeNodeOptions;
                removeNodeOptions.PrerequisiteTransactionIds.push_back(LeaderTransactionId);
                Send(YtWrapper, new TEvRemoveNode(removePath, removeNodeOptions));
            }
        }

        void OnDropOperation(TEvDropOperation::TPtr& ev, const NActors::TActorContext& ctx) {
            Y_UNUSED(ctx);
            auto operationId = ev->Get()->OperationId;
            auto mutationId = ev->Get()->MutationId;
            auto maybeOperation = RunningOperations.find(mutationId);
            if (maybeOperation != RunningOperations.end()) {
                UnregisterChild(maybeOperation->second.ActorId);

                RM_LOG(DEBUG) << "Stop operation " << operationId << "|" << mutationId;
                DropRunningOperation(mutationId);
            } else {
                RM_LOG(WARN) << "Unknown operation " << operationId << "|" << mutationId;
            }
        }

        void OnListResponse(TEvListNodeResponse::TPtr& ev, const NActors::TActorContext& ctx) {
            auto result = std::get<0>(*ev->Get());

            try {
                MaybeStartOperations(ctx);
            } catch (...) {
                RM_LOG(ERROR) << "Error on list node " << CurrentExceptionMessage();
            }
        }

        void OnListOperations(TEvListNodeResponse::TPtr& ev, const TActorContext& ctx)
        {
            auto result = std::get<0>(*ev->Get());

            if (!result.IsOK()) {
                if (result.FindMatching(NYT::NYTree::EErrorCode::ResolveError)) {
                    Become(&TYtResourceManager::Leader);
                } else {
                    RM_LOG(ERROR) << "Error on list node " << ToString(result);
                }
                Tick();
                return;
            }

            RM_LOG(DEBUG) << "OnListOperations ";
            auto nodes = NYT::NodeFromYsonString(result.Value()).AsList();

            for (auto node : nodes)
            {
                const auto& attributes = node.GetAttributes().AsMap();

                RM_LOG(DEBUG) << "Check " << node.AsString();

                auto mutationId = NYT::TGuid::FromString(attributes.find("yql_mutation_id")->second.AsString());
                auto command = attributes.find("yql_command")->second.AsString();
                auto file_paths = attributes.find("yql_file_paths")->second;
                auto maybeNodes = attributes.find(NCommonAttrs::ACTOR_NODEID_ATTR);

                if (maybeNodes == attributes.end()) {
                    // unsupported
                    DropRunningOperation(ToString(mutationId));
                    continue;
                }

                TVector<ui32> nodes;

                for (auto node : maybeNodes->second.AsList()) {
                    nodes.push_back(node.AsUint64());
                }

                NodeIdAllocator.Allocate(nodes);

                auto maybeOperationId = attributes.find(NCommonAttrs::OPERATIONID_ATTR);
                if (maybeOperationId == attributes.end()) {
                    RM_LOG(DEBUG) << "Start or attach to " << ToString(mutationId);
                    StartOrAttachOperation(mutationId, nodes, command, file_paths);
                } else {
                    auto operationId = maybeOperationId->second.AsString();
                    RM_LOG(DEBUG) << "Attach to " << operationId << "|" << ToString(mutationId);

                    auto& status = RunningOperations[ToString(mutationId)];
                    status.MutationId = ToString(mutationId);
                    status.OperationId = operationId;
                    status.Nodes = nodes;

                    StartOperationWatcher(operationId, ToString(mutationId), ctx);
                }
            }

            if (PendingStartOperationRequests.empty() && CurrentStateFunc() != &TYtResourceManager::Leader) {
                Become(&TYtResourceManager::Leader);
                Tick();
            }
        }

        void OnStartOperationResponse(TEvStartOperationResponse::TPtr& ev, const NActors::TActorContext& ctx) {
            auto result = std::get<0>(*ev->Get());
            auto requestId = ev->Get()->RequestId;

            auto maybeJobs = PendingStartOperationRequests.find(requestId);

            Y_ABORT_UNLESS(maybeJobs != PendingStartOperationRequests.end());

            auto mutationId = maybeJobs->second.MutationId;

            if (!result.IsOK()) {
                // TODO: Check response code
                RM_LOG(WARN) << "Failed to start operation " << ToString(result);
                DropRunningOperation(mutationId, maybeJobs->second.Nodes);
            } else {
                auto operationId = ToString(result.Value());
                Y_ABORT_UNLESS(RunningOperations.contains(mutationId));

                RunningOperations[mutationId].OperationId = operationId;

                NYT::NApi::TSetNodeOptions setNodeOptions;
                setNodeOptions.PrerequisiteTransactionIds.push_back(LeaderTransactionId);

                Send(YtWrapper, new TEvSetNode(
                    Options.YtBackend.GetPrefix() + "/operations/" + ClusterName + "/" + mutationId + "/@" + NCommonAttrs::OPERATIONID_ATTR,
                    NYT::NYson::TYsonString(NYT::NodeToYsonString(NYT::TNode(operationId))),
                    setNodeOptions));
                StartOperationWatcher(operationId, mutationId, ctx);
            }

            PendingStartOperationRequests.erase(maybeJobs);

            if (PendingStartOperationRequests.empty() && CurrentStateFunc() != &TYtResourceManager::Leader) {
                Become(&TYtResourceManager::Leader);
                Tick();
            }
        }

        void ListOperations() {
            NYT::NApi::TListNodeOptions options;
            options.Attributes = {
                "yql_mutation_id",
                NCommonAttrs::OPERATIONSIZE_ATTR,
                NCommonAttrs::OPERATIONID_ATTR,
                NCommonAttrs::CLUSTERNAME_ATTR,
                NCommonAttrs::ACTOR_NODEID_ATTR,
                "yql_command",
                "yql_file_paths"
            };
            auto command = new TEvListNode(Options.YtBackend.GetPrefix() + "/operations/" + ClusterName, options);
            RM_LOG(DEBUG) << "List " << Options.YtBackend.GetPrefix() + "/operations/" + ClusterName;
            Send(YtWrapper, command);
        }

        void ListWorkers() {
            NYT::NApi::TListNodeOptions options;
            options.Attributes = {
                NCommonAttrs::ACTOR_NODEID_ATTR,
                NCommonAttrs::OPERATIONID_ATTR,
                NCommonAttrs::OPERATIONSIZE_ATTR,
                NCommonAttrs::JOBID_ATTR,
                NCommonAttrs::ROLE_ATTR,
                NCommonAttrs::CLUSTERNAME_ATTR,
                "modification_time"
            };
            options.ReadFrom = NYT::NApi::EMasterChannelKind::Cache;
            auto command = new TEvListNode(CoordinatorConfig.GetPrefix() + "/worker_node", options);
            Send(CoordinatorWrapper, command);
        }

        void StartOperations(int jobs, const NActors::TActorContext& ctx) {
            int jobsPerOperation = Options.YtBackend.HasJobsPerOperation()
                ? Options.YtBackend.GetJobsPerOperation()
                : Options.YtBackend.GetMaxJobs();

            Y_ABORT_UNLESS(jobsPerOperation > 0);

            for (int i = 0; i < jobs; i += jobsPerOperation) {
                if (jobs - i >= jobsPerOperation) {
                    StartOperation(jobsPerOperation, ctx);
                }
            }
        }

        TString GetOperationSpec(const TVector<ui32>& nodes, const TString& command, const TMaybe<NYT::TNode>& filePaths) const
        {
            int actorPort = Options.YtBackend.HasActorStartPort()
                ? Options.YtBackend.GetActorStartPort()
                : 31002;

            bool samePorts = Options.YtBackend.HasSameActorPorts()
                ? Options.YtBackend.GetSameActorPorts()
                : true;

            auto minNodeId = Options.YtBackend.GetMinNodeId();

            Y_ABORT_UNLESS(!nodes.empty());

            TString coordinatorStr;
            TStringOutput output1(coordinatorStr);
            SerializeToTextFormat(CoordinatorConfig, output1);

            TString backendStr;
            TStringOutput output2(backendStr);
            SerializeToTextFormat(Options.YtBackend, output2);
            ui32 tableNumber = *nodes.begin();
            TString fileCache = "file_cache2";

            TVector<std::pair<TString, TString>> initialFileList;
            for (const auto& fname : Options.Files) {
                initialFileList.push_back(std::make_pair(Options.UploadPrefix, fname.GetRemoteFileName()));
            }
            if (Options.YtBackend.HasEnablePorto()) {
                for (const auto& layer : Options.YtBackend.GetPortoLayer()) {
                    auto pos = layer.rfind('/');
                    auto baseName = layer.substr(0, pos);
                    auto name = layer.substr(pos+1);
                    initialFileList.push_back(std::make_pair(baseName, name));
                }
            }

            TVector<TString> operationLayersList;
            for (const auto& operationLayer : Options.YtBackend.GetOperationLayer()) {
                operationLayersList.push_back(operationLayer);
            }

            auto operationSpec = NYT::BuildYsonNodeFluently()
                .BeginMap()
                    .DoIf(Options.YtBackend.GetOwner().size() > 0, [&] (NYT::TFluentMap fluent) {
                        fluent.Item("acl").BeginList()
                            .Item()
                                .BeginMap()
                                    .Item("action").Value("allow")
                                    .Item("permissions")
                                        .BeginList()
                                            .Item().Value("read")
                                            .Item().Value("manage")
                                        .EndList()
                                    .Item("subjects")
                                        .BeginList()
                                            .DoFor(Options.YtBackend.GetOwner(), [&] (NYT::TFluentList fluent1, const TString& subject) {
                                                fluent1.Item().Value(subject);
                                            })
                                        .EndList()
                                .EndMap()
                        .EndList();
                    })
                    .Item("secure_vault")
                        .BeginMap()
                            .Item(NCommonJobVars::YT_COORDINATOR).Value(coordinatorStr)
                            .Item(NCommonJobVars::YT_BACKEND).Value(backendStr)
                            .Item(NCommonJobVars::YT_FORCE_IPV4).Value(Options.ForceIPv4)
                            .DoFor(Options.YtBackend.GetVaultEnv(), [&] (NYT::TFluentMap fluent, const NYql::NProto::TDqConfig::TAttr& envVar) { // Добавляем env variables
                                TString tokenValue;
                                try {
                                    tokenValue = StripString(TFileInput(envVar.GetValue()).ReadLine());
                                } catch (...) {
                                    throw yexception() << "Cannot read file " << envVar.GetValue() << " Reason: " << CurrentExceptionMessage();
                                }
                                fluent.Item(envVar.GetName()).Value(tokenValue);
                            })
                        .EndMap()
                    .Item("core_table_path").Value(Options.UploadPrefix + "/CoreTable-"+ToString(tableNumber))
                    .Item("stderr_table_path").Value(Options.UploadPrefix + "/StderrTable-"+ToString(tableNumber))
                    .Item("try_avoid_duplicating_jobs").Value(true)
                    .DoIf(!Options.YtBackend.GetPool().empty(), [&] (NYT::TFluentMap fluent) {
                        fluent.Item("pool").Value(Options.YtBackend.GetPool());
                    })
                    .DoIf(Options.YtBackend.GetPoolTrees().size() > 0, [&] (NYT::TFluentMap fluent) {
                        fluent.Item("pool_trees")
                            .BeginList()
                                .DoFor(Options.YtBackend.GetPoolTrees(), [&](NYT::TFluentList fluent1, const TString& subject) {
                                    fluent1.Item().Value(subject);
                                })
                            .EndList();
                    })
                    .Item("tasks")
                        .BeginMap()
                            .DoFor(nodes, [&] (NYT::TFluentMap fluent1, const auto& nodeId) {
                                fluent1.Item("yql_worker_" + ToString(nodeId))
                                    .BeginMap()
                                        .DoIf(Options.YtBackend.GetNetworkProject().size() > 0, [&] (NYT::TFluentMap fluent) {
                                            fluent.Item("network_project").Value(Options.YtBackend.GetNetworkProject());
                                        })
                                        .DoIf(Options.YtBackend.HasEnablePorto(), [&] (NYT::TFluentMap fluent) {
                                            fluent.Item("enable_porto").Value(Options.YtBackend.GetEnablePorto());
                                        })
                                        .DoIf(Options.YtBackend.HasContainerCpuLimit(), [&] (NYT::TFluentMap fluent) {
                                            fluent.Item("set_container_cpu_limit").Value(Options.YtBackend.GetContainerCpuLimit());
                                        })
                                        .Item("command").Value(command)
                                        .DoIf(!operationLayersList.empty(), [&] (NYT::TFluentMap fluent) {
                                            fluent.Item("layer_paths").DoListFor(operationLayersList, [&] (NYT::TFluentList list, const TString& operationLayer) {
                                                list.Item().Value(operationLayer);
                                            });
                                        })
                                        .Item("environment")
                                            .BeginMap()
                                                .Item(NCommonJobVars::ACTOR_PORT).Value(ToString(
                                                    samePorts
                                                        ? actorPort
                                                        : actorPort + nodeId - minNodeId))
                                                .Item(NCommonJobVars::OPERATION_SIZE).Value(ToString(nodes.size()))
                                                .Item(NCommonJobVars::UDFS_PATH).Value(fileCache)
                                                .Item(NCommonJobVars::ACTOR_NODE_ID).Value(ToString(nodeId))
                                                .DoIf(!!GetEnv("YQL_DETERMINISTIC_MODE"), [&](NYT::TFluentMap fluent) {
                                                    fluent.Item("YQL_DETERMINISTIC_MODE").Value("1");
                                                })
                                            .EndMap()
                                        .DoIf((Options.YtBackend.GetClusterName().find("localhost") != 0) && filePaths.Empty(), [&] (NYT::TFluentMap fluent) {
                                            fluent.Item("file_paths").DoListFor(initialFileList, [&] (NYT::TFluentList list, const std::pair<TString, TString>& item) {
                                                auto baseName = item.second;
                                                list.Item()
                                                    .BeginAttributes()
                                                        .Item("executable").Value(true)
                                                        .Item("file_name").Value(baseName)
                                                    .EndAttributes()
                                                    .Value(item.first + "/" + baseName);
                                            });
                                        })
                                        .DoIf(!filePaths.Empty(), [&] (NYT::TFluentMap fluent) {
                                            fluent.Item("file_paths").Value(*filePaths);
                                        })
                                        .Item("job_count").Value(1)
                                        .DoIf(Options.YtBackend.HasMemoryLimit(), [&] (NYT::TFluentMap fluent) {
                                            fluent.Item("memory_limit").Value(Options.YtBackend.GetMemoryLimit());
                                        })
                                        .DoIf(Options.YtBackend.HasCpuLimit(), [&] (NYT::TFluentMap fluent) {
                                            fluent.Item("cpu_limit").Value(Options.YtBackend.GetCpuLimit());
                                        })
                                        .DoIf(Options.YtBackend.HasUseTmpFs() && Options.YtBackend.GetUseTmpFs(), [&] (NYT::TFluentMap fluent) {
                                            fluent.Item("tmpfs_path").Value(fileCache);
                                        })
                                        .DoIf(Options.YtBackend.HasDiskRequest(), [&] (NYT::TFluentMap fluent) {
                                            auto& diskRequest = Options.YtBackend.GetDiskRequest();
                                            fluent.Item("disk_request")
                                                .BeginMap()
                                                    .DoIf(diskRequest.HasDiskSpace(), [&] (NYT::TFluentMap fluent) { fluent.Item("disk_space").Value(diskRequest.GetDiskSpace()); } )
                                                    .DoIf(diskRequest.HasInodeCount(), [&] (NYT::TFluentMap fluent) { fluent.Item("inode_count").Value(diskRequest.GetInodeCount()); } )
                                                    .DoIf(diskRequest.HasAccount(), [&] (NYT::TFluentMap fluent) { fluent.Item("account").Value(diskRequest.GetAccount()); } )
                                                    .DoIf(diskRequest.HasMediumName(), [&] (NYT::TFluentMap fluent) { fluent.Item("medium_name").Value(diskRequest.GetMediumName()); } )
                                                .EndMap();
                                        })
                                    .EndMap();
                            })
                        .EndMap()
                .EndMap();

            return NYT::NodeToYsonString(operationSpec);
        }

        void StartOrAttachOperation(
            const NYT::TGuid& mutationId,
            const TVector<ui32>& nodes,
            const TString& command,
            const NYT::TNode& filePaths)
        {
            int jobs = static_cast<int>(nodes.size());
            RM_LOG(INFO) << "Creating " << jobs << " workers " << ToString(mutationId);

            auto operationSpec = GetOperationSpec(nodes, command, TMaybe<NYT::TNode>(filePaths));

            auto startOperationOptions = NYT::NApi::TStartOperationOptions();
            startOperationOptions.MutationId = mutationId;
            startOperationOptions.Retry = true;

            auto& state = RunningOperations[ToString(mutationId)];
            state.MutationId = ToString(mutationId);
            state.Nodes = nodes;

            RM_LOG(DEBUG) << "Attaching to operation with mutationId " << ToString(mutationId);

            CreateCoreTable(*nodes.begin());

            Send(YtWrapper, MakeHolder<TEvStartOperation>(
                YtRequestId,
                NYT::NScheduler::EOperationType::Vanilla,
                operationSpec,
                startOperationOptions).Release());

            PendingStartOperationRequests[YtRequestId++] = {nodes,
                ToString(mutationId), THolder<TEvStartOperation>()};
        }

        void StartOperation(int jobs, const NActors::TActorContext& ctx) {
            Y_UNUSED(ctx);

            RM_LOG(INFO) << "Creating " << jobs << " workers ";

            TString executableName = (Options.YtBackend.GetClusterName().find("localhost") == 0)
                ? Options.Files[0].LocalFileName
                : TString("./") + Options.Files[0].GetRemoteFileName();

            RM_LOG(INFO) << "Executable " << executableName;

            TString command = Options.YtBackend.GetVanillaJobCommand();

            RM_LOG(INFO) << "Executable " << command;

            TVector<ui32> nodes;

            auto startOperationOptions = NYT::NApi::TStartOperationOptions();

            if (MutationsCache.empty()) {
                startOperationOptions.MutationId = startOperationOptions.GetOrGenerateMutationId();
                NodeIdAllocator.Allocate(&nodes, jobs);
            } else {
                startOperationOptions.MutationId = NYT::TGuid::FromString(MutationsCache.begin()->first);
                nodes = MutationsCache.begin()->second;
                NodeIdAllocator.Allocate(nodes);
                RM_LOG(INFO) << "Get mutation from cache " << ToString(startOperationOptions.MutationId) << "," << nodes.size();
                MutationsCache.erase(MutationsCache.begin());
            }

            if (nodes.empty()) {
                RM_LOG(WARN) << "Cannot allocate node ids for " << jobs << " jobs";
                return;
            }

            startOperationOptions.Retry = true;

            auto operationSpec = GetOperationSpec(nodes, command, TMaybe<NYT::TNode>());

            auto mutationId = startOperationOptions.MutationId;

            RM_LOG(DEBUG) << "Start operation with mutationId " << ToString(mutationId) ;

            auto& state = RunningOperations[ToString(mutationId)];
            state.MutationId = ToString(mutationId);
            state.Nodes = nodes;

            NYT::NApi::TCreateNodeOptions createOptions;
            createOptions.IgnoreExisting = true;
            createOptions.Recursive = true;

            RM_LOG(DEBUG) << "Creating operation with mutationId " << ToString(mutationId);

            auto filesAttribute = Options.Files;
            if (Options.YtBackend.GetClusterName().find("localhost") == 0) {
                filesAttribute.clear();
            }

            auto attributes = NYT::BuildYsonNodeFluently()
                .BeginMap()
                    .Item("yql_mutation_id").Value(ToString(mutationId))
                    .Item(NCommonAttrs::OPERATIONSIZE_ATTR).Value(jobs)
                    .Item("yql_command").Value(command)
                    .Item("yql_file_paths")
                        .DoListFor(filesAttribute, [&] (NYT::TFluentList list, const TResourceFile& item) {
                            auto baseName = item.GetRemoteFileName();
                            list.Item()
                                .BeginAttributes()
                                    .Item("executable").Value(true)
                                    .Item("file_name").Value(baseName)
                                .EndAttributes()
                                .Value(Options.UploadPrefix + "/" + baseName);
                        })
                    .Item(NCommonAttrs::ROLE_ATTR).Value("worker_node")
                    .Item(NCommonAttrs::ACTOR_NODEID_ATTR)
                        .BeginList()
                            .DoFor(nodes, [&] (NYT::TFluentList fluent1, const auto& nodeId) {
                                fluent1.Item().Value(nodeId);
                            })
                        .EndList()
                    .Item(NCommonAttrs::CLUSTERNAME_ATTR).Value(ClusterName)
                .EndMap();

            createOptions.Attributes = NYT::NYTree::IAttributeDictionary::FromMap(
                NYT::NYTree::ConvertToNode(NYT::NYson::TYsonString(NYT::NodeToYsonString(attributes)))->AsMap()
            );

            createOptions.PrerequisiteTransactionIds.push_back(LeaderTransactionId);

            CreateCoreTable(*nodes.begin());

            Send(YtWrapper, new TEvCreateNode(
                YtRequestId,
                Options.YtBackend.GetPrefix() + "/operations/" + ClusterName + "/" + ToString(mutationId),
                NYT::NObjectClient::EObjectType::StringNode,
                createOptions));

            PendingStartOperationRequests[YtRequestId] = {
                nodes,
                ToString(mutationId),
                MakeHolder<TEvStartOperation>(
                    YtRequestId+1,
                    NYT::NScheduler::EOperationType::Vanilla,
                    operationSpec,
                    startOperationOptions)
            };
            YtRequestId += 2;
        }

        void OnCreateNode(TEvCreateNodeResponse::TPtr& ev, const TActorContext& ctx) {
            Y_UNUSED(ctx);
            auto requestId = ev->Get()->RequestId;
            auto result = std::get<0>(*ev->Get());
            if (requestId == static_cast<ui64>(-1)) {
                // CoreTable
                if (!result.IsOK()) {
                    YQL_CLOG(DEBUG, ProviderDq) << "Error on creating core table " << ToString(result);
                }
                return;
            }
            if (!PendingStartOperationRequests.contains(requestId)) {
                return;
            }
            auto& op = PendingStartOperationRequests[requestId];
            if (result.IsOK()) {
                Y_ABORT_UNLESS(!PendingStartOperationRequests.contains(requestId+1));
                PendingStartOperationRequests[requestId+1] = {
                    op.Nodes,
                    op.MutationId,
                    THolder<TEvStartOperation>()
                };
                Send(YtWrapper, op.Ev.Release());
            } else if (RunningOperations.contains(op.MutationId)) {
                YQL_CLOG(DEBUG, ProviderDq) << "Error on create node " << ToString(result);
                DropRunningOperation(op.MutationId);
            }
            PendingStartOperationRequests.erase(requestId);
            // retry in ListOperations
        }

        const TResourceManagerOptions Options;
        TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
        const TString ClusterName;

        const ICoordinationHelper::TPtr Coordinator;

        const NProto::TDqConfig::TYtCoordinator CoordinatorConfig;

        TActorId YtWrapper;
        const TActorId CoordinatorWrapper;

        NYT::NObjectClient::TTransactionId LeaderTransactionId;

        TNodeIdAllocator NodeIdAllocator;

        struct TOperationStatus {
            TString OperationId;
            TString MutationId;
            TActorId ActorId;
            TVector<ui32> Nodes;
        };

        // mutationId -> operation
        THashMap<TString, TOperationStatus> RunningOperations;

        THashMap<TString, TVector<ui32>> MutationsCache;

        // RequestId -> Jobs
        struct TPendingStartOperation {
            TVector<ui32> Nodes;
            TString MutationId;
            THolder<TEvStartOperation> Ev;
        };
        THashMap<ui64, TPendingStartOperation> PendingStartOperationRequests;
        ui64 YtRequestId = 1;
        NActors::TSchedulerCookieHolder TimerCookieHolder;
    };

    IActor* CreateYtResourceManager(
        const TResourceManagerOptions& options,
        const ICoordinationHelper::TPtr& coordinator)
    {
        Y_ABORT_UNLESS(!options.YtBackend.GetClusterName().empty());
        Y_ABORT_UNLESS(!options.YtBackend.GetUser().empty());
        Y_ABORT_UNLESS(options.YtBackend.HasMinNodeId());
        Y_ABORT_UNLESS(options.YtBackend.HasMaxNodeId());
        Y_ABORT_UNLESS(options.YtBackend.HasPrefix());
        Y_ABORT_UNLESS(!options.Files.empty());
        Y_ABORT_UNLESS(!options.UploadPrefix.empty());

        return new TYtResourceManager(options, coordinator);
    }
} // namespace NYql
