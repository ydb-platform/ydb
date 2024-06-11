#include "node_warden_impl.h"
#include <ydb/core/base/nameservice.h>


namespace NKikimr::NStorage {

    static constexpr TDuration GroupResolverStartTimeout = TDuration::Seconds(5); // time to wait before resolving
    static constexpr TDuration QueryTimeout = TDuration::Seconds(3); // timeout before considering unanswered
    static constexpr TDuration NodeTimeout = TDuration::Seconds(1); // timeout before next retry after receiving NodeDisconnected
    static constexpr ui32 MaxQueriesInFlight = 3;

    struct TNodeWarden::TGroupResolverContext::TImpl {
        std::unordered_set<ui32> NodeIds; // all static node ids of this cluster
        bool NodeIdsRequestPending = false;
        std::unordered_set<TActorId, THash<TActorId>> NodeIdsWaitQueue;

        struct TNodeInfo {
            std::vector<ui32> StartedGroupIds;
        };
        std::unordered_map<ui32, TNodeInfo> NodeInfo;
        std::set<std::pair<ui32, ui32>> StartedGroupIdToNodes;
    };

    TNodeWarden::TGroupResolverContext::TGroupResolverContext()
        : Impl(std::make_unique<TImpl>())
    {}

    TNodeWarden::TGroupResolverContext::~TGroupResolverContext()
    {}

    class TNodeWarden::TGroupResolverActor : public TActorBootstrapped<TGroupResolverActor> {
        struct TEvPrivate {
            enum {
                EvQueryTimeout = EventSpaceBegin(TEvents::ES_PRIVATE),
                EvNodeTimeout,
                EvNodeIdsObtained,
            };
            struct TEvQueryTimeout : TEventLocal<TEvQueryTimeout, EvQueryTimeout> {};
            struct TEvNodeTimeout : TEventLocal<TEvNodeTimeout, EvNodeTimeout> {};
        };

    private:
        struct TQueryTimeout {
            TInstant Expires;
            ui32 NodeId;
            ui64 Cookie;
        };

        struct TNodeTimeout {
            TInstant Expires;
            ui32 NodeId;
        };

    private:
        const ui32 GroupId;
        const TIntrusivePtr<TGroupResolverContext> GroupResolverContext;
        TGroupResolverContext::TImpl& Ctx;
        std::unordered_set<ui32> NodeIdsUnprocessed;
        std::unordered_map<ui32, ui64> QueriesInFlight;
        std::unordered_set<ui32> SubscribedNodes;
        std::unordered_map<ui32, NKikimrBlobStorage::TGroupInfo> GroupInfoReceived;
        std::deque<TQueryTimeout> QueryTimeoutQ;
        std::deque<TNodeTimeout> NodeTimeoutQ;
        ui64 NextCookie = 1;

    public:
        TGroupResolverActor(ui32 groupId, TIntrusivePtr<TGroupResolverContext> groupResolverContext)
            : GroupId(groupId)
            , GroupResolverContext(std::move(groupResolverContext))
            , Ctx(*GroupResolverContext->Impl)
        {}

        void Bootstrap() {
            STLOG(PRI_INFO, BS_NODE, NW79, "TGroupResolverActor::Bootstrap", (GroupId, GroupId));
            Become(&TThis::StateWaitStart, GroupResolverStartTimeout, new TEvents::TEvWakeup);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        void StartResolving() {
            STLOG(PRI_INFO, BS_NODE, NW85, "TGroupResolverActor::StartResolving", (GroupId, GroupId));
            Become(&TThis::StateFunc);

            if (!Ctx.NodeIds.empty()) { // we already have list of static nodes, so we can process immediately
                HandleNodeIdsObtained();
            } else if (Ctx.NodeIdsRequestPending) { // no nodes available and request is in flight, put us to queue
                Ctx.NodeIdsWaitQueue.emplace(SelfId());
            } else { // start new request
                Ctx.NodeIdsRequestPending = true;
                Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes);
            }
        }

        void Handle(TEvInterconnect::TEvNodesInfo::TPtr ev) {
            const auto& dyn = AppData()->DynamicNameserviceConfig;
            ui32 maxStaticNodeId = dyn ? dyn->MaxStaticNodeId : Max<ui32>();
            for (const auto& node : ev->Get()->Nodes) {
                if (node.NodeId <= maxStaticNodeId) {
                    Ctx.NodeIds.emplace(node.NodeId);
                }
            }
            for (TActorId actorId : std::exchange(Ctx.NodeIdsWaitQueue, {})) { // notify waiting actors
                TActivationContext::Send(new IEventHandle(TEvPrivate::EvNodeIdsObtained, 0, actorId, {}, {}, 0));
            }
            HandleNodeIdsObtained();
        }

        void HandleNodeIdsObtained() {
            NodeIdsUnprocessed = Ctx.NodeIds;
            IssueQueriesAndCheckIfDone();
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        void IssueQueriesAndCheckIfDone() {
            while (QueriesInFlight.size() < MaxQueriesInFlight && IssueQuery())
            {}
            if (QueriesInFlight.empty()) { // nothing more to do
                ProcessResultAndFinish();
            }
        }

        bool IssueQuery() {
            auto issueQueryToNode = [this](ui32 nodeId) {
                STLOG(PRI_DEBUG, BS_NODE, NW80, "TGroupResolverActor::IssueQuery", (GroupId, GroupId), (NodeId, nodeId));

                // issue message to target node
                Send(MakeBlobStorageNodeWardenID(nodeId), new TEvNodeWardenQueryGroupInfo(GroupId),
                    IEventHandle::FlagSubscribeOnSession, nodeId);

                // mark this node as subscribed one
                SubscribedNodes.insert(nodeId);

                // register in flight request and push it into timeout queue
                const ui64 cookie = NextCookie++;
                QueriesInFlight.emplace(nodeId, cookie);
                QueryTimeoutQ.push_back(TQueryTimeout{TActivationContext::Now() + QueryTimeout, nodeId, cookie});
                if (QueryTimeoutQ.size() == 1) {
                    Schedule(QueryTimeoutQ.front().Expires, new TEvPrivate::TEvQueryTimeout);
                }

                // remove this node from unprocessed node list
                NodeIdsUnprocessed.erase(nodeId);
            };

            // try to scan all nodes that are known to contains our group at a moment of time of near past
            auto& m = Ctx.StartedGroupIdToNodes;
            for (auto it = m.lower_bound(std::make_pair(GroupId, 0)); it != m.end() && it->first == GroupId; ++it) {
                const ui32 nodeId = it->second;
                if (NodeIdsUnprocessed.count(nodeId)) {
                    issueQueryToNode(nodeId);
                    return true;
                }
            }

            // check if we have the result and we have scanned all the nodes of the group
            if (const auto *result = GetResultingGroupInfo()) {
                std::unordered_set<ui32> needed;
                for (const auto& realm : result->GetRings()) {
                    for (const auto& domain : realm.GetFailDomains()) {
                        for (const auto& vdisk : domain.GetVDiskLocations()) {
                            needed.insert(vdisk.GetNodeID());
                        }
                    }
                }
                for (auto it = m.lower_bound(std::make_pair(GroupId, 0)); it != m.end() && it->first == GroupId; ++it) {
                    needed.erase(it->second);
                }
                if (needed.empty()) {
                    Y_DEBUG_ABORT_UNLESS(GetResultingGroupInfo());
                    return false; // information we have is quite conclusive, nothing more to scan
                }
            }

            // scan all nodes one-by-one
            for (ui32 nodeId : NodeIdsUnprocessed) {
                issueQueryToNode(nodeId);
                return true;
            }

            return false; // all nodes scanned, nothing more to do
        }

        void Handle(TEvNodeWardenGroupInfo::TPtr ev) {
            const ui32 nodeId = ev->Cookie;
            const auto& record = ev->Get()->Record;
            STLOG(PRI_DEBUG, BS_NODE, NW84, "TGroupResolverActor::TEvNodeWardenGroupInfo", (GroupId, GroupId),
                (NodeId, nodeId), (Msg, ev->Get()->ToString()));

            // remove group<->node mapping for selected node and clear started groups vector
            auto& nodeInfo = Ctx.NodeInfo[nodeId];
            for (ui32 groupId : nodeInfo.StartedGroupIds) {
                const size_t num = Ctx.StartedGroupIdToNodes.erase(std::make_pair(groupId, nodeId));
                Y_ABORT_UNLESS(num);
            }
            nodeInfo.StartedGroupIds.clear();

            // fill in new started groups vector and create appropriate map
            const auto& startedGroupIds = record.GetStartedGroupIds();
            nodeInfo.StartedGroupIds.insert(nodeInfo.StartedGroupIds.end(), startedGroupIds.begin(), startedGroupIds.end());
            bool trustworthy = false;
            for (ui32 groupId : nodeInfo.StartedGroupIds) {
                const bool inserted = Ctx.StartedGroupIdToNodes.emplace(groupId, nodeId).second;
                Y_ABORT_UNLESS(inserted);
                trustworthy |= groupId == GroupId; // the group we are looking for
            }

            if (trustworthy && record.HasGroup()) { // remember group info
                GroupInfoReceived.emplace(nodeId, record.GetGroup());
            }

            QueriesInFlight.erase(nodeId);
            IssueQueriesAndCheckIfDone();
        }

        void HandleQueryTimeout() {
            const TInstant now = TActivationContext::Now();
            std::deque<TQueryTimeout>::iterator it;
            for (it = QueryTimeoutQ.begin(); it != QueryTimeoutQ.end() && it->Expires <= now; ++it) {
                if (const auto inflightIt = QueriesInFlight.find(it->NodeId); inflightIt != QueriesInFlight.end() &&
                        inflightIt->second == it->Cookie) {
                    QueriesInFlight.erase(inflightIt);
                }
            }
            QueryTimeoutQ.erase(QueryTimeoutQ.begin(), it);
            if (!QueryTimeoutQ.empty()) {
                Schedule(QueryTimeoutQ.front().Expires, new TEvPrivate::TEvQueryTimeout);
            }
            IssueQueriesAndCheckIfDone();
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        const NKikimrBlobStorage::TGroupInfo *GetResultingGroupInfo() const {
            const NKikimrBlobStorage::TGroupInfo *result = nullptr;
            for (const auto& [nodeId, group] : GroupInfoReceived) {
                if (!result || result->GetGroupGeneration() < group.GetGroupGeneration()) {
                    result = &group;
                }
            }
            if (result) {
                TIntrusivePtr<TBlobStorageGroupInfo> info = TBlobStorageGroupInfo::Parse(*result, nullptr, nullptr);
                TBlobStorageGroupInfo::TGroupVDisks received(&info->GetTopology());
                for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
                    const TActorId& actorId = info->GetActorId(i);
                    if (GroupInfoReceived.count(actorId.NodeId())) {
                        received |= {&info->GetTopology(), info->GetVDiskId(i)};
                    }
                }
                if (!info->GetQuorumChecker().CheckQuorumForGroup(received)) {
                    result = nullptr;
                }
            }
            return result;
        }

        void ProcessResultAndFinish() {
            if (auto *result = GetResultingGroupInfo()) {
                STLOG(PRI_INFO, BS_NODE, NW86, "TGroupResolverActor::ProcessResultAndFinish", (GroupId, GroupId),
                    (Result, *result));
                Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvBlobStorage::TEvUpdateGroupInfo(TGroupId::FromValue(GroupId),
                    result->GetGroupGeneration(), *result));
                PassAway();
            } else { // restart from the beginning
                StartResolving();
            }
        }

        void PassAway() {
            STLOG(PRI_INFO, BS_NODE, NW81, "TGroupResolverActor::PassAway", (GroupId, GroupId));
            for (ui32 nodeId : SubscribedNodes) {
                Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe);
            }
            TActorBootstrapped::PassAway();
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        void Handle(TEvInterconnect::TEvNodeConnected::TPtr ev) {
            STLOG(PRI_DEBUG, BS_NODE, NW82, "TGroupResolverActor::TEvNodeConnected", (GroupId, GroupId),
                (NodeId, ev->Get()->NodeId));
        }

        void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr ev) {
            const ui32 nodeId = ev->Get()->NodeId;
            STLOG(PRI_DEBUG, BS_NODE, NW83, "TGroupResolverActor::TEvNodeDisconnected", (GroupId, GroupId),
                (NodeId, ev->Get()->NodeId));
            QueriesInFlight.erase(nodeId);
            NodeTimeoutQ.push_back(TNodeTimeout{TActivationContext::Now() + NodeTimeout, nodeId});
            if (NodeTimeoutQ.size() == 1) {
                Schedule(NodeTimeoutQ.front().Expires, new TEvPrivate::TEvNodeTimeout);
            }
        }

        void HandleNodeTimeout() {
            const TInstant now = TActivationContext::Now();
            std::deque<TNodeTimeout>::iterator it;
            for (it = NodeTimeoutQ.begin(); it != NodeTimeoutQ.end() && it->Expires <= now; ++it) {
                NodeIdsUnprocessed.insert(it->NodeId); // try processing for this node
            }
            NodeTimeoutQ.erase(NodeTimeoutQ.begin(), it);
            if (!NodeTimeoutQ.empty()) {
                Schedule(NodeTimeoutQ.front().Expires, new TEvPrivate::TEvNodeTimeout);
            }
            IssueQueriesAndCheckIfDone();
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        STRICT_STFUNC(StateWaitStart,
            cFunc(TEvents::TSystem::Wakeup, StartResolving);
            cFunc(TEvents::TSystem::Poison, PassAway);
        )

        STRICT_STFUNC(StateFunc,
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            cFunc(TEvPrivate::EvNodeIdsObtained, HandleNodeIdsObtained);

            hFunc(TEvNodeWardenGroupInfo, Handle);
            cFunc(TEvPrivate::EvQueryTimeout, HandleQueryTimeout);
            hFunc(TEvInterconnect::TEvNodeConnected, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
            cFunc(TEvPrivate::EvNodeTimeout, HandleNodeTimeout);
            cFunc(TEvents::TSystem::Poison, PassAway);
        )
    };

    IActor *TNodeWarden::CreateGroupResolverActor(ui32 groupId) {
        return new TGroupResolverActor(groupId, GroupResolverContext);
    }

    void TNodeWarden::Handle(TEvNodeWardenQueryGroupInfo::TPtr ev) {
        const auto& r = ev->Get()->Record;
        auto res = std::make_unique<TEvNodeWardenGroupInfo>();
        auto& record = res->Record;
        if (const auto it = Groups.find(r.GetGroupId()); it != Groups.end() && it->second.Group) {
            record.MutableGroup()->CopyFrom(*it->second.Group);
        }
        for (const auto& [key, value] : LocalVDisks) {
            if (const auto& r = value.RuntimeData; r && !r->DonorMode) {
                record.AddStartedGroupIds(r->GroupInfo->GroupID.GetRawId());
            }
        }
        Send(ev->Sender, res.release(), 0, ev->Cookie);
    }

} // NKikimr
