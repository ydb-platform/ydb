#pragma once

#include "events.h"
#include "event_local.h"
#include <ydb/library/actors/util/intrusive_vector.h>
#include <ydb/library/actors/protos/interconnect.pb.h>
#include <util/string/cast.h>
#include <util/string/builder.h>

namespace NActors {
    class TNodeLocation {
    public:
        struct TKeys {
            enum E : int {
                DataCenter = 10,
                Module = 20,
                Rack = 30,
                Unit = 40,
            };
        };

        struct TLegacyValue {
            ui32 DataCenter;
            ui32 Room;
            ui32 Rack;
            ui32 Body;

            auto ConvertToTuple() const { return std::make_tuple(DataCenter, Room, Rack, Body); }

            int Compare(const TLegacyValue& other) const {
                const auto x = ConvertToTuple();
                const auto y = other.ConvertToTuple();
                if (x < y) {
                    return -1;
                } else if (y < x) {
                    return 1;
                } else {
                    return 0;
                }
            }

            friend bool operator ==(const TLegacyValue& x, const TLegacyValue& y) { return x.Compare(y) == 0; }

            void Serialize(NActorsInterconnect::TNodeLocation *pb) const {
                pb->SetDataCenterNum(DataCenter);
                pb->SetRoomNum(Room);
                pb->SetRackNum(Rack);
                pb->SetBodyNum(Body);
            }
        };

    private:
        std::optional<TLegacyValue> LegacyValue;
        std::vector<std::pair<TKeys::E, TString>> Items;

    public:
        // generic ctors
        TNodeLocation() = default;
        TNodeLocation(const TNodeLocation&) = default;
        TNodeLocation(TNodeLocation&&) = default;
        TNodeLocation(const TString& DataCenter, const TString& Module = "", const TString& Rack = "", const TString& Unit = "");

        // protobuf-parser ctor
        explicit TNodeLocation(const NActorsInterconnect::TNodeLocation& location);

        // serialized protobuf ctor
        static constexpr struct TFromSerialized {} FromSerialized {};
        TNodeLocation(TFromSerialized, const TString& s);

        // parser helper function
        static NActorsInterconnect::TNodeLocation ParseLocation(const TString& s);

        // assignment operators
        TNodeLocation& operator =(const TNodeLocation&) = default;
        TNodeLocation& operator =(TNodeLocation&&) = default;

        // compatibleWithOlderVersions should be set to true when this protobuf is possibly going to be delivered to 21-4
        void Serialize(NActorsInterconnect::TNodeLocation *pb, bool compatibleWithOlderVersions) const;
        TString GetSerializedLocation() const;

        TString GetDataCenterId() const { return ToStringUpTo(TKeys::DataCenter); }
        TString GetModuleId() const { return ToStringUpTo(TKeys::Module); }
        TString GetRackId() const { return ToStringUpTo(TKeys::Rack); }
        TString GetUnitId() const { return ToStringUpTo(TKeys::Unit); }
        TString ToString() const { return ToStringUpTo(TKeys::E(Max<int>())); }
        TString ToStringUpTo(TKeys::E upToKey) const;

        TLegacyValue GetLegacyValue() const;

        const std::vector<std::pair<TKeys::E, TString>>& GetItems() const { return Items; }

        bool HasKey(TKeys::E key) const {
            auto comp = [](const auto& p, TKeys::E value) { return p.first < value; };
            const auto it = std::lower_bound(Items.begin(), Items.end(), key, comp);
            return it != Items.end() && it->first == key;
        }

        int Compare(const TNodeLocation& other) const {
            if (LegacyValue || other.LegacyValue) {
                return GetLegacyValue().Compare(other.GetLegacyValue());
            } else if (Items < other.Items) {
                return -1;
            } else if (other.Items < Items) {
                return 1;
            } else {
                return 0;
            }
        }

        void InheritLegacyValue(const TNodeLocation& other) {
            LegacyValue = other.GetLegacyValue();
        }

        friend bool operator ==(const TNodeLocation& x, const TNodeLocation& y) { return x.Compare(y) == 0; }
        friend bool operator !=(const TNodeLocation& x, const TNodeLocation& y) { return x.Compare(y) != 0; }
        friend bool operator < (const TNodeLocation& x, const TNodeLocation& y) { return x.Compare(y) <  0; }
        friend bool operator <=(const TNodeLocation& x, const TNodeLocation& y) { return x.Compare(y) <= 0; }
        friend bool operator > (const TNodeLocation& x, const TNodeLocation& y) { return x.Compare(y) >  0; }
        friend bool operator >=(const TNodeLocation& x, const TNodeLocation& y) { return x.Compare(y) >= 0; }
    };

    struct TEvInterconnect {
        enum EEv {
            EvForward = EventSpaceBegin(TEvents::ES_INTERCONNECT),
            EvResolveNode, // resolve info about node (internal)
            EvNodeAddress, // node info (internal)
            EvConnectNode, // request proxy to establish connection (like: we would send something there soon)
            EvAcceptIncoming,
            EvNodeConnected,    // node connected notify
            EvNodeDisconnected, // node disconnected notify
            EvRegisterNode,
            EvRegisterNodeResult,
            EvListNodes,
            EvNodesInfo,
            EvDisconnect,
            EvGetNode,
            EvNodeInfo,
            EvClosePeerSocket,
            EvCloseInputSession,
            EvPoisonSession,
            EvTerminate,
            EvForwardDelayed,
            EvPrepareOutgoingConnection,
            EvPrepareOutgoingConnectionResult,
            EvCheckIncomingConnection,
            EvCheckIncomingConnectionResult,
            EvNotifyOutgoingConnectionEstablished,
            EvEnd
        };

        enum ESubscribes {
            SubConnected,
            SubDisconnected,
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_INTERCONNECT), "expect EvEnd < EventSpaceEnd(TEvents::ES_INTERCONNECT)");

        struct TEvNodeAddress;

        struct TEvConnectNode: public TEventLocal<TEvConnectNode, EvConnectNode> {
        };

        struct TEvAcceptIncoming;

        struct TEvNodeConnected: public TEventLocal<TEvNodeConnected, EvNodeConnected> {
            TEvNodeConnected(ui32 node) noexcept
                : NodeId(node)
            {
            }
            const ui32 NodeId;
        };

        struct TEvNodeDisconnected: public TEventLocal<TEvNodeDisconnected, EvNodeDisconnected> {
            TEvNodeDisconnected(ui32 node) noexcept
                : NodeId(node)
            {
            }
            const ui32 NodeId;
        };

        struct TEvRegisterNode;
        struct TEvRegisterNodeResult;

        struct TEvListNodes: public TEventLocal<TEvListNodes, EvListNodes> {
            const bool SubscribeToStaticNodeChanges = false;

            TEvListNodes() = default;

            TEvListNodes(bool subscribeToStaticNodeChanges)
                : SubscribeToStaticNodeChanges(subscribeToStaticNodeChanges)
            {}
        };

        struct TNodeInfo {
            ui32 NodeId;
            TString Address;
            TString Host;
            TString ResolveHost;
            ui16 Port;
            TNodeLocation Location;
            bool IsStatic = true;

            TNodeInfo() = default;
            TNodeInfo(const TNodeInfo&) = default;
            TNodeInfo& operator =(const TNodeInfo&) = default;
            TNodeInfo(ui32 nodeId,
                      const TString& address,
                      const TString& host,
                      const TString& resolveHost,
                      ui16 port,
                      const TNodeLocation& location,
                      bool isStatic = true)
                : NodeId(nodeId)
                , Address(address)
                , Host(host)
                , ResolveHost(resolveHost)
                , Port(port)
                , Location(location)
                , IsStatic(isStatic)
            {
            }

            operator ui32() const {
                return NodeId;
            }
        };

        struct TEvNodesInfo: public TEventLocal<TEvNodesInfo, EvNodesInfo> {
            using TPileMap = std::vector<std::vector<ui32>>; // a vector of sorted node ids among each pile (when bridge mode is on)

            TIntrusiveVector<TNodeInfo>::TConstPtr NodesPtr;
            const TVector<TNodeInfo>& Nodes;
            std::shared_ptr<const TPileMap> PileMap;

            TEvNodesInfo(TIntrusiveVector<TNodeInfo>::TConstPtr nodesPtr, std::shared_ptr<const TPileMap>&& pileMap = nullptr)
                : NodesPtr(nodesPtr)
                , Nodes(*nodesPtr)
                , PileMap(std::move(pileMap))
            {}

            const TNodeInfo* GetNodeInfo(ui32 nodeId) const {
                for (const auto& x : Nodes) {
                    if (x.NodeId == nodeId)
                        return &x;
                }
                return nullptr;
            }
        };

        struct TEvDisconnect;

        struct TEvGetNode: public TEventLocal<TEvGetNode, EvGetNode> {
            ui32 NodeId;
            TMonotonic Deadline;

            TEvGetNode(ui32 nodeId, TMonotonic deadline = TMonotonic::Max())
                : NodeId(nodeId)
                , Deadline(deadline)
            {
            }

            TString ToString() const override;
        };

        struct TEvResolveNode: public TEventLocal<TEvResolveNode, EvResolveNode> {
            ui32 NodeId;
            TMonotonic Deadline;

            TEvResolveNode(ui32 nodeId, TMonotonic deadline = TMonotonic::Max())
                : NodeId(nodeId)
                , Deadline(deadline)
            {
            }

            TString ToString() const override;
        };

        struct TEvNodeInfo: public TEventLocal<TEvNodeInfo, EvNodeInfo> {
            TEvNodeInfo(ui32 nodeId)
                : NodeId(nodeId)
            {
            }

            ui32 NodeId;
            THolder<TNodeInfo> Node;
        };

        struct TEvClosePeerSocket : TEventLocal<TEvClosePeerSocket, EvClosePeerSocket> {};

        struct TEvCloseInputSession : TEventLocal<TEvCloseInputSession, EvCloseInputSession> {};

        struct TEvPoisonSession : TEventLocal<TEvPoisonSession, EvPoisonSession> {};

        struct TEvTerminate : TEventLocal<TEvTerminate, EvTerminate> {};

        struct TEvForwardDelayed : TEventLocal<TEvForwardDelayed, EvForwardDelayed> {};

        struct TEvPrepareOutgoingConnection : TEventLocal<TEvPrepareOutgoingConnection, EvPrepareOutgoingConnection> {
            const ui32 PeerNodeId;

            TEvPrepareOutgoingConnection(ui32 peerNodeId)
                : PeerNodeId(peerNodeId)
            {}
        };

        struct TEvPrepareOutgoingConnectionResult : TEventLocal<TEvPrepareOutgoingConnectionResult, EvPrepareOutgoingConnectionResult> {
            std::variant<TString, THashMap<TString, TString>> Conclusion;

            TEvPrepareOutgoingConnectionResult(std::variant<TString, THashMap<TString, TString>>&& conclusion)
                : Conclusion(std::move(conclusion))
            {}
        };

        struct TEvCheckIncomingConnection : TEventLocal<TEvCheckIncomingConnection, EvCheckIncomingConnection> {
            const ui32 PeerNodeId;
            THashMap<TString, TString> Params; // incoming key-value pairs from the other peer

            TEvCheckIncomingConnection(ui32 peerNodeId, THashMap<TString, TString>&& params)
                : PeerNodeId(peerNodeId)
                , Params(std::move(params))
            {}
        };

        struct TEvCheckIncomingConnectionResult : TEventLocal<TEvCheckIncomingConnectionResult, EvCheckIncomingConnectionResult> {
            std::optional<TString> ErrorReason;
            THashMap<TString, TString> ParamsToSend;

            TEvCheckIncomingConnectionResult(std::optional<TString>&& errorReason, THashMap<TString, TString>&& paramsToSend)
                : ErrorReason(std::move(errorReason))
                , ParamsToSend(std::move(paramsToSend))
             {}
        };

        struct TEvNotifyOutgoingConnectionEstablished : TEventLocal<TEvNotifyOutgoingConnectionEstablished, EvNotifyOutgoingConnectionEstablished> {
            const ui32 PeerNodeId;
            const bool Success;
            THashMap<TString, TString> Params;

            TEvNotifyOutgoingConnectionEstablished(ui32 peerNodeId, const bool success, THashMap<TString, TString>&& params)
                : PeerNodeId(peerNodeId)
                , Success(success)
                , Params(std::move(params))
            {}
        };
    };
}
