#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/interconnect.h>
#include <util/generic/map.h>
#include <util/network/address.h>

namespace NActors {
    struct TInterconnectGlobalState: public TThrRefBase {
        TString SelfAddress;
        ui32 SelfPort;

        TVector<TActorId> GlobalNameservers; // todo: add some info about (like expected reply time)
    };

    struct TInterconnectProxySetup: public TThrRefBase {
        // synchronous (session -> proxy)
        struct IProxy : TNonCopyable {
            virtual ~IProxy() {
            }

            virtual void ActivateSession(const TActorContext& ctx) = 0; // session activated
            virtual void DetachSession(const TActorContext& ctx) = 0;   // session is dead
        };

        // synchronous (proxy -> session)
        struct ISession : TNonCopyable {
            virtual ~ISession() {
            }

            virtual void DetachSession(const TActorContext& ownerCtx, const TActorContext& sessionCtx) = 0;                                      // kill yourself
            virtual void ForwardPacket(TAutoPtr<IEventHandle>& ev, const TActorContext& ownerCtx, const TActorContext& sessionCtx) = 0;          // receive packet for forward
            virtual void Connect(const TActorContext& ownerCtx, const TActorContext& sessionCtx) = 0;                                            // begin connection
            virtual bool ReceiveIncomingSession(TAutoPtr<IEventHandle>& ev, const TActorContext& ownerCtx, const TActorContext& sessionCtx) = 0; // handle incoming session, if returns true - then session is dead and must be recreated with new one
        };

        ui32 DestinationNode;

        TString StaticAddress; // if set - would be used as main destination address
        int StaticPort;

        TIntrusivePtr<TInterconnectGlobalState> GlobalState;

        virtual IActor* CreateSession(const TActorId& ownerId, IProxy* owner) = 0; // returned actor is session and would be attached to same mailbox as proxy to allow sync calls
        virtual TActorSetupCmd CreateAcceptor() = 0;
    };

    struct TNameserverSetup {
        TActorId ServiceID;

        TIntrusivePtr<TInterconnectGlobalState> GlobalState;
    };

    struct TTableNameserverSetup: public TThrRefBase {
        struct TNodeInfo {
            TString Address;
            TString Host;
            TString ResolveHost;
            ui16 Port;
            TNodeLocation Location;
            TString& first;
            ui16& second;

            TNodeInfo()
                : first(Address)
                , second(Port)
            {
            }

            TNodeInfo(const TNodeInfo&) = default;

            // for testing purposes only
            TNodeInfo(const TString& address, const TString& host, ui16 port)
                : TNodeInfo()
            {
                Address = address;
                Host = host;
                ResolveHost = host;
                Port = port;
            }

            TNodeInfo(const TString& address,
                      const TString& host,
                      const TString& resolveHost,
                      ui16 port,
                      const TNodeLocation& location)
                : TNodeInfo()
            {
                Address = address;
                Host = host;
                ResolveHost = resolveHost;
                Port = port;
                Location = location;
            }

            // for testing purposes only
            TNodeInfo& operator=(const std::pair<TString, ui32>& pr) {
                Address = pr.first;
                Host = pr.first;
                ResolveHost = pr.first;
                Port = pr.second;
                return *this;
            }

            TNodeInfo& operator=(const TNodeInfo& ni) {
                Address = ni.Address;
                Host = ni.Host;
                ResolveHost = ni.ResolveHost;
                Port = ni.Port;
                Location = ni.Location;
                return *this;
            }

            friend bool operator ==(const TNodeInfo& x, const TNodeInfo& y) {
                return x.Address == y.Address && x.Host == y.Host && x.ResolveHost == y.ResolveHost && x.Port == y.Port
                    && x.Location == y.Location;
            }

            friend bool operator !=(const TNodeInfo& x, const TNodeInfo& y) {
                return !(x == y);
            }
        };

        TMap<ui32, TNodeInfo> StaticNodeTable;

        bool IsEntriesUnique() const;
    };

    struct TNodeRegistrarSetup {
        TActorId ServiceID;

        TIntrusivePtr<TInterconnectGlobalState> GlobalState;
    };

    TActorId GetNameserviceActorId();

    /**
     * Const table-lookup based name service
     */

    IActor* CreateNameserverTable(
        const TIntrusivePtr<TTableNameserverSetup>& setup,
        ui32 poolId = 0);

    /**
     * Name service which can be paired with external discovery service.
     * Copies information from setup on the start (table may be empty).
     * Handles TEvNodesInfo to change list of known nodes.
     *
     * If PendingPeriod is not zero, wait for unknown nodeId
     */

    IActor* CreateDynamicNameserver(
        const TIntrusivePtr<TTableNameserverSetup>& setup,
        const TDuration& pendingPeriod = TDuration::Zero(),
        ui32 poolId = 0);

    /**
     * Creates an actor that resolves host/port and replies with either:
     *
     * - TEvLocalNodeInfo on success
     * - TEvResolveError on errors
     *
     * Optional defaultAddress may be used as fallback.
     */
    IActor* CreateResolveActor(
        const TString& host, ui16 port, ui32 nodeId, const TString& defaultAddress,
        const TActorId& replyTo, const TActorId& replyFrom, TInstant deadline);

    inline IActor* CreateResolveActor(
        ui32 nodeId, const TTableNameserverSetup::TNodeInfo& nodeInfo,
        const TActorId& replyTo, const TActorId& replyFrom, TInstant deadline)
    {
        return CreateResolveActor(nodeInfo.ResolveHost, nodeInfo.Port, nodeId, nodeInfo.Address,
            replyTo, replyFrom, deadline);
    }

    /**
     * Creates an actor that resolves host/port and replies with either:
     *
     * - TEvAddressInfo on success
     * - TEvResolveError on errors
     */
    IActor* CreateResolveActor(
        const TString& host, ui16 port,
        const TActorId& replyTo, const TActorId& replyFrom, TInstant deadline);

}
