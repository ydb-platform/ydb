#pragma once

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/protos/interconnect.pb.h>
#include <util/generic/deque.h>
#include <util/network/address.h>

#include "interconnect_stream.h"
#include "types.h"

namespace NActors {
    enum class ENetwork : ui32 {
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // local messages
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        Start = EventSpaceBegin(TEvents::ES_INTERCONNECT_TCP),

        SocketReadyRead = Start,
        SocketReadyWrite,
        SocketError,
        Connect,
        Disconnect,
        IncomingConnection,
        HandshakeAsk,
        HandshakeAck,
        HandshakeNak,
        HandshakeDone,
        HandshakeFail,
        Kick,
        Flush,
        NodeInfo,
        BunchOfEventsToDestroy,
        HandshakeRequest,
        HandshakeReplyOK,
        HandshakeReplyError,
        ResolveAddress,
        AddressInfo,
        ResolveError,
        HTTPStreamStatus,
        HTTPSendContent,
        ConnectProtocolWakeup,
        HTTPProtocolRetry,
        EvPollerRegister,
        EvPollerRegisterResult,
        EvPollerReady,
        EvUpdateFromInputSession,
        EvConfirmUpdate,
        EvSessionBufferSizeRequest,
        EvSessionBufferSizeResponse,
        EvProcessPingRequest,
        EvGetSecureSocket,
        EvSecureSocket,
        HandshakeBrokerTake,
        HandshakeBrokerFree,
        HandshakeBrokerPermit,

        // external data channel messages
        EvSubscribeForConnection,
        EvReportConnection,

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // nonlocal messages; their indices must be preserved in order to work properly while doing rolling update
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // interconnect load test message
        EvLoadMessage = Start + 256,
    };

    struct TEvSocketReadyRead: public TEventLocal<TEvSocketReadyRead, ui32(ENetwork::SocketReadyRead)> {
    };

    struct TEvSocketReadyWrite: public TEventLocal<TEvSocketReadyWrite, ui32(ENetwork::SocketReadyWrite)> {
    };

    struct TEvSocketError: public TEventLocal<TEvSocketError, ui32(ENetwork::SocketError)> {
        TString GetReason() const {
            return ::strerror(Error);
        }
        const int Error;
        TIntrusivePtr<NInterconnect::TStreamSocket> Socket;

        TEvSocketError(int error, TIntrusivePtr<NInterconnect::TStreamSocket> sock)
            : Error(error)
            , Socket(std::move(sock))
        {
        }
    };

    struct TEvSocketConnect: public TEventLocal<TEvSocketConnect, ui32(ENetwork::Connect)> {
    };

    struct TEvSocketDisconnect: public TEventLocal<TEvSocketDisconnect, ui32(ENetwork::Disconnect)> {
        TDisconnectReason Reason;

        TEvSocketDisconnect(TDisconnectReason reason)
            : Reason(std::move(reason))
        {
        }
    };

    struct TEvHandshakeBrokerTake: TEventLocal<TEvHandshakeBrokerTake, ui32(ENetwork::HandshakeBrokerTake)> {
    };

    struct TEvHandshakeBrokerFree: TEventLocal<TEvHandshakeBrokerFree, ui32(ENetwork::HandshakeBrokerFree)> {
    };

    struct TEvHandshakeBrokerPermit: TEventLocal<TEvHandshakeBrokerPermit, ui32(ENetwork::HandshakeBrokerPermit)> {
    };

    struct TEvHandshakeAsk: public TEventLocal<TEvHandshakeAsk, ui32(ENetwork::HandshakeAsk)> {
        TEvHandshakeAsk(const TActorId& self,
                        const TActorId& peer,
                        ui64 counter)
            : Self(self)
            , Peer(peer)
            , Counter(counter)
        {
        }
        const TActorId Self;
        const TActorId Peer;
        const ui64 Counter;
    };

    struct TEvHandshakeAck: public TEventLocal<TEvHandshakeAck, ui32(ENetwork::HandshakeAck)> {
        TEvHandshakeAck(const TActorId& self, ui64 nextPacket, TSessionParams params)
            : Self(self)
            , NextPacket(nextPacket)
            , Params(std::move(params))
        {}

        const TActorId Self;
        const ui64 NextPacket;
        const TSessionParams Params;
    };

    struct TEvHandshakeNak : TEventLocal<TEvHandshakeNak, ui32(ENetwork::HandshakeNak)> {
    };

    struct TEvHandshakeRequest
       : public TEventLocal<TEvHandshakeRequest,
                             ui32(ENetwork::HandshakeRequest)> {

        NActorsInterconnect::THandshakeRequest Record;
    };

    struct TEvHandshakeReplyOK
       : public TEventLocal<TEvHandshakeReplyOK,
                             ui32(ENetwork::HandshakeReplyOK)> {

        NActorsInterconnect::THandshakeReply Record;
    };

    struct TEvHandshakeReplyError
       : public TEventLocal<TEvHandshakeReplyError,
                             ui32(ENetwork::HandshakeReplyError)> {

        TEvHandshakeReplyError(TString error) {
            Record.SetErrorExplaination(error);
        }

        NActorsInterconnect::THandshakeReply Record;
    };

    struct TEvIncomingConnection: public TEventLocal<TEvIncomingConnection, ui32(ENetwork::IncomingConnection)> {
        TIntrusivePtr<NInterconnect::TStreamSocket> Socket;
        NInterconnect::TAddress Address;

        TEvIncomingConnection(TIntrusivePtr<NInterconnect::TStreamSocket> socket, NInterconnect::TAddress address)
            : Socket(std::move(socket))
            , Address(std::move(address))
        {}
    };

    struct TEvHandshakeDone: public TEventLocal<TEvHandshakeDone, ui32(ENetwork::HandshakeDone)> {
        TEvHandshakeDone(
                TIntrusivePtr<NInterconnect::TStreamSocket> socket,
                const TActorId& peer,
                const TActorId& self,
                ui64 nextPacket,
                TAutoPtr<TProgramInfo>&& programInfo,
                TSessionParams params,
                TIntrusivePtr<NInterconnect::TStreamSocket> xdcSocket)
            : Socket(std::move(socket))
            , Peer(peer)
            , Self(self)
            , NextPacket(nextPacket)
            , ProgramInfo(std::move(programInfo))
            , Params(std::move(params))
            , XdcSocket(std::move(xdcSocket))
        {
        }

        TIntrusivePtr<NInterconnect::TStreamSocket> Socket;
        const TActorId Peer;
        const TActorId Self;
        const ui64 NextPacket;
        TAutoPtr<TProgramInfo> ProgramInfo;
        const TSessionParams Params;
        TIntrusivePtr<NInterconnect::TStreamSocket> XdcSocket;
    };

    struct TEvHandshakeFail: public TEventLocal<TEvHandshakeFail, ui32(ENetwork::HandshakeFail)> {
        enum EnumHandshakeFail {
            HANDSHAKE_FAIL_TRANSIENT,
            HANDSHAKE_FAIL_PERMANENT,
            HANDSHAKE_FAIL_SESSION_MISMATCH,
        };

        TEvHandshakeFail(EnumHandshakeFail temporary, TString explanation)
            : Temporary(temporary)
            , Explanation(std::move(explanation))
        {
        }

        const EnumHandshakeFail Temporary;
        const TString Explanation;
    };

    struct TEvKick: public TEventLocal<TEvKick, ui32(ENetwork::Kick)> {
    };

    struct TEvFlush: public TEventLocal<TEvFlush, ui32(ENetwork::Flush)> {
    };

    struct TEvLocalNodeInfo
       : public TEventLocal<TEvLocalNodeInfo, ui32(ENetwork::NodeInfo)> {

        ui32 NodeId;
        std::vector<NInterconnect::TAddress> Addresses;
    };

    struct TEvBunchOfEventsToDestroy : TEventLocal<TEvBunchOfEventsToDestroy, ui32(ENetwork::BunchOfEventsToDestroy)> {
        TEvBunchOfEventsToDestroy(TDeque<TAutoPtr<IEventBase>> events)
            : Events(std::move(events))
        {
        }

        TDeque<TAutoPtr<IEventBase>> Events;
    };

    struct TEvResolveAddress
       : public TEventLocal<TEvResolveAddress, ui32(ENetwork::ResolveAddress)> {

        TString Address;
        ui16 Port;
    };

    struct TEvAddressInfo
       : public TEventLocal<TEvAddressInfo, ui32(ENetwork::AddressInfo)> {

        NAddr::IRemoteAddrPtr Address;
    };

    struct TEvResolveError
       : public TEventLocal<TEvResolveError, ui32(ENetwork::ResolveError)> {

        TString Explain;
        TString Host;
    };

    struct TEvHTTPStreamStatus
       : public TEventLocal<TEvHTTPStreamStatus, ui32(ENetwork::HTTPStreamStatus)> {
        enum EStatus {
            READY,
            COMPLETE,
            ERROR,
        };

        EStatus Status;
        TString Error;
        TString HttpHeaders;
    };

    struct TEvHTTPSendContent
       : public TEventLocal<TEvHTTPSendContent, ui32(ENetwork::HTTPSendContent)> {

        const char* Data;
        size_t Len;
        bool Last;
    };

    struct TEvConnectWakeup
       : public TEventLocal<TEvConnectWakeup,
                             ui32(ENetwork::ConnectProtocolWakeup)> {
    };

    struct TEvHTTPProtocolRetry
       : public TEventLocal<TEvHTTPProtocolRetry,
                             ui32(ENetwork::HTTPProtocolRetry)> {
    };

    struct TEvLoadMessage
        : TEventPB<TEvLoadMessage, NActorsInterconnect::TEvLoadMessage, static_cast<ui32>(ENetwork::EvLoadMessage)> {
        TEvLoadMessage() = default;

        template <typename TContainer>
        TEvLoadMessage(const TContainer& route, const TString& id, const TString* payload) {
            for (const TActorId& actorId : route) {
                auto* hop = Record.AddHops();
                if (actorId) {
                    ActorIdToProto(actorId, hop->MutableNextHop());
                }
            }
            Record.SetId(id);
            if (payload) {
                Record.SetPayload(*payload);
            }
        }

        template <typename TContainer>
        TEvLoadMessage(const TContainer& route, const TString& id, TRope&& payload) {
            for (const TActorId& actorId : route) {
                auto* hop = Record.AddHops();
                if (actorId) {
                    ActorIdToProto(actorId, hop->MutableNextHop());
                }
            }
            Record.SetId(id);
            AddPayload(std::move(payload));
        }
    };

    struct TEvUpdateFromInputSession : TEventLocal<TEvUpdateFromInputSession, static_cast<ui32>(ENetwork::EvUpdateFromInputSession)> {
        ui64 ConfirmedByInput; // latest Confirm value from processed input packet
        ui64 NumDataBytes;
        TDuration Ping;

        TEvUpdateFromInputSession(ui64 confirmedByInput, ui64 numDataBytes, TDuration ping)
            : ConfirmedByInput(confirmedByInput)
            , NumDataBytes(numDataBytes)
            , Ping(ping)
        {
        }
    };

    struct TEvConfirmUpdate : TEventLocal<TEvConfirmUpdate, static_cast<ui32>(ENetwork::EvConfirmUpdate)>
    {};

    struct TEvSessionBufferSizeRequest : TEventLocal<TEvSessionBufferSizeRequest, static_cast<ui32>(ENetwork::EvSessionBufferSizeRequest)> {
    };

    struct TEvSessionBufferSizeResponse : TEventLocal<TEvSessionBufferSizeResponse, static_cast<ui32>(ENetwork::EvSessionBufferSizeResponse)> {
        TEvSessionBufferSizeResponse(const TActorId& sessionId, ui64 outputBufferSize)
            : SessionID(sessionId)
            , BufferSize(outputBufferSize)
        {
        }

        TActorId SessionID;
        ui64 BufferSize;
    };

    struct TEvProcessPingRequest : TEventLocal<TEvProcessPingRequest, static_cast<ui32>(ENetwork::EvProcessPingRequest)> {
        const ui64 Payload;

        TEvProcessPingRequest(ui64 payload)
            : Payload(payload)
        {}
    };

    struct TEvGetSecureSocket : TEventLocal<TEvGetSecureSocket, (ui32)ENetwork::EvGetSecureSocket> {
        TIntrusivePtr<NInterconnect::TStreamSocket> Socket;

        TEvGetSecureSocket(TIntrusivePtr<NInterconnect::TStreamSocket> socket)
            : Socket(std::move(socket))
        {}
    };

    struct TEvSecureSocket : TEventLocal<TEvSecureSocket, (ui32)ENetwork::EvSecureSocket> {
        TIntrusivePtr<NInterconnect::TSecureSocket> Socket;

        TEvSecureSocket(TIntrusivePtr<NInterconnect::TSecureSocket> socket)
            : Socket(std::move(socket))
        {}
    };

    struct TEvSubscribeForConnection : TEventLocal<TEvSubscribeForConnection, (ui32)ENetwork::EvSubscribeForConnection> {
        TString HandshakeId;
        bool Subscribe;

        TEvSubscribeForConnection(TString handshakeId, bool subscribe)
            : HandshakeId(std::move(handshakeId))
            , Subscribe(subscribe)
        {}
    };

    struct TEvReportConnection : TEventLocal<TEvReportConnection, (ui32)ENetwork::EvReportConnection> {
        TString HandshakeId;
        TIntrusivePtr<NInterconnect::TStreamSocket> Socket;

        TEvReportConnection(TString handshakeId, TIntrusivePtr<NInterconnect::TStreamSocket> socket)
            : HandshakeId(std::move(handshakeId))
            , Socket(std::move(socket))
        {}
    };
}
