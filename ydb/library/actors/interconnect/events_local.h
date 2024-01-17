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
        DEFINE_SIMPLE_LOCAL_EVENT(TEvSocketReadyRead, "Network: TEvSocketReadyRead")
    };

    struct TEvSocketReadyWrite: public TEventLocal<TEvSocketReadyWrite, ui32(ENetwork::SocketReadyWrite)> {
        DEFINE_SIMPLE_LOCAL_EVENT(TEvSocketReadyWrite, "Network: TEvSocketReadyWrite")
    };

    struct TEvSocketError: public TEventLocal<TEvSocketError, ui32(ENetwork::SocketError)> {
        DEFINE_SIMPLE_LOCAL_EVENT(TEvSocketError, ::strerror(Error))
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
        DEFINE_SIMPLE_LOCAL_EVENT(TEvSocketConnect, "Network: TEvSocketConnect")
    };

    struct TEvSocketDisconnect: public TEventLocal<TEvSocketDisconnect, ui32(ENetwork::Disconnect)> {
        DEFINE_SIMPLE_LOCAL_EVENT(TEvSocketDisconnect, "Network: TEvSocketDisconnect")
        TDisconnectReason Reason;

        TEvSocketDisconnect(TDisconnectReason reason)
            : Reason(std::move(reason))
        {
        }
    };

    struct TEvHandshakeBrokerTake: TEventLocal<TEvHandshakeBrokerTake, ui32(ENetwork::HandshakeBrokerTake)> {
        DEFINE_SIMPLE_LOCAL_EVENT(TEvHandshakeBrokerTake, "Network: TEvHandshakeBrokerTake")
    };

    struct TEvHandshakeBrokerFree: TEventLocal<TEvHandshakeBrokerFree, ui32(ENetwork::HandshakeBrokerFree)> {
        DEFINE_SIMPLE_LOCAL_EVENT(TEvHandshakeBrokerFree, "Network: TEvHandshakeBrokerFree")
    };

    struct TEvHandshakeBrokerPermit: TEventLocal<TEvHandshakeBrokerPermit, ui32(ENetwork::HandshakeBrokerPermit)> {
        DEFINE_SIMPLE_LOCAL_EVENT(TEvHandshakeBrokerPermit, "Network: TEvHandshakeBrokerPermit")
    };

    struct TEvHandshakeAsk: public TEventLocal<TEvHandshakeAsk, ui32(ENetwork::HandshakeAsk)> {
        DEFINE_SIMPLE_LOCAL_EVENT(TEvHandshakeAsk, "Network: TEvHandshakeAsk")
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
        DEFINE_SIMPLE_LOCAL_EVENT(TEvHandshakeAck, "Network: TEvHandshakeAck")

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
        DEFINE_SIMPLE_LOCAL_EVENT(TEvSocketReadyRead, "Network: TEvHandshakeNak")
    };

    struct TEvHandshakeRequest
       : public TEventLocal<TEvHandshakeRequest,
                             ui32(ENetwork::HandshakeRequest)> {
        DEFINE_SIMPLE_LOCAL_EVENT(TEvHandshakeRequest,
                                  "Network: TEvHandshakeRequest")

        NActorsInterconnect::THandshakeRequest Record;
    };

    struct TEvHandshakeReplyOK
       : public TEventLocal<TEvHandshakeReplyOK,
                             ui32(ENetwork::HandshakeReplyOK)> {
        DEFINE_SIMPLE_LOCAL_EVENT(TEvHandshakeReplyOK,
                                  "Network: TEvHandshakeReplyOK")

        NActorsInterconnect::THandshakeReply Record;
    };

    struct TEvHandshakeReplyError
       : public TEventLocal<TEvHandshakeReplyError,
                             ui32(ENetwork::HandshakeReplyError)> {
        DEFINE_SIMPLE_LOCAL_EVENT(TEvHandshakeReplyError,
                                  "Network: TEvHandshakeReplyError")

        TEvHandshakeReplyError(TString error) {
            Record.SetErrorExplaination(error);
        }

        NActorsInterconnect::THandshakeReply Record;
    };

    struct TEvIncomingConnection: public TEventLocal<TEvIncomingConnection, ui32(ENetwork::IncomingConnection)> {
        DEFINE_SIMPLE_LOCAL_EVENT(TEvIncomingConnection, "Network: TEvIncomingConnection")
        TIntrusivePtr<NInterconnect::TStreamSocket> Socket;
        NInterconnect::TAddress Address;

        TEvIncomingConnection(TIntrusivePtr<NInterconnect::TStreamSocket> socket, NInterconnect::TAddress address)
            : Socket(std::move(socket))
            , Address(std::move(address))
        {}
    };

    struct TEvHandshakeDone: public TEventLocal<TEvHandshakeDone, ui32(ENetwork::HandshakeDone)> {
        DEFINE_SIMPLE_LOCAL_EVENT(TEvHandshakeDone, "Network: TEvHandshakeDone")

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
        DEFINE_SIMPLE_LOCAL_EVENT(TEvHandshakeFail, "Network: TEvHandshakeFail")

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
        DEFINE_SIMPLE_LOCAL_EVENT(TEvKick, "Network: TEvKick")
    };

    struct TEvFlush: public TEventLocal<TEvFlush, ui32(ENetwork::Flush)> {
        DEFINE_SIMPLE_LOCAL_EVENT(TEvFlush, "Network: TEvFlush")
    };

    struct TEvLocalNodeInfo
       : public TEventLocal<TEvLocalNodeInfo, ui32(ENetwork::NodeInfo)> {
        DEFINE_SIMPLE_LOCAL_EVENT(TEvLocalNodeInfo, "Network: TEvLocalNodeInfo")

        ui32 NodeId;
        std::vector<NInterconnect::TAddress> Addresses;
    };

    struct TEvBunchOfEventsToDestroy : TEventLocal<TEvBunchOfEventsToDestroy, ui32(ENetwork::BunchOfEventsToDestroy)> {
        DEFINE_SIMPLE_LOCAL_EVENT(TEvBunchOfEventsToDestroy,
                                  "Network: TEvBunchOfEventsToDestroy")

        TEvBunchOfEventsToDestroy(TDeque<TAutoPtr<IEventBase>> events)
            : Events(std::move(events))
        {
        }

        TDeque<TAutoPtr<IEventBase>> Events;
    };

    struct TEvResolveAddress
       : public TEventLocal<TEvResolveAddress, ui32(ENetwork::ResolveAddress)> {
        DEFINE_SIMPLE_LOCAL_EVENT(TEvResolveAddress, "Network: TEvResolveAddress")

        TString Address;
        ui16 Port;
    };

    struct TEvAddressInfo
       : public TEventLocal<TEvAddressInfo, ui32(ENetwork::AddressInfo)> {
        DEFINE_SIMPLE_LOCAL_EVENT(TEvAddressInfo, "Network: TEvAddressInfo")

        NAddr::IRemoteAddrPtr Address;
    };

    struct TEvResolveError
       : public TEventLocal<TEvResolveError, ui32(ENetwork::ResolveError)> {
        DEFINE_SIMPLE_LOCAL_EVENT(TEvResolveError, "Network: TEvResolveError")

        TString Explain;
        TString Host;
    };

    struct TEvHTTPStreamStatus
       : public TEventLocal<TEvHTTPStreamStatus, ui32(ENetwork::HTTPStreamStatus)> {
        DEFINE_SIMPLE_LOCAL_EVENT(TEvHTTPStreamStatus,
                                  "Network: TEvHTTPStreamStatus")
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
        DEFINE_SIMPLE_LOCAL_EVENT(TEvHTTPSendContent, "Network: TEvHTTPSendContent")

        const char* Data;
        size_t Len;
        bool Last;
    };

    struct TEvConnectWakeup
       : public TEventLocal<TEvConnectWakeup,
                             ui32(ENetwork::ConnectProtocolWakeup)> {
        DEFINE_SIMPLE_LOCAL_EVENT(TEvConnectWakeup, "Protocols: TEvConnectWakeup")
    };

    struct TEvHTTPProtocolRetry
       : public TEventLocal<TEvHTTPProtocolRetry,
                             ui32(ENetwork::HTTPProtocolRetry)> {
        DEFINE_SIMPLE_LOCAL_EVENT(TEvHTTPProtocolRetry,
                                  "Protocols: TEvHTTPProtocolRetry")
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
        //DEFINE_SIMPLE_LOCAL_EVENT(TEvSessionBufferSizeRequest, "Session: TEvSessionBufferSizeRequest")
        DEFINE_SIMPLE_LOCAL_EVENT(TEvSessionBufferSizeRequest, "Network: TEvSessionBufferSizeRequest");
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
