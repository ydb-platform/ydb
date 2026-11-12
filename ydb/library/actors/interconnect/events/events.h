#pragma once

#include <util/system/types.h>
#include <ydb/library/actors/core/events.h>

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
}
