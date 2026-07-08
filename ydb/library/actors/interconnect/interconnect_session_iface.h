#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/mon.h>

#include "types.h"
#include "events_local.h"

#include <optional>

namespace NActors {

    // Abstraction over the interconnect session actor implementations (TInterconnectSessionTCP and
    // TInterconnectSessionTCPv2). The proxy holds a pointer to this interface and performs all of its
    // synchronous (same-mailbox) calls through it. Implementations are also actors; SessionActor()
    // provides the bridge required by IActor::InvokeOtherActor.
    struct IInterconnectSession {
        virtual ~IInterconnectSession() = default;

        // bridge to the underlying actor object (both implementations derive from TActor<>)
        virtual IActor& SessionActor() noexcept = 0;

        // synchronous call surface used by the proxy (invoked via InvokeOtherActor within actor context)
        virtual void Init() = 0;
        virtual void SetNewConnection(TEvHandshakeDone::TPtr& ev) = 0;
        virtual void Terminate(TDisconnectReason reason) = 0;
        virtual THolder<TEvHandshakeAck> ProcessHandshakeRequest(TEvHandshakeAsk::TPtr& ev) = 0;
        virtual void StartHandshake() = 0;
        virtual void ReestablishConnectionWithHandshake(TDisconnectReason reason) = 0;
        virtual void CloseInputSession() = 0;
        virtual bool IsRdmaInUse() = 0;
        virtual bool HasRdmaState() const = 0;

        // Whether this session type supports resuming over consecutive TCP connections (continuation).
        // TInterconnectSessionTCP returns true; TInterconnectSessionTCPv2 returns false.
        virtual bool SupportsContinuation() const = 0;

        // field-like accessors used by the proxy (replacing former direct member access)
        virtual const TSessionParams& GetParams() const = 0;
        virtual const TIntrusivePtr<NInterconnect::TStreamSocket>& GetSocket() const = 0;
        virtual ui64 GetTotalOutputQueueSize() const = 0;
        virtual std::optional<ui8> GetXDCFlags() const = 0;
        virtual TDuration GetPingRTT() const = 0;
        virtual i64 GetClockSkew() const = 0;
        virtual void GenerateHttpInfo(NMon::TEvHttpInfoRes::TPtr& ev) = 0;
    };

}
