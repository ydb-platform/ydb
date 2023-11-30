#pragma once

#include "defs.h"
#include "events.h"

#include <ydb/core/protos/base.pb.h>
#include <ydb/core/protos/tablet_pipe.pb.h>
#include <ydb/library/actors/core/event_local.h>


namespace NKikimr {

    struct TEvTabletPipe {
        enum EEv {
            //-- Remote events
            EvConnect = EventSpaceBegin(TKikimrEvents::ES_TABLET_PIPE),
            EvConnectResult,
            EvPush,
            EvPeerClosed,
            EvPeerShutdown,

            //-- Local events
            EvClientConnected = EvConnect + 512,
            EvServerConnected,
            EvSend,
            EvClientDestroyed,
            EvServerDisconnected,
            EvServerDestroyed,
            EvActivate,
            EvShutdown,
            EvClientRetry,
            EvClientCheckDelay,
            EvClientShuttingDown,
            EvMessage, // replacement for EvSend

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TABLET_PIPE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TABLET_PIPE)");

        struct TEvConnect : public TEventPB<TEvConnect, NKikimrTabletPipe::TEvConnect, EvConnect> {
            TEvConnect() {}

            TEvConnect(ui64 tabletId, const TActorId& clientId, ui32 features = 0)
            {
                Record.SetTabletId(tabletId);
                ActorIdToProto(clientId, Record.MutableClientId());
                if (features) {
                    Record.SetFeatures(features);
                }
            }
        };

        struct TEvConnectResult : public TEventPB<TEvConnectResult, NKikimrTabletPipe::TEvConnectResult, EvConnectResult> {
            TEvConnectResult() {}

            TEvConnectResult(NKikimrProto::EReplyStatus status, ui64 tabletId, const TActorId& clientId, const TActorId& serverId, bool leader, ui32 generation)
            {
                Record.SetStatus(status);
                Record.SetTabletId(tabletId);
                ActorIdToProto(clientId, Record.MutableClientId());
                ActorIdToProto(serverId, Record.MutableServerId());
                Record.SetLeader(leader);
                Record.SetSupportsDataInPayload(true);
                Record.SetGeneration(generation);
            }
        };

        struct TEvPush : public TEventPB<TEvPush, NKikimrTabletPipe::TEvPush, EvPush> {
            TEvPush() {}

            TEvPush(ui64 tabletId, ui32 type, const TActorId& sender, const TIntrusivePtr<TEventSerializedData>& buffer,
                ui64 cookie, const TEventSerializationInfo& serializationInfo, bool supportsDataInPayload)
            {
                Record.SetTabletId(tabletId);
                Record.SetType(type);
                ActorIdToProto(sender, Record.MutableSender());
                if (supportsDataInPayload) {
                    AddPayload(buffer->GetRope());
                } else {
                    Record.SetBuffer(buffer->GetString());
                }
                Record.SetCookie(cookie);
                Record.SetExtendedFormat(serializationInfo.IsExtendedFormat);
            }

            void SetSeqNo(ui64 seqNo) {
                Record.SetSeqNo(seqNo);
            }
        };

        struct TEvPeerClosed : public TEventPB<TEvPeerClosed, NKikimrTabletPipe::TEvPeerClosed, EvPeerClosed> {
            TEvPeerClosed() {}

            TEvPeerClosed(ui64 tabletId, const TActorId& clientId, const TActorId& serverId)
            {
                Record.SetTabletId(tabletId);
                ActorIdToProto(clientId, Record.MutableClientId());
                ActorIdToProto(serverId, Record.MutableServerId());
            }
        };

        struct TEvPeerShutdown : public TEventPB<TEvPeerShutdown, NKikimrTabletPipe::TEvPeerShutdown, EvPeerShutdown> {
            TEvPeerShutdown() {}

            TEvPeerShutdown(ui64 tabletId, const TActorId& clientId, const TActorId& serverId, ui64 maxForwardedSeqNo)
            {
                Record.SetTabletId(tabletId);
                ActorIdToProto(clientId, Record.MutableClientId());
                ActorIdToProto(serverId, Record.MutableServerId());
                Record.SetMaxForwardedSeqNo(maxForwardedSeqNo);
            }

            ui64 GetMaxForwardedSeqNo() const {
                if (Record.HasMaxForwardedSeqNo()) {
                    return Record.GetMaxForwardedSeqNo();
                } else {
                    return Max<ui64>();
                }
            }
        };

        struct TEvClientConnected : public TEventLocal<TEvClientConnected, EvClientConnected> {
            TEvClientConnected(ui64 tabletId, NKikimrProto::EReplyStatus status, const TActorId& clientId, const TActorId& serverId, bool leader, bool dead, ui64 generation)
                : TabletId(tabletId)
                , Status(status)
                , ClientId(clientId)
                , ServerId(serverId)
                , Leader(leader)
                , Dead(dead)
                , Generation(generation)
            {}

            const ui64 TabletId;
            const NKikimrProto::EReplyStatus Status;
            const TActorId ClientId;
            const TActorId ServerId;
            const bool Leader;
            const bool Dead;
            const ui64 Generation;
        };

        struct TEvServerConnected : public TEventLocal<TEvServerConnected, EvServerConnected> {
            TEvServerConnected(ui64 tabletId, const TActorId& clientId, const TActorId& serverId,
                    const TActorId& interconnectSession = {})
                : TabletId(tabletId)
                , ClientId(clientId)
                , ServerId(serverId)
                , InterconnectSession(interconnectSession)
            {}

            const ui64 TabletId;
            const TActorId ClientId;
            const TActorId ServerId;
            const TActorId InterconnectSession;
        };

        struct TEvClientDestroyed : public TEventLocal<TEvClientDestroyed, EvClientDestroyed> {
            TEvClientDestroyed(ui64 tabletId, const TActorId& clientId, const TActorId& serverId)
                : TabletId(tabletId)
                , ClientId(clientId)
                , ServerId(serverId)
            {}

            const ui64 TabletId;
            const TActorId ClientId;
            const TActorId ServerId;
        };

        struct TEvClientShuttingDown : public TEventLocal<TEvClientShuttingDown, EvClientShuttingDown> {
            TEvClientShuttingDown(ui64 tabletId, const TActorId& clientId, const TActorId& serverId, ui64 maxForwardedSeqNo)
                : TabletId(tabletId)
                , ClientId(clientId)
                , ServerId(serverId)
                , MaxForwardedSeqNo(maxForwardedSeqNo)
            {}

            const ui64 TabletId;
            const TActorId ClientId;
            const TActorId ServerId;
            const ui64 MaxForwardedSeqNo;
        };

        struct TEvServerDisconnected : public TEventLocal<TEvServerDisconnected, EvServerDisconnected> {
            TEvServerDisconnected(ui64 tabletId, const TActorId& clientId, const TActorId& serverId)
                : TabletId(tabletId)
                , ClientId(clientId)
                , ServerId(serverId)
            {}

            const ui64 TabletId;
            const TActorId ClientId;
            const TActorId ServerId;
        };

        struct TEvServerDestroyed : public TEventLocal<TEvServerDestroyed, EvServerDestroyed> {
            TEvServerDestroyed(ui64 tabletId, const TActorId& clientId, const TActorId& serverId)
                : TabletId(tabletId)
                , ClientId(clientId)
                , ServerId(serverId)
            {}

            const ui64 TabletId;
            const TActorId ClientId;
            const TActorId ServerId;
        };

        struct TEvActivate : public TEventLocal<TEvActivate, EvActivate> {
            TEvActivate(ui64 tabletId, const TActorId& ownerId, const TActorId& recipientId, bool leader, ui32 generation)
                : TabletId(tabletId)
                , OwnerId(ownerId)
                , RecipientId(recipientId)
                , Leader(leader)
                , Generation(generation)
                
            {}

            const ui64 TabletId;
            const TActorId OwnerId;
            const TActorId RecipientId;
            const bool Leader;
            const ui32 Generation;
        };

        struct TEvShutdown : public TEventLocal<TEvShutdown, EvShutdown> {
            TEvShutdown() {}
        };

        struct TEvClientRetry : public TEventLocal<TEvClientRetry, EvClientRetry> {
            TEvClientRetry() {}
        };

        struct TEvClientCheckDelay : public TEventLocal<TEvClientCheckDelay, EvClientCheckDelay> {
            TEvClientCheckDelay() {}
        };

        class TEvMessage : public TEventLocal<TEvMessage, EvMessage> {
        public:
            TEvMessage(const TActorId& sender, THolder<IEventBase> event)
                : Type(event->Type())
                , Sender(sender)
                , Event(std::move(event))
            { }

            TEvMessage(const TActorId& sender, ui32 type, TIntrusivePtr<TEventSerializedData> buffer)
                : Type(type)
                , Sender(sender)
                , Buffer(std::move(buffer))
            { }

        public:
            void SetSeqNo(ui64 seqNo) {
                SeqNo = seqNo;
            }

            ui64 GetSeqNo() const {
                return SeqNo;
            }

        public:
            bool HasEvent() const {
                return bool(Event);
            }

            const THolder<IEventBase>& GetEvent() const {
                return Event;
            }

            const TIntrusivePtr<TEventSerializedData>& GetBuffer() const {
                return Buffer;
            }

            THolder<IEventBase> ReleaseEvent() {
                return std::move(Event);
            }

            TIntrusivePtr<TEventSerializedData> ReleaseBuffer() {
                return std::move(Buffer);
            }

        public:
            const ui32 Type;
            const TActorId Sender;

        private:
            THolder<IEventBase> Event;
            TIntrusivePtr<TEventSerializedData> Buffer;
            ui64 SeqNo = 0;
        };
    };

    namespace NTabletPipe {
        class IConnectAcceptor {
        public:
            virtual ~IConnectAcceptor() {}

            // Creates and activated server, returns serverId.
            // Created server will forward messages to the specified recipent.
            virtual TActorId Accept(TEvTabletPipe::TEvConnect::TPtr &ev, TActorIdentity owner, TActorId recipient,
                                    bool leader = true, ui64 generation = 0) = 0;

            // Rejects connect with an error.
            virtual void Reject(TEvTabletPipe::TEvConnect::TPtr &ev, TActorIdentity owner, NKikimrProto::EReplyStatus status, bool leader = true) = 0;

            // Stop all servers, gracefully notifying clients.
            virtual void Stop(TActorIdentity owner) = 0;

            // Destroys all servers, created by Accept or Enqueue.
            virtual void Detach(TActorIdentity owner) = 0;

            // Creates an inactive server, returns server Id.
            // Owner of context is not captured at this time.
            virtual TActorId Enqueue(TEvTabletPipe::TEvConnect::TPtr &ev, TActorIdentity owner) = 0;

            // Activates all inactive servers, created by Enqueue.
            // All activated servers will forward messages to the specified recipent.
            virtual void Activate(TActorIdentity owner, TActorId recipientId, bool leader = true, ui64 generation = 0) = 0;

            // Cleanup resources after reset
            virtual void Erase(TEvTabletPipe::TEvServerDestroyed::TPtr &ev) = 0;

            virtual bool IsActive() const = 0;
            virtual bool IsStopped() const = 0;
        };

        struct TClientRetryPolicy {
            ui32 RetryLimitCount = std::numeric_limits<ui32>::max();
            TDuration MinRetryTime = TDuration::MilliSeconds(10);
            TDuration MaxRetryTime = TDuration::Minutes(10);
            ui32 BackoffMultiplier = 2;
            bool DoFirstRetryInstantly = true;

            operator bool() const {
                return RetryLimitCount != 0;
            }

            static TClientRetryPolicy WithoutRetries() {
                return {
                    .RetryLimitCount = {},
                    .MinRetryTime = {},
                    .MaxRetryTime = {},
                    .BackoffMultiplier = {},
                    .DoFirstRetryInstantly = {},
                    };
            }

            static TClientRetryPolicy WithRetries() {
                return {};
            }
        };

        struct TClientRetryState {
            bool IsAllowedToRetry(TDuration& wait, const TClientRetryPolicy& policy);
            TDuration MakeCheckDelay();
        protected:
            ui64 RetryNumber = 0;
            TDuration RetryDuration;
        };

        struct TClientConfig {
            bool ConnectToUserTablet = false;
            bool AllowFollower = false;
            bool ForceFollower = false;
            bool ForceLocal = false;
            bool PreferLocal = false;
            bool CheckAliveness = false;
            bool ExpectShutdown = false;
            TClientRetryPolicy RetryPolicy;

            TClientConfig()
                : RetryPolicy(TClientRetryPolicy::WithoutRetries())
            {}

            TClientConfig(TClientRetryPolicy retryPolicy)
                : RetryPolicy(std::move(retryPolicy))
            {}
        };

        // Returns client actor.
        IActor* CreateClient(const TActorId& owner, ui64 tabletId, const TClientConfig& config = TClientConfig());

        // Sends data via client actor.
        // Payload will be delivered as a event to the owner of server.
        // Sender field in that event will be taken from the passed context.
        // Recipient field in that event will be filled by serverId.
        void SendData(const TActorContext& ctx, const TActorId& clientId, IEventBase* payload, ui64 cookie = 0, NWilson::TTraceId traceId = {});
        void SendData(const TActorContext& ctx, const TActorId& clientId, ui32 eventType, TIntrusivePtr<TEventSerializedData> buffer, ui64 cookie = 0, NWilson::TTraceId traceId = {});
        void SendData(TActorId self, TActorId clientId, IEventBase* payload, ui64 cookie = 0, NWilson::TTraceId traceId = {});
        void SendData(TActorId self, TActorId clientId, THolder<IEventBase>&& payload, ui64 cookie = 0, NWilson::TTraceId traceId = {});
        void SendDataWithSeqNo(TActorId self, TActorId clientId, IEventBase* payload, ui64 seqNo, ui64 cookie = 0, NWilson::TTraceId traceId = {});
        void SendData(TActorId self, TActorId clientId, ui32 eventType, TIntrusivePtr<TEventSerializedData> buffer, ui64 cookie = 0, NWilson::TTraceId traceId = {});

        // Shutdown client actor.
        void ShutdownClient(const TActorContext& ctx, const TActorId& clientId);

        // Destroys client actor.
        void CloseClient(const TActorContext& ctx, const TActorId& clientId);
        void CloseClient(TActorIdentity self, TActorId clientId);
        void CloseAndForgetClient(TActorIdentity self, TActorId &clientId);

        // Returns server actor in inactive state.
        IActor* CreateServer(ui64 tabletId, const TActorId& clientId, const TActorId& interconnectSession, ui32 features, ui64 connectCookie);

        // Promotes server actor to the active state.
        void ActivateServer(ui64 tabletId, TActorId serverId, TActorIdentity owner, TActorId recipientId, bool leader, ui64 generation = 0);

        // Destroys server actor.
        void CloseServer(TActorIdentity owner, TActorId serverId);

        // Returns a controller for handling connects.
        IConnectAcceptor* CreateConnectAcceptor(ui64 tabletId);
    }
}
