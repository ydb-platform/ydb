#include "interconnect_tcp_proxy.h"
#include "interconnect_tcp_session.h"
#include "interconnect_handshake.h"

#include <ydb/library/actors/core/probes.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/util/datetime.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <library/cpp/monlib/service/pages/templates.h>

namespace NActors {
    LWTRACE_USING(ACTORLIB_PROVIDER);

    template<typename T>
    T Coalesce(T&& x) {
        return x;
    }

    template<typename T, typename T2, typename... TRest>
    typename std::common_type<T, T2, TRest...>::type Coalesce(T&& first, T2&& mid, TRest&&... rest) {
        if (first != typename std::remove_reference<T>::type()) {
            return first;
        } else {
            return Coalesce(std::forward<T2>(mid), std::forward<TRest>(rest)...);
        }
    }

    TInterconnectSessionTCP::TInterconnectSessionTCP(TInterconnectProxyTCP* const proxy, TSessionParams params)
        : TActor(&TInterconnectSessionTCP::StateFunc)
        , Created(TInstant::Now())
        , Proxy(proxy)
        , CloseOnIdleWatchdog(GetCloseOnIdleTimeout(), std::bind(&TThis::OnCloseOnIdleTimerHit, this))
        , LostConnectionWatchdog(GetLostConnectionTimeout(), std::bind(&TThis::OnLostConnectionTimerHit, this))
        , Params(std::move(params))
        , TotalOutputQueueSize(0)
        , OutputStuckFlag(false)
        , OutputQueueUtilization(16)
        , OutputCounter(0ULL)
    {
        Proxy->Metrics->SetConnected(0);
        ReceiveContext.Reset(new TReceiveContext);
    }

    TInterconnectSessionTCP::~TInterconnectSessionTCP() {
        // close socket ASAP when actor system is being shut down
        if (Socket) {
            Socket->Shutdown(SHUT_RDWR);
        }
        if (XdcSocket) {
            XdcSocket->Shutdown(SHUT_RDWR);
        }
    }

    void TInterconnectSessionTCP::Init() {
        auto destroyCallback = [as = TlsActivationContext->ExecutorThread.ActorSystem, id = Proxy->Common->DestructorId](THolder<IEventBase> event) {
            as->Send(id, event.Release());
        };
        Pool.ConstructInPlace(Proxy->Common, std::move(destroyCallback));
        ChannelScheduler.ConstructInPlace(Proxy->PeerNodeId, Proxy->Common->ChannelsConfig, Proxy->Metrics, *Pool,
            Proxy->Common->Settings.MaxSerializedEventSize, Params);

        LOG_INFO(*TlsActivationContext, NActorsServices::INTERCONNECT_STATUS, "[%u] session created", Proxy->PeerNodeId);
        SetPrefix(Sprintf("Session %s [node %" PRIu32 "]", SelfId().ToString().data(), Proxy->PeerNodeId));
        SendUpdateToWhiteboard();
    }

    void TInterconnectSessionTCP::CloseInputSession() {
        Send(ReceiverId, new TEvInterconnect::TEvCloseInputSession);
    }

    void TInterconnectSessionTCP::Handle(TEvTerminate::TPtr& ev) {
        Terminate(ev->Get()->Reason);
    }

    void TInterconnectSessionTCP::HandlePoison() {
        Terminate(TDisconnectReason());
    }

    void TInterconnectSessionTCP::Terminate(TDisconnectReason reason) {
        LOG_INFO_IC_SESSION("ICS01", "socket: %" PRIi64 " reason# %s", (Socket ? i64(*Socket) : -1), reason.ToString().data());

        IActor::InvokeOtherActor(*Proxy, &TInterconnectProxyTCP::UnregisterSession, this);
        ShutdownSocket(std::move(reason));

        for (const auto& kv : Subscribers) {
            Send(kv.first, new TEvInterconnect::TEvNodeDisconnected(Proxy->PeerNodeId), 0, kv.second);
        }
        Proxy->Metrics->SubSubscribersCount(Subscribers.size());
        Subscribers.clear();

        for (auto& d : DelayedEvents) {
            d.Span.EndError("nondelivery");
            TActivationContext::Send(IEventHandle::ForwardOnNondelivery(d.Event, TEvents::TEvUndelivered::Disconnected));
        }
        DelayedEvents.clear();

        ChannelScheduler->ForEach([&](TEventOutputChannel& channel) {
            channel.NotifyUndelivered();
        });

        if (ReceiverId) {
            Send(ReceiverId, new TEvents::TEvPoisonPill);
        }

        SendUpdateToWhiteboard(false);

        Proxy->Metrics->SubOutputBuffersTotalSize(TotalOutputQueueSize);
        Proxy->Metrics->SubInflightDataAmount(InflightDataAmount);

        LOG_INFO(*TlsActivationContext, NActorsServices::INTERCONNECT_STATUS, "[%u] session destroyed", Proxy->PeerNodeId);

        if (!Subscribers.empty()) {
            Proxy->Metrics->SubSubscribersCount(Subscribers.size());
        }

        TActor::PassAway();
    }

    void TInterconnectSessionTCP::PassAway() {
        Y_ABORT("TInterconnectSessionTCP::PassAway() can't be called directly");
    }

    void TInterconnectSessionTCP::Enqueue(STATEFN_SIG) {
        Proxy->ValidateEvent(ev, "Enqueue");

        LOG_DEBUG_IC_SESSION("ICS02", "send event from: %s to: %s", ev->Sender.ToString().data(), ev->Recipient.ToString().data());
        ++MessagesGot;

        ui16 evChannel = ev->GetChannel();
        auto& oChannel = ChannelScheduler->GetOutputChannel(evChannel);
        const bool wasWorking = oChannel.IsWorking();

        const auto [dataSize, event] = oChannel.Push(*ev);
        LWTRACK(ForwardEvent, event->Orbit, Proxy->PeerNodeId, event->Descr.Type, event->Descr.Flags, LWACTORID(event->Descr.Recipient), LWACTORID(event->Descr.Sender), event->Descr.Cookie, event->EventSerializedSize);

        TotalOutputQueueSize += dataSize;
        Proxy->Metrics->AddOutputBuffersTotalSize(dataSize);
        if (!wasWorking) {
            // this channel has returned to work -- it was empty and this we have just put first event in the queue
            ChannelScheduler->AddToHeap(oChannel, EqualizeCounter);
        }

        SetOutputStuckFlag(true);
        ++NumEventsInQueue;
        RearmCloseOnIdle();

        LWTRACK(EnqueueEvent, event->Orbit, Proxy->PeerNodeId, NumEventsInQueue, GetWriteBlockedTotal(), evChannel, oChannel.GetQueueSize(), oChannel.GetBufferedAmountOfData());

        // check for overloaded queues
        ui64 sendBufferDieLimit = Proxy->Common->Settings.SendBufferDieLimitInMB * ui64(1 << 20);
        if (sendBufferDieLimit != 0 && TotalOutputQueueSize > sendBufferDieLimit) {
            LOG_ERROR_IC_SESSION("ICS03", "socket: %" PRIi64 " output queue is overloaded, actual %" PRIu64 " bytes, limit is %" PRIu64,
                         Socket ? i64(*Socket) : -1, TotalOutputQueueSize, sendBufferDieLimit);
            return Terminate(TDisconnectReason::QueueOverload());
        }

        ui64 outputBuffersTotalSizeLimit = Proxy->Common->Settings.OutputBuffersTotalSizeLimitInMB * ui64(1 << 20);
        if (outputBuffersTotalSizeLimit != 0 && static_cast<ui64>(Proxy->Metrics->GetOutputBuffersTotalSize()) > outputBuffersTotalSizeLimit) {
            LOG_ERROR_IC_SESSION("ICS77", "Exceeded total limit on output buffers size");
            if (AtomicTryLock(&Proxy->Common->StartedSessionKiller)) {
                CreateSessionKillingActor(Proxy->Common);
            }
        }

        IssueRam(true);
    }

    void TInterconnectSessionTCP::Forward(STATEFN_SIG) {
        Proxy->ValidateEvent(ev, "Forward");

        if (ev->Flags & IEventHandle::FlagSubscribeOnSession) {
            Subscribe(ev);
        }

        if (Y_UNLIKELY(Proxy->Common->Settings.EventDelay)) {
            auto& d = DelayedEvents.emplace_back();
            d.Event = std::move(ev);
            if (Y_UNLIKELY(d.Event->TraceId)) {
                d.Span = NWilson::TSpan(15 /*max verbosity*/, std::move(d.Event->TraceId), "Interconnect.Delay");
                // Reparent event to the delay span
                d.Event->TraceId = d.Span.GetTraceId();
            }
            Schedule(Proxy->Common->Settings.EventDelay, new TEvInterconnect::TEvForwardDelayed);
        } else {
            Enqueue(ev);
        }
    }

    void TInterconnectSessionTCP::ForwardDelayed() {
        Y_ABORT_UNLESS(!DelayedEvents.empty());
        auto d = std::move(DelayedEvents.front());
        DelayedEvents.pop_front();
        d.Span.End();
        Enqueue(d.Event);
    }

    void TInterconnectSessionTCP::Subscribe(STATEFN_SIG) {
        LOG_DEBUG_IC_SESSION("ICS04", "subscribe for session state for %s", ev->Sender.ToString().data());
        const auto [it, inserted] = Subscribers.emplace(ev->Sender, ev->Cookie);
        if (inserted) {
            Proxy->Metrics->IncSubscribersCount();
        } else {
            it->second = ev->Cookie;
        }
        Send(ev->Sender, new TEvInterconnect::TEvNodeConnected(Proxy->PeerNodeId), 0, ev->Cookie);
    }

    void TInterconnectSessionTCP::Unsubscribe(STATEFN_SIG) {
        LOG_DEBUG_IC_SESSION("ICS05", "unsubscribe for session state for %s", ev->Sender.ToString().data());
        Proxy->Metrics->SubSubscribersCount(Subscribers.erase(ev->Sender));
    }

    THolder<TEvHandshakeAck> TInterconnectSessionTCP::ProcessHandshakeRequest(TEvHandshakeAsk::TPtr& ev) {
        TEvHandshakeAsk *msg = ev->Get();

        // close existing input session, if any, and do nothing upon its destruction
        ReestablishConnection({}, false, TDisconnectReason::NewSession());
        const ui64 lastInputSerial = ReceiveContext->LockLastPacketSerialToConfirm();

        LOG_INFO_IC_SESSION("ICS08", "incoming handshake Self# %s Peer# %s Counter# %" PRIu64 " LastInputSerial# %" PRIu64,
            msg->Self.ToString().data(), msg->Peer.ToString().data(), msg->Counter, lastInputSerial);

        return MakeHolder<TEvHandshakeAck>(msg->Peer, lastInputSerial, Params);
    }

    void TInterconnectSessionTCP::SetNewConnection(TEvHandshakeDone::TPtr& ev) {
        if (ReceiverId) {
            // upon destruction of input session actor invoke this callback again
            ReestablishConnection(std::move(ev), false, TDisconnectReason::NewSession());
            return;
        }

        LOG_INFO_IC_SESSION("ICS09", "handshake done sender: %s self: %s peer: %s socket: %" PRIi64,
            ev->Sender.ToString().data(), ev->Get()->Self.ToString().data(), ev->Get()->Peer.ToString().data(),
            i64(*ev->Get()->Socket));

        NewConnectionSet = TActivationContext::Now();
        BytesWrittenToSocket = 0;

        SendBufferSize = ev->Get()->Socket->GetSendBufferSize();
        Socket = std::move(ev->Get()->Socket);
        XdcSocket = std::move(ev->Get()->XdcSocket);

        // there may be a race
        const ui64 nextPacket = Max(LastConfirmed, ev->Get()->NextPacket);

        // arm watchdogs
        RearmCloseOnIdle();

        // reset activity timestamps
        LastInputActivityTimestamp = LastPayloadActivityTimestamp = TActivationContext::Monotonic();

        LOG_INFO_IC_SESSION("ICS10", "traffic start");

        // reset parameters to initial values
        WriteBlockedByFullSendBuffer = false;
        ReceiveContext->MainWriteBlocked = false;
        ReceiveContext->XdcWriteBlocked = false;
        ReceiveContext->MainReadPending = false;
        ReceiveContext->XdcReadPending = false;

        // create input session actor
        ReceiveContext->UnlockLastPacketSerialToConfirm();
        auto actor = MakeHolder<TInputSessionTCP>(SelfId(), Socket, XdcSocket, ReceiveContext, Proxy->Common,
            Proxy->Metrics, Proxy->PeerNodeId, nextPacket, GetDeadPeerTimeout(), Params);
        ReceiverId = RegisterWithSameMailbox(actor.Release());

        // register our socket in poller actor
        LOG_DEBUG_IC_SESSION("ICS11", "registering socket in PollerActor");
        const bool success = Send(MakePollerActorId(), new TEvPollerRegister(Socket, ReceiverId, SelfId()));
        Y_ABORT_UNLESS(success);
        if (XdcSocket) {
            const bool success = Send(MakePollerActorId(), new TEvPollerRegister(XdcSocket, ReceiverId, SelfId()));
            Y_ABORT_UNLESS(success);
        }

        LostConnectionWatchdog.Disarm();
        Proxy->Metrics->SetConnected(1);
        LOG_INFO(*TlsActivationContext, NActorsServices::INTERCONNECT_STATUS, "[%u] connected", Proxy->PeerNodeId);

        // arm pinger timer
        ResetFlushLogic();

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // REINITIALIZE SEND QUEUE
        //
        // scan through send queue and leave only those packets who have data -- we will simply resend them; drop all other
        // auxiliary packets; also reset packet metrics to zero to start sending from the beginning
        // also reset send queue

        // drop confirmed packets first as we do not need unwanted retransmissions
        OutgoingStream.RewindToEnd();
        XdcStream.RewindToEnd();
        XdcOffset = Max<size_t>();
        OutgoingOffset = 0;
        OutgoingIndex = SendQueue.size();
        DropConfirmed(nextPacket);
        OutgoingStream.Rewind();
        OutOfBandStream = {};
        XdcStream.Rewind();
        OutgoingOffset = XdcOffset = 0;
        OutgoingIndex = 0;
        ForcedWriteLength = 0;

        const ui64 serial = OutputCounter - SendQueue.size() + 1;
        Y_ABORT_UNLESS(serial > LastConfirmed, "%s serial# %" PRIu64 " LastConfirmed# %" PRIu64, LogPrefix.data(), serial, LastConfirmed);
        LOG_DEBUG_IC_SESSION("ICS06", "rewind SendQueue size# %zu LastConfirmed# %" PRIu64 " NextSerial# %" PRIu64,
            SendQueue.size(), LastConfirmed, serial);

        SwitchStuckPeriod();

        LastHandshakeDone = TActivationContext::Now();

        GenerateTraffic();
    }

    void TInterconnectSessionTCP::Handle(TEvUpdateFromInputSession::TPtr& ev) {
        if (ev->Sender == ReceiverId) {
            TEvUpdateFromInputSession& msg = *ev->Get();

            // update ping time
            Ping = msg.Ping;
            LWPROBE(UpdateFromInputSession, Proxy->PeerNodeId, Ping.MillisecondsFloat());

            bool needConfirm = false;

            // update activity timer for dead peer checker
            LastInputActivityTimestamp = TActivationContext::Monotonic();

            if (msg.NumDataBytes) {
                UnconfirmedBytes += msg.NumDataBytes;
                if (UnconfirmedBytes >= GetTotalInflightAmountOfData() / 4) {
                    needConfirm = true;
                } else {
                    SetForcePacketTimestamp(Proxy->Common->Settings.ForceConfirmPeriod);
                }

                // reset payload watchdog that controls close-on-idle behaviour
                LastPayloadActivityTimestamp = TActivationContext::Monotonic();
                RearmCloseOnIdle();
            }

            LWPROBE_IF_TOO_LONG(SlowICDropConfirmed, Proxy->PeerNodeId, ms) {
                DropConfirmed(msg.ConfirmedByInput);
            }

            // if we haven't generated any packets, then make a lone Flush packet without any data
            if (needConfirm && Socket) {
                ++ConfirmPacketsForcedBySize;
                MakePacket(false);
            }

            GenerateTraffic();

            for (;;) {
                switch (EUpdateState state = ReceiveContext->UpdateState) {
                    case EUpdateState::NONE:
                    case EUpdateState::CONFIRMING:
                        Y_ABORT("unexpected state");

                    case EUpdateState::INFLIGHT:
                        // this message we are processing was the only one in flight, so we can reset state to NONE here
                        if (ReceiveContext->UpdateState.compare_exchange_weak(state, EUpdateState::NONE)) {
                            return;
                        }
                        break;

                    case EUpdateState::INFLIGHT_AND_PENDING:
                        // there is more messages pending from the input session actor, so we have to inform it to release
                        // that message
                        if (ReceiveContext->UpdateState.compare_exchange_weak(state, EUpdateState::CONFIRMING)) {
                            Send(ev->Sender, new TEvConfirmUpdate);
                            return;
                        }
                        break;
                }
            }
        }
    }

    void TInterconnectSessionTCP::IssueRam(bool batching) {
        const auto& batchPeriod = Proxy->Common->Settings.BatchPeriod;
        if (!RamInQueue || (!batching && RamInQueue->Batching && batchPeriod != TDuration())) {
            auto ev = std::make_unique<TEvRam>(batching);
            RamInQueue = ev.get();
            auto handle = std::make_unique<IEventHandle>(SelfId(), SelfId(), ev.release());
            if (batching && batchPeriod != TDuration()) {
                TActivationContext::Schedule(batchPeriod, handle.release());
            } else {
                TActivationContext::Send(handle.release());
            }
            LWPROBE(StartRam, Proxy->PeerNodeId);
            RamStartedCycles = GetCycleCountFast();
        }
    }

    void TInterconnectSessionTCP::HandleRam(TEvRam::TPtr& ev) {
        if (ev->Get() == RamInQueue) {
            LWPROBE(FinishRam, Proxy->PeerNodeId, NHPTimer::GetSeconds(GetCycleCountFast() - ev->SendTime) * 1000.0);
            RamInQueue = nullptr;
            GenerateTraffic();
        }
    }

    void TInterconnectSessionTCP::GenerateTraffic() {
        if (!TimeLimit) {
            TimeLimit.emplace(GetMaxCyclesPerEvent());
        }

        // generate ping request, if needed
        IssuePingRequest();

        while (Socket) {
            ProducePackets();
            if (!Socket) {
                return;
            }

            WriteData();
            if (!Socket) {
                return;
            }

            bool canProducePackets;
            bool canWriteData;

            canProducePackets = NumEventsInQueue && InflightDataAmount < GetTotalInflightAmountOfData() &&
                GetUnsentSize() < GetUnsentLimit();

            canWriteData = ((OutgoingStream || OutOfBandStream) && !ReceiveContext->MainWriteBlocked) ||
                (XdcStream && !ReceiveContext->XdcWriteBlocked);

            if (!canProducePackets && !canWriteData) {
                SetEnoughCpu(true); // we do not starve
                StarvingInRow = 0;
                break;
            } else if (TimeLimit->CheckExceeded()) {
                SetEnoughCpu(++StarvingInRow < StarvingInRowForNotEnoughCpu);
                IssueRam(false);
                break;
            }
        }

        // account traffic changes
        ChannelScheduler->ForEach([](TEventOutputChannel& channel) {
            channel.AccountTraffic();
        });

        // equalize channel weights
        EqualizeCounter += ChannelScheduler->Equalize();
    }

    void TInterconnectSessionTCP::ProducePackets() {
        // first, we create as many data packets as we can generate under certain conditions; they include presence
        // of events in channels queues and in flight fitting into requested limit; after we hit one of these conditions
        // we exit cycle
        static constexpr ui32 maxBytesToProduce = 64 * 1024;
        ui32 bytesProduced = 0;
        while (NumEventsInQueue && InflightDataAmount < GetTotalInflightAmountOfData() && GetUnsentSize() < GetUnsentLimit()) {
            if ((bytesProduced && TimeLimit->CheckExceeded()) || bytesProduced >= maxBytesToProduce) {
                break;
            }
            try {
                bytesProduced += MakePacket(true);
            } catch (const TExSerializedEventTooLarge& ex) {
                // terminate session if the event can't be serialized properly
                LOG_CRIT_IC("ICS31", "serialized event Type# 0x%08" PRIx32 " is too large", ex.Type);
                return Terminate(TDisconnectReason::EventTooLarge());
            }
        }
    }

    void TInterconnectSessionTCP::StartHandshake() {
        LOG_INFO_IC_SESSION("ICS15", "start handshake");
        IActor::InvokeOtherActor(*Proxy, &TInterconnectProxyTCP::StartResumeHandshake, ReceiveContext->LockLastPacketSerialToConfirm());
    }

    void TInterconnectSessionTCP::ReestablishConnectionWithHandshake(TDisconnectReason reason) {
        ReestablishConnection({}, true, std::move(reason));
    }

    void TInterconnectSessionTCP::ReestablishConnection(TEvHandshakeDone::TPtr&& ev, bool startHandshakeOnSessionClose,
            TDisconnectReason reason) {
        if (Socket) {
            LOG_INFO_IC_SESSION("ICS13", "reestablish connection");
            ShutdownSocket(std::move(reason)); // stop sending/receiving on socket
            PendingHandshakeDoneEvent = std::move(ev);
            StartHandshakeOnSessionClose = startHandshakeOnSessionClose;
            if (!ReceiverId) {
                ReestablishConnectionExecute();
            }
        }
    }

    void TInterconnectSessionTCP::OnDisconnect(TEvSocketDisconnect::TPtr& ev) {
        if (ev->Sender == ReceiverId) {
            if (ev->Get()->Reason == TDisconnectReason::EndOfStream() && !NumEventsInQueue && OutputCounter == LastConfirmed) {
                return Terminate(ev->Get()->Reason);
            }

            const bool wasConnected(Socket);
            LOG_INFO_IC_SESSION("ICS07", "socket disconnect %" PRIi64 " reason# %s", Socket ? i64(*Socket) : -1, ev->Get()->Reason.ToString().data());
            ReceiverId = TActorId(); // reset receiver actor id as we have no more receiver yet
            if (wasConnected) {
                // we were sucessfully connected and did not expect failure, so it arrived from the input side; we should
                // restart handshake process, closing our part of socket first
                ShutdownSocket(ev->Get()->Reason);
                StartHandshake();
            } else {
                ReestablishConnectionExecute();
            }
        }
    }

    void TInterconnectSessionTCP::ShutdownSocket(TDisconnectReason reason) {
        if (Socket) {
            if (const TString& s = reason.ToString()) {
                Proxy->Metrics->IncDisconnectByReason(s);
            }

            LOG_INFO_IC_SESSION("ICS25", "shutdown socket, reason# %s", reason.ToString().data());
            Proxy->UpdateErrorStateLog(TActivationContext::Now(), "close_socket", reason.ToString().data());
            Socket->Shutdown(SHUT_RDWR);
            Socket.Reset();
            Proxy->Metrics->IncDisconnections();
            CloseOnIdleWatchdog.Disarm();
            LostConnectionWatchdog.Rearm(SelfId());
            Proxy->Metrics->SetConnected(0);
            LOG_INFO(*TlsActivationContext, NActorsServices::INTERCONNECT_STATUS, "[%u] disconnected", Proxy->PeerNodeId);
        }
        if (XdcSocket) {
            XdcSocket->Shutdown(SHUT_RDWR);
            XdcSocket.Reset();
        }
    }

    void TInterconnectSessionTCP::ReestablishConnectionExecute() {
        bool startHandshakeOnSessionClose = std::exchange(StartHandshakeOnSessionClose, false);
        TEvHandshakeDone::TPtr ev = std::move(PendingHandshakeDoneEvent);

        if (startHandshakeOnSessionClose) {
            StartHandshake();
        } else if (ev) {
            SetNewConnection(ev);
        }
    }

    void TInterconnectSessionTCP::Handle(TEvPollerReady::TPtr& ev) {
        LOG_DEBUG_IC_SESSION("ICS29", "HandleReadyWrite WriteBlockedByFullSendBuffer# %s",
            WriteBlockedByFullSendBuffer ? "true" : "false");

        auto *msg = ev->Get();
        bool useful = false;
        bool readPending = false;

        if (msg->Socket == Socket) {
            useful = std::exchange(ReceiveContext->MainWriteBlocked, false);
            readPending = ReceiveContext->MainReadPending;
        } else if (msg->Socket == XdcSocket) {
            useful = std::exchange(ReceiveContext->XdcWriteBlocked, false);
            readPending = ReceiveContext->XdcReadPending;
        }

        if (useful) {
            Proxy->Metrics->IncUsefulWriteWakeups();
        } else if (!ev->Cookie) {
            Proxy->Metrics->IncSpuriousWriteWakeups();
        }

        if (Params.Encryption && readPending && ev->Sender != ReceiverId) {
            Send(ReceiverId, ev->Release().Release());
        }

        GenerateTraffic();
    }

    void TInterconnectSessionTCP::Handle(TEvPollerRegisterResult::TPtr ev) {
        auto *msg = ev->Get();
        bool sendPollerReady = false;

        if (msg->Socket == Socket) {
            PollerToken = std::move(msg->PollerToken);
            sendPollerReady = ReceiveContext->MainWriteBlocked;
        } else if (msg->Socket == XdcSocket) {
            XdcPollerToken = std::move(msg->PollerToken);
            sendPollerReady = ReceiveContext->XdcWriteBlocked;
        }

        if (sendPollerReady) {
            Send(SelfId(), new TEvPollerReady(msg->Socket, false, true));
        }
    }

    void TInterconnectSessionTCP::WriteData() {
        // total bytes written during this call
        ui64 written = 0;

        auto process = [&](NInterconnect::TOutgoingStream& stream, const TIntrusivePtr<NInterconnect::TStreamSocket>& socket,
                const TPollerToken::TPtr& token, bool *writeBlocked, size_t maxBytes) {
            size_t totalWritten = 0;

            if (stream && socket && !*writeBlocked) {
                for (;;) {
                    if (const ssize_t r = Write(stream, *socket, maxBytes); r > 0) {
                        stream.Advance(r);
                        totalWritten += r;
                    } else if (r == -1) {
                        if (token && socket->RequestWriteNotificationAfterWouldBlock(*token)) {
                            continue; // we can try again
                        }
                        *writeBlocked = true;
                    } else if (r == 0) {
                        // error condition
                    } else {
                        Y_UNREACHABLE();
                    }
                    break;
                }
            }

            written += totalWritten;
            return totalWritten;
        };

        auto sendQueueIt = SendQueue.begin() + OutgoingIndex;
        static constexpr size_t maxBytesAtOnce = 256 * 1024;
        size_t bytesToSendInMain = maxBytesAtOnce;

        Y_DEBUG_ABORT_UNLESS(OutgoingIndex < SendQueue.size() || (OutgoingIndex == SendQueue.size() && !OutgoingOffset && !OutgoingStream));

        if (OutOfBandStream) {
            bytesToSendInMain = 0;

            if (!ForcedWriteLength && OutgoingOffset) {
                ForcedWriteLength = 1; // send at least one byte from current packet
            }

            // align send up to packet boundary
            size_t offset = OutgoingOffset;
            for (auto it = sendQueueIt; ForcedWriteLength; ++it, offset = 0) {
                Y_DEBUG_ABORT_UNLESS(it != SendQueue.end());
                bytesToSendInMain += it->PacketSize - offset; // send remainder of current packet
                ForcedWriteLength -= Min(it->PacketSize - offset, ForcedWriteLength);
            }
        }

        if (bytesToSendInMain) {
            const size_t w = process(OutgoingStream, Socket, PollerToken, &ReceiveContext->MainWriteBlocked, bytesToSendInMain);

            // adjust sending queue iterator
            for (OutgoingOffset += w; OutgoingOffset && sendQueueIt->PacketSize <= OutgoingOffset; ++sendQueueIt, ++OutgoingIndex) {
                OutgoingOffset -= sendQueueIt->PacketSize;
            }

            BytesWrittenToSocket += w;

            if (OutOfBandStream) {
                BytesAlignedForOutOfBand += w;
                bytesToSendInMain -= w;
            }

            ForcedWriteLength = Socket ? Socket->ExpectedWriteLength() : 0;
        }

        if (!bytesToSendInMain && !ForcedWriteLength) {
            if (const size_t w = process(OutOfBandStream, Socket, PollerToken, &ReceiveContext->MainWriteBlocked, maxBytesAtOnce)) {
                OutOfBandStream.DropFront(w);
                BytesWrittenToSocket += w;
                OutOfBandBytesSent += w;
            }
        }

        if (const size_t w = process(XdcStream, XdcSocket, XdcPollerToken, &ReceiveContext->XdcWriteBlocked, maxBytesAtOnce)) {
            XdcBytesSent += w;
            XdcOffset += w;
        }

        if (written) {
            Proxy->Metrics->AddTotalBytesWritten(written);
        }

        DropConfirmed(LastConfirmed);

        const bool writeBlockedByFullSendBuffer = ReceiveContext->MainWriteBlocked || ReceiveContext->XdcWriteBlocked;
        if (WriteBlockedByFullSendBuffer < writeBlockedByFullSendBuffer) { // became blocked
            WriteBlockedCycles = GetCycleCountFast();
            LOG_DEBUG_IC_SESSION("ICS18", "hit send buffer limit");
        } else if (writeBlockedByFullSendBuffer < WriteBlockedByFullSendBuffer) { // became unblocked
            WriteBlockedTotal += TDuration::Seconds(NHPTimer::GetSeconds(GetCycleCountFast() - WriteBlockedCycles));
        }
        WriteBlockedByFullSendBuffer = writeBlockedByFullSendBuffer;
    }

    ssize_t TInterconnectSessionTCP::Write(NInterconnect::TOutgoingStream& stream, NInterconnect::TStreamSocket& socket,
            size_t maxBytes) {
        LWPROBE_IF_TOO_LONG(SlowICWriteData, Proxy->PeerNodeId, ms) {
            constexpr ui32 iovLimit = 256;

            ui32 maxElementsInIOV;
            if (Params.Encryption) {
                maxElementsInIOV = 1;
            } else {
#if defined(_win_)
                maxElementsInIOV = 1;
#elif defined(_linux_)
                maxElementsInIOV = Min<ui32>(iovLimit, sysconf(_SC_IOV_MAX));
#else
                maxElementsInIOV = 64;
#endif
            }

            TStackVec<TConstIoVec, iovLimit> wbuffers;

            stream.ProduceIoVec(wbuffers, maxElementsInIOV, maxBytes);
            Y_ABORT_UNLESS(!wbuffers.empty());

            TString err;
            ssize_t r = 0;
            { // issue syscall with timing
                const ui64 begin = GetCycleCountFast();

                do {
                    if (wbuffers.size() == 1) {
                        auto& front = wbuffers.front();
                        r = socket.Send(front.Data, front.Size, &err);
                    } else {
                        r = socket.WriteV(reinterpret_cast<const iovec*>(wbuffers.data()), wbuffers.size());
                    }
                } while (r == -EINTR);

                const ui64 end = GetCycleCountFast();
                Proxy->Metrics->IncSendSyscalls((end - begin) * 1'000'000 / GetCyclesPerMillisecond());
            }

            if (r > 0) {
                return r;
            } else if (-r != EAGAIN && -r != EWOULDBLOCK) {
                const TString message = r == 0 ? "connection closed by peer"
                    : err ? err
                    : Sprintf("socket: %s", strerror(-r));
                LOG_NOTICE_NET(Proxy->PeerNodeId, "%s", message.data());
                if (r == 0 && !NumEventsInQueue && LastConfirmed == OutputCounter) {
                    Terminate(TDisconnectReason::EndOfStream());
                } else {
                    ReestablishConnectionWithHandshake(r == 0 ? TDisconnectReason::EndOfStream() : TDisconnectReason::FromErrno(-r));
                }
                return 0; // error indicator
            } else {
                return -1; // temporary error
            }
        }

        Y_UNREACHABLE();
    }

    void TInterconnectSessionTCP::SetForcePacketTimestamp(TDuration period) {
        if (period != TDuration::Max()) {
            // randomize period a bit
            period = TDuration::FromValue(period.GetValue() - RandomNumber<ui64>(period.GetValue() / 10));
            const TMonotonic when = TActivationContext::Monotonic() + period;
            if (when < ForcePacketTimestamp) {
                ForcePacketTimestamp = when;
                ScheduleFlush();
            }
        }
    }

    void TInterconnectSessionTCP::ScheduleFlush() {
        if (FlushSchedule.empty() || ForcePacketTimestamp < FlushSchedule.top()) {
            Schedule(ForcePacketTimestamp, new TEvFlush);
            FlushSchedule.push(ForcePacketTimestamp);
            MaxFlushSchedule = Max(MaxFlushSchedule, FlushSchedule.size());
            ++FlushEventsScheduled;
        }
    }

    void TInterconnectSessionTCP::HandleFlush() {
        const TMonotonic now = TActivationContext::Monotonic();
        while (FlushSchedule && now >= FlushSchedule.top()) {
            FlushSchedule.pop();
        }
        if (Socket) {
            if (now >= ForcePacketTimestamp) {
                ++ConfirmPacketsForcedByTimeout;
                ++FlushEventsProcessed;
                MakePacket(false); // just generate confirmation packet if we have preconditions for this
            } else if (ForcePacketTimestamp != TMonotonic::Max()) {
                ScheduleFlush();
            }
            GenerateTraffic();
        }
    }

    void TInterconnectSessionTCP::ResetFlushLogic() {
        ForcePacketTimestamp = TMonotonic::Max();
        UnconfirmedBytes = 0;
        const TDuration ping = Proxy->Common->Settings.PingPeriod;
        if (ping != TDuration::Zero() && !NumEventsInQueue) {
            SetForcePacketTimestamp(ping);
        }
    }

    ui32 TInterconnectSessionTCP::MakePacket(bool data, TMaybe<ui64> pingMask) {
        NInterconnect::TOutgoingStream& stream = data ? OutgoingStream : OutOfBandStream;

#ifndef NDEBUG
        const size_t outgoingStreamSizeBefore = stream.CalculateOutgoingSize();
        const size_t xdcStreamSizeBefore = XdcStream.CalculateOutgoingSize();
#endif

        stream.Align();
        XdcStream.Align();

        TTcpPacketOutTask packet(Params, stream, XdcStream);
        ui64 serial = 0;

        if (data) {
            // generate serial for this data packet
            serial = ++OutputCounter;

            // fill the data packet
            Y_ABORT_UNLESS(NumEventsInQueue);
            LWPROBE_IF_TOO_LONG(SlowICFillSendingBuffer, Proxy->PeerNodeId, ms) {
                FillSendingBuffer(packet, serial);
            }
            Y_ABORT_UNLESS(!packet.IsEmpty());

            InflightDataAmount += packet.GetDataSize();
            Proxy->Metrics->AddInflightDataAmount(packet.GetDataSize());
            if (InflightDataAmount > GetTotalInflightAmountOfData()) {
                Proxy->Metrics->IncInflyLimitReach();
            }

            if (AtomicGet(ReceiveContext->ControlPacketId) == 0) {
                AtomicSet(ReceiveContext->ControlPacketSendTimer, GetCycleCountFast());
                AtomicSet(ReceiveContext->ControlPacketId, OutputCounter);
            }

            // update payload activity timer
            LastPayloadActivityTimestamp = TActivationContext::Monotonic();
        } else if (pingMask) {
            serial = *pingMask;
        }

        const ui64 lastInputSerial = ReceiveContext->GetLastPacketSerialToConfirm();

        packet.Finish(serial, lastInputSerial);

        // count number of bytes pending for write
        const size_t packetSize = packet.GetPacketSize();

#ifndef NDEBUG
        const size_t outgoingStreamSizeAfter = stream.CalculateOutgoingSize();
        const size_t xdcStreamSizeAfter = XdcStream.CalculateOutgoingSize();

        Y_ABORT_UNLESS(outgoingStreamSizeAfter == outgoingStreamSizeBefore + packetSize &&
            xdcStreamSizeAfter == xdcStreamSizeBefore + packet.GetExternalSize(),
            "outgoingStreamSizeBefore# %zu outgoingStreamSizeAfter# %zu packetSize# %zu"
            " xdcStreamSizeBefore# %zu xdcStreamSizeAfter# %zu externalSize# %" PRIu32,
            outgoingStreamSizeBefore, outgoingStreamSizeAfter, packetSize,
            xdcStreamSizeBefore, xdcStreamSizeAfter, packet.GetExternalSize());
#endif

        // put outgoing packet metadata here
        if (data) {
            SendQueue.push_back(TOutgoingPacket{
                static_cast<ui32>(packetSize),
                static_cast<ui32>(packet.GetExternalSize())
            });
        }

        LOG_DEBUG_IC_SESSION("ICS22", "outgoing packet Serial# %" PRIu64 " Confirm# %" PRIu64 " DataSize# %" PRIu32
            " InflightDataAmount# %" PRIu64, serial, lastInputSerial, packet.GetDataSize(), InflightDataAmount);

        // reset forced packet sending timestamp as we have confirmed all received data
        ResetFlushLogic();

        ++PacketsGenerated;

        return packetSize;
    }

    void TInterconnectSessionTCP::DropConfirmed(ui64 confirm) {
        LOG_DEBUG_IC_SESSION("ICS23", "confirm count: %" PRIu64, confirm);

        Y_ABORT_UNLESS(LastConfirmed <= confirm && confirm <= OutputCounter,
            "%s confirm# %" PRIu64 " LastConfirmed# %" PRIu64 " OutputCounter# %" PRIu64,
            LogPrefix.data(), confirm, LastConfirmed, OutputCounter);
        LastConfirmed = confirm;

        std::optional<ui64> lastDroppedSerial;
        ui32 numDropped = 0;

        // drop confirmed packets; this also includes any auxiliary packets as their serial is set to zero, effectively
        // making Serial <= confirm true
        size_t bytesDropped = 0;
        size_t bytesDroppedFromXdc = 0;
        ui64 frontPacketSerial = OutputCounter - SendQueue.size() + 1;
        Y_DEBUG_ABORT_UNLESS(OutgoingIndex < SendQueue.size() || (OutgoingIndex == SendQueue.size() && !OutgoingOffset && !OutgoingStream),
            "OutgoingIndex# %zu SendQueue.size# %zu OutgoingOffset# %zu Unsent# %zu Total# %zu",
            OutgoingIndex, SendQueue.size(), OutgoingOffset, OutgoingStream.CalculateUnsentSize(),
            OutgoingStream.CalculateOutgoingSize());
        while (OutgoingIndex && frontPacketSerial <= confirm && SendQueue.front().ExternalSize <= XdcOffset) {
            auto& front = SendQueue.front();
            lastDroppedSerial.emplace(frontPacketSerial);
            XdcOffset -= front.ExternalSize;
            bytesDropped += front.PacketSize;
            bytesDroppedFromXdc += front.ExternalSize;
            ++numDropped;

            ++frontPacketSerial;
            SendQueue.pop_front();
            --OutgoingIndex;
        }

        if (!numDropped) {
            return;
        }

        const ui64 droppedDataAmount = bytesDropped + bytesDroppedFromXdc - sizeof(TTcpPacketHeader_v2) * numDropped;
        OutgoingStream.DropFront(bytesDropped);
        XdcStream.DropFront(bytesDroppedFromXdc);
        if (lastDroppedSerial) {
            ChannelScheduler->ForEach([&](TEventOutputChannel& channel) {
                channel.DropConfirmed(*lastDroppedSerial);
            });
        }

        PacketsConfirmed += numDropped;
        InflightDataAmount -= droppedDataAmount;
        Proxy->Metrics->SubInflightDataAmount(droppedDataAmount);
        LWPROBE(DropConfirmed, Proxy->PeerNodeId, droppedDataAmount, InflightDataAmount);

        LOG_DEBUG_IC_SESSION("ICS24", "exit InflightDataAmount: %" PRIu64 " bytes droppedDataAmount: %" PRIu64 " bytes"
            " dropped %" PRIu32 " packets", InflightDataAmount, droppedDataAmount, numDropped);

        Pool->Trim(); // send any unsent free requests

        RearmCloseOnIdle();
    }

    void TInterconnectSessionTCP::FillSendingBuffer(TTcpPacketOutTask& task, ui64 serial) {
        ui32 bytesGenerated = 0;

        Y_ABORT_UNLESS(NumEventsInQueue);
        while (NumEventsInQueue) {
            TEventOutputChannel *channel = ChannelScheduler->PickChannelWithLeastConsumedWeight();
            Y_DEBUG_ABORT_UNLESS(!channel->IsEmpty());

            // generate some data within this channel
            const ui64 netBefore = channel->GetBufferedAmountOfData();
            ui64 gross = 0;
            const bool eventDone = channel->FeedBuf(task, serial, &gross);
            channel->UnaccountedTraffic += gross;
            const ui64 netAfter = channel->GetBufferedAmountOfData();
            Y_DEBUG_ABORT_UNLESS(netAfter <= netBefore); // net amount should shrink
            const ui64 net = netBefore - netAfter; // number of net bytes serialized

            // adjust metrics for local and global queue size
            TotalOutputQueueSize -= net;
            Proxy->Metrics->SubOutputBuffersTotalSize(net);
            bytesGenerated += gross;
            Y_DEBUG_ABORT_UNLESS(!!net == !!gross && gross >= net, "net# %" PRIu64 " gross# %" PRIu64, net, gross);

            // return it back to queue or delete, depending on whether this channel is still working or not
            ChannelScheduler->FinishPick(gross, EqualizeCounter);

            // update some stats if the packet was fully serialized
            if (eventDone) {
                ++MessagesWrittenToBuffer;

                Y_ABORT_UNLESS(NumEventsInQueue);
                --NumEventsInQueue;

                if (!NumEventsInQueue) {
                    SetOutputStuckFlag(false);
                }
            }

            if (!gross) { // no progress -- almost full packet buffer
                break;
            }
        }

        Y_ABORT_UNLESS(bytesGenerated); // ensure we are not stalled in serialization
    }

    ui32 TInterconnectSessionTCP::CalculateQueueUtilization() {
        SwitchStuckPeriod();
        ui64 sumBusy = 0, sumPeriod = 0;
        for (auto iter = OutputQueueUtilization.begin(); iter != OutputQueueUtilization.end() - 1; ++iter) {
            sumBusy += iter->first;
            sumPeriod += iter->second;
        }
        return sumBusy * 1000000 / sumPeriod;
    }

    void TInterconnectSessionTCP::SendUpdateToWhiteboard(bool connected) {
        const ui32 utilization = Socket ? CalculateQueueUtilization() : 0;

        if (const auto& callback = Proxy->Common->UpdateWhiteboard) {
            enum class EFlag {
                GREEN,
                YELLOW,
                ORANGE,
                RED,
            };
            EFlag flagState = EFlag::RED;

            if (Socket) {
                flagState = EFlag::GREEN;

                do {
                    auto lastInputDelay = TActivationContext::Monotonic() - LastInputActivityTimestamp;
                    if (lastInputDelay * 4 >= GetDeadPeerTimeout() * 3) {
                        flagState = EFlag::ORANGE;
                        break;
                    } else if (lastInputDelay * 2 >= GetDeadPeerTimeout()) {
                        flagState = EFlag::YELLOW;
                    }

                    // check utilization
                    if (utilization > 875000) { // 7/8
                        flagState = EFlag::ORANGE;
                        break;
                    } else if (utilization > 500000) { // 1/2
                        flagState = EFlag::YELLOW;
                    }
                } while (false);
            }

            // we need track clockskew only if it's one tenant nodes connection
            // they have one scope in this case
            bool reportClockSkew = Proxy->Common->LocalScopeId.first != 0 && Proxy->Common->LocalScopeId == Params.PeerScopeId;

            callback({TlsActivationContext->ExecutorThread.ActorSystem,
                     Proxy->PeerNodeId,
                     Proxy->Metrics->GetHumanFriendlyPeerHostName(),
                     connected,
                     flagState == EFlag::GREEN,
                     flagState == EFlag::YELLOW,
                     flagState == EFlag::ORANGE,
                     flagState == EFlag::RED,
                     ReceiveContext->ClockSkew_us.load(),
                     reportClockSkew});
        }

        if (connected) {
            Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup);
        }
    }

    void TInterconnectSessionTCP::SetOutputStuckFlag(bool state) {
        if (OutputStuckFlag == state)
            return;

        if (OutputQueueUtilization.Size() == 0)
            return;

        auto& lastpair = OutputQueueUtilization.Last();
        if (state)
            lastpair.first -= GetCycleCountFast();
        else
            lastpair.first += GetCycleCountFast();

        OutputStuckFlag = state;
    }

    void TInterconnectSessionTCP::SwitchStuckPeriod() {
        auto now = GetCycleCountFast();
        if (OutputQueueUtilization.Size() != 0) {
            auto& lastpair = OutputQueueUtilization.Last();
            lastpair.second = now - lastpair.second;
            if (OutputStuckFlag)
                lastpair.first += now;
        }

        OutputQueueUtilization.Push(std::pair<ui64, ui64>(0, now));
        if (OutputStuckFlag)
            OutputQueueUtilization.Last().first -= now;
    }

    TDuration TInterconnectSessionTCP::GetDeadPeerTimeout() const {
        return Coalesce(Proxy->Common->Settings.DeadPeer, DEFAULT_DEADPEER_TIMEOUT);
    }

    TDuration TInterconnectSessionTCP::GetCloseOnIdleTimeout() const {
        return Proxy->Common->Settings.CloseOnIdle;
    }

    TDuration TInterconnectSessionTCP::GetLostConnectionTimeout() const {
        return Coalesce(Proxy->Common->Settings.LostConnection, DEFAULT_LOST_CONNECTION_TIMEOUT);
    }

    ui32 TInterconnectSessionTCP::GetTotalInflightAmountOfData() const {
        return Coalesce(Proxy->Common->Settings.TotalInflightAmountOfData, DEFAULT_TOTAL_INFLIGHT_DATA);
    }

    ui64 TInterconnectSessionTCP::GetMaxCyclesPerEvent() const {
        return DurationToCycles(TDuration::MicroSeconds(50));
    }

    void TInterconnectSessionTCP::IssuePingRequest() {
        const TMonotonic now = TActivationContext::Monotonic();
        if (now >= LastPingTimestamp + PingPeriodicity) {
            LOG_DEBUG_IC_SESSION("ICS00", "Issuing ping request");
            if (Socket) {
                MakePacket(false, GetCycleCountFast() | TTcpPacketBuf::PingRequestMask);
                MakePacket(false, TInstant::Now().MicroSeconds() | TTcpPacketBuf::ClockMask);
            }
            LastPingTimestamp = now;
        }
    }

    void TInterconnectSessionTCP::Handle(TEvProcessPingRequest::TPtr ev) {
        if (Socket) {
            MakePacket(false, ev->Get()->Payload | TTcpPacketBuf::PingResponseMask);
            GenerateTraffic();
        }
    }

    void TInterconnectSessionTCP::GenerateHttpInfo(NMon::TEvHttpInfoRes::TPtr& ev) {
        TStringStream str;
        ev->Get()->Output(str);

        HTML(str) {
            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "Session";
                }
                DIV_CLASS("panel-body") {
                    TABLE_CLASS("table") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() {
                                    str << "Sensor";
                                }
                                TABLEH() {
                                    str << "Value";
                                }
                            }
                        }
                        TABLEBODY() {
                            TABLER() {
                                TABLED() {
                                    str << "Encryption";
                                }
                                TABLED() {
                                    str << (Params.Encryption ? "<font color=green>Enabled</font>" : "<font color=red>Disabled</font>");
                                }
                            }
                            if (auto *x = dynamic_cast<NInterconnect::TSecureSocket*>(Socket.Get())) {
                                TABLER() {
                                    TABLED() {
                                        str << "Cipher name";
                                    }
                                    TABLED() {
                                        str << x->GetCipherName();
                                    }
                                }
                                TABLER() {
                                    TABLED() {
                                        str << "Cipher bits";
                                    }
                                    TABLED() {
                                        str << x->GetCipherBits();
                                    }
                                }
                                TABLER() {
                                    TABLED() {
                                        str << "Protocol";
                                    }
                                    TABLED() {
                                        str << x->GetProtocolName();
                                    }
                                }
                                TABLER() {
                                    TABLED() {
                                        str << "Peer CN";
                                    }
                                    TABLED() {
                                        str << x->GetPeerCommonName();
                                    }
                                }
                            }
                            TABLER() {
                                TABLED() { str << "AuthOnly CN"; }
                                TABLED() { str << Params.AuthCN; }
                            }
                            TABLER() {
                                TABLED() {
                                    str << "Local scope id";
                                }
                                TABLED() {
                                    str << ScopeIdToString(Proxy->Common->LocalScopeId);
                                }
                            }
                            TABLER() {
                                TABLED() {
                                    str << "Peer scope id";
                                }
                                TABLED() {
                                    str << ScopeIdToString(Params.PeerScopeId);
                                }
                            }
                            TABLER() {
                                TABLED() {
                                    str << "This page generated at";
                                }
                                TABLED() {
                                    str << TActivationContext::Now() << " / " << Now();
                                }
                            }
                            TABLER() {
                                TABLED() {
                                    str << "SelfID";
                                }
                                TABLED() {
                                    str << SelfId().ToString();
                                }
                            }
                            TABLER() {
                                TABLED() { str << "Frame version/Checksum"; }
                                TABLED() { str << (Params.Encryption ? "v2/none" : Params.UseXxhash ? "v2/xxhash" : "v2/crc32c"); }
                            }
#define MON_VAR(NAME)     \
    TABLER() {            \
        TABLED() {        \
            str << #NAME; \
        }                 \
        TABLED() {        \
            str << NAME;  \
        }                 \
    }

                            MON_VAR(Created)
                            MON_VAR(Params.UseExternalDataChannel)
                            MON_VAR(NewConnectionSet)
                            MON_VAR(ReceiverId)
                            MON_VAR(MessagesGot)
                            MON_VAR(MessagesWrittenToBuffer)
                            MON_VAR(PacketsGenerated)
                            MON_VAR(PacketsConfirmed)
                            MON_VAR(ConfirmPacketsForcedBySize)
                            MON_VAR(ConfirmPacketsForcedByTimeout)

                            TABLER() {
                                TABLED() {
                                    str << "Virtual self ID";
                                }
                                TABLED() {
                                    str << Proxy->SessionVirtualId.ToString();
                                }
                            }
                            TABLER() {
                                TABLED() {
                                    str << "Virtual peer ID";
                                }
                                TABLED() {
                                    str << Proxy->RemoteSessionVirtualId.ToString();
                                }
                            }
                            TABLER() {
                                TABLED() {
                                    str << "Socket";
                                }
                                TABLED() {
                                    str << (Socket ? i64(*Socket) : -1);
                                }
                            }
                            TABLER() {
                                TABLED() {
                                    str << "XDC socket";
                                }
                                TABLED() {
                                    str << (XdcSocket ? i64(*XdcSocket) : -1);
                                }
                            }

                            ui32 unsentQueueSize = Socket ? Socket->GetUnsentQueueSize() : 0;

                            const TMonotonic now = TActivationContext::Monotonic();

                            MON_VAR(OutputStuckFlag)
                            MON_VAR(SendQueue.size())
                            MON_VAR(NumEventsInQueue)
                            MON_VAR(TotalOutputQueueSize)
                            MON_VAR(InflightDataAmount)
                            MON_VAR(unsentQueueSize)
                            MON_VAR(SendBufferSize)
                            MON_VAR(now - LastInputActivityTimestamp)
                            MON_VAR(now - LastPayloadActivityTimestamp)
                            MON_VAR(LastHandshakeDone)
                            MON_VAR(OutputCounter)
                            MON_VAR(LastConfirmed)
                            MON_VAR(FlushSchedule.size())
                            MON_VAR(MaxFlushSchedule)
                            MON_VAR(FlushEventsScheduled)
                            MON_VAR(FlushEventsProcessed)

                            MON_VAR(GetWriteBlockedTotal())

                            MON_VAR(BytesWrittenToSocket)
                            MON_VAR(XdcBytesSent)

                            MON_VAR(OutgoingStream.CalculateOutgoingSize())
                            MON_VAR(OutgoingStream.CalculateUnsentSize())
                            MON_VAR(OutgoingStream.GetSendQueueSize())
                            MON_VAR(OutgoingOffset)
                            MON_VAR(OutgoingIndex)

                            MON_VAR(OutOfBandStream.CalculateOutgoingSize())
                            MON_VAR(OutOfBandStream.CalculateUnsentSize())
                            MON_VAR(OutOfBandStream.GetSendQueueSize())
                            MON_VAR(BytesAlignedForOutOfBand)
                            MON_VAR(OutOfBandBytesSent)

                            MON_VAR(XdcStream.CalculateOutgoingSize())
                            MON_VAR(XdcStream.CalculateUnsentSize())
                            MON_VAR(XdcStream.GetSendQueueSize())
                            MON_VAR(XdcOffset)

                            MON_VAR(CpuStarvationEvents)
                            MON_VAR(CpuStarvationEventsOnWriteData)

                            TString clockSkew;
                            i64 x = GetClockSkew();
                            if (x < 0) {
                                clockSkew = Sprintf("-%s", TDuration::MicroSeconds(-x).ToString().data());
                            } else {
                                clockSkew = Sprintf("+%s", TDuration::MicroSeconds(x).ToString().data());
                            }

                            MON_VAR(now - LastPingTimestamp)
                            MON_VAR(GetPingRTT())
                            MON_VAR(clockSkew)

                            MON_VAR(GetDeadPeerTimeout())
                            MON_VAR(GetTotalInflightAmountOfData())
                            MON_VAR(GetCloseOnIdleTimeout())
                            MON_VAR(Subscribers.size())
                        }
                    }
                }
            }
        }

        auto h = std::make_unique<IEventHandle>(ev->Recipient, ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
        if (ReceiverId) {
            h->Rewrite(h->Type, ReceiverId);
        }
        TActivationContext::Send(h.release());
    }

    void CreateSessionKillingActor(TInterconnectProxyCommon::TPtr common) {
        TlsActivationContext->ExecutorThread.ActorSystem->Register(new TInterconnectSessionKiller(common));
    }
}
