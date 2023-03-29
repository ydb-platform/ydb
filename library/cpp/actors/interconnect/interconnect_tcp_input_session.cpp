#include "interconnect_tcp_session.h"
#include "interconnect_tcp_proxy.h"
#include <library/cpp/actors/core/probes.h>
#include <library/cpp/actors/util/datetime.h>

namespace NActors {
    LWTRACE_USING(ACTORLIB_PROVIDER);

    TInputSessionTCP::TInputSessionTCP(const TActorId& sessionId, TIntrusivePtr<NInterconnect::TStreamSocket> socket,
                                       TIntrusivePtr<TReceiveContext> context, TInterconnectProxyCommon::TPtr common,
                                       std::shared_ptr<IInterconnectMetrics> metrics, ui32 nodeId, ui64 lastConfirmed,
                                       TDuration deadPeerTimeout, TSessionParams params)
        : SessionId(sessionId)
        , Socket(std::move(socket))
        , Context(std::move(context))
        , Common(std::move(common))
        , NodeId(nodeId)
        , Params(std::move(params))
        , ConfirmedByInput(lastConfirmed)
        , Metrics(std::move(metrics))
        , DeadPeerTimeout(deadPeerTimeout)
    {
        Y_VERIFY(Context);
        Y_VERIFY(Socket);
        Y_VERIFY(SessionId);

        AtomicSet(Context->PacketsReadFromSocket, 0);

        Metrics->SetClockSkewMicrosec(0);

        Context->UpdateState = EUpdateState::NONE;

        // ensure that we do not spawn new session while the previous one is still alive
        TAtomicBase sessions = AtomicIncrement(Context->NumInputSessions);
        Y_VERIFY(sessions == 1, "sessions# %" PRIu64, ui64(sessions));
    }

    void TInputSessionTCP::Bootstrap() {
        SetPrefix(Sprintf("InputSession %s [node %" PRIu32 "]", SelfId().ToString().data(), NodeId));
        Become(&TThis::WorkingState, DeadPeerTimeout, new TEvCheckDeadPeer);
        LOG_DEBUG_IC_SESSION("ICIS01", "InputSession created");
        LastReceiveTimestamp = TActivationContext::Monotonic();
        ReceiveData();
    }

    void TInputSessionTCP::CloseInputSession() {
        CloseInputSessionRequested = true;
        ReceiveData();
    }

    void TInputSessionTCP::Handle(TEvPollerReady::TPtr ev) {
        if (Context->ReadPending) {
            Metrics->IncUsefulReadWakeups();
        } else if (!ev->Cookie) {
            Metrics->IncSpuriousReadWakeups();
        }
        Context->ReadPending = false;
        ReceiveData();
        if (Params.Encryption && Context->WriteBlockedByFullSendBuffer && !ev->Cookie) {
            Send(SessionId, ev->Release().Release(), 0, 1);
        }
    }

    void TInputSessionTCP::Handle(TEvPollerRegisterResult::TPtr ev) {
        PollerToken = std::move(ev->Get()->PollerToken);
        ReceiveData();
    }

    void TInputSessionTCP::HandleResumeReceiveData() {
        ReceiveData();
    }

    void TInputSessionTCP::ReceiveData() {
        TTimeLimit limit(GetMaxCyclesPerEvent());
        ui64 numDataBytes = 0;
        const size_t headerLen = Params.UseModernFrame ? sizeof(TTcpPacketHeader_v2) : sizeof(TTcpPacketHeader_v1);

        LOG_DEBUG_IC_SESSION("ICIS02", "ReceiveData called");

        bool enoughCpu = true;
        for (int iteration = 0; Socket; ++iteration) {
            if (iteration && limit.CheckExceeded()) {
                // we have hit processing time limit for this message, send notification to resume processing a bit later
                Send(SelfId(), new TEvResumeReceiveData);
                enoughCpu = false;
                break;
            }

            switch (State) {
                case EState::HEADER:
                    if (IncomingData.GetSize() < headerLen) {
                        break;
                    } else {
                        ProcessHeader(headerLen);
                    }
                    continue;

                case EState::PAYLOAD:
                    if (!IncomingData) {
                        break;
                    } else {
                        ProcessPayload(numDataBytes);
                    }
                    continue;
            }

            // if we have reached this point, it means that we do not have enough data in read buffer; try to obtain some
            if (!ReadMore()) {
                // we have no data from socket, so we have some free time to spend -- preallocate buffers using this time
                PreallocateBuffers();
                break;
            }
        }

        SetEnoughCpu(enoughCpu);

        // calculate ping time
        auto it = std::min_element(PingQ.begin(), PingQ.end());
        const TDuration ping = it != PingQ.end() ? *it : TDuration::Zero();

        // send update to main session actor if something valuable has changed
        if (!UpdateFromInputSession) {
            UpdateFromInputSession = MakeHolder<TEvUpdateFromInputSession>(ConfirmedByInput, numDataBytes, ping);
        } else {
            Y_VERIFY(ConfirmedByInput >= UpdateFromInputSession->ConfirmedByInput);
            UpdateFromInputSession->ConfirmedByInput = ConfirmedByInput;
            UpdateFromInputSession->NumDataBytes += numDataBytes;
            UpdateFromInputSession->Ping = Min(UpdateFromInputSession->Ping, ping);
        }

        for (;;) {
            EUpdateState state = Context->UpdateState;
            EUpdateState next;

            // calculate next state
            switch (state) {
                case EUpdateState::NONE:
                case EUpdateState::CONFIRMING:
                    // we have no inflight messages to session actor, we will issue one a bit later
                    next = EUpdateState::INFLIGHT;
                    break;

                case EUpdateState::INFLIGHT:
                case EUpdateState::INFLIGHT_AND_PENDING:
                    // we already have inflight message, so we will keep pending message and session actor will issue
                    // TEvConfirmUpdate to kick processing
                    next = EUpdateState::INFLIGHT_AND_PENDING;
                    break;
            }

            if (Context->UpdateState.compare_exchange_weak(state, next)) {
                switch (next) {
                    case EUpdateState::INFLIGHT:
                        Send(SessionId, UpdateFromInputSession.Release());
                        break;

                    case EUpdateState::INFLIGHT_AND_PENDING:
                        Y_VERIFY(UpdateFromInputSession);
                        break;

                    default:
                        Y_FAIL("unexpected state");
                }
                break;
            }
        }
    }

    void TInputSessionTCP::ProcessHeader(size_t headerLen) {
        const bool success = IncomingData.ExtractFrontPlain(Header.Data, headerLen);
        Y_VERIFY(success);
        if (Params.UseModernFrame) {
            PayloadSize = Header.v2.PayloadLength;
            HeaderSerial = Header.v2.Serial;
            HeaderConfirm = Header.v2.Confirm;
            if (!Params.Encryption) {
                ChecksumExpected = std::exchange(Header.v2.Checksum, 0);
                Checksum = Crc32cExtendMSanCompatible(0, &Header.v2, sizeof(Header.v2)); // start calculating checksum now
                if (!PayloadSize && Checksum != ChecksumExpected) {
                    LOG_ERROR_IC_SESSION("ICIS10", "payload checksum error");
                    return ReestablishConnection(TDisconnectReason::ChecksumError());
                }
            }
        } else if (!Header.v1.Check()) {
            LOG_ERROR_IC_SESSION("ICIS03", "header checksum error");
            return ReestablishConnection(TDisconnectReason::ChecksumError());
        } else {
            PayloadSize = Header.v1.DataSize;
            HeaderSerial = Header.v1.Serial;
            HeaderConfirm = Header.v1.Confirm;
            ChecksumExpected = Header.v1.PayloadCRC32;
            Checksum = 0;
        }
        if (PayloadSize >= 65536) {
            LOG_CRIT_IC_SESSION("ICIS07", "payload is way too big");
            return DestroySession(TDisconnectReason::FormatError());
        }
        if (ConfirmedByInput < HeaderConfirm) {
            ConfirmedByInput = HeaderConfirm;
            if (AtomicGet(Context->ControlPacketId) <= HeaderConfirm && !NewPingProtocol) {
                ui64 sendTime = AtomicGet(Context->ControlPacketSendTimer);
                TDuration duration = CyclesToDuration(GetCycleCountFast() - sendTime);
                const auto durationUs = duration.MicroSeconds();
                Metrics->UpdatePingTimeHistogram(durationUs);
                PingQ.push_back(duration);
                if (PingQ.size() > 16) {
                    PingQ.pop_front();
                }
                AtomicSet(Context->ControlPacketId, 0ULL);
            }
        }
        if (PayloadSize) {
            const ui64 expected = Context->GetLastProcessedPacketSerial() + 1;
            if (HeaderSerial == 0 || HeaderSerial > expected) {
                LOG_CRIT_IC_SESSION("ICIS06", "packet serial %" PRIu64 ", but %" PRIu64 " expected", HeaderSerial, expected);
                return DestroySession(TDisconnectReason::FormatError());
            }
            IgnorePayload = HeaderSerial != expected;
            State = EState::PAYLOAD;
        } else if (HeaderSerial & TTcpPacketBuf::PingRequestMask) {
            Send(SessionId, new TEvProcessPingRequest(HeaderSerial & ~TTcpPacketBuf::PingRequestMask));
        } else if (HeaderSerial & TTcpPacketBuf::PingResponseMask) {
            const ui64 sent = HeaderSerial & ~TTcpPacketBuf::PingResponseMask;
            const ui64 received = GetCycleCountFast();
            HandlePingResponse(CyclesToDuration(received - sent));
        } else if (HeaderSerial & TTcpPacketBuf::ClockMask) {
            HandleClock(TInstant::MicroSeconds(HeaderSerial & ~TTcpPacketBuf::ClockMask));
        }
    }

    void TInputSessionTCP::ProcessPayload(ui64& numDataBytes) {
        const size_t numBytes = Min(PayloadSize, IncomingData.GetSize());
        IncomingData.ExtractFront(numBytes, &Payload);
        numDataBytes += numBytes;
        PayloadSize -= numBytes;
        if (PayloadSize) {
            return; // there is still some data to receive in the Payload rope
        }
        State = EState::HEADER; // we'll continue with header next time
        if (!Params.UseModernFrame || !Params.Encryption) { // see if we are checksumming packet body
            for (const auto&& [data, size] : Payload) {
                Checksum = Crc32cExtendMSanCompatible(Checksum, data, size);
            }
            if (Checksum != ChecksumExpected) { // validate payload checksum
                LOG_ERROR_IC_SESSION("ICIS04", "payload checksum error");
                return ReestablishConnection(TDisconnectReason::ChecksumError());
            }
        }
        if (Y_UNLIKELY(IgnorePayload)) {
            return;
        }
        if (!Context->AdvanceLastProcessedPacketSerial()) {
            return DestroySession(TDisconnectReason::NewSession());
        }

        while (Payload && Socket) {
            // extract channel part header from the payload stream
            TChannelPart part;
            if (!Payload.ExtractFrontPlain(&part, sizeof(part))) {
                LOG_CRIT_IC_SESSION("ICIS14", "missing TChannelPart header in payload");
                return DestroySession(TDisconnectReason::FormatError());
            }
            if (!part.Size) { // bogus frame
                continue;
            } else if (Payload.GetSize() < part.Size) {
                LOG_CRIT_IC_SESSION("ICIS08", "payload format error ChannelPart# %s", part.ToString().data());
                return DestroySession(TDisconnectReason::FormatError());
            }

            const ui16 channel = part.Channel & ~TChannelPart::LastPartFlag;
            TRope *eventData = channel < Context->ChannelArray.size()
                ? &Context->ChannelArray[channel]
                : &Context->ChannelMap[channel];

            Metrics->AddInputChannelsIncomingTraffic(channel, sizeof(part) + part.Size);

            char buffer[Max(sizeof(TEventDescr1), sizeof(TEventDescr2))];
            auto& v1 = reinterpret_cast<TEventDescr1&>(buffer);
            auto& v2 = reinterpret_cast<TEventDescr2&>(buffer);
            if (~part.Channel & TChannelPart::LastPartFlag) {
                Payload.ExtractFront(part.Size, eventData);
            } else if (part.Size != sizeof(v1) && part.Size != sizeof(v2)) {
                LOG_CRIT_IC_SESSION("ICIS11", "incorrect last part of an event");
                return DestroySession(TDisconnectReason::FormatError());
            } else if (Payload.ExtractFrontPlain(buffer, part.Size)) {
                TEventData descr;

                switch (part.Size) {
                    case sizeof(TEventDescr1):
                        descr = {
                            v1.Type,
                            v1.Flags,
                            v1.Recipient,
                            v1.Sender,
                            v1.Cookie,
                            NWilson::TTraceId(), // do not accept traces with old format
                            v1.Checksum
                        };
                        break;

                    case sizeof(TEventDescr2):
                        descr = {
                            v2.Type,
                            v2.Flags,
                            v2.Recipient,
                            v2.Sender,
                            v2.Cookie,
                            NWilson::TTraceId(v2.TraceId),
                            v2.Checksum
                        };
                        break;
                }

                Metrics->IncInputChannelsIncomingEvents(channel);
                ProcessEvent(*eventData, descr);
                *eventData = TRope();
            } else {
                Y_FAIL();
            }
        }
    }

    void TInputSessionTCP::ProcessEvent(TRope& data, TEventData& descr) {
        if (!Params.UseModernFrame || descr.Checksum) {
            ui32 checksum = 0;
            for (const auto&& [data, size] : data) {
                checksum = Crc32cExtendMSanCompatible(checksum, data, size);
            }
            if (checksum != descr.Checksum) {
                LOG_CRIT_IC_SESSION("ICIS05", "event checksum error");
                return ReestablishConnection(TDisconnectReason::ChecksumError());
            }
        }
        TEventSerializationInfo serializationInfo{
            .IsExtendedFormat = bool(descr.Flags & IEventHandle::FlagExtendedFormat),
        };
        auto ev = std::make_unique<IEventHandle>(SessionId,
            descr.Type,
            descr.Flags & ~IEventHandle::FlagExtendedFormat,
            descr.Recipient,
            descr.Sender,
            MakeIntrusive<TEventSerializedData>(std::move(data), std::move(serializationInfo)),
            descr.Cookie,
            Params.PeerScopeId,
            std::move(descr.TraceId));
        if (Common->EventFilter && !Common->EventFilter->CheckIncomingEvent(*ev, Common->LocalScopeId)) {
            LOG_CRIT_IC_SESSION("ICIC03", "Event dropped due to scope error LocalScopeId# %s PeerScopeId# %s Type# 0x%08" PRIx32,
                ScopeIdToString(Common->LocalScopeId).data(), ScopeIdToString(Params.PeerScopeId).data(), descr.Type);
            ev.reset();
        }
        if (ev) {
            TActivationContext::Send(ev.release());
        }
    }

    void TInputSessionTCP::HandleConfirmUpdate() {
        for (;;) {
            switch (EUpdateState state = Context->UpdateState) {
                case EUpdateState::NONE:
                case EUpdateState::INFLIGHT:
                case EUpdateState::INFLIGHT_AND_PENDING:
                    // here we may have a race
                    return;

                case EUpdateState::CONFIRMING:
                    Y_VERIFY(UpdateFromInputSession);
                    if (Context->UpdateState.compare_exchange_weak(state, EUpdateState::INFLIGHT)) {
                        Send(SessionId, UpdateFromInputSession.Release());
                        return;
                    }
            }
        }
    }

    bool TInputSessionTCP::ReadMore() {
        PreallocateBuffers();

        TStackVec<TIoVec, 16> buffs;
        size_t offset = FirstBufferOffset;
        for (const auto& item : Buffers) {
            TIoVec iov{item->GetBuffer() + offset, item->GetCapacity() - offset};
            buffs.push_back(iov);
            if (Params.Encryption) {
                break; // do not put more than one buffer in queue to prevent using ReadV
            }
            offset = 0;
        }

        const struct iovec* iovec = reinterpret_cast<const struct iovec*>(buffs.data());
        int iovcnt = buffs.size();

        ssize_t recvres = 0;
        TString err;
        LWPROBE_IF_TOO_LONG(SlowICReadFromSocket, ms) {
            do {
                const ui64 begin = GetCycleCountFast();
#ifndef _win_
                recvres = iovcnt == 1 ? Socket->Recv(iovec->iov_base, iovec->iov_len, &err) : Socket->ReadV(iovec, iovcnt);
#else
                recvres = Socket->Recv(iovec[0].iov_base, iovec[0].iov_len, &err);
#endif
                const ui64 end = GetCycleCountFast();
                Metrics->IncRecvSyscalls((end - begin) * 1'000'000 / GetCyclesPerMillisecond());
            } while (recvres == -EINTR);
        }

        LOG_DEBUG_IC_SESSION("ICIS12", "ReadMore recvres# %zd iovcnt# %d err# %s", recvres, iovcnt, err.data());

        if (recvres <= 0 || CloseInputSessionRequested) {
            if ((-recvres != EAGAIN && -recvres != EWOULDBLOCK) || CloseInputSessionRequested) {
                TString message = CloseInputSessionRequested ? "connection closed by debug command"
                    : recvres == 0 ? "connection closed by peer"
                    : err ? err
                    : Sprintf("socket: %s", strerror(-recvres));
                LOG_NOTICE_NET(NodeId, "%s", message.data());
                ReestablishConnection(CloseInputSessionRequested ? TDisconnectReason::Debug() :
                    recvres == 0 ? TDisconnectReason::EndOfStream() : TDisconnectReason::FromErrno(-recvres));
            } else if (PollerToken && !std::exchange(Context->ReadPending, true)) {
                if (Params.Encryption) {
                    auto *secure = static_cast<NInterconnect::TSecureSocket*>(Socket.Get());
                    const bool wantRead = secure->WantRead(), wantWrite = secure->WantWrite();
                    Y_VERIFY_DEBUG(wantRead || wantWrite);
                    PollerToken->Request(wantRead, wantWrite);
                } else {
                    PollerToken->Request(true, false);
                }
            }
            return false;
        }

        Y_VERIFY(recvres > 0);
        Metrics->AddTotalBytesRead(recvres);

        while (recvres) {
            Y_VERIFY(!Buffers.empty());
            auto& buffer = Buffers.front();
            const size_t bytes = Min<size_t>(recvres, buffer->GetCapacity() - FirstBufferOffset);
            IncomingData.Insert(IncomingData.End(), TRcBuf{buffer, {buffer->GetBuffer() + FirstBufferOffset, bytes}});
            recvres -= bytes;
            FirstBufferOffset += bytes;
            if (FirstBufferOffset == buffer->GetCapacity()) {
                Buffers.pop_front();
                FirstBufferOffset = 0;
            }
        }

        LastReceiveTimestamp = TActivationContext::Monotonic();

        return true;
    }

    void TInputSessionTCP::PreallocateBuffers() {
        // ensure that we have exactly "numBuffers" in queue
        LWPROBE_IF_TOO_LONG(SlowICReadLoopAdjustSize, ms) {
            while (Buffers.size() < Common->Settings.NumPreallocatedBuffers) {
                Buffers.emplace_back(TRopeAlignedBuffer::Allocate(Common->Settings.PreallocatedBufferSize));
            }
        }
    }

    void TInputSessionTCP::ReestablishConnection(TDisconnectReason reason) {
        LOG_DEBUG_IC_SESSION("ICIS09", "ReestablishConnection, reason# %s", reason.ToString().data());
        AtomicDecrement(Context->NumInputSessions);
        Send(SessionId, new TEvSocketDisconnect(std::move(reason)));
        PassAway();
        Socket.Reset();
    }

    void TInputSessionTCP::DestroySession(TDisconnectReason reason) {
        LOG_DEBUG_IC_SESSION("ICIS13", "DestroySession, reason# %s", reason.ToString().data());
        AtomicDecrement(Context->NumInputSessions);
        Send(SessionId, TInterconnectSessionTCP::NewEvTerminate(std::move(reason)));
        PassAway();
        Socket.Reset();
    }

    void TInputSessionTCP::PassAway() {
        Metrics->SetClockSkewMicrosec(0);
        TActorBootstrapped::PassAway();
    }

    void TInputSessionTCP::HandleCheckDeadPeer() {
        const TMonotonic now = TActivationContext::Monotonic();
        if (now >= LastReceiveTimestamp + DeadPeerTimeout) {
            ReceiveData();
            if (Socket && now >= LastReceiveTimestamp + DeadPeerTimeout) {
                // nothing has changed, terminate session
                DestroySession(TDisconnectReason::DeadPeer());
            }
        }
        Schedule(LastReceiveTimestamp + DeadPeerTimeout, new TEvCheckDeadPeer);
    }

    void TInputSessionTCP::HandlePingResponse(TDuration passed) {
        PingQ.push_back(passed);
        if (PingQ.size() > 16) {
            PingQ.pop_front();
        }
        const TDuration ping = *std::min_element(PingQ.begin(), PingQ.end());
        const auto pingUs = ping.MicroSeconds();
        Context->PingRTT_us = pingUs;
        NewPingProtocol = true;
        Metrics->UpdatePingTimeHistogram(pingUs);
    }

    void TInputSessionTCP::HandleClock(TInstant clock) {
        const TInstant here = TInstant::Now(); // wall clock
        const TInstant remote = clock + TDuration::MicroSeconds(Context->PingRTT_us / 2);
        i64 skew = remote.MicroSeconds() - here.MicroSeconds();
        SkewQ.push_back(skew);
        if (SkewQ.size() > 16) {
            SkewQ.pop_front();
        }
        i64 clockSkew = SkewQ.front();
        for (i64 skew : SkewQ) {
            if (abs(skew) < abs(clockSkew)) {
                clockSkew = skew;
            }
        }
        Context->ClockSkew_us = clockSkew;
        Metrics->SetClockSkewMicrosec(clockSkew);
    }


}
