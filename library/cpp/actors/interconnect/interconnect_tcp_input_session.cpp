#include "interconnect_tcp_session.h"
#include "interconnect_tcp_proxy.h"
#include <library/cpp/actors/core/probes.h>
#include <library/cpp/actors/util/datetime.h>

namespace NActors {
    LWTRACE_USING(ACTORLIB_PROVIDER);

    void TReceiveContext::TPerChannelContext::CalculateBytesToCatch() {
        XdcBytesToCatch = FetchOffset;
        for (auto it = XdcBuffers.begin(), end = it + FetchIndex; it != end; ++it) {
            XdcBytesToCatch += it->size();
        }
    }

    void TReceiveContext::TPerChannelContext::FetchBuffers(ui16 channel, size_t numBytes,
            std::deque<std::tuple<ui16, TMutableContiguousSpan>>& outQ) {
        Y_VERIFY_DEBUG(numBytes);
        auto it = XdcBuffers.begin() + FetchIndex;
        for (;;) {
            Y_VERIFY_DEBUG(it != XdcBuffers.end());
            const TMutableContiguousSpan span = it->SubSpan(FetchOffset, numBytes);
            outQ.emplace_back(channel, span);
            numBytes -= span.size();
            FetchOffset += span.size();
            if (FetchOffset == it->size()) {
                ++FetchIndex;
                ++it;
                FetchOffset = 0;
            }
            if (!numBytes) {
                break;
            }
        }
    }

    void TReceiveContext::TPerChannelContext::DropFront(TRope *from, size_t numBytes) {
        size_t n = numBytes;
        for (auto& pendingEvent : PendingEvents) {
            const size_t numBytesInEvent = Min(n, pendingEvent.XdcSizeLeft);
            pendingEvent.XdcSizeLeft -= numBytesInEvent;
            n -= numBytesInEvent;
            if (!n) {
                break;
            }
        }

        while (numBytes) {
            Y_VERIFY_DEBUG(!XdcBuffers.empty());
            auto& front = XdcBuffers.front();
            if (from) {
                from->ExtractFrontPlain(front.data(), Min(numBytes, front.size()));
            }
            if (numBytes < front.size()) {
                front = front.SubSpan(numBytes, Max<size_t>());
                if (!FetchIndex) { // we are sending this very buffer, adjust sending offset
                    Y_VERIFY_DEBUG(numBytes <= FetchOffset);
                    FetchOffset -= numBytes;
                }
                break;
            } else {
                numBytes -= front.size();
                Y_VERIFY_DEBUG(FetchIndex);
                --FetchIndex;
                XdcBuffers.pop_front();
            }
        }
    }

    TInputSessionTCP::TInputSessionTCP(const TActorId& sessionId, TIntrusivePtr<NInterconnect::TStreamSocket> socket,
            TIntrusivePtr<NInterconnect::TStreamSocket> xdcSocket, TIntrusivePtr<TReceiveContext> context,
            TInterconnectProxyCommon::TPtr common, std::shared_ptr<IInterconnectMetrics> metrics, ui32 nodeId,
            ui64 lastConfirmed, TDuration deadPeerTimeout, TSessionParams params)
        : SessionId(sessionId)
        , Socket(std::move(socket))
        , XdcSocket(std::move(xdcSocket))
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
        Y_VERIFY(!Params.UseExternalDataChannel == !XdcSocket);

        Metrics->SetClockSkewMicrosec(0);

        Context->UpdateState = EUpdateState::NONE;

        // ensure that we do not spawn new session while the previous one is still alive
        TAtomicBase sessions = AtomicIncrement(Context->NumInputSessions);
        Y_VERIFY(sessions == 1, "sessions# %" PRIu64, ui64(sessions));

        // calculate number of bytes to catch
        for (auto& context : Context->ChannelArray) {
            context.CalculateBytesToCatch();
        }
        for (auto& [channel, context] : Context->ChannelMap) {
            context.CalculateBytesToCatch();
        }

        UsageHisto.fill(0);
    }

    void TInputSessionTCP::Bootstrap() {
        SetPrefix(Sprintf("InputSession %s [node %" PRIu32 "]", SelfId().ToString().data(), NodeId));
        Become(&TThis::WorkingState, DeadPeerTimeout, new TEvCheckDeadPeer);
        LOG_DEBUG_IC_SESSION("ICIS01", "InputSession created");
        LastReceiveTimestamp = TActivationContext::Monotonic();
        TActivationContext::Send(new IEventHandle(EvResumeReceiveData, 0, SelfId(), {}, nullptr, 0));
    }

    STATEFN(TInputSessionTCP::WorkingState) {
        std::unique_ptr<IEventBase> termEv;

        try {
            WorkingStateImpl(ev);
        } catch (const TExReestablishConnection& ex) {
            LOG_DEBUG_IC_SESSION("ICIS09", "ReestablishConnection, reason# %s", ex.Reason.ToString().data());
            termEv = std::make_unique<TEvSocketDisconnect>(std::move(ex.Reason));
        } catch (const TExDestroySession& ex) {
            LOG_DEBUG_IC_SESSION("ICIS13", "DestroySession, reason# %s", ex.Reason.ToString().data());
            termEv.reset(TInterconnectSessionTCP::NewEvTerminate(std::move(ex.Reason)));
        }

        if (termEv) {
            AtomicDecrement(Context->NumInputSessions);
            Send(SessionId, termEv.release());
            PassAway();
            Socket.Reset();
        }
    }

    void TInputSessionTCP::CloseInputSession() {
        CloseInputSessionRequested = true;
        ReceiveData();
    }

    void TInputSessionTCP::Handle(TEvPollerReady::TPtr ev) {
        auto *msg = ev->Get();

        bool useful = false;
        bool writeBlocked = false;

        if (msg->Socket == Socket) {
            useful = std::exchange(Context->MainReadPending, false);
            writeBlocked = Context->MainWriteBlocked;
        } else if (msg->Socket == XdcSocket) {
            useful = std::exchange(Context->XdcReadPending, false);
            writeBlocked = Context->XdcWriteBlocked;
        }

        if (useful) {
            Metrics->IncUsefulReadWakeups();
        } else if (!ev->Cookie) {
            Metrics->IncSpuriousReadWakeups();
        }

        ReceiveData();

        if (Params.Encryption && writeBlocked && ev->Sender != SessionId) {
            Send(SessionId, ev->Release().Release());
        }
    }

    void TInputSessionTCP::Handle(TEvPollerRegisterResult::TPtr ev) {
        auto *msg = ev->Get();
        if (msg->Socket == Socket) {
            PollerToken = std::move(msg->PollerToken);
        } else if (msg->Socket == XdcSocket) {
            XdcPollerToken = std::move(msg->PollerToken);
        }
        ReceiveData();
    }

    void TInputSessionTCP::ReceiveData() {
        TTimeLimit limit(GetMaxCyclesPerEvent());
        ui64 numDataBytes = 0;

        LOG_DEBUG_IC_SESSION("ICIS02", "ReceiveData called");

        bool enoughCpu = true;
        bool progress = false;

        for (;;) {
            if (progress && limit.CheckExceeded()) {
                // we have hit processing time limit for this message, send notification to resume processing a bit later
                TActivationContext::Send(new IEventHandle(EvResumeReceiveData, 0, SelfId(), {}, nullptr, 0));
                enoughCpu = false;
                break;
            }

            // clear iteration progress
            progress = false;

            // try to process already fetched part from IncomingData
            switch (State) {
                case EState::HEADER:
                    if (IncomingData.GetSize() < sizeof(TTcpPacketHeader_v2)) {
                        break;
                    } else {
                        ProcessHeader();
                        progress = true;
                        continue;
                    }

                case EState::PAYLOAD:
                    Y_VERIFY_DEBUG(PayloadSize);
                    if (!IncomingData) {
                        break;
                    } else {
                        ProcessPayload(&numDataBytes);
                        progress = true;
                        continue;
                    }
            }

            // try to read more data into buffers
            progress |= ReadMore();
            progress |= ReadXdc(&numDataBytes);

            if (!progress) { // no progress was made during this iteration
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

    void TInputSessionTCP::ProcessHeader() {
        TTcpPacketHeader_v2 header;
        const bool success = IncomingData.ExtractFrontPlain(&header, sizeof(header));
        Y_VERIFY(success);
        PayloadSize = header.PayloadLength;
        const ui64 serial = header.Serial;
        const ui64 confirm = header.Confirm;
        if (!Params.Encryption) {
            ChecksumExpected = std::exchange(header.Checksum, 0);
            Checksum = Crc32cExtendMSanCompatible(0, &header, sizeof(header)); // start calculating checksum now
            if (!PayloadSize && Checksum != ChecksumExpected) {
                LOG_ERROR_IC_SESSION("ICIS10", "payload checksum error");
                throw TExReestablishConnection{TDisconnectReason::ChecksumError()};
            }
        }
        if (PayloadSize >= 65536) {
            LOG_CRIT_IC_SESSION("ICIS07", "payload is way too big");
            throw TExDestroySession{TDisconnectReason::FormatError()};
        }
        if (ConfirmedByInput < confirm) {
            ConfirmedByInput = confirm;
            if (AtomicGet(Context->ControlPacketId) <= confirm && !NewPingProtocol) {
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
            const ui64 expected = Context->LastProcessedSerial + 1;
            if (serial == 0 || serial > expected) {
                LOG_CRIT_IC_SESSION("ICIS06", "packet serial %" PRIu64 ", but %" PRIu64 " expected", serial, expected);
                throw TExDestroySession{TDisconnectReason::FormatError()};
            }
            if (Context->LastProcessedSerial <= serial) {
                XdcCatchStreamFinalPending = true; // we can't switch it right now, only after packet is fully processed
            }
            if (serial == expected) {
                InboundPacketQ.push_back(TInboundPacket{serial, 0});
                IgnorePayload = false;
            } else {
                IgnorePayload = true;
            }
            State = EState::PAYLOAD;
            Y_VERIFY_DEBUG(!Payload);
        } else if (serial & TTcpPacketBuf::PingRequestMask) {
            Send(SessionId, new TEvProcessPingRequest(serial & ~TTcpPacketBuf::PingRequestMask));
        } else if (serial & TTcpPacketBuf::PingResponseMask) {
            const ui64 sent = serial & ~TTcpPacketBuf::PingResponseMask;
            const ui64 received = GetCycleCountFast();
            HandlePingResponse(CyclesToDuration(received - sent));
        } else if (serial & TTcpPacketBuf::ClockMask) {
            HandleClock(TInstant::MicroSeconds(serial & ~TTcpPacketBuf::ClockMask));
        }
        if (!PayloadSize) {
            ++PacketsReadFromSocket;
        }
    }

    void TInputSessionTCP::ProcessPayload(ui64 *numDataBytes) {
        const size_t numBytes = Min(PayloadSize, IncomingData.GetSize());
        IncomingData.ExtractFront(numBytes, &Payload);
        *numDataBytes += numBytes;
        PayloadSize -= numBytes;
        if (PayloadSize) {
            return; // there is still some data to receive in the Payload rope
        }
        State = EState::HEADER;
        if (!Params.Encryption) { // see if we are checksumming packet body
            for (const auto&& [data, size] : Payload) {
                Checksum = Crc32cExtendMSanCompatible(Checksum, data, size);
            }
            if (Checksum != ChecksumExpected) { // validate payload checksum
                LOG_ERROR_IC_SESSION("ICIS04", "payload checksum error");
                throw TExReestablishConnection{TDisconnectReason::ChecksumError()};
            }
        }
        while (Payload) {
            // extract channel part header from the payload stream
            TChannelPart part;
            if (!Payload.ExtractFrontPlain(&part, sizeof(part))) {
                LOG_CRIT_IC_SESSION("ICIS14", "missing TChannelPart header in payload");
                throw TExDestroySession{TDisconnectReason::FormatError()};
            } else if (Payload.GetSize() < part.Size) {
                LOG_CRIT_IC_SESSION("ICIS08", "payload format error ChannelPart# %s", part.ToString().data());
                throw TExDestroySession{TDisconnectReason::FormatError()};
            }

            const ui16 channel = part.GetChannel();
            auto& context = GetPerChannelContext(channel);
            auto& pendingEvent = context.PendingEvents.empty() || context.PendingEvents.back().EventData
                ? context.PendingEvents.emplace_back()
                : context.PendingEvents.back();

            if (part.IsXdc()) { // external data channel command packet
                XdcCommands.resize(part.Size);
                const bool success = Payload.ExtractFrontPlain(XdcCommands.data(), XdcCommands.size());
                Y_VERIFY(success);
                ProcessXdcCommand(channel, context);
            } else if (IgnorePayload) { // throw payload out
                Payload.EraseFront(part.Size);
            } else if (!part.IsLastPart()) { // just ordinary inline event data
                Payload.ExtractFront(part.Size, &pendingEvent.Payload);
            } else { // event final block
                TEventDescr2 v2;

                if (part.Size != sizeof(v2)) {
                    LOG_CRIT_IC_SESSION("ICIS11", "incorrect last part of an event");
                    throw TExDestroySession{TDisconnectReason::FormatError()};
                }

                const bool success = Payload.ExtractFrontPlain(&v2, sizeof(v2));
                Y_VERIFY(success);

                pendingEvent.EventData = TEventData{
                    v2.Type,
                    v2.Flags,
                    v2.Recipient,
                    v2.Sender,
                    v2.Cookie,
                    NWilson::TTraceId(v2.TraceId),
                    v2.Checksum,
#if IC_FORCE_HARDENED_PACKET_CHECKS
                    v2.Len
#endif
                };

                Metrics->IncInputChannelsIncomingEvents(channel);
                ProcessEvents(context);
            }

            Metrics->AddInputChannelsIncomingTraffic(channel, sizeof(part) + part.Size);
        }

        // mark packet as processed
        XdcCatchStreamFinal = XdcCatchStreamFinalPending;
        Context->LastProcessedSerial += !IgnorePayload;
        ProcessInboundPacketQ(0);

        ++PacketsReadFromSocket;
        ++DataPacketsReadFromSocket;
        IgnoredDataPacketsFromSocket += IgnorePayload;
    }

    void TInputSessionTCP::ProcessInboundPacketQ(size_t numXdcBytesRead) {
        for (; !InboundPacketQ.empty(); InboundPacketQ.pop_front()) {
            auto& front = InboundPacketQ.front();

            const size_t n = Min(numXdcBytesRead, front.XdcUnreadBytes);
            front.XdcUnreadBytes -= n;
            numXdcBytesRead -= n;

            if (front.XdcUnreadBytes) { // we haven't finished this packet yet
                Y_VERIFY(!numXdcBytesRead);
                break;
            }

            const bool success = Context->AdvanceLastPacketSerialToConfirm(front.Serial);
            Y_VERIFY_DEBUG(Context->GetLastPacketSerialToConfirm() <= Context->LastProcessedSerial);
            if (!success) {
                throw TExReestablishConnection{TDisconnectReason::NewSession()};
            }
        }
    }

    void TInputSessionTCP::ProcessXdcCommand(ui16 channel, TReceiveContext::TPerChannelContext& context) {
        const char *ptr = XdcCommands.data();
        const char *end = ptr + XdcCommands.size();
        while (ptr != end) {
            switch (static_cast<EXdcCommand>(*ptr++)) {
                case EXdcCommand::DECLARE_SECTION: {
                    // extract and validate command parameters
                    const ui64 headroom = NInterconnect::NDetail::DeserializeNumber(&ptr, end);
                    const ui64 size = NInterconnect::NDetail::DeserializeNumber(&ptr, end);
                    const ui64 tailroom = NInterconnect::NDetail::DeserializeNumber(&ptr, end);
                    const ui64 alignment = NInterconnect::NDetail::DeserializeNumber(&ptr, end);
                    if (headroom == Max<ui64>() || size == Max<ui64>() || tailroom == Max<ui64>() || alignment == Max<ui64>()) {
                        LOG_CRIT_IC_SESSION("ICIS00", "XDC command format error");
                        throw TExDestroySession{TDisconnectReason::FormatError()};
                    }

                    if (!IgnorePayload) { // process command if packet is being applied
                        // allocate buffer and push it into the payload
                        auto& pendingEvent = context.PendingEvents.back();
                        pendingEvent.SerializationInfo.Sections.push_back(TEventSectionInfo{headroom, size, tailroom, alignment});
                        auto buffer = TRcBuf::Uninitialized(size, headroom, tailroom);
                        if (size) {
                            context.XdcBuffers.push_back(buffer.GetContiguousSpanMut());
                        }
                        pendingEvent.Payload.Insert(pendingEvent.Payload.End(), TRope(std::move(buffer)));
                        pendingEvent.XdcSizeLeft += size;

                        ++XdcSections;
                    }
                    continue;
                }

                case EXdcCommand::PUSH_DATA: {
                    const size_t cmdLen = sizeof(ui16) + (Params.Encryption ? 0 : sizeof(ui32));
                    if (static_cast<size_t>(end - ptr) < cmdLen) {
                        LOG_CRIT_IC_SESSION("ICIS18", "XDC command format error");
                        throw TExDestroySession{TDisconnectReason::FormatError()};
                    }

                    auto size = *reinterpret_cast<const ui16*>(ptr);
                    if (!size) {
                        LOG_CRIT_IC_SESSION("ICIS03", "XDC empty payload");
                        throw TExDestroySession{TDisconnectReason::FormatError()};
                    }

                    if (!Params.Encryption) {
                        const ui32 checksumExpected = *reinterpret_cast<const ui32*>(ptr + sizeof(ui16));
                        XdcChecksumQ.emplace_back(size, checksumExpected);
                    }

                    if (IgnorePayload) {
                        // this packet was already marked as 'processed', all commands had been executed, but we must
                        // parse XDC stream correctly
                        XdcCatchStreamBytesPending += size;
                        XdcCatchStreamMarkup.emplace_back(channel, size);
                    } else {
                        // account channel and number of bytes in XDC for this packet
                        auto& packet = InboundPacketQ.back();
                        packet.XdcUnreadBytes += size;

                        // find buffers and acquire data buffer pointers
                        context.FetchBuffers(channel, size, XdcInputQ);
                    }

                    ptr += cmdLen;
                    ++XdcRefs;
                    continue;
                }
            }

            LOG_CRIT_IC_SESSION("ICIS15", "unexpected XDC command");
            throw TExDestroySession{TDisconnectReason::FormatError()};
        }
    }

    void TInputSessionTCP::ProcessEvents(TReceiveContext::TPerChannelContext& context) {
        for (; !context.PendingEvents.empty(); context.PendingEvents.pop_front()) {
            auto& pendingEvent = context.PendingEvents.front();
            if (!pendingEvent.EventData || pendingEvent.XdcSizeLeft) {
                break; // event is not ready yet
            }

            auto& descr = *pendingEvent.EventData;
#if IC_FORCE_HARDENED_PACKET_CHECKS
            if (descr.Len != pendingEvent.Payload.GetSize()) {
                LOG_CRIT_IC_SESSION("ICISxx", "event length mismatch Type# 0x%08" PRIx32 " received# %zu expected# %" PRIu32,
                    descr.Type, pendingEvent.Payload.GetSize(), descr.Len);
                throw TExReestablishConnection{TDisconnectReason::ChecksumError()};
            }
#endif
            if (descr.Checksum) {
                ui32 checksum = 0;
                for (const auto&& [data, size] : pendingEvent.Payload) {
                    checksum = Crc32cExtendMSanCompatible(checksum, data, size);
                }
                if (checksum != descr.Checksum) {
                    LOG_CRIT_IC_SESSION("ICIS05", "event checksum error Type# 0x%08" PRIx32, descr.Type);
                    throw TExReestablishConnection{TDisconnectReason::ChecksumError()};
                }
            }
            pendingEvent.SerializationInfo.IsExtendedFormat = descr.Flags & IEventHandle::FlagExtendedFormat;
            auto ev = std::make_unique<IEventHandle>(SessionId,
                descr.Type,
                descr.Flags & ~IEventHandle::FlagExtendedFormat,
                descr.Recipient,
                descr.Sender,
                MakeIntrusive<TEventSerializedData>(std::move(pendingEvent.Payload), std::move(pendingEvent.SerializationInfo)),
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

    ssize_t TInputSessionTCP::Read(NInterconnect::TStreamSocket& socket, const TPollerToken::TPtr& token,
            bool *readPending, const TIoVec *iov, size_t num) {
        ssize_t recvres = 0;
        TString err;
        LWPROBE_IF_TOO_LONG(SlowICReadFromSocket, ms) {
            do {
                const ui64 begin = GetCycleCountFast();
#ifndef _win_
                if (num == 1) {
                    recvres = socket.Recv(iov->Data, iov->Size, &err);
                } else {
                    recvres = socket.ReadV(reinterpret_cast<const iovec*>(iov), num);
                }
#else
                recvres = socket.Recv(iov->Data, iov->Size, &err);
#endif
                const ui64 end = GetCycleCountFast();
                Metrics->IncRecvSyscalls((end - begin) * 1'000'000 / GetCyclesPerMillisecond());
            } while (recvres == -EINTR);
        }

        LOG_DEBUG_IC_SESSION("ICIS12", "Read recvres# %zd num# %zu err# %s", recvres, num, err.data());

        if (recvres <= 0 || CloseInputSessionRequested) {
            if ((-recvres != EAGAIN && -recvres != EWOULDBLOCK) || CloseInputSessionRequested) {
                TString message = CloseInputSessionRequested ? "connection closed by debug command"
                    : recvres == 0 ? "connection closed by peer"
                    : err ? err
                    : Sprintf("socket: %s", strerror(-recvres));
                LOG_NOTICE_NET(NodeId, "%s", message.data());
                throw TExReestablishConnection{CloseInputSessionRequested ? TDisconnectReason::Debug() :
                    recvres == 0 ? TDisconnectReason::EndOfStream() : TDisconnectReason::FromErrno(-recvres)};
            } else if (token && !std::exchange(*readPending, true)) {
                socket.Request(*token, true, false);
            }
            return -1;
        }

        return recvres;
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

        ssize_t recvres = Read(*Socket, PollerToken, &Context->MainReadPending, buffs.data(), buffs.size());
        if (recvres == -1) {
            return false;
        }

        Y_VERIFY(recvres > 0);
        Metrics->AddTotalBytesRead(recvres);
        BytesReadFromSocket += recvres;

        size_t numBuffersCovered = 0;

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
            ++numBuffersCovered;
        }

        if (Buffers.empty()) { // we have read all the data, increase number of buffers
            CurrentBuffers = Min(CurrentBuffers * 2, MaxBuffers);
        } else if (++UsageHisto[numBuffersCovered - 1] == 64) { // time to shift
            for (auto& value : UsageHisto) {
                value /= 2;
            }
            while (CurrentBuffers > 1 && !UsageHisto[CurrentBuffers - 1]) {
                --CurrentBuffers;
            }
            Y_VERIFY_DEBUG(UsageHisto[CurrentBuffers - 1]);
        }

        LastReceiveTimestamp = TActivationContext::Monotonic();

        return true;
    }

    bool TInputSessionTCP::ReadXdcCatchStream(ui64 *numDataBytes) {
        bool progress = false;

        while (XdcCatchStreamBytesPending) { // read data into catch stream if we still have to
            if (!XdcCatchStreamBuffer) {
                XdcCatchStreamBuffer = TRcBuf::Uninitialized(64 * 1024);
            }

            const size_t numBytesToRead = Min<size_t>(XdcCatchStreamBytesPending, XdcCatchStreamBuffer.size() - XdcCatchStreamBufferOffset);

            TIoVec iov{XdcCatchStreamBuffer.GetDataMut() + XdcCatchStreamBufferOffset, numBytesToRead};
            ssize_t recvres = Read(*XdcSocket, XdcPollerToken, &Context->XdcReadPending, &iov, 1);
            if (recvres == -1) {
                return progress;
            }

            HandleXdcChecksum({XdcCatchStreamBuffer.data() + XdcCatchStreamBufferOffset, static_cast<size_t>(recvres)});

            XdcCatchStreamBufferOffset += recvres;
            XdcCatchStreamBytesPending -= recvres;
            *numDataBytes += recvres;
            BytesReadFromXdcSocket += recvres;

            if (XdcCatchStreamBufferOffset == XdcCatchStreamBuffer.size() || XdcCatchStreamBytesPending == 0) {
                TRope(std::exchange(XdcCatchStreamBuffer, {})).ExtractFront(XdcCatchStreamBufferOffset, &XdcCatchStream);
                XdcCatchStreamBufferOffset = 0;
            }

            progress = true;
        }

        if (XdcCatchStreamFinal && XdcCatchStream) {
            // calculate total number of bytes to catch
            size_t totalBytesToCatch = 0;
            for (auto& context : Context->ChannelArray) {
                totalBytesToCatch += context.XdcBytesToCatch;
            }
            for (auto& [channel, context] : Context->ChannelMap) {
                totalBytesToCatch += context.XdcBytesToCatch;
            }

            // calculate ignored offset
            Y_VERIFY(totalBytesToCatch <= XdcCatchStream.GetSize());
            size_t bytesToIgnore = XdcCatchStream.GetSize() - totalBytesToCatch;

            // process catch stream markup
            THashSet<ui16> channels;
            for (auto [channel, size] : XdcCatchStreamMarkup) {
                if (const size_t n = Min<size_t>(bytesToIgnore, size)) {
                    XdcCatchStream.EraseFront(n);
                    bytesToIgnore -= n;
                    size -= n;
                }
                if (const size_t n = Min<size_t>(totalBytesToCatch, size)) {
                    GetPerChannelContext(channel).DropFront(&XdcCatchStream, n);
                    channels.insert(channel);
                    totalBytesToCatch -= n;
                    size -= n;
                }
                Y_VERIFY(!size);
            }
            for (ui16 channel : channels) {
                ProcessEvents(GetPerChannelContext(channel));
            }

            // ensure everything was processed
            Y_VERIFY(!XdcCatchStream);
            Y_VERIFY(!bytesToIgnore);
            Y_VERIFY(!totalBytesToCatch);
            XdcCatchStreamMarkup = {};
        }

        return progress;
    }

    bool TInputSessionTCP::ReadXdc(ui64 *numDataBytes) {
        bool progress = ReadXdcCatchStream(numDataBytes);

        // exit if we have no work to do
        if (XdcInputQ.empty() || XdcCatchStreamBytesPending) {
            return progress;
        }

        TStackVec<TIoVec, 64> buffs;
        size_t size = 0;
        for (auto& [channel, span] : XdcInputQ) {
            buffs.push_back(TIoVec{span.data(), span.size()});
            size += span.size();
            if (buffs.size() == 64 || size >= 1024 * 1024 || Params.Encryption) {
                break;
            }
        }

        ssize_t recvres = Read(*XdcSocket, XdcPollerToken, &Context->XdcReadPending, buffs.data(), buffs.size());
        if (recvres == -1) {
            return progress;
        }

        // calculate stream checksums
        {
            size_t bytesToChecksum = recvres;
            for (const auto& iov : buffs) {
                const size_t n = Min(bytesToChecksum, iov.Size);
                HandleXdcChecksum({static_cast<const char*>(iov.Data), n});
                bytesToChecksum -= n;
                if (!bytesToChecksum) {
                    break;
                }
            }
        }

        Y_VERIFY(recvres > 0);
        Metrics->AddTotalBytesRead(recvres);
        *numDataBytes += recvres;
        BytesReadFromXdcSocket += recvres;

        // cut the XdcInputQ deque
        for (size_t bytesToCut = recvres; bytesToCut; ) {
            Y_VERIFY(!XdcInputQ.empty());
            auto& [channel, span] = XdcInputQ.front();
            size_t n = Min(bytesToCut, span.size());
            bytesToCut -= n;
            if (n == span.size()) {
                XdcInputQ.pop_front();
            } else {
                span = span.SubSpan(n, Max<size_t>());
                Y_VERIFY(!bytesToCut);
            }

            Y_VERIFY_DEBUG(n);
            auto& context = GetPerChannelContext(channel);
            context.DropFront(nullptr, n);
            ProcessEvents(context);
        }

        // drop fully processed inbound packets
        ProcessInboundPacketQ(recvres);

        LastReceiveTimestamp = TActivationContext::Monotonic();

        return true;
    }

    void TInputSessionTCP::HandleXdcChecksum(TContiguousSpan span) {
        if (Params.Encryption) {
            return;
        }
        while (span.size()) {
            Y_VERIFY_DEBUG(!XdcChecksumQ.empty());
            auto& [size, expected] = XdcChecksumQ.front();
            const size_t n = Min<size_t>(size, span.size());
            XdcCurrentChecksum = Crc32cExtendMSanCompatible(XdcCurrentChecksum, span.data(), n);
            span = span.SubSpan(n, Max<size_t>());
            size -= n;
            if (!size) {
                if (XdcCurrentChecksum != expected) {
                    LOG_ERROR_IC_SESSION("ICIS16", "payload checksum error");
                    throw TExReestablishConnection{TDisconnectReason::ChecksumError()};
                }
                XdcChecksumQ.pop_front();
                XdcCurrentChecksum = 0;
            }
        }
    }

    void TInputSessionTCP::PreallocateBuffers() {
        // ensure that we have exactly "numBuffers" in queue
        LWPROBE_IF_TOO_LONG(SlowICReadLoopAdjustSize, ms) {
            while (Buffers.size() < CurrentBuffers) {
                Buffers.emplace_back(TRopeAlignedBuffer::Allocate(Common->Settings.PreallocatedBufferSize));
            }
        }
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
                throw TExDestroySession{TDisconnectReason::DeadPeer()};
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

    TReceiveContext::TPerChannelContext& TInputSessionTCP::GetPerChannelContext(ui16 channel) const {
        return channel < std::size(Context->ChannelArray)
            ? Context->ChannelArray[channel]
            : Context->ChannelMap[channel];
    }

    void TInputSessionTCP::GenerateHttpInfo(NMon::TEvHttpInfoRes::TPtr ev) {
        TStringStream str;
        ev->Get()->Output(str);

        HTML(str) {
            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "Input Session";
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
#define MON_VAR(KEY) \
    TABLER() { \
        TABLED() { str << #KEY; } \
        TABLED() { str << (KEY); } \
    }

                        TABLEBODY() {
                            MON_VAR(BytesReadFromSocket)
                            MON_VAR(PacketsReadFromSocket)
                            MON_VAR(DataPacketsReadFromSocket)
                            MON_VAR(IgnoredDataPacketsFromSocket)

                            MON_VAR(BytesReadFromXdcSocket)
                            MON_VAR(XdcSections)
                            MON_VAR(XdcRefs)

                            MON_VAR(PayloadSize)
                            MON_VAR(InboundPacketQ.size())
                            MON_VAR(XdcInputQ.size())
                            MON_VAR(Buffers.size())
                            MON_VAR(IncomingData.GetSize())
                            MON_VAR(Payload.GetSize())
                            MON_VAR(CurrentBuffers)

                            MON_VAR(Context->LastProcessedSerial)
                            MON_VAR(ConfirmedByInput)
                        }
                    }
                }
            }
        }

        TActivationContext::Send(new IEventHandle(ev->Recipient, ev->Sender, new NMon::TEvHttpInfoRes(str.Str())));
    }

}
