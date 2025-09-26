#include "interconnect_tcp_session.h"
#include "interconnect_tcp_proxy.h"
#include <ydb/library/actors/core/probes.h>
#include <ydb/library/actors/util/datetime.h>

namespace NActors {
    LWTRACE_USING(ACTORLIB_PROVIDER);

    void TReceiveContext::TPerChannelContext::PrepareCatchBuffer() {
        size_t bytesToCatch = FetchOffset;
        for (auto it = XdcBuffers.begin(), end = it + FetchIndex; it != end; ++it) {
            bytesToCatch += it->size();
        }

        XdcCatchBuffer = TRcBuf::Uninitialized(bytesToCatch);
        XdcCatchBytesRead = 0;
    }

    void TReceiveContext::TPerChannelContext::ApplyCatchBuffer() {
        if (auto buffer = std::exchange(XdcCatchBuffer, {})) {
            Y_ABORT_UNLESS(XdcCatchBytesRead >= buffer.size());

            const size_t offset = XdcCatchBytesRead % buffer.size();
            const char *begin = buffer.data();
            const char *mid = begin + offset;
            const char *end = begin + buffer.size();
            Y_DEBUG_ABORT_UNLESS(begin <= mid && mid < end);

            TRope rope;
            rope.Insert(rope.End(), TRcBuf(TRcBuf::Piece, mid, end, buffer));
            if (begin != mid) {
                rope.Insert(rope.End(), TRcBuf(TRcBuf::Piece, begin, mid, buffer));
            }

            DropFront(&rope, buffer.size());
        } else {
            Y_DEBUG_ABORT_UNLESS(!XdcCatchBytesRead);
        }
    }

    void TReceiveContext::TPerChannelContext::FetchBuffers(ui16 channel, size_t numBytes,
            std::deque<std::tuple<ui16, TMutableContiguousSpan>>& outQ) {
        Y_DEBUG_ABORT_UNLESS(numBytes);
        auto it = XdcBuffers.begin() + FetchIndex;
        for (;;) {
            Y_DEBUG_ABORT_UNLESS(it != XdcBuffers.end());
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
        Y_DEBUG_ABORT_UNLESS(from || !XdcCatchBuffer);

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
            Y_DEBUG_ABORT_UNLESS(!XdcBuffers.empty());
            auto& front = XdcBuffers.front();
            if (from) {
                from->ExtractFrontPlain(front.data(), Min(numBytes, front.size()));
            }
            if (numBytes < front.size()) {
                front = front.SubSpan(numBytes, Max<size_t>());
                if (!FetchIndex) { // we are sending this very buffer, adjust sending offset
                    Y_DEBUG_ABORT_UNLESS(numBytes <= FetchOffset);
                    FetchOffset -= numBytes;
                }
                break;
            } else {
                numBytes -= front.size();
                Y_DEBUG_ABORT_UNLESS(FetchIndex);
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
        Y_ABORT_UNLESS(Context);
        Y_ABORT_UNLESS(Socket);
        Y_ABORT_UNLESS(SessionId);
        Y_ABORT_UNLESS(!Params.UseExternalDataChannel == !XdcSocket);

        Metrics->SetClockSkewMicrosec(0);

        Context->UpdateState = EUpdateState::NONE;

        // ensure that we do not spawn new session while the previous one is still alive
        TAtomicBase sessions = AtomicIncrement(Context->NumInputSessions);
        Y_ABORT_UNLESS(sessions == 1, "sessions# %" PRIu64, ui64(sessions));

        // calculate number of bytes to catch
        for (auto& context : Context->ChannelArray) {
            context.PrepareCatchBuffer();
        }
        for (auto& [channel, context] : Context->ChannelMap) {
            context.PrepareCatchBuffer();
        }

        UsageHisto.fill(0);
        InputTrafficArray.fill(0);

        XXH3_64bits_reset(&XxhashXdcState);
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
                ++CpuStarvationEvents;
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
                    Y_DEBUG_ABORT_UNLESS(PayloadSize);
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

        if (enoughCpu) {
            SetEnoughCpu(true);
            StarvingInRow = 0;
        } else {
            SetEnoughCpu(++StarvingInRow < StarvingInRowForNotEnoughCpu);
        }

        // calculate ping time
        auto it = std::min_element(PingQ.begin(), PingQ.end());
        const TDuration ping = it != PingQ.end() ? *it : TDuration::Zero();

        // send update to main session actor if something valuable has changed
        if (!UpdateFromInputSession) {
            UpdateFromInputSession = MakeHolder<TEvUpdateFromInputSession>(ConfirmedByInput, numDataBytes, ping);
        } else {
            Y_ABORT_UNLESS(ConfirmedByInput >= UpdateFromInputSession->ConfirmedByInput);
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
                        Y_ABORT_UNLESS(UpdateFromInputSession);
                        break;

                    default:
                        Y_ABORT("unexpected state");
                }
                break;
            }
        }

        for (size_t channel = 0; channel < InputTrafficArray.size(); ++channel) {
            if (auto& value = InputTrafficArray[channel]) {
                Metrics->AddInputChannelsIncomingTraffic(channel, std::exchange(value, 0));
            }
        }
        for (auto& [channel, value] : std::exchange(InputTrafficMap, {})) {
            if (value) {
                Metrics->AddInputChannelsIncomingTraffic(channel, std::exchange(value, 0));
            }
        }
    }

    void TInputSessionTCP::ProcessHeader() {
        TTcpPacketHeader_v2 header;
        const bool success = IncomingData.ExtractFrontPlain(&header, sizeof(header));
        Y_ABORT_UNLESS(success);
        PayloadSize = header.PayloadLength;
        const ui64 serial = header.Serial;
        const ui64 confirm = header.Confirm;
        if (!Params.Encryption) {
            ChecksumExpected = std::exchange(header.Checksum, 0);
            if (Params.UseXxhash) {
                XXH3_64bits_reset(&XxhashState);
                XXH3_64bits_update(&XxhashState, &header, sizeof(header));
                if (!PayloadSize) {
                    Checksum = XXH3_64bits_digest(&XxhashState);
                }
            } else {
                Checksum = Crc32cExtendMSanCompatible(0, &header, sizeof(header)); // start calculating checksum now
            }
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
            const ui64 expectedMin = Context->GetLastPacketSerialToConfirm() + 1;
            const ui64 expectedMax = Context->LastProcessedSerial + 1;
            Y_DEBUG_ABORT_UNLESS(expectedMin <= expectedMax);
            if (CurrentSerial ? serial != CurrentSerial + 1 : (serial == 0 || serial > expectedMin)) {
                LOG_CRIT_IC_SESSION("ICIS06", "%s", TString(TStringBuilder()
                        << "packet serial number mismatch"
                        << " Serial# " << serial
                        << " ExpectedMin# " << expectedMin
                        << " ExpectedMax# " << expectedMax
                        << " CurrentSerial# " << CurrentSerial
                    ).data());
                throw TExDestroySession{TDisconnectReason::FormatError()};
            }
            IgnorePayload = serial != expectedMax;
            CurrentSerial = serial;
            State = EState::PAYLOAD;
            Y_DEBUG_ABORT_UNLESS(!Payload);
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
        InboundPacketQ.push_back(TInboundPacket{CurrentSerial, 0});
        State = EState::HEADER;
        if (!Params.Encryption) { // see if we are checksumming packet body
            for (const auto&& [data, size] : Payload) {
                if (Params.UseXxhash) {
                    XXH3_64bits_update(&XxhashState, data, size);
                } else {
                    Checksum = Crc32cExtendMSanCompatible(Checksum, data, size);
                }
            }
            if (Params.UseXxhash) {
                Checksum = XXH3_64bits_digest(&XxhashState);
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
                Y_ABORT_UNLESS(success);
                ProcessXdcCommand(channel, context);
            } else if (IgnorePayload) { // throw payload out
                Payload.EraseFront(part.Size);
            } else if (!part.IsLastPart()) { // just ordinary inline event data
                Payload.ExtractFront(part.Size, &pendingEvent.InternalPayload);
            } else { // event final block
                TEventDescr2 v2;

                if (part.Size != sizeof(v2)) {
                    LOG_CRIT_IC_SESSION("ICIS11", "incorrect last part of an event");
                    throw TExDestroySession{TDisconnectReason::FormatError()};
                }

                const bool success = Payload.ExtractFrontPlain(&v2, sizeof(v2));
                Y_ABORT_UNLESS(success);

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

            const ui32 traffic = sizeof(part) + part.Size;
            if (channel < InputTrafficArray.size()) {
                InputTrafficArray[channel] += traffic;
            } else {
                InputTrafficMap[channel] += traffic;
            }
        }

        // mark packet as processed
        if (IgnorePayload) {
            Y_DEBUG_ABORT_UNLESS(CurrentSerial <= Context->LastProcessedSerial);
        } else {
            ++Context->LastProcessedSerial;
            Y_DEBUG_ABORT_UNLESS(CurrentSerial == Context->LastProcessedSerial);
        }
        XdcCatchStream.Ready = Context->LastProcessedSerial == CurrentSerial;
        ApplyXdcCatchStream();
        ProcessInboundPacketQ(0);

        ++PacketsReadFromSocket;
        ++DataPacketsReadFromSocket;
        IgnoredDataPacketsFromSocket += IgnorePayload;
    }

    void TInputSessionTCP::ProcessInboundPacketQ(ui64 numXdcBytesRead) {
        for (; !InboundPacketQ.empty(); InboundPacketQ.pop_front()) {
            auto& front = InboundPacketQ.front();

            const size_t n = Min(numXdcBytesRead, front.XdcUnreadBytes);
            front.XdcUnreadBytes -= n;
            numXdcBytesRead -= n;

            if (front.XdcUnreadBytes) { // we haven't finished this packet yet
                Y_ABORT_UNLESS(!numXdcBytesRead);
                break;
            }

            Y_DEBUG_ABORT_UNLESS(front.Serial + InboundPacketQ.size() - 1 <= Context->LastProcessedSerial,
                "front.Serial# %" PRIu64 " LastProcessedSerial# %" PRIu64 " InboundPacketQ.size# %zu",
                front.Serial, Context->LastProcessedSerial, InboundPacketQ.size());

            if (Context->GetLastPacketSerialToConfirm() < front.Serial && !Context->AdvanceLastPacketSerialToConfirm(front.Serial)) {
                throw TExReestablishConnection{TDisconnectReason::NewSession()};
            }
        }
    }

    void TInputSessionTCP::ProcessXdcCommand(ui16 channel, TReceiveContext::TPerChannelContext& context) {
        const char *ptr = XdcCommands.data();
        const char *end = ptr + XdcCommands.size();
        while (ptr != end) {
            switch (const auto cmd = static_cast<EXdcCommand>(*ptr++)) {
                case EXdcCommand::DECLARE_SECTION:
                case EXdcCommand::DECLARE_SECTION_INLINE: {
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
                        auto& pendingEvent = context.PendingEvents.back();
                        const bool isInline = cmd == EXdcCommand::DECLARE_SECTION_INLINE;
                        pendingEvent.SerializationInfo.Sections.push_back(TEventSectionInfo{headroom, size, tailroom,
                            alignment, isInline});

                        Y_ABORT_UNLESS(!isInline || Params.UseXdcShuffle);
                        if (!isInline) {
                            // allocate buffer and push it into the payload
                            auto buffer = TRcBuf::Uninitialized(size, headroom, tailroom);
                            if (size) {
                                context.XdcBuffers.push_back(buffer.GetContiguousSpanMut());
                            }
                            pendingEvent.ExternalPayload.Insert(pendingEvent.ExternalPayload.End(), TRope(std::move(buffer)));
                            pendingEvent.XdcSizeLeft += size;
                            ++XdcSections;
                        }
                    }
                    continue;
                }

                case EXdcCommand::PUSH_DATA: {
                    const size_t cmdLen = sizeof(ui16) + (Params.Encryption ? 0 : sizeof(ui32));
                    if (static_cast<size_t>(end - ptr) < cmdLen) {
                        LOG_CRIT_IC_SESSION("ICIS18", "XDC command format error");
                        throw TExDestroySession{TDisconnectReason::FormatError()};
                    }

                    const ui16 size = ReadUnaligned<ui16>(ptr);
                    if (!size) {
                        LOG_CRIT_IC_SESSION("ICIS03", "XDC empty payload");
                        throw TExDestroySession{TDisconnectReason::FormatError()};
                    }

                    if (!Params.Encryption) {
                        const ui32 checksumExpected = ReadUnaligned<ui32>(ptr + sizeof(ui16));
                        XdcChecksumQ.emplace_back(size, checksumExpected);
                    }

                    // account channel and number of bytes in XDC for this packet
                    auto& packet = InboundPacketQ.back();
                    packet.XdcUnreadBytes += size;

                    if (IgnorePayload) {
                        // this packet was already marked as 'processed', all commands had been executed, but we must
                        // parse XDC stream from this packet correctly
                        const bool apply = Context->GetLastPacketSerialToConfirm() < CurrentSerial &&
                            GetPerChannelContext(channel).XdcCatchBuffer;
                        XdcCatchStream.BytesPending += size;
                        XdcCatchStream.Markup.emplace_back(channel, apply, size);
                    } else {
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

            // create aggregated payload
            TRope payload;
            if (!pendingEvent.SerializationInfo.Sections.empty()) {
                // unshuffle inline and external payloads into single event content
                TRope *prev = nullptr;
                size_t accumSize = 0;
                for (const auto& s : pendingEvent.SerializationInfo.Sections) {
                    TRope *rope = s.IsInline
                        ? &pendingEvent.InternalPayload
                        : &pendingEvent.ExternalPayload;
                    if (rope != prev) {
                        if (accumSize) {
                            prev->ExtractFront(accumSize, &payload);
                        }
                        prev = rope;
                        accumSize = 0;
                    }
                    accumSize += s.Size;
                }
                if (accumSize) {
                    prev->ExtractFront(accumSize, &payload);
                }

                if (pendingEvent.InternalPayload || pendingEvent.ExternalPayload) {
                    LOG_CRIT_IC_SESSION("ICIS19", "unprocessed payload remains after shuffling"
                        " Type# 0x%08" PRIx32 " InternalPayload.size# %zu ExternalPayload.size# %zu",
                        descr.Type, pendingEvent.InternalPayload.size(), pendingEvent.ExternalPayload.size());
                    Y_DEBUG_ABORT_UNLESS(false);
                    throw TExReestablishConnection{TDisconnectReason::FormatError()};
                }
            }

            // we add any remains of internal payload to the end
            if (auto& rope = pendingEvent.InternalPayload) {
                rope.ExtractFront(rope.size(), &payload);
            }
            // and ensure there is no unprocessed external payload
            Y_ABORT_UNLESS(!pendingEvent.ExternalPayload);

#if IC_FORCE_HARDENED_PACKET_CHECKS
            if (descr.Len != payload.GetSize()) {
                LOG_CRIT_IC_SESSION("ICIS17", "event length mismatch Type# 0x%08" PRIx32 " received# %zu expected# %" PRIu32,
                    descr.Type, payload.GetSize(), descr.Len);
                throw TExReestablishConnection{TDisconnectReason::ChecksumError()};
            }
#endif
            if (descr.Checksum) {
                ui32 checksum = 0;
                for (const auto&& [data, size] : payload) {
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
                MakeIntrusive<TEventSerializedData>(std::move(payload), std::move(pendingEvent.SerializationInfo)),
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
                    Y_ABORT_UNLESS(UpdateFromInputSession);
                    if (Context->UpdateState.compare_exchange_weak(state, EUpdateState::INFLIGHT)) {
                        Send(SessionId, UpdateFromInputSession.Release());
                        return;
                    }
            }
        }
    }

    ssize_t TInputSessionTCP::Read(NInterconnect::TStreamSocket& socket, const TPollerToken::TPtr& token,
            bool *readPending, const TIoVec *iov, size_t num) {
        for (;;) {
            ssize_t recvres = 0;
            TString err;
            LWPROBE_IF_TOO_LONG(SlowICReadFromSocket, ms) {
                do {
                    const ui64 begin = GetCycleCountFast();
                    if (num == 1) {
                        recvres = socket.Recv(iov->Data, iov->Size, &err);
                    } else {
                        recvres = socket.ReadV(reinterpret_cast<const iovec*>(iov), num);
                    }
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
                } else if (token && !*readPending) {
                    if (socket.RequestReadNotificationAfterWouldBlock(*token)) {
                        continue; // can try again
                    } else {
                        *readPending = true;
                    }
                }
                return -1;
            }

            return recvres;
        }
    }

    constexpr ui64 GetUsageCountClearMask(size_t items, int bits) {
        ui64 mask = 0;
        for (size_t i = 0; i < items; ++i) {
            mask |= ui64(1 << bits - 2) << i * bits;
        }
        return mask;
    }

    bool TInputSessionTCP::ReadMore() {
        PreallocateBuffers();

        TStackVec<TIoVec, MaxBuffers> buffs;
        for (auto& item : Buffers) {
            buffs.push_back({item.GetDataMut(), item.size()});
            if (Params.Encryption) {
                break; // do not put more than one buffer in queue to prevent using ReadV
            }
#ifdef _win_
            break; // do the same thing for Windows build
#endif
        }

        ssize_t recvres = Read(*Socket, PollerToken, &Context->MainReadPending, buffs.data(), buffs.size());
        if (recvres == -1) {
            return false;
        }

        Y_ABORT_UNLESS(recvres > 0);
        Metrics->AddTotalBytesRead(recvres);
        BytesReadFromSocket += recvres;

        size_t numBuffersCovered = 0;

        while (recvres) {
            Y_ABORT_UNLESS(!Buffers.empty());
            auto& buffer = Buffers.front();
            const size_t bytes = Min<size_t>(recvres, buffer.size());
            recvres -= bytes;
            if (const size_t newSize = buffer.size() - bytes) {
                IncomingData.Insert(IncomingData.End(), TRcBuf(TRcBuf::Piece, buffer.data(), bytes, buffer));
                buffer.TrimFront(newSize);
            } else {
                IncomingData.Insert(IncomingData.End(), std::move(buffer));
                Buffers.pop_front();
            }
            ++numBuffersCovered;
        }

        if (Buffers.empty()) { // we have read all the data, increase number of buffers
            CurrentBuffers = Min(CurrentBuffers * 2, MaxBuffers);
        } else {
            Y_DEBUG_ABORT_UNLESS(numBuffersCovered);

            const size_t index = numBuffersCovered - 1;

            static constexpr ui64 itemMask = (1 << BitsPerUsageCount) - 1;

            const size_t word = index / ItemsPerUsageCount;
            const size_t offset = index % ItemsPerUsageCount * BitsPerUsageCount;

            if ((UsageHisto[word] >> offset & itemMask) == itemMask) { // time to shift
                for (ui64& w : UsageHisto) {
                    static constexpr ui64 mask = GetUsageCountClearMask(ItemsPerUsageCount, BitsPerUsageCount);
                    w = (w & mask) >> 1;
                }
            }
            UsageHisto[word] += ui64(1) << offset;

            while (CurrentBuffers > 1) {
                const size_t index = CurrentBuffers - 1;
                if (UsageHisto[index / ItemsPerUsageCount] >> index % ItemsPerUsageCount * BitsPerUsageCount & itemMask) {
                    break;
                } else {
                    --CurrentBuffers;
                }
            }
        }

        LastReceiveTimestamp = TActivationContext::Monotonic();

        return true;
    }

    bool TInputSessionTCP::ReadXdcCatchStream(ui64 *numDataBytes) {
        bool progress = false;

        while (XdcCatchStream.BytesPending) {
            if (!XdcCatchStream.Buffer) {
                XdcCatchStream.Buffer = TRcBuf::Uninitialized(64 * 1024);
            }

            const size_t numBytesToRead = Min<size_t>(XdcCatchStream.BytesPending, XdcCatchStream.Buffer.size());

            TIoVec iov{XdcCatchStream.Buffer.GetDataMut(), numBytesToRead};
            ssize_t recvres = Read(*XdcSocket, XdcPollerToken, &Context->XdcReadPending, &iov, 1);
            if (recvres == -1) {
                return progress;
            }

            HandleXdcChecksum({XdcCatchStream.Buffer.data(), static_cast<size_t>(recvres)});

            XdcCatchStream.BytesPending -= recvres;
            XdcCatchStream.BytesProcessed += recvres;
            *numDataBytes += recvres;
            BytesReadFromXdcSocket += recvres;

            // scatter read data
            const char *in = XdcCatchStream.Buffer.data();
            while (recvres) {
                Y_DEBUG_ABORT_UNLESS(!XdcCatchStream.Markup.empty());
                auto& [channel, apply, bytes] = XdcCatchStream.Markup.front();
                size_t bytesInChannel = Min<size_t>(recvres, bytes);
                bytes -= bytesInChannel;
                recvres -= bytesInChannel;

                if (apply) {
                    auto& context = GetPerChannelContext(channel);
                    while (bytesInChannel) {
                        const size_t offset = context.XdcCatchBytesRead % context.XdcCatchBuffer.size();
                        TMutableContiguousSpan out = context.XdcCatchBuffer.GetContiguousSpanMut().SubSpan(offset, bytesInChannel);
                        memcpy(out.data(), in, out.size());
                        context.XdcCatchBytesRead += out.size();
                        in += out.size();
                        bytesInChannel -= out.size();
                    }
                } else {
                    in += bytesInChannel;
                }

                if (!bytes) {
                    XdcCatchStream.Markup.pop_front();
                }
            }

            progress = true;
        }

        ApplyXdcCatchStream();

        return progress;
    }

    void TInputSessionTCP::ApplyXdcCatchStream() {
        if (!XdcCatchStream.Applied && XdcCatchStream.Ready && !XdcCatchStream.BytesPending) {
            Y_DEBUG_ABORT_UNLESS(XdcCatchStream.Markup.empty());

            auto process = [&](auto& context) {
                context.ApplyCatchBuffer();
                ProcessEvents(context);
            };
            for (auto& context : Context->ChannelArray) {
                process(context);
            }
            for (auto& [channel, context] : Context->ChannelMap) {
                process(context);
            }

            ProcessInboundPacketQ(XdcCatchStream.BytesProcessed);

            XdcCatchStream.Buffer = {};
            XdcCatchStream.Applied = true;
        }
    }

    bool TInputSessionTCP::ReadXdc(ui64 *numDataBytes) {
        bool progress = ReadXdcCatchStream(numDataBytes);

        // exit if we have no work to do
        if (XdcInputQ.empty() || !XdcCatchStream.Applied) {
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

        Y_ABORT_UNLESS(recvres > 0);
        Metrics->AddTotalBytesRead(recvres);
        *numDataBytes += recvres;
        BytesReadFromXdcSocket += recvres;

        // cut the XdcInputQ deque
        for (size_t bytesToCut = recvres; bytesToCut; ) {
            Y_ABORT_UNLESS(!XdcInputQ.empty());
            auto& [channel, span] = XdcInputQ.front();
            size_t n = Min(bytesToCut, span.size());
            bytesToCut -= n;
            if (n == span.size()) {
                XdcInputQ.pop_front();
            } else {
                span = span.SubSpan(n, Max<size_t>());
                Y_ABORT_UNLESS(!bytesToCut);
            }

            Y_DEBUG_ABORT_UNLESS(n);
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
            Y_DEBUG_ABORT_UNLESS(!XdcChecksumQ.empty());
            auto& [size, expected] = XdcChecksumQ.front();
            const size_t n = Min<size_t>(size, span.size());
            if (Params.UseXxhash) {
                XXH3_64bits_update(&XxhashXdcState, span.data(), n);
            } else {
                XdcCurrentChecksum = Crc32cExtendMSanCompatible(XdcCurrentChecksum, span.data(), n);
            }
            span = span.SubSpan(n, Max<size_t>());
            size -= n;
            if (!size) {
                if (Params.UseXxhash) {
                    XdcCurrentChecksum = XXH3_64bits_digest(&XxhashXdcState);
                    XXH3_64bits_reset(&XxhashXdcState);
                }
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
                Buffers.push_back(TRcBuf::Uninitialized(Common->Settings.PreallocatedBufferSize));
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
                            MON_VAR(CpuStarvationEvents)

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
