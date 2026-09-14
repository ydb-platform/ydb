#include "http2.h"

#include <util/string/builder.h>

namespace NHttp::NHttp2 {

// ============== TSession ==============

TSession::TSession(ERole role, TSessionCallbacks callbacks)
    : Role(role)
    , Callbacks(std::move(callbacks))
{
    if (Role == ERole::Server) {
        FrameParseState = EFrameParseState::WaitingPreface;
        NextLocalStreamId = 2; // server uses even stream IDs
    } else {
        FrameParseState = EFrameParseState::ReadingHeader;
        NextLocalStreamId = 1; // client uses odd stream IDs
    }
}

void TSession::Initialize() {
    if (Initialized) return;
    Initialized = true;

    if (Role == ERole::Client) {
        // Client sends connection preface (magic + SETTINGS)
        QueueOutput(TString(CONNECTION_PREFACE));
    }
    // Both sides send initial SETTINGS
    SendSettings();
    if (Callbacks.OnSend) {
        Callbacks.OnSend(FlushOutputBuffer());
    }
}

void TSession::QueueOutput(TString data) {
    OutputBuffer.append(data);
}

void TSession::QueueOutput(const char* data, size_t len) {
    OutputBuffer.append(data, len);
}

TString TSession::FlushOutputBuffer() {
    TString result;
    result.swap(OutputBuffer);
    return result;
}

void TSession::SendSettings() {
    TVector<TSettingsEntry> entries;
    entries.push_back({ESettingsId::MAX_CONCURRENT_STREAMS, LocalSettings.MaxConcurrentStreams});
    entries.push_back({ESettingsId::INITIAL_WINDOW_SIZE, LocalSettings.InitialWindowSize});
    entries.push_back({ESettingsId::MAX_FRAME_SIZE, LocalSettings.MaxFrameSize});
    entries.push_back({ESettingsId::ENABLE_PUSH, 0}); // disable server push
    QueueOutput(TFrameBuilder::BuildSettings(0, NFrameFlag::NONE, entries));
}

void TSession::SendSettingsAck() {
    QueueOutput(TFrameBuilder::BuildSettingsAck());
}

void TSession::SendWindowUpdate(uint32_t streamId, uint32_t increment) {
    if (increment > 0) {
        QueueOutput(TFrameBuilder::BuildWindowUpdate(streamId, increment));
    }
}

void TSession::SendRstStream(uint32_t streamId, EErrorCode errorCode) {
    QueueOutput(TFrameBuilder::BuildRstStream(streamId, errorCode));
}

TStream& TSession::GetOrCreateStream(uint32_t streamId) {
    auto [it, inserted] = Streams.try_emplace(streamId);
    if (inserted) {
        it->second.StreamId = streamId;
        it->second.State = EStreamState::Open;
        it->second.SendWindowSize = PeerSettings.InitialWindowSize;
        it->second.RecvWindowSize = LocalSettings.InitialWindowSize;
    }
    return it->second;
}

TStream* TSession::FindStream(uint32_t streamId) {
    auto it = Streams.find(streamId);
    if (it == Streams.end()) return nullptr;
    return &it->second;
}

size_t TSession::Feed(const char* data, size_t len) {
    size_t consumed = 0;

    while (consumed < len) {
        switch (FrameParseState) {
            case EFrameParseState::WaitingPreface: {
                // Server waits for "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n" (24 bytes)
                while (consumed < len && PrefaceBytesMatched < CONNECTION_PREFACE.size()) {
                    if (data[consumed] == CONNECTION_PREFACE[PrefaceBytesMatched]) {
                        PrefaceBytesMatched++;
                        consumed++;
                    } else {
                        if (Callbacks.OnError) {
                            Callbacks.OnError("Invalid HTTP/2 connection preface");
                        }
                        return consumed;
                    }
                }
                if (PrefaceBytesMatched == CONNECTION_PREFACE.size()) {
                    PrefaceReceived = true;
                    FrameParseState = EFrameParseState::ReadingHeader;
                    FrameBuffer.clear();
                }
                break;
            }

            case EFrameParseState::ReadingHeader: {
                size_t need = FRAME_HEADER_SIZE - FrameBuffer.size();
                size_t avail = len - consumed;
                size_t take = std::min(need, avail);
                FrameBuffer.append(data + consumed, take);
                consumed += take;

                if (FrameBuffer.size() == FRAME_HEADER_SIZE) {
                    CurrentFrameHeader.Parse(FrameBuffer.data());
                    FrameBuffer.clear();

                    if (CurrentFrameHeader.Length > PeerSettings.MaxFrameSize) {
                        // For unrecognized frame types, we still need to respect FRAME_SIZE_ERROR
                        ConnectionError(EErrorCode::FRAME_SIZE_ERROR, "Frame too large");
                        return consumed;
                    }

                    if (CurrentFrameHeader.Length == 0) {
                        ProcessFrame(CurrentFrameHeader, {});
                        FrameParseState = EFrameParseState::ReadingHeader;
                    } else {
                        FrameParseState = EFrameParseState::ReadingPayload;
                    }
                }
                break;
            }

            case EFrameParseState::ReadingPayload: {
                size_t need = CurrentFrameHeader.Length - FrameBuffer.size();
                size_t avail = len - consumed;
                size_t take = std::min(need, avail);
                FrameBuffer.append(data + consumed, take);
                consumed += take;

                if (FrameBuffer.size() == CurrentFrameHeader.Length) {
                    ProcessFrame(CurrentFrameHeader, FrameBuffer);
                    FrameBuffer.clear();
                    FrameParseState = EFrameParseState::ReadingHeader;
                }
                break;
            }
        }
    }

    // Flush accumulated output
    if (HasPendingOutput() && Callbacks.OnSend) {
        Callbacks.OnSend(FlushOutputBuffer());
    }

    return consumed;
}

void TSession::ProcessFrame(const TFrameHeader& header, TStringBuf payload) {
    // If waiting for CONTINUATION, only CONTINUATION for the same stream is acceptable
    if (ExpectingContinuationStream != 0) {
        if (header.Type != EFrameType::CONTINUATION || header.StreamId != ExpectingContinuationStream) {
            return ConnectionError(EErrorCode::PROTOCOL_ERROR, "Expected CONTINUATION frame");
        }
    }

    switch (header.Type) {
        case EFrameType::DATA:
            ProcessDataFrame(header, payload);
            break;
        case EFrameType::HEADERS:
            ProcessHeadersFrame(header, payload);
            break;
        case EFrameType::PRIORITY:
            ProcessPriorityFrame(header, payload);
            break;
        case EFrameType::RST_STREAM:
            ProcessRstStreamFrame(header, payload);
            break;
        case EFrameType::SETTINGS:
            ProcessSettingsFrame(header, payload);
            break;
        case EFrameType::PUSH_PROMISE:
            // We don't support server push, send PROTOCOL_ERROR
            return ConnectionError(EErrorCode::PROTOCOL_ERROR, "PUSH_PROMISE not supported");
        case EFrameType::PING:
            ProcessPingFrame(header, payload);
            break;
        case EFrameType::GOAWAY:
            ProcessGoawayFrame(header, payload);
            break;
        case EFrameType::WINDOW_UPDATE:
            ProcessWindowUpdateFrame(header, payload);
            break;
        case EFrameType::CONTINUATION:
            ProcessContinuationFrame(header, payload);
            break;
        default:
            // Unknown frame types MUST be ignored (RFC 7540 Section 4.1)
            break;
    }
}

void TSession::ProcessDataFrame(const TFrameHeader& header, TStringBuf payload) {
    if (header.StreamId == 0) {
        return ConnectionError(EErrorCode::PROTOCOL_ERROR, "DATA frame on stream 0");
    }

    TStream* stream = FindStream(header.StreamId);
    if (!stream) {
        // Stream might have been closed/reset - ignore or send error
        return;
    }

    TStringBuf data = payload;
    // Handle PADDED flag
    if (header.HasFlag(NFrameFlag::PADDED)) {
        if (data.empty()) {
            return ConnectionError(EErrorCode::FRAME_SIZE_ERROR);
        }
        uint8_t padLen = static_cast<uint8_t>(data[0]);
        data.Skip(1);
        if (padLen >= data.size()) {
            return ConnectionError(EErrorCode::PROTOCOL_ERROR);
        }
        data.Chop(padLen);
    }

    stream->Body.append(data);

    // Update flow control
    RecvWindowSize -= static_cast<int32_t>(payload.size());
    stream->RecvWindowSize -= static_cast<int32_t>(payload.size());

    // Send WINDOW_UPDATE if window is getting low
    if (RecvWindowSize < static_cast<int32_t>(DEFAULT_INITIAL_WINDOW_SIZE / 2)) {
        uint32_t increment = DEFAULT_INITIAL_WINDOW_SIZE - RecvWindowSize;
        SendWindowUpdate(0, increment);
        RecvWindowSize += increment;
    }
    if (stream->RecvWindowSize < static_cast<int32_t>(DEFAULT_INITIAL_WINDOW_SIZE / 2)) {
        uint32_t increment = DEFAULT_INITIAL_WINDOW_SIZE - stream->RecvWindowSize;
        SendWindowUpdate(header.StreamId, increment);
        stream->RecvWindowSize += increment;
    }

    if (header.HasFlag(NFrameFlag::END_STREAM)) {
        stream->EndStream = true;
        if (stream->State == EStreamState::Open) {
            stream->State = EStreamState::HalfClosedRemote;
        } else if (stream->State == EStreamState::HalfClosedLocal) {
            stream->State = EStreamState::Closed;
        }

        if (Role == ERole::Server && stream->HeadersComplete) {
            if (Callbacks.OnRequest) {
                Callbacks.OnRequest(header.StreamId, *stream);
            }
        } else if (Role == ERole::Client && stream->HeadersComplete) {
            if (Callbacks.OnResponse) {
                Callbacks.OnResponse(header.StreamId, *stream);
            }
        }
        // Callback may have sent a response and erased the stream,
        // invalidating `stream`. Re-look up before further access.
        stream = FindStream(header.StreamId);
        if (stream && stream->State == EStreamState::Closed) {
            Streams.erase(header.StreamId);
        }
    }
}

void TSession::ProcessHeadersFrame(const TFrameHeader& header, TStringBuf payload) {
    if (header.StreamId == 0) {
        return ConnectionError(EErrorCode::PROTOCOL_ERROR, "HEADERS frame on stream 0");
    }

    TStringBuf data = payload;

    // Handle PADDED flag
    uint8_t padLen = 0;
    if (header.HasFlag(NFrameFlag::PADDED)) {
        if (data.empty()) {
            return ConnectionError(EErrorCode::FRAME_SIZE_ERROR);
        }
        padLen = static_cast<uint8_t>(data[0]);
        data.Skip(1);
    }

    // Handle PRIORITY flag
    if (header.HasFlag(NFrameFlag::PRIORITY)) {
        if (data.size() < 5) {
            return ConnectionError(EErrorCode::FRAME_SIZE_ERROR);
        }
        // Skip priority fields (stream dependency + weight)
        data.Skip(5);
    }

    if (padLen > 0) {
        if (padLen >= data.size()) {
            return ConnectionError(EErrorCode::PROTOCOL_ERROR);
        }
        data.Chop(padLen);
    }

    // Track stream
    bool isNewStream = (Role == ERole::Server && (header.StreamId & 1) != 0 && header.StreamId > LastPeerStreamId);
    if (isNewStream) {
        LastPeerStreamId = header.StreamId;
    }

    TStream& stream = GetOrCreateStream(header.StreamId);
    stream.HeaderBlockFragment.append(data);

    bool endHeaders = header.HasFlag(NFrameFlag::END_HEADERS);
    bool endStream = header.HasFlag(NFrameFlag::END_STREAM);

    if (endHeaders) {
        CompleteHeaderBlock(header.StreamId, stream, endStream);
    } else {
        // Need CONTINUATION frames
        ExpectingContinuationStream = header.StreamId;
        stream.EndStream = endStream; // remember for when headers complete
    }
}

void TSession::ProcessPriorityFrame(const TFrameHeader& header, TStringBuf payload) {
    if (header.StreamId == 0) {
        return ConnectionError(EErrorCode::PROTOCOL_ERROR, "PRIORITY frame on stream 0");
    }
    if (payload.size() != 5) {
        return ConnectionError(EErrorCode::FRAME_SIZE_ERROR);
    }
    // We ignore priority information
}

void TSession::ProcessRstStreamFrame(const TFrameHeader& header, TStringBuf payload) {
    if (header.StreamId == 0) {
        return ConnectionError(EErrorCode::PROTOCOL_ERROR, "RST_STREAM on stream 0");
    }
    if (payload.size() != 4) {
        return ConnectionError(EErrorCode::FRAME_SIZE_ERROR);
    }

    CloseStream(header.StreamId);
}

void TSession::ProcessSettingsFrame(const TFrameHeader& header, TStringBuf payload) {
    if (header.StreamId != 0) {
        return ConnectionError(EErrorCode::PROTOCOL_ERROR, "SETTINGS on non-zero stream");
    }

    if (header.HasFlag(NFrameFlag::ACK)) {
        if (payload.size() != 0) {
            return ConnectionError(EErrorCode::FRAME_SIZE_ERROR, "SETTINGS ACK with payload");
        }
        SettingsAckPending = false;
        if (!PrefaceReceived && Role == ERole::Client) {
            PrefaceReceived = true;
        }
        return;
    }

    if (payload.size() % 6 != 0) {
        return ConnectionError(EErrorCode::FRAME_SIZE_ERROR, "SETTINGS payload not multiple of 6");
    }

    const uint8_t* p = reinterpret_cast<const uint8_t*>(payload.data());
    for (size_t i = 0; i < payload.size(); i += 6) {
        uint16_t id = (static_cast<uint16_t>(p[i]) << 8) | p[i + 1];
        uint32_t val = (static_cast<uint32_t>(p[i + 2]) << 24)
                     | (static_cast<uint32_t>(p[i + 3]) << 16)
                     | (static_cast<uint32_t>(p[i + 4]) << 8)
                     | p[i + 5];

        // Save old value before applying (needed for window size delta calculation)
        uint32_t oldInitialWindowSize = PeerSettings.InitialWindowSize;

        if (!PeerSettings.Apply(static_cast<ESettingsId>(id), val)) {
            if (static_cast<ESettingsId>(id) == ESettingsId::INITIAL_WINDOW_SIZE) {
                return ConnectionError(EErrorCode::FLOW_CONTROL_ERROR);
            }
            return ConnectionError(EErrorCode::PROTOCOL_ERROR);
        }

        // Handle initial window size change (update all existing streams)
        if (static_cast<ESettingsId>(id) == ESettingsId::INITIAL_WINDOW_SIZE) {
            int32_t delta = static_cast<int32_t>(val) - static_cast<int32_t>(oldInitialWindowSize);
            for (auto& [sid, s] : Streams) {
                s.SendWindowSize += delta;
            }
        }

        // Update encoder table size if header table size changed
        if (static_cast<ESettingsId>(id) == ESettingsId::HEADER_TABLE_SIZE) {
            Encoder.SetMaxTableSize(val);
        }
    }

    // Always ACK settings
    SendSettingsAck();

    if (!PrefaceReceived && Role == ERole::Server) {
        PrefaceReceived = true;
    }
}

void TSession::ProcessPingFrame(const TFrameHeader& header, TStringBuf payload) {
    if (header.StreamId != 0) {
        return ConnectionError(EErrorCode::PROTOCOL_ERROR, "PING on non-zero stream");
    }
    if (payload.size() != 8) {
        return ConnectionError(EErrorCode::FRAME_SIZE_ERROR, "PING payload not 8 bytes");
    }
    if (header.HasFlag(NFrameFlag::ACK)) {
        return; // PING response, ignore
    }
    // Echo back with ACK
    QueueOutput(TFrameBuilder::BuildPing(NFrameFlag::ACK, payload.data()));
}

void TSession::ProcessGoawayFrame(const TFrameHeader& header, TStringBuf payload) {
    if (header.StreamId != 0) {
        return ConnectionError(EErrorCode::PROTOCOL_ERROR, "GOAWAY on non-zero stream");
    }
    if (payload.size() < 8) {
        return ConnectionError(EErrorCode::FRAME_SIZE_ERROR);
    }

    const uint8_t* p = reinterpret_cast<const uint8_t*>(payload.data());
    uint32_t lastStreamId = (static_cast<uint32_t>(p[0]) << 24)
                          | (static_cast<uint32_t>(p[1]) << 16)
                          | (static_cast<uint32_t>(p[2]) << 8)
                          | p[3];
    lastStreamId &= 0x7FFFFFFF;

    uint32_t errorCode = (static_cast<uint32_t>(p[4]) << 24)
                       | (static_cast<uint32_t>(p[5]) << 16)
                       | (static_cast<uint32_t>(p[6]) << 8)
                       | p[7];

    GoawayReceived = true;

    if (Callbacks.OnGoaway) {
        Callbacks.OnGoaway(lastStreamId, static_cast<EErrorCode>(errorCode));
    }
}

void TSession::ProcessWindowUpdateFrame(const TFrameHeader& header, TStringBuf payload) {
    if (payload.size() != 4) {
        return ConnectionError(EErrorCode::FRAME_SIZE_ERROR);
    }

    const uint8_t* p = reinterpret_cast<const uint8_t*>(payload.data());
    uint32_t increment = (static_cast<uint32_t>(p[0]) << 24)
                       | (static_cast<uint32_t>(p[1]) << 16)
                       | (static_cast<uint32_t>(p[2]) << 8)
                       | p[3];
    increment &= 0x7FFFFFFF;

    if (increment == 0) {
        if (header.StreamId == 0) {
            return ConnectionError(EErrorCode::PROTOCOL_ERROR, "Zero WINDOW_UPDATE increment");
        } else {
            SendRstStream(header.StreamId, EErrorCode::PROTOCOL_ERROR);
            return;
        }
    }

    if (header.StreamId == 0) {
        int64_t newSize = static_cast<int64_t>(SendWindowSize) + increment;
        if (newSize > static_cast<int64_t>(MAX_WINDOW_SIZE)) {
            return ConnectionError(EErrorCode::FLOW_CONTROL_ERROR);
        }
        SendWindowSize = static_cast<int32_t>(newSize);
    } else {
        TStream* stream = FindStream(header.StreamId);
        if (stream) {
            int64_t newSize = static_cast<int64_t>(stream->SendWindowSize) + increment;
            if (newSize > static_cast<int64_t>(MAX_WINDOW_SIZE)) {
                SendRstStream(header.StreamId, EErrorCode::FLOW_CONTROL_ERROR);
                CloseStream(header.StreamId);
            } else {
                stream->SendWindowSize = static_cast<int32_t>(newSize);
            }
        }
    }
}

void TSession::ProcessContinuationFrame(const TFrameHeader& header, TStringBuf payload) {
    TStream* stream = FindStream(header.StreamId);
    if (!stream) {
        return ConnectionError(EErrorCode::PROTOCOL_ERROR, "CONTINUATION for unknown stream");
    }

    stream->HeaderBlockFragment.append(payload);

    if (header.HasFlag(NFrameFlag::END_HEADERS)) {
        ExpectingContinuationStream = 0;
        CompleteHeaderBlock(header.StreamId, *stream, stream->EndStream);
    }
}

void TSession::CompleteHeaderBlock(uint32_t streamId, TStream& stream, bool endStream) {
    // Decode the accumulated header block
    TVector<std::pair<TString, TString>> headers;
    if (!Decoder.Decode(stream.HeaderBlockFragment, headers)) {
        return ConnectionError(EErrorCode::COMPRESSION_ERROR, "HPACK decode failure");
    }
    stream.HeaderBlockFragment.clear();
    stream.HeadersComplete = true;

    // Separate pseudo-headers from regular headers
    for (auto& [name, value] : headers) {
        if (name == ":method") {
            stream.Method = std::move(value);
        } else if (name == ":path") {
            stream.Path = std::move(value);
        } else if (name == ":scheme") {
            stream.Scheme = std::move(value);
        } else if (name == ":authority") {
            stream.Authority = std::move(value);
        } else if (name == ":status") {
            stream.Status = std::move(value);
        } else {
            stream.Headers.emplace_back(std::move(name), std::move(value));
        }
    }

    if (endStream) {
        stream.EndStream = true;
        if (stream.State == EStreamState::Open) {
            stream.State = EStreamState::HalfClosedRemote;
        } else if (stream.State == EStreamState::HalfClosedLocal) {
            stream.State = EStreamState::Closed;
        }
    }

    // If END_STREAM was set on HEADERS (no body), deliver immediately
    if (endStream) {
        if (Role == ERole::Server && Callbacks.OnRequest) {
            Callbacks.OnRequest(streamId, stream);
        } else if (Role == ERole::Client && Callbacks.OnResponse) {
            Callbacks.OnResponse(streamId, stream);
        }
        // Callback may have sent a response and erased the stream,
        // invalidating `stream`. Re-look up before further access.
        TStream* reStream = FindStream(streamId);
        if (reStream && reStream->State == EStreamState::Closed) {
            Streams.erase(streamId);
        }
    }
    // If not endStream, we wait for DATA frames to deliver the body
}

void TSession::CloseStream(uint32_t streamId, EErrorCode) {
    auto it = Streams.find(streamId);
    if (it != Streams.end()) {
        it->second.State = EStreamState::Closed;
        Streams.erase(it);
    }
}

void TSession::ConnectionError(EErrorCode errorCode, TStringBuf message) {
    SendGoaway(errorCode, message);
    if (Callbacks.OnError) {
        TString errorMsg = TStringBuilder() << "HTTP/2 connection error: " << static_cast<uint32_t>(errorCode);
        if (!message.empty()) {
            errorMsg += ": ";
            errorMsg += message;
        }
        Callbacks.OnError(errorMsg);
    }
}

void TSession::SendResponse(uint32_t streamId, TStringBuf status,
                             const TVector<std::pair<TString, TString>>& headers, TStringBuf body, bool endStream) {
    TStream* stream = FindStream(streamId);
    if (!stream) return;

    // Build pseudo-headers + regular headers
    TVector<std::pair<TString, TString>> allHeaders;
    allHeaders.emplace_back(":status", TString(status));
    allHeaders.insert(allHeaders.end(), headers.begin(), headers.end());

    TString headerBlock = Encoder.Encode(allHeaders);

    bool hasBody = !body.empty();
    uint8_t headerFlags = NFrameFlag::END_HEADERS;
    if (!hasBody && endStream) {
        headerFlags |= NFrameFlag::END_STREAM;
    }

    // Split header block into frames if needed
    size_t maxPayload = PeerSettings.MaxFrameSize;
    if (headerBlock.size() <= maxPayload) {
        QueueOutput(TFrameBuilder::BuildHeaders(streamId, headerFlags, headerBlock));
    } else {
        // END_STREAM goes on the HEADERS frame (per RFC 7540 §6.2, §6.10)
        uint8_t firstFlags = (!hasBody && endStream) ? NFrameFlag::END_STREAM : NFrameFlag::NONE;
        QueueOutput(TFrameBuilder::BuildHeaders(streamId, firstFlags, TStringBuf(headerBlock).Head(maxPayload)));
        size_t offset = maxPayload;
        while (offset < headerBlock.size()) {
            size_t chunkSize = std::min(maxPayload, headerBlock.size() - offset);
            uint8_t contFlags = (offset + chunkSize >= headerBlock.size()) ? NFrameFlag::END_HEADERS : NFrameFlag::NONE;
            QueueOutput(TFrameBuilder::BuildContinuation(streamId, contFlags,
                TStringBuf(headerBlock).SubStr(offset, chunkSize)));
            offset += chunkSize;
        }
    }

    // Send body as DATA frames
    if (hasBody) {
        size_t offset = 0;
        while (offset < body.size()) {
            size_t chunkSize = std::min(maxPayload, body.size() - offset);
            bool isLast = (offset + chunkSize >= body.size());
            uint8_t dataFlags = (isLast && endStream) ? NFrameFlag::END_STREAM : NFrameFlag::NONE;
            QueueOutput(TFrameBuilder::BuildData(streamId, dataFlags, body.SubStr(offset, chunkSize)));
            offset += chunkSize;
        }
    }

    // Update stream state
    if (endStream) {
        if (stream->State == EStreamState::HalfClosedRemote) {
            stream->State = EStreamState::Closed;
            Streams.erase(streamId);
        } else if (stream->State == EStreamState::Open) {
            stream->State = EStreamState::HalfClosedLocal;
        }
    }

    // Flush
    if (HasPendingOutput() && Callbacks.OnSend) {
        Callbacks.OnSend(FlushOutputBuffer());
    }
}

uint32_t TSession::SendRequest(TStringBuf method, TStringBuf path, TStringBuf authority, TStringBuf scheme,
                                const TVector<std::pair<TString, TString>>& headers, TStringBuf body) {
    uint32_t streamId = NextLocalStreamId;
    NextLocalStreamId += 2;

    TStream& stream = GetOrCreateStream(streamId);

    TVector<std::pair<TString, TString>> allHeaders;
    allHeaders.emplace_back(":method", TString(method));
    allHeaders.emplace_back(":path", TString(path));
    allHeaders.emplace_back(":scheme", TString(scheme));
    allHeaders.emplace_back(":authority", TString(authority));
    allHeaders.insert(allHeaders.end(), headers.begin(), headers.end());

    TString headerBlock = Encoder.Encode(allHeaders);

    bool hasBody = !body.empty();
    uint8_t headerFlags = NFrameFlag::END_HEADERS;
    if (!hasBody) {
        headerFlags |= NFrameFlag::END_STREAM;
    }

    size_t maxPayload = PeerSettings.MaxFrameSize;
    if (headerBlock.size() <= maxPayload) {
        QueueOutput(TFrameBuilder::BuildHeaders(streamId, headerFlags, headerBlock));
    } else {
        // END_STREAM goes on the HEADERS frame (per RFC 7540 §6.2, §6.10)
        uint8_t firstFlags = !hasBody ? NFrameFlag::END_STREAM : NFrameFlag::NONE;
        QueueOutput(TFrameBuilder::BuildHeaders(streamId, firstFlags, TStringBuf(headerBlock).Head(maxPayload)));
        size_t offset = maxPayload;
        while (offset < headerBlock.size()) {
            size_t chunkSize = std::min(maxPayload, headerBlock.size() - offset);
            uint8_t contFlags = (offset + chunkSize >= headerBlock.size()) ? NFrameFlag::END_HEADERS : NFrameFlag::NONE;
            QueueOutput(TFrameBuilder::BuildContinuation(streamId, contFlags,
                TStringBuf(headerBlock).SubStr(offset, chunkSize)));
            offset += chunkSize;
        }
    }

    if (hasBody) {
        size_t offset = 0;
        while (offset < body.size()) {
            size_t chunkSize = std::min(maxPayload, body.size() - offset);
            uint8_t dataFlags = (offset + chunkSize >= body.size()) ? NFrameFlag::END_STREAM : NFrameFlag::NONE;
            QueueOutput(TFrameBuilder::BuildData(streamId, dataFlags, body.SubStr(offset, chunkSize)));
            offset += chunkSize;
        }
    }

    if (!hasBody) {
        stream.State = EStreamState::HalfClosedLocal;
    }

    if (HasPendingOutput() && Callbacks.OnSend) {
        Callbacks.OnSend(FlushOutputBuffer());
    }

    return streamId;
}

void TSession::RegisterUpgradeStream(TStringBuf path, TStringBuf authority) {
    const uint32_t streamId = 1;
    TStream& stream = GetOrCreateStream(streamId);
    stream.Path = TString(path);
    stream.Authority = TString(authority);
    // Client already sent the request as HTTP/1.1
    // Server already received the request as HTTP/1.1
    stream.State = (Role == ERole::Client) ? EStreamState::HalfClosedLocal : EStreamState::HalfClosedRemote;
    if (NextLocalStreamId <= streamId) {
        NextLocalStreamId = streamId + 2;
    }
}

void TSession::SendGoaway(EErrorCode errorCode, TStringBuf debugData) {
    if (GoawaySent) return;
    GoawaySent = true;
    QueueOutput(TFrameBuilder::BuildGoaway(LastPeerStreamId, errorCode, debugData));
    if (HasPendingOutput() && Callbacks.OnSend) {
        Callbacks.OnSend(FlushOutputBuffer());
    }
}

// ============== Conversion Helpers ==============

static bool ContainsCrLf(TStringBuf s) {
    return s.Contains('\r') || s.Contains('\n');
}

THttpIncomingRequestPtr StreamToIncomingRequest(
    const TStream& stream,
    std::shared_ptr<THttpEndpointInfo> endpoint,
    THttpConfig::SocketAddressType address) {

    // Build an HTTP/1.1-style request string for THttpIncomingRequest to parse
    TStringBuilder req;
    req << stream.Method << " " << stream.Path << " HTTP/2.0\r\n";
    if (!stream.Authority.empty()) {
        req << "Host: " << stream.Authority << "\r\n";
    }
    for (const auto& [name, value] : stream.Headers) {
        if (!ContainsCrLf(name) && !ContainsCrLf(value)) {
            req << name << ": " << value << "\r\n";
        }
    }
    if (!stream.Body.empty()) {
        req << "Content-Length: " << stream.Body.size() << "\r\n";
    }
    req << "\r\n";
    req << stream.Body;

    TString reqStr = req;
    THttpIncomingRequestPtr request = new THttpIncomingRequest(reqStr, std::move(endpoint), address);
    return request;
}

void OutgoingResponseToStream(
    const THttpOutgoingResponsePtr& response,
    TString& status,
    TVector<std::pair<TString, TString>>& headers,
    TString& body) {

    status = TString(response->Status);

    // Parse headers from the response
    // response->Body is always uncompressed (THttpOutgoingResponse::SetBody stores uncompressed copy).
    // If content-encoding was set, we need to re-compress the body for HTTP/2.
    TString contentEncoding;
    THeaders parsedHeaders(response->Headers);
    for (const auto& [name, value] : parsedHeaders.Headers) {
        TString lowerName = TString(name);
        lowerName.to_lower();
        // Skip HTTP/1.1-specific headers
        if (lowerName == "connection" || lowerName == "transfer-encoding" || lowerName == "keep-alive") {
            continue;
        }
        // Remember content-encoding but skip content-length (will be recalculated after compression)
        if (lowerName == "content-encoding") {
            contentEncoding = TString(value);
            headers.emplace_back(lowerName, TString(value));
            continue;
        }
        if (lowerName == "content-length") {
            continue; // will be set below after potential compression
        }
        headers.emplace_back(lowerName, TString(value));
    }

    // Add known headers that might not be in the generic headers map
    if (!response->ContentType.empty()) {
        bool found = false;
        for (const auto& [name, _] : headers) {
            if (TEqNoCase()(name, "content-type")) { found = true; break; }
        }
        if (!found) {
            headers.emplace_back("content-type", TString(response->ContentType));
        }
    }

    // Re-compress body if content-encoding was set
    if (!contentEncoding.empty() && response->Body) {
        TCompressContext compressor;
        compressor.InitCompress(contentEncoding);
        body = compressor.Compress(response->Body, true);
    } else {
        body = TString(response->Body);
    }

    // Set content-length
    if (!body.empty()) {
        headers.emplace_back("content-length", ToString(body.size()));
    }
}

void OutgoingRequestToStream(
    const THttpOutgoingRequestPtr& request,
    TString& method,
    TString& path,
    TString& authority,
    TString& scheme,
    TVector<std::pair<TString, TString>>& headers,
    TString& body) {

    method = TString(request->Method);
    path = TString(request->URL);
    authority = TString(request->Host);
    scheme = request->Secure ? "https" : "http";

    THeaders parsedHeaders(request->Headers);
    for (const auto& [name, value] : parsedHeaders.Headers) {
        TString lowerName = TString(name);
        lowerName.to_lower();
        if (lowerName == "host" || lowerName == "connection" || lowerName == "transfer-encoding") {
            continue;
        }
        headers.emplace_back(lowerName, TString(value));
    }

    body = TString(request->Body);
}

THttpIncomingResponsePtr StreamToIncomingResponse(
    const TStream& stream,
    THttpOutgoingRequestPtr request) {

    TStringBuilder resp;
    resp << "HTTP/2.0 " << stream.Status << " ";
    // HTTP/2 doesn't have a reason phrase, use a placeholder
    int statusCode = 0;
    TryFromString(stream.Status, statusCode);
    switch (statusCode) {
        case 200: resp << "OK"; break;
        case 201: resp << "Created"; break;
        case 204: resp << "No Content"; break;
        case 301: resp << "Moved Permanently"; break;
        case 302: resp << "Found"; break;
        case 304: resp << "Not Modified"; break;
        case 400: resp << "Bad Request"; break;
        case 401: resp << "Unauthorized"; break;
        case 403: resp << "Forbidden"; break;
        case 404: resp << "Not Found"; break;
        case 500: resp << "Internal Server Error"; break;
        case 502: resp << "Bad Gateway"; break;
        case 503: resp << "Service Unavailable"; break;
        case 504: resp << "Gateway Timeout"; break;
        default: resp << "Unknown"; break;
    }
    resp << "\r\n";

    for (const auto& [name, value] : stream.Headers) {
        if (!ContainsCrLf(name) && !ContainsCrLf(value)) {
            resp << name << ": " << value << "\r\n";
        }
    }
    if (!stream.Body.empty()) {
        resp << "Content-Length: " << stream.Body.size() << "\r\n";
    }
    resp << "\r\n";
    resp << stream.Body;

    TString respStr = resp;
    THttpIncomingResponsePtr response = new THttpIncomingResponse(std::move(request));
    response->EnsureEnoughSpaceAvailable(respStr.size());
    std::memcpy(response->Pos(), respStr.data(), respStr.size());
    response->Advance(respStr.size());
    return response;
}

} // namespace NHttp::NHttp2
