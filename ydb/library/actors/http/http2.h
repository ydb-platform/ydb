#pragma once

#include "http.h"
#include "http2_frames.h"
#include "http2_hpack.h"

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <deque>
#include <functional>

namespace NHttp::NHttp2 {

// HTTP/2 stream
struct TStream {
    uint32_t StreamId = 0;
    EStreamState State = EStreamState::Idle;

    // Accumulated header block fragments (HEADERS + CONTINUATION)
    TString HeaderBlockFragment;
    bool HeadersComplete = false;

    // Request data (server-side)
    TString Method;
    TString Path;
    TString Scheme;
    TString Authority;
    TVector<std::pair<TString, TString>> Headers;
    TString Body;
    bool EndStream = false; // received END_STREAM

    // Response data (client-side)
    TString Status;

    // Flow control
    int32_t SendWindowSize = DEFAULT_INITIAL_WINDOW_SIZE;
    int32_t RecvWindowSize = DEFAULT_INITIAL_WINDOW_SIZE;
};

// Callback interface for session events
struct TSessionCallbacks {
    // Server-side: called when a complete request is received
    std::function<void(uint32_t streamId, TStream& stream)> OnRequest;
    // Client-side: called when a complete response is received
    std::function<void(uint32_t streamId, TStream& stream)> OnResponse;
    // Called when data needs to be sent on the wire
    std::function<void(TString data)> OnSend;
    // Called on error
    std::function<void(TString error)> OnError;
    // Called when GOAWAY is received
    std::function<void(uint32_t lastStreamId, EErrorCode errorCode)> OnGoaway;
};

// HTTP/2 session state machine
// Handles framing, HPACK, stream management, flow control
class TSession {
public:
    enum class ERole {
        Server,
        Client,
    };

    TSession(ERole role, TSessionCallbacks callbacks);

    // Feed raw bytes from the wire. Returns number of bytes consumed.
    size_t Feed(const char* data, size_t len);

    // For server: send response on a stream
    // If endStream is false, the stream remains open for subsequent DATA frames (streaming)
    void SendResponse(uint32_t streamId, TStringBuf status, const TVector<std::pair<TString, TString>>& headers, TStringBuf body, bool endStream = true);

    // For client: send request
    uint32_t SendRequest(TStringBuf method, TStringBuf path, TStringBuf authority, TStringBuf scheme,
                         const TVector<std::pair<TString, TString>>& headers, TStringBuf body);

    // For client: register stream 1 for HTTP/1.1 upgrade (RFC 7540 §3.2)
    // The request was already sent as HTTP/1.1; this just creates the stream
    // so we can receive the response on it via HTTP/2.
    void RegisterUpgradeStream(TStringBuf path, TStringBuf authority);

    // Send GOAWAY and close
    void SendGoaway(EErrorCode errorCode = EErrorCode::SUCCESS, TStringBuf debugData = {});

    // Returns true if session has been initialized (preface received/sent)
    bool IsReady() const { return PrefaceReceived; }
    bool IsGoaway() const { return GoawaySent || GoawayReceived; }

    // Initialize the connection (send preface + initial SETTINGS)
    void Initialize();

    // Get pending outgoing data (batched write optimization)
    TString FlushOutputBuffer();
    bool HasPendingOutput() const { return !OutputBuffer.empty(); }

private:
    ERole Role;
    TSessionCallbacks Callbacks;

    // Connection state
    bool Initialized = false;
    bool PrefaceReceived = false;
    bool GoawaySent = false;
    bool GoawayReceived = false;
    size_t PrefaceBytesMatched = 0; // for server: tracking client preface

    // Settings
    TSettings LocalSettings;     // our settings (sent to peer)
    TSettings PeerSettings;      // peer's settings (received from peer)
    bool SettingsAckPending = false;

    // HPACK
    THPackEncoder Encoder;
    THPackDecoder Decoder;

    // Streams
    THashMap<uint32_t, TStream> Streams;
    uint32_t LastPeerStreamId = 0;   // highest stream ID from peer
    uint32_t NextLocalStreamId = 0;  // next stream ID we'll use

    // Connection-level flow control
    int32_t SendWindowSize = DEFAULT_INITIAL_WINDOW_SIZE;
    int32_t RecvWindowSize = DEFAULT_INITIAL_WINDOW_SIZE;

    // Frame parsing state
    enum class EFrameParseState {
        WaitingPreface,  // server: waiting for client connection preface
        ReadingHeader,
        ReadingPayload,
    };
    EFrameParseState FrameParseState;
    TString FrameBuffer; // accumulates partial frame data
    TFrameHeader CurrentFrameHeader;

    // For CONTINUATION frames
    uint32_t ExpectingContinuationStream = 0; // non-zero if we're waiting for CONTINUATION

    // Output buffer
    TString OutputBuffer;

    // Internal methods
    void QueueOutput(TString data);
    void QueueOutput(const char* data, size_t len);

    void SendSettings();
    void SendSettingsAck();
    void SendWindowUpdate(uint32_t streamId, uint32_t increment);
    void SendRstStream(uint32_t streamId, EErrorCode errorCode);

    TStream& GetOrCreateStream(uint32_t streamId);
    TStream* FindStream(uint32_t streamId);

    // Frame processing
    void ProcessFrame(const TFrameHeader& header, TStringBuf payload);
    void ProcessDataFrame(const TFrameHeader& header, TStringBuf payload);
    void ProcessHeadersFrame(const TFrameHeader& header, TStringBuf payload);
    void ProcessPriorityFrame(const TFrameHeader& header, TStringBuf payload);
    void ProcessRstStreamFrame(const TFrameHeader& header, TStringBuf payload);
    void ProcessSettingsFrame(const TFrameHeader& header, TStringBuf payload);
    void ProcessPingFrame(const TFrameHeader& header, TStringBuf payload);
    void ProcessGoawayFrame(const TFrameHeader& header, TStringBuf payload);
    void ProcessWindowUpdateFrame(const TFrameHeader& header, TStringBuf payload);
    void ProcessContinuationFrame(const TFrameHeader& header, TStringBuf payload);

    void CompleteHeaderBlock(uint32_t streamId, TStream& stream, bool endStream);
    void CloseStream(uint32_t streamId, EErrorCode errorCode = EErrorCode::SUCCESS);

    void ConnectionError(EErrorCode errorCode, TStringBuf message = {});
};

// Helper: Convert HTTP/2 stream to THttpIncomingRequest
THttpIncomingRequestPtr StreamToIncomingRequest(
    const TStream& stream,
    std::shared_ptr<THttpEndpointInfo> endpoint,
    THttpConfig::SocketAddressType address);

// Helper: Convert THttpOutgoingResponse to HTTP/2 headers + body
void OutgoingResponseToStream(
    const THttpOutgoingResponsePtr& response,
    TString& status,
    TVector<std::pair<TString, TString>>& headers,
    TString& body);

// Helper: Convert THttpOutgoingRequest to HTTP/2 headers + body
void OutgoingRequestToStream(
    const THttpOutgoingRequestPtr& request,
    TString& method,
    TString& path,
    TString& authority,
    TString& scheme,
    TVector<std::pair<TString, TString>>& headers,
    TString& body);

// Helper: Convert HTTP/2 stream to THttpIncomingResponse
THttpIncomingResponsePtr StreamToIncomingResponse(
    const TStream& stream,
    THttpOutgoingRequestPtr request);

} // namespace NHttp::NHttp2
