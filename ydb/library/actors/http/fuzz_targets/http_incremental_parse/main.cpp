#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_thread.h>
#include <ydb/library/actors/core/mailbox.h>
#include <ydb/library/actors/http/http.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <algorithm>
#include <cstring>

namespace {

class TActorContextGuard {
public:
    TActorContextGuard()
        : Prev(NActors::TlsActivationContext)
    {
        NActors::TlsActivationContext = &GetContext();
    }

    ~TActorContextGuard() {
        NActors::TlsActivationContext = Prev;
    }

private:
    static NActors::TActorContext& GetContext() {
        static THolder<NActors::TActorSystemSetup> setup(new NActors::TActorSystemSetup());
        static NActors::TActorSystem actorSystem(setup);
        static NActors::TExecutorThread executorThread(0, &actorSystem, nullptr, "http-incremental-parse-fuzz");
        static NActors::TMailbox mailbox;
        static NActors::TActorContext context(mailbox, executorThread, 0, NActors::TActorId());
        return context;
    }

private:
    NActors::TActivationContext* Prev = nullptr;
};

template <typename THttpType>
void FeedInChunks(const TIntrusivePtr<THttpType>& message, TStringBuf payload, size_t maxChunk, bool streaming) {
    if (streaming) {
        message->SwitchToStreaming();
    }

    size_t cursor = 0;
    maxChunk = std::max<size_t>(1, maxChunk);
    while (!payload.empty()) {
        size_t chunk = 1 + (static_cast<unsigned char>(payload[cursor % payload.size()]) % maxChunk);
        chunk = std::min(chunk, payload.size());

        message->EnsureEnoughSpaceAvailable(chunk);
        std::memcpy(message->Pos(), payload.data(), chunk);
        message->Advance(chunk);
        if (message->HasNewStreamingDataChunk()) {
            auto extracted = message->ExtractDataChunk();
            (void)extracted;
        }

        payload.Skip(chunk);
        ++cursor;
    }
}

void FuzzRequest(TStringBuf payload, size_t maxChunk, bool streaming) {
    NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
    FeedInChunks(request, payload, maxChunk, streaming);

    (void)request->GetURL();
    (void)request->GetURI();
    (void)request->GetParameters();
    (void)request->IsConnectionClose();
    (void)request->GetConnection();
    (void)request->GetErrorText();
    (void)request->AsReadableString();
    (void)request->GetObfuscatedData();

    if (request->HasHeaders()) {
        NHttp::THeadersBuilder extraHeaders;
        extraHeaders.Set("X-Fuzz", "1");
        extraHeaders.Set("Connection", request->GetConnection());

        auto duplicate = request->Duplicate(extraHeaders);
        (void)duplicate->GetConnection();

        auto forwarded = request->Forward("https://example.com/mirror");
        (void)forwarded->GetDestination();
        (void)forwarded->IsConnectionClose();

        if (request->IsReady()) {
            auto response = request->CreateResponse("200", "OK", "text/plain", request->Body);
            (void)response->IsConnectionClose();
            (void)response->IsNeedBody();
            auto reversed = response->Reverse(forwarded);
            (void)reversed->IsConnectionClose();
        }

        request->TruncateToHeaders();
    }
}

void FuzzResponse(TStringBuf payload, size_t maxChunk, bool streaming, bool closeConnection) {
    auto outgoingRequest = NHttp::THttpOutgoingRequest::CreateRequestGet("http://example.com/");
    NHttp::THttpIncomingResponsePtr response = new NHttp::THttpIncomingResponse(outgoingRequest);
    FeedInChunks(response, payload, maxChunk, streaming);

    if (closeConnection) {
        response->ConnectionClosed();
    }

    (void)response->GetErrorText();
    (void)response->IsConnectionClose();
    (void)response->AsReadableString();
    (void)response->GetObfuscatedData();

    if (response->HasHeaders() || response->Protocol) {
        NHttp::THeadersBuilder extraHeaders;
        extraHeaders.Set("X-Fuzz", "1");

        auto duplicate = response->Duplicate(outgoingRequest, extraHeaders);
        (void)duplicate->IsConnectionClose();

        auto reversedRequest = outgoingRequest->Reverse();
        auto reversed = response->Reverse(reversedRequest);
        (void)reversed->IsConnectionClose();
        (void)reversed->GetRequest();

        response->TruncateToHeaders();
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    TActorContextGuard actorContext;
    FuzzedDataProvider fdp(data, size);

    const bool requestStreaming = fdp.ConsumeBool();
    const size_t requestMaxChunk = fdp.ConsumeIntegralInRange<size_t>(1, 64);
    const bool responseStreaming = fdp.ConsumeBool();
    const size_t responseMaxChunk = fdp.ConsumeIntegralInRange<size_t>(1, 64);
    const bool closeResponse = fdp.ConsumeBool();
    const TString input = fdp.ConsumeRemainingBytesAsString();
    const TStringBuf payload(input);

    try {
        FuzzRequest(payload, requestMaxChunk, requestStreaming);
    } catch (...) {
    }

    try {
        FuzzResponse(payload, responseMaxChunk, responseStreaming, closeResponse);
    } catch (...) {
    }

    return 0;
}
