#include "remote_embedder_http.h"

#include <library/cpp/http/io/compression.h>
#include <library/cpp/http/misc/httpcodes.h>
#include <library/cpp/openssl/io/stream.h>
#include <library/cpp/threading/future/async.h>
#include <library/cpp/time_provider/monotonic.h>

#include <util/generic/hash_set.h>
#include <util/generic/singleton.h>
#include <util/generic/scope.h>
#include <util/network/socket.h>
#include <util/stream/buffered.h>
#include <util/stream/length.h>
#include <util/stream/output.h>
#include <util/stream/str.h>
#include <util/string/ascii.h>
#include <util/string/cast.h>
#include <util/string/strip.h>

#include <chrono>
#include <condition_variable>
#include <map>
#include <mutex>
#include <thread>

namespace NKnn {
namespace {

constexpr size_t HttpWorkers = 8;
constexpr size_t MaxQueuedRequests = 64;
constexpr size_t MaxQueuedTimers = 128;
constexpr size_t MaxResponseBytes = 16 * 1024 * 1024;
constexpr size_t MaxMetadataLineBytes = 8 * 1024;
constexpr size_t MaxHeaderBytes = 64 * 1024;
constexpr size_t MaxChunkFramingBytes = 1024 * 1024;

class TResponseTooLarge : public yexception {};

class TBoundedResponse final : public IOutputStream {
public:
    TString ExtractBody() {
        return std::move(Body_);
    }

private:
    void DoWrite(const void* data, size_t size) override {
        if (size > MaxResponseBytes - Body_.size()) {
            ythrow TResponseTooLarge();
        }
        Body_.append(static_cast<const char*>(data), size);
    }

    TString Body_;
};

// Read from a buffered socket and check each wire byte before appending it.
TString ReadMetadataLine(IInputStream& input, size_t& budget) {
    TString line;
    for (size_t i = 0; i < MaxMetadataLineBytes && budget; ++i, --budget) {
        char ch;
        Y_ENSURE(input.Read(&ch, 1) == 1, "Truncated HTTP metadata");
        if (ch == '\n') {
            --budget;
            if (line.EndsWith('\r')) {
                line.pop_back();
            }
            return line;
        }
        line += ch;
    }
    ythrow TResponseTooLarge();
}

TString ReadMetadataBlock(IInputStream& input) {
    size_t budget = MaxHeaderBytes;
    TString block;
    for (;;) {
        auto line = ReadMetadataLine(input, budget);
        if (line.empty()) {
            return block;
        }
        block += line;
        block += '\n';
    }
}

class TBoundedChunkedInput final : public IInputStream {
public:
    explicit TBoundedChunkedInput(IInputStream& input)
        : Input_(input)
    {}

private:
    size_t DoRead(void* data, size_t len) override {
        if (!len || Finished_) {
            return 0;
        }
        if (!Remaining_) {
            if (HasChunk_) {
                Y_ENSURE(ReadMetadataLine(Input_, FramingBudget_).empty(), "Invalid HTTP chunk delimiter");
            }
            const TString line = ReadMetadataLine(Input_, FramingBudget_);
            Y_ENSURE(TryIntFromString<16>(StripString(TStringBuf(line).Before(';')), Remaining_),
                "Invalid HTTP chunk length");
            if (!Remaining_) {
                ReadMetadataBlock(Input_);
                Finished_ = true;
                return 0;
            }
            HasChunk_ = true;
        }
        const size_t read = Input_.Read(data, Min<ui64>(len, Remaining_));
        Y_ENSURE(read, "Truncated HTTP chunk");
        Remaining_ -= read;
        return read;
    }

    IInputStream& Input_;
    size_t FramingBudget_ = MaxChunkFramingBytes;
    ui64 Remaining_ = 0;
    bool HasChunk_ = false;
    bool Finished_ = false;
};

TRemoteEmbedderHttpResponse ReadResponse(IInputStream& socketInput) {
    TBufferedInput input(&socketInput, 8192);
    size_t statusBudget = MaxMetadataLineBytes;
    const auto status = ParseHttpRetCode(ReadMetadataLine(input, statusBudget));
    const TString metadata = ReadMetadataBlock(input);
    if (status < 200 || status >= 300 || status == HTTP_NO_CONTENT) {
        return {status, {}};
    }

    TStringInput metadataInput(metadata);
    const THttpHeaders headers(&metadataInput);
    const auto* transferEncoding = headers.FindHeader("Transfer-Encoding");
    const auto* contentLength = headers.FindHeader("Content-Length");

    const bool chunked = transferEncoding && AsciiEqualsIgnoreCase(transferEncoding->Value(), "chunked");
    Y_ENSURE(!transferEncoding || chunked, "Unsupported HTTP transfer encoding");
    const ui64 length = contentLength && !chunked ? FromString<ui64>(contentLength->Value()) : 0;
    if (length > MaxResponseBytes) {
        ythrow TResponseTooLarge();
    }

    TBoundedChunkedInput chunkedBody(input);
    TLengthLimitedInput fixedBody(&input, length);
    IInputStream* body = &input;
    if (chunked) {
        body = &chunkedBody;
    } else if (contentLength) {
        body = &fixedBody;
    }
    THolder<IInputStream> decoder;
    if (const auto* encoding = headers.FindHeader("Content-Encoding")) {
        if (const auto* factory = TCompressionCodecFactory::Instance().FindDecoder(encoding->Value())) {
            decoder = (*factory)(body);
        }
    }
    TBoundedResponse response;
    TransferData(decoder ? decoder.Get() : body, &response);
    if (decoder && chunked) {
        char ch;
        Y_ENSURE(!chunkedBody.Read(&ch, 1), "Extra data after the compressed HTTP body");
    }
    Y_ENSURE(!fixedBody.Left(), "Truncated HTTP body");
    return {status, response.ExtractBody()};
}

class TRemoteEmbedderHttpExecutor {
public:
    TRemoteEmbedderHttpExecutor()
    {
        Workers_.Start(HttpWorkers, MaxQueuedRequests);
        Timer_ = std::thread([this] { RunTimers(); });
    }

    ~TRemoteEmbedderHttpExecutor() {
        {
            std::lock_guard lock(Mutex_);
            Stopping_ = true;
            for (auto* socket : ActiveSockets_) {
                try {
                    socket->ShutDown(SHUT_RDWR);
                } catch (const std::exception&) {
                    // A peer can close the socket before shutdown reaches it.
                }
            }
        }
        Ready_.notify_one();
        Timer_.join();
        Workers_.Stop();
        for (auto& [_, promise] : Timers_) {
            promise.SetException("RemoteEmbedder: HTTP executor is shutting down");
        }
    }

    NThreading::TFuture<TRemoteEmbedderHttpResponse> Post(TString host, ui16 port, bool useTls, TString request) {
        std::lock_guard lock(Mutex_);
        Y_ENSURE(!Stopping_, "RemoteEmbedder: HTTP executor is shutting down");
        return NThreading::Async(
            [this, host = std::move(host), port, useTls, request = std::move(request)] {
                return PostOnce(host, port, useTls, request);
            }, Workers_);
    }

    NThreading::TFuture<void> Wait(TDuration delay) {
        const auto deadline = NMonotonic::TMonotonic::Now() + delay;
        auto promise = NThreading::NewPromise();
        auto future = promise.GetFuture();
        {
            std::lock_guard lock(Mutex_);
            Y_ENSURE(!Stopping_ && Timers_.size() < MaxQueuedTimers, "RemoteEmbedder: HTTP timer queue is unavailable");
            Timers_.emplace(deadline, std::move(promise));
        }
        Ready_.notify_one();
        return future;
    }

private:
    void EnsureRunning() {
        std::lock_guard lock(Mutex_);
        Y_ENSURE(!Stopping_, "RemoteEmbedder: HTTP executor is shutting down");
    }

    TRemoteEmbedderHttpResponse PostOnce(const TString& host, ui16 port, bool useTls, const TString& request) {
        EnsureRunning();
        using TVerifyCert = TOpenSslClientIO::TOptions::TVerifyCert;
        try {
            // Each attempt owns a fresh connection and cannot implicitly resend a POST.
            TSocket socket(TNetworkAddress(host, port), TDuration::Seconds(10));
            socket.SetSocketTimeout(60, 0);
            {
                std::lock_guard lock(Mutex_);
                Y_ENSURE(!Stopping_, "RemoteEmbedder: HTTP executor is shutting down");
                ActiveSockets_.insert(&socket);
            }
            Y_DEFER {
                std::lock_guard lock(Mutex_);
                ActiveSockets_.erase(&socket);
            };
            TSocketInput socketInput(socket);
            TSocketOutput socketOutput(socket);
            if (useTls) {
                TOpenSslClientIO::TOptions options;
                options.VerifyCert_ = TVerifyCert{host};
                TOpenSslClientIO tls(&socketInput, &socketOutput, options);
                tls.Write(request);
                tls.Flush();
                return ReadResponse(tls);
            }
            socketOutput.Write(request);
            socketOutput.Flush();
            return ReadResponse(socketInput);
        } catch (const TResponseTooLarge&) {
            return {502, {}, true};
        } catch (const std::exception&) {
            EnsureRunning();
            // HTTP diagnostics may contain credentials or input text.
            return {0, {}};
        }
    }

    void RunTimers() {
        std::unique_lock lock(Mutex_);
        while (!Stopping_) {
            if (Timers_.empty()) {
                Ready_.wait(lock);
                continue;
            }
            const auto deadline = Timers_.begin()->first;
            const auto now = NMonotonic::TMonotonic::Now();
            if (now < deadline) {
                const auto delay = Min(deadline - now, TDuration::Hours(1));
                Ready_.wait_for(lock, std::chrono::microseconds(delay.MicroSeconds()));
                continue;
            }
            auto timer = Timers_.extract(Timers_.begin());
            lock.unlock();
            timer.mapped().SetValue();
            lock.lock();
        }
    }

    TThreadPool Workers_;
    std::mutex Mutex_;
    THashSet<TSocket*> ActiveSockets_;
    std::condition_variable Ready_;
    std::multimap<NMonotonic::TMonotonic, NThreading::TPromise<void>> Timers_;
    bool Stopping_ = false;
    std::thread Timer_;
};

} // namespace

NThreading::TFuture<TRemoteEmbedderHttpResponse> PostRemoteEmbedderRequest(
    TString host, ui16 port, bool useTls, TString request)
{
    return Singleton<TRemoteEmbedderHttpExecutor>()->Post(std::move(host), port, useTls, std::move(request));
}

NThreading::TFuture<void> WaitBeforeRemoteEmbedderRequest(TDuration delay) {
    return Singleton<TRemoteEmbedderHttpExecutor>()->Wait(delay);
}

} // namespace NKnn
