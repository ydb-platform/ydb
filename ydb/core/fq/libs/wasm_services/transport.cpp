#include "transport.h"

#include <curl/curl.h>
#include <grpcpp/generic/generic_stub.h>
#include <grpcpp/grpcpp.h>

#include <util/generic/hash.h>
#include <util/generic/yexception.h>

#include <atomic>
#include <algorithm>
#include <chrono>
#include <limits>
#include <mutex>
#include <thread>

namespace NFq::NWasmServices {
namespace {

using NAsync::EOperationKind;
using NAsync::EOperationStatus;
using NAsync::THandle;

std::string_view View(TStringBuf bytes) {
    return {bytes.data(), bytes.size()};
}

TString Response(int code, TStringBuf payload = {}) {
    const TResponseHeader header{WireVersion, code, payload.size()};
    TString bytes;
    bytes.resize(sizeof(header) + payload.size());
    std::memcpy(bytes.Detach(), &header, sizeof(header));
    if (!payload.empty()) {
        std::memcpy(bytes.Detach() + sizeof(header), payload.data(), payload.size());
    }
    return bytes;
}

void Notify(const NAsync::ITransport::TCompletion& completion, EOperationStatus status, TString bytes) noexcept {
    try {
        completion(status, std::move(bytes));
    } catch (...) {
        // One owner's callback failure must not terminate the shared I/O worker.
    }
}

} // namespace

TString MakeRequest(ui32 binding, TStringBuf payload) {
    auto bytes = Encode(TRequestHeader{WireVersion, binding, payload.size()}, View(payload));
    return TString(bytes.data(), bytes.size());
}

bool ParseResponse(TStringBuf bytes, TResponseHeader& header, TStringBuf& payload) {
    std::string_view body;
    if (!Decode(View(bytes), header, body) || header.Version != WireVersion || header.PayloadBytes != body.size()) {
        return false;
    }
    payload = TStringBuf(body.data(), body.size());
    return true;
}

struct TTransport::TImpl : std::enable_shared_from_this<TImpl> {
    struct TAccounting {
        std::mutex Mutex;
        TTransportStats Stats;
    };

    struct TLease {
        std::shared_ptr<TAccounting> Accounting;
        ui64 Bytes = 0;
        ~TLease() {
            if (Accounting) {
                std::lock_guard lock(Accounting->Mutex);
                --Accounting->Stats.Operations;
                Accounting->Stats.ReservedBytes -= Bytes;
            }
        }
    };

    struct TRequest {
        // Destroy the lease last: credits remain charged through physical cleanup.
        TLease Lease;
        THandle Handle;
        EOperationKind Kind;
        ui32 Binding = 0;
        TString Payload;
        TString Body;
        TInstant Deadline;
        TInstant TimerDue;
        TCompletion Completion;
        std::atomic<bool> Cancelled{false};
        std::atomic<bool> Finished{false};
        ui64 MaxResponse = 0;
        CURL* Easy = nullptr;
        curl_slist* HttpHeaders = nullptr;
        grpc::ClientContext Context;
        grpc::ByteBuffer GrpcRequest;
        grpc::ByteBuffer GrpcResponse;
        grpc::Status GrpcStatus;
        std::unique_ptr<grpc::GenericClientAsyncResponseReader> Reader;

        ~TRequest() {
            if (Easy) {
                curl_easy_cleanup(Easy);
            }
            curl_slist_free_all(HttpHeaders);
        }
    };

    struct TResolvedBinding {
        TBinding Config;
        std::shared_ptr<grpc::Channel> Channel;
        std::unique_ptr<grpc::GenericStub> Stub;
        ui64 Bytes = 0;
    };

    TTransportLimits Limits;
    TVector<TResolvedBinding> Bindings;
    std::shared_ptr<TAccounting> Accounting = std::make_shared<TAccounting>();
    std::mutex Mutex;
    THashMap<THandle, std::shared_ptr<TRequest>> Requests;
    bool Closing = false;
    CURLM* Multi = nullptr;
    grpc::CompletionQueue Queue;
    std::thread HttpWorker;
    std::thread GrpcWorker;

    TImpl(TVector<TBinding> bindings, TTransportLimits limits)
        : Limits(limits)
    {
        Y_ENSURE(limits.MaxPayloadBytes && limits.MaxPayloadBytes <= std::numeric_limits<int>::max(), "Invalid transport payload quota");
        static const auto initialized = curl_global_init(CURL_GLOBAL_DEFAULT);
        Y_ENSURE(initialized == CURLE_OK, "curl initialization failed");
        Multi = curl_multi_init();
        Y_ENSURE(Multi, "curl multi initialization failed");
        try {
            for (auto& binding : bindings) {
                TResolvedBinding resolved;
                resolved.Bytes = binding.Endpoint.size() + binding.Method.size();
                for (const auto& [name, value] : binding.Headers) {
                    Y_ENSURE(!name.empty() && name.find_first_of("\r\n:") == TString::npos && value.find_first_of("\r\n") == TString::npos,
                             "Invalid trusted transport header");
                    resolved.Bytes += name.size() + value.size() + 4;
                }
                Y_ENSURE(resolved.Bytes <= Limits.MaxPayloadBytes, "Binding exceeds transport quota");
                if (binding.Protocol == EProtocol::Grpc) {
                    Y_ENSURE(binding.GrpcCredentials && binding.Method.StartsWith('/') && !binding.Endpoint.empty(),
                             "Invalid gRPC binding");
                    for (const auto& [name, _] : binding.Headers) {
                        Y_ENSURE(std::all_of(name.begin(), name.end(), [](char c) {
                                     return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.';
                                 }), "Invalid gRPC metadata key");
                    }
                    grpc::ChannelArguments args;
                    args.SetMaxReceiveMessageSize(Limits.MaxPayloadBytes);
                    args.SetMaxSendMessageSize(Limits.MaxPayloadBytes);
                    args.SetInt(GRPC_ARG_ENABLE_RETRIES, 0);
                    resolved.Channel = grpc::CreateCustomChannel(binding.Endpoint, binding.GrpcCredentials, args);
                    resolved.Stub = std::make_unique<grpc::GenericStub>(resolved.Channel);
                } else {
                    Y_ENSURE(binding.Endpoint.StartsWith("http://") || binding.Endpoint.StartsWith("https://"),
                             "Invalid HTTP binding scheme");
                    Y_ENSURE(binding.Method == "GET" || binding.Method == "POST" || binding.Method == "PUT" || binding.Method == "DELETE",
                             "Unsupported HTTP method");
                }
                resolved.Config = std::move(binding);
                Bindings.push_back(std::move(resolved));
            }
        } catch (...) {
            curl_multi_cleanup(Multi);
            throw;
        }
    }

    ~TImpl() {
        curl_multi_cleanup(Multi);
    }

    void Run() {
        HttpWorker = std::thread([this] { HttpLoop(); });
        try {
            GrpcWorker = std::thread([this] { GrpcLoop(); });
        } catch (...) {
            {
                std::lock_guard lock(Mutex);
                Closing = true;
            }
            curl_multi_wakeup(Multi);
            HttpWorker.join();
            throw;
        }
    }

    void Stop() {
        {
            std::lock_guard lock(Mutex);
            Closing = true;
            for (auto& [_, request] : Requests) {
                request->Cancelled = true;
                request->Context.TryCancel();
            }
        }
        curl_multi_wakeup(Multi);
        HttpWorker.join();
        Queue.Shutdown();
        GrpcWorker.join();
    }

    static size_t Write(char* data, size_t size, size_t count, void* context) noexcept {
        auto& request = *static_cast<TRequest*>(context);
        if (size && count > std::numeric_limits<size_t>::max() / size) {
            return 0;
        }
        const auto bytes = size * count;
        if (request.Cancelled || bytes > request.MaxResponse - request.Body.size()) {
            return 0;
        }
        try {
            request.Body.append(data, bytes);
            return bytes;
        } catch (...) {
            return 0;
        }
    }

    void Finish(const std::shared_ptr<TRequest>& request, EOperationStatus status, int code, TString body = {}) {
        if (request->Finished.exchange(true)) {
            return;
        }
        {
            std::lock_guard lock(Mutex);
            Requests.erase(request->Handle);
        }
        Notify(request->Completion, status, Response(code, body));
    }

    void AddHttp(TRequest& request) {
        const auto& binding = Bindings[request.Binding].Config;
        request.Easy = curl_easy_init();
        Y_ENSURE(request.Easy, "curl request allocation failed");
        auto set = [&](CURLoption option, auto value) {
            Y_ENSURE(curl_easy_setopt(request.Easy, option, value) == CURLE_OK, "curl option failed");
        };
        set(CURLOPT_URL, binding.Endpoint.c_str());
        set(CURLOPT_CUSTOMREQUEST, binding.Method.c_str());
        set(CURLOPT_NOSIGNAL, 1L);
        set(CURLOPT_FOLLOWLOCATION, 0L);
        set(CURLOPT_PROTOCOLS_STR, "http,https");
        set(CURLOPT_PROXY, "");
        set(CURLOPT_ACCEPT_ENCODING, "");
        set(CURLOPT_SSL_VERIFYPEER, 1L);
        set(CURLOPT_SSL_VERIFYHOST, 2L);
        if (!binding.CaFile.empty()) {
            set(CURLOPT_CAINFO, binding.CaFile.c_str());
        }
        const auto timeout = std::max<ui64>(1, (request.Deadline - TInstant::Now()).MilliSeconds());
        set(CURLOPT_TIMEOUT_MS, static_cast<long>(std::min<ui64>(timeout, std::numeric_limits<long>::max())));
        set(CURLOPT_WRITEFUNCTION, &Write);
        set(CURLOPT_WRITEDATA, &request);
        if (binding.Method == "POST" || binding.Method == "PUT" || !request.Payload.empty()) {
            set(CURLOPT_POSTFIELDS, request.Payload.data());
            set(CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(request.Payload.size()));
        }
        for (const auto& [name, value] : binding.Headers) {
            auto* headers = curl_slist_append(request.HttpHeaders, (name + ": " + value).c_str());
            Y_ENSURE(headers, "curl header allocation failed");
            request.HttpHeaders = headers;
        }
        auto* headers = curl_slist_append(request.HttpHeaders, "Expect:");
        Y_ENSURE(headers, "curl header allocation failed");
        request.HttpHeaders = headers;
        set(CURLOPT_HTTPHEADER, request.HttpHeaders);
        Y_ENSURE(curl_multi_add_handle(Multi, request.Easy) == CURLM_OK, "curl request registration failed");
    }

    void HttpLoop() {
        THashMap<CURL*, std::shared_ptr<TRequest>> active;
        for (;;) {
            TVector<std::shared_ptr<TRequest>> requests;
            bool closing;
            {
                std::lock_guard lock(Mutex);
                closing = Closing;
                for (const auto& [_, request] : Requests) {
                    if (request->Kind == EOperationKind::Timer || Bindings[request->Binding].Config.Protocol == EProtocol::Http) {
                        requests.push_back(request);
                    }
                }
            }
            const auto now = TInstant::Now();
            for (const auto& request : requests) {
                if (request->Cancelled || now >= request->Deadline) {
                    if (request->Easy) {
                        curl_multi_remove_handle(Multi, request->Easy);
                        active.erase(request->Easy);
                    }
                    Finish(request, request->Cancelled ? EOperationStatus::Cancelled : EOperationStatus::Failed, 0);
                } else if (request->Kind == EOperationKind::Timer) {
                    if (now >= request->TimerDue) {
                        Finish(request, EOperationStatus::Ready, 0);
                    }
                } else if (!request->Easy) {
                    try {
                        AddHttp(*request);
                        active.emplace(request->Easy, request);
                    } catch (...) {
                        if (request->Easy) {
                            curl_multi_remove_handle(Multi, request->Easy);
                        }
                        Finish(request, EOperationStatus::Failed, 0);
                    }
                }
            }
            int running = 0;
            curl_multi_perform(Multi, &running);
            int messages = 0;
            while (auto* message = curl_multi_info_read(Multi, &messages)) {
                if (message->msg != CURLMSG_DONE) {
                    continue;
                }
                auto it = active.find(message->easy_handle);
                if (it == active.end()) {
                    continue;
                }
                auto request = it->second;
                const auto curlStatus = message->data.result;
                long code = 0;
                curl_easy_getinfo(request->Easy, CURLINFO_RESPONSE_CODE, &code);
                curl_multi_remove_handle(Multi, request->Easy);
                active.erase(it);
                const auto status = request->Cancelled                                    ? EOperationStatus::Cancelled
                                    : curlStatus == CURLE_OK && code >= 200 && code < 300 ? EOperationStatus::Ready
                                                                                          : EOperationStatus::Failed;
                Finish(request, status, code, std::move(request->Body));
            }
            if (closing && active.empty()) {
                return;
            }
            // curl owns socket readiness; this bound also services cancellation and timers.
            int descriptors = 0;
            curl_multi_poll(Multi, nullptr, 0, 20, &descriptors);
        }
    }

    void GrpcLoop() {
        void* tag;
        bool ok;
        while (Queue.Next(&tag, &ok)) {
            auto* raw = static_cast<TRequest*>(tag);
            std::shared_ptr<TRequest> request;
            {
                std::lock_guard lock(Mutex);
                auto it = Requests.find(raw->Handle);
                if (it != Requests.end()) {
                    request = it->second;
                }
            }
            if (!request) {
                continue;
            }
            auto status = request->Cancelled               ? EOperationStatus::Cancelled
                          : ok && request->GrpcStatus.ok() ? EOperationStatus::Ready
                                                           : EOperationStatus::Failed;
            TString body;
            if (status == EOperationStatus::Ready) {
                std::vector<grpc::Slice> slices;
                if (request->GrpcResponse.Length() > request->MaxResponse || !request->GrpcResponse.Dump(&slices).ok()) {
                    status = EOperationStatus::Failed;
                } else {
                    for (const auto& slice : slices) {
                        body.append(reinterpret_cast<const char*>(slice.begin()), slice.size());
                    }
                }
            }
            Finish(request, status, request->GrpcStatus.error_code(), std::move(body));
        }
    }

    TCancel Start(THandle handle, EOperationKind kind, TString bytes, TInstant deadline, TCompletion completion) {
        auto request = std::make_shared<TRequest>();
        request->Handle = handle;
        request->Kind = kind;
        request->Deadline = deadline;
        request->Completion = std::move(completion);
        request->MaxResponse = Limits.MaxPayloadBytes;
        ui64 bindingBytes = 0;
        if (kind == EOperationKind::Timer) {
            ui64 delay;
            if (bytes.size() != sizeof(delay)) {
                Notify(request->Completion, EOperationStatus::Failed, Response(0));
                return {};
            }
            std::memcpy(&delay, bytes.data(), sizeof(delay));
            const auto now = TInstant::Now();
            if (delay > (TInstant::Max() - now).MicroSeconds()) {
                Notify(request->Completion, EOperationStatus::Failed, Response(0));
                return {};
            }
            request->TimerDue = now + TDuration::MicroSeconds(delay);
        } else {
            TRequestHeader header;
            std::string_view payload;
            if (bytes.size() > Limits.MaxPayloadBytes + sizeof(header) || !Decode(View(bytes), header, payload) ||
                header.Version != WireVersion || header.Binding >= Bindings.size() || header.PayloadBytes != payload.size()) {
                Notify(request->Completion, EOperationStatus::Failed, Response(0));
                return {};
            }
            request->Binding = header.Binding;
            request->Payload = TString(payload.data(), payload.size());
            bindingBytes = Bindings[header.Binding].Bytes;
        }
        // Reserve response capacity before dispatch, including framing and gRPC copies.
        const auto reserve = 2 * (request->Payload.size() + bindingBytes) + 3 * Limits.MaxPayloadBytes + sizeof(TResponseHeader);
        {
            std::lock_guard lock(Mutex);
            Y_ENSURE(!Closing && !Requests.contains(handle), "Transport is closed or operation handle is duplicated");
            {
                std::lock_guard accountingLock(Accounting->Mutex);
                if (deadline <= TInstant::Now() || Accounting->Stats.Operations >= Limits.MaxOperations ||
                    reserve > Limits.MaxReservedBytes - Accounting->Stats.ReservedBytes) {
                    // Complete outside the registry lock to allow owner notification.
                    request->Cancelled = true;
                } else {
                    request->Lease.Accounting = Accounting;
                    request->Lease.Bytes = reserve;
                    ++Accounting->Stats.Operations;
                    Accounting->Stats.ReservedBytes += reserve;
                }
            }
            if (!request->Cancelled && kind != EOperationKind::Timer && Bindings[request->Binding].Stub) {
                const auto& binding = Bindings[request->Binding];
                const auto maxMicros =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::time_point::max().time_since_epoch())
                        .count();
                const auto micros = std::min<ui64>(deadline.MicroSeconds(), maxMicros);
                request->Context.set_deadline(std::chrono::system_clock::time_point(std::chrono::microseconds(micros)));
                for (const auto& [name, value] : binding.Config.Headers) {
                    request->Context.AddMetadata(name, value);
                }
                const grpc::Slice slice(request->Payload.data(), request->Payload.size());
                request->GrpcRequest = grpc::ByteBuffer(&slice, 1);
                request->Reader = binding.Stub->PrepareUnaryCall(&request->Context, binding.Config.Method, request->GrpcRequest, &Queue);
                Y_ENSURE(request->Reader, "gRPC request preparation failed");
            }
            if (!request->Cancelled) {
                // Preparation may throw: publish only after it succeeds, before CQ dispatch.
                Requests.emplace(handle, request);
            }
            if (request->Reader) {
                request->Reader->StartCall();
                request->Reader->Finish(&request->GrpcResponse, &request->GrpcStatus, request.get());
            }
        }
        if (request->Cancelled) {
            Notify(request->Completion, EOperationStatus::Failed, Response(0));
            return {};
        }
        curl_multi_wakeup(Multi);
        return [weak = std::weak_ptr<TRequest>(request), impl = weak_from_this()] {
            if (auto owner = impl.lock()) {
                std::lock_guard lock(owner->Mutex);
                if (auto request = weak.lock()) {
                    request->Cancelled = true;
                    request->Context.TryCancel();
                    curl_multi_wakeup(owner->Multi);
                }
            }
        };
    }
};

TTransport::TTransport(TVector<TBinding> bindings, TTransportLimits limits)
    : Impl_(std::make_shared<TImpl>(std::move(bindings), limits))
{
    Impl_->Run();
}

TTransport::~TTransport() {
    Impl_->Stop();
}

TTransport::TCancel TTransport::Start(THandle operation, EOperationKind kind, TString request, TInstant deadline, TCompletion completion) {
    return Impl_->Start(operation, kind, std::move(request), deadline, std::move(completion));
}

TTransportStats TTransport::Stats() const {
    std::lock_guard lock(Impl_->Accounting->Mutex);
    return Impl_->Accounting->Stats;
}

} // namespace NFq::NWasmServices
