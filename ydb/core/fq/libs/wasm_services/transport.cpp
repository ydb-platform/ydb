#include "transport.h"

#include <curl/curl.h>
#include <grpcpp/generic/generic_stub.h>
#include <grpcpp/grpcpp.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/yexception.h>
#include <util/string/ascii.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <limits>
#include <mutex>

namespace NFq::NWasmServices {
namespace {

using NAsync::EOperationKind;
using NAsync::EOperationStatus;
using NAsync::THandle;
constexpr ui64 MaxHeaderBytes = 16384;

std::string_view View(TStringBuf bytes) {
    return {bytes.data(), bytes.size()};
}

TString Response(int code, EClientError error = EClientError::None, int nativeCode = 0, TStringBuf payload = {}) {
    const TResponseHeader header{WireVersion, code, error, nativeCode, payload.size()};
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
        // One owner's callback failure must not terminate the shared executor.
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

struct TTransportExecutor::TImpl {
    struct IJob {
        virtual ~IJob() = default;
        virtual bool Poll() = 0;
        virtual void ForceCancel() = 0;
        virtual void ForceCleanup() = 0;
    };
    struct ICompletion {
        virtual ~ICompletion() = default;
        virtual void Complete(bool ok) = 0;
    };
    struct TEvRun : NActors::TEventLocal<TEvRun, NActors::TEvents::ES_PRIVATE << 16> {};
    class TActor;

    explicit TImpl(NActors::TActorSystem* system, TTransportExecutorLimits limits)
        : System(system), Limits(limits) {}

    NActors::TActorSystem* const System;
    const TTransportExecutorLimits Limits;
    NActors::TActorId Actor;
    grpc::CompletionQueue Queue;
    mutable std::mutex Mutex;
    TTransportStats Stats;
    TDeque<std::shared_ptr<IJob>> Pending;
    bool Closed = false;
    bool WakePosted = false;

    bool Reserve(ui64 bytes) {
        std::lock_guard lock(Mutex);
        if (Closed || Stats.Operations >= Limits.MaxOperations || bytes > Limits.MaxReservedBytes - Stats.ReservedBytes)
            return false;
        ++Stats.Operations;
        Stats.ReservedBytes += bytes;
        return true;
    }
    void Release(ui64 bytes) {
        std::lock_guard lock(Mutex);
        --Stats.Operations;
        Stats.ReservedBytes -= bytes;
    }
    void Submit(std::shared_ptr<IJob> job) {
        {
            std::lock_guard lock(Mutex);
            if (!Closed) {
                Pending.push_back(std::move(job));
                if (!std::exchange(WakePosted, true))
                    System->Send(new NActors::IEventHandle(Actor, {}, new TEvRun));
                return;
            }
        }
        // A not-yet-submitted session has no physical I/O. Do not retain credits
        // if admission raced hard shutdown. Never take a session lock under Mutex.
        job->ForceCancel();
        job->ForceCleanup();
    }
};

// All client progress happens in short turns on the user pool. Socket and CQ
// probes are nonblocking; idle waiting is an actor timer, never an OS-thread wait.
class TTransportExecutor::TImpl::TActor final : public NActors::TActorBootstrapped<TActor> {
public:
    explicit TActor(std::shared_ptr<TImpl> state) : State(std::move(state)) {}

    ~TActor() override {
        // Hard actor-system teardown, after executor workers have stopped. Normal
        // transform cancellation never enters this blocking CQ drain.
        {
            std::lock_guard lock(State->Mutex);
            State->Closed = true;
            for (auto& job : State->Pending)
                Active.push_back(std::move(job));
            State->Pending.clear();
        }
        for (auto& job : Active)
            job->ForceCancel();
        State->Queue.Shutdown();
        void* tag;
        bool ok;
        while (State->Queue.Next(&tag, &ok))
            delete static_cast<ICompletion*>(tag);
        for (auto& job : Active)
            job->ForceCleanup();
    }

    void Bootstrap() { Become(&TActor::StateWork); }

private:
    STRICT_STFUNC(StateWork,
        cFunc(TEvRun::EventType, OnRun);
        cFunc(NActors::TEvents::TEvWakeup::EventType, OnTick);)

    void OnRun() {
        {
            std::lock_guard lock(State->Mutex);
            State->WakePosted = false;
            while (!State->Pending.empty()) {
                Active.push_back(std::move(State->Pending.front()));
                State->Pending.pop_front();
            }
        }
        Pump();
    }
    void OnTick() { TickScheduled = false; Pump(); }
    void Pump() {
        // Bound each mailbox turn, rotating sessions so a busy transform cannot
        // monopolize the executor. Global admission also bounds Active/Pending.
        for (size_t i = 0, count = std::min<size_t>(32, Active.size()); i < count; ++i) {
            auto job = std::move(Active.front());
            Active.pop_front();
            if (job->Poll())
                Active.push_back(std::move(job));
        }
        void* tag;
        bool ok;
        for (size_t i = 0; i < 128; ++i) {
            if (State->Queue.AsyncNext(&tag, &ok, std::chrono::system_clock::now()) != grpc::CompletionQueue::GOT_EVENT)
                break;
            std::unique_ptr<ICompletion> completion(static_cast<ICompletion*>(tag));
            completion->Complete(ok);
        }
        if (!Active.empty() && !std::exchange(TickScheduled, true))
            Schedule(TDuration::MilliSeconds(5), new NActors::TEvents::TEvWakeup);
    }
    std::shared_ptr<TImpl> State;
    TDeque<std::shared_ptr<IJob>> Active;
    bool TickScheduled = false;
};

TTransportExecutor::TTransportExecutor(std::shared_ptr<TImpl> impl) : Impl_(std::move(impl)) {}

std::shared_ptr<TTransportExecutor> TTransportExecutor::Create(NActors::TActorSystem* system, ui32 userPool,
                                                              TTransportExecutorLimits limits) {
    Y_ENSURE(system && limits.MaxOperations && limits.MaxReservedBytes, "Invalid shared transport executor limits");
    auto impl = std::make_shared<TImpl>(system, limits);
    impl->Actor = system->Register(new TImpl::TActor(impl), NActors::TMailboxType::HTSwap, userPool);
    return std::shared_ptr<TTransportExecutor>(new TTransportExecutor(std::move(impl)));
}

TTransportStats TTransportExecutor::Stats() const {
    std::lock_guard lock(Impl_->Mutex);
    return Impl_->Stats;
}

struct TTransport::TImpl : std::enable_shared_from_this<TImpl>, TTransportExecutor::TImpl::IJob {
    struct TAccounting {
        std::mutex Mutex;
        TTransportStats Stats;
    };

    struct TLease {
        std::shared_ptr<TAccounting> Accounting;
        std::shared_ptr<TTransportExecutor::TImpl> Executor;
        ui64 Bytes = 0;
        ~TLease() {
            if (Accounting) {
                std::lock_guard lock(Accounting->Mutex);
                --Accounting->Stats.Operations;
                Accounting->Stats.ReservedBytes -= Bytes;
                Executor->Release(Bytes);
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
        TString TvmTicket;
        std::shared_ptr<ITvmTicketRequest> TicketRequest;
        std::atomic<bool> GrpcStarted{false};
        TInstant Deadline;
        TInstant TimerDue;
        TCompletion Completion;
        std::atomic<bool> Cancelled{false};
        std::atomic<bool> Finished{false};
        ui64 MaxResponse = 0;
        bool ResponseLimitExceeded = false;
        ui64 HeaderBytes = 0;
        CURL* Easy = nullptr;
        curl_slist* HttpHeaders = nullptr;
        grpc::ClientContext Context;
        grpc::ByteBuffer GrpcRequest;
        grpc::ByteBuffer GrpcResponse;
        grpc::Status GrpcStatus;
        std::unique_ptr<grpc::GenericClientAsyncResponseReader> Reader;

        ~TRequest() {
            if (TicketRequest)
                TicketRequest->Cancel();
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

    std::shared_ptr<TTransportExecutor::TImpl> Executor;
    TTransportLimits Limits;
    TVector<TResolvedBinding> Bindings;
    std::shared_ptr<TAccounting> Accounting = std::make_shared<TAccounting>();
    std::mutex Mutex;
    THashMap<THandle, std::shared_ptr<TRequest>> Requests;
    bool Closing = false;
    CURLM* Multi = nullptr;
    THashMap<CURL*, std::shared_ptr<TRequest>> ActiveHttp;
    bool Scheduled = false;

    TImpl(TVector<TBinding> bindings, std::shared_ptr<TTransportExecutor> executor, TTransportLimits limits)
        : Executor(executor ? executor->Impl_ : nullptr), Limits(limits)
    {
        Y_ENSURE(Executor, "Shared transport executor is required");
        Y_ENSURE(limits.MaxPayloadBytes && limits.MaxPayloadBytes <= std::numeric_limits<int>::max(), "Invalid transport payload quota");
        static const auto initialized = curl_global_init(CURL_GLOBAL_DEFAULT);
        Y_ENSURE(initialized == CURLE_OK, "curl initialization failed");
        Y_ENSURE(curl_version_info(CURLVERSION_NOW)->features & CURL_VERSION_ASYNCHDNS, "Nonblocking DNS resolver is required");
        Multi = curl_multi_init();
        Y_ENSURE(Multi, "curl multi initialization failed");
        try {
            for (auto& binding : bindings) {
                TResolvedBinding resolved;
                resolved.Bytes = binding.Endpoint.size() + binding.Method.size();
                Y_ENSURE(binding.TvmProvider || (!binding.TvmCredentials.SourceId && !binding.TvmCredentials.DestinationId &&
                         binding.TvmCredentials.Secret.empty()), "TVM credentials require a ticket provider");
                if (binding.TvmProvider) {
                    Y_ENSURE(binding.TvmCredentials.SourceId && binding.TvmCredentials.DestinationId &&
                             !binding.TvmCredentials.Secret.empty() && binding.TvmCredentials.Secret.size() <= 4096,
                             "Invalid TVM credentials");
                    resolved.Bytes += MaxTvmTicketBytes + binding.TvmCredentials.Secret.size();
                }
                for (const auto& [name, value] : binding.Headers) {
                    Y_ENSURE(!name.empty() && name.find_first_of("\r\n:") == TString::npos && value.find_first_of("\r\n") == TString::npos,
                             "Invalid trusted transport header");
                    if (binding.TvmProvider) {
                        const auto lower = to_lower(name);
                        Y_ENSURE(lower != "x-ya-service-ticket" && lower != "x-ya-user-ticket" && lower != "authorization",
                                 "TVM authentication headers are host-owned");
                    }
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

    void Wake() {
        bool submit;
        {
            std::lock_guard lock(Mutex);
            submit = !Requests.empty() && !std::exchange(Scheduled, true);
        }
        if (submit)
            Executor->Submit(shared_from_this());
    }

    void ForceCancel() override {
        std::lock_guard lock(Mutex);
        Closing = true;
        for (auto& [_, request] : Requests) {
            request->Cancelled = true;
            request->Context.TryCancel();
        }
    }

    void Stop() {
        ForceCancel();
        Wake(); // no join: actor retains the session until physical I/O is drained
    }

    void ForceCleanup() override {
        for (auto& [easy, _] : ActiveHttp)
            curl_multi_remove_handle(Multi, easy);
        ActiveHttp.clear();
        std::lock_guard lock(Mutex);
        Requests.clear();
    }

    bool Poll() override {
        PollHttp();
        std::lock_guard lock(Mutex);
        if (Requests.empty()) {
            Scheduled = false;
            return false;
        }
        return true;
    }

    static size_t Header(char*, size_t size, size_t count, void* context) noexcept {
        auto& request = *static_cast<TRequest*>(context);
        if ((size && count > std::numeric_limits<size_t>::max() / size) ||
            size * count > MaxHeaderBytes - request.HeaderBytes) {
            request.ResponseLimitExceeded = true;
            return 0;
        }
        request.HeaderBytes += size * count;
        return size * count;
    }

    static size_t Write(char* data, size_t size, size_t count, void* context) noexcept {
        auto& request = *static_cast<TRequest*>(context);
        if (size && count > std::numeric_limits<size_t>::max() / size) {
            return 0;
        }
        const auto bytes = size * count;
        if (request.Cancelled) {
            return 0;
        }
        if (bytes > request.MaxResponse - request.Body.size()) {
            request.ResponseLimitExceeded = true;
            return 0;
        }
        try {
            request.Body.append(data, bytes);
            return bytes;
        } catch (...) {
            return 0;
        }
    }

    void Finish(const std::shared_ptr<TRequest>& request, EOperationStatus status, int code, EClientError error, int nativeCode = 0,
                TString body = {}) {
        if (request->Finished.exchange(true)) {
            return;
        }
        {
            std::lock_guard lock(Mutex);
            Requests.erase(request->Handle);
        }
        if (request->TicketRequest) {
            request->TicketRequest->Cancel();
            request->TicketRequest.reset();
        }
        Notify(request->Completion, status, Response(code, error, nativeCode, body));
    }

    bool AcquireTicket(const std::shared_ptr<TRequest>& request) {
        const auto& binding = Bindings[request->Binding].Config;
        if (!binding.TvmProvider)
            return true;
        try {
            if (!request->TicketRequest)
                request->TicketRequest = binding.TvmProvider->Request(binding.TvmCredentials, request->Deadline);
            Y_ENSURE(request->TicketRequest, "Missing TVM ticket request");
            auto result = request->TicketRequest->Poll();
            const auto& ticket = result.Ticket;
            if (result.Status == ETvmTicketStatus::Pending)
                return false;
            if (result.Status == ETvmTicketStatus::Ready && !result.Ticket.empty() && result.Ticket.size() <= MaxTvmTicketBytes &&
                std::all_of(ticket.begin(), ticket.end(), [](unsigned char c) { return c > 32 && c < 127; })) {
                request->TvmTicket = std::move(result.Ticket);
                request->TicketRequest.reset();
                return true;
            }
        } catch (...) {
            // Provider errors may contain credentials. Only return the fixed error code.
        }
        Finish(request, EOperationStatus::Failed, 0, EClientError::Authentication);
        return false;
    }

    struct TGrpcCompletion final : TTransportExecutor::TImpl::ICompletion {
        std::shared_ptr<TImpl> Owner;
        std::shared_ptr<TRequest> Request;
        void Complete(bool ok) override { Owner->FinishGrpc(Request, ok); }
    };

    void StartGrpc(const std::shared_ptr<TRequest>& request) {
        const auto& binding = Bindings[request->Binding];
        const auto maxMicros = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::time_point::max().time_since_epoch()).count();
        request->Context.set_deadline(std::chrono::system_clock::time_point(
            std::chrono::microseconds(std::min<ui64>(request->Deadline.MicroSeconds(), maxMicros))));
        for (const auto& [name, value] : binding.Config.Headers)
            request->Context.AddMetadata(name, value);
        if (!request->TvmTicket.empty())
            request->Context.AddMetadata("x-ya-service-ticket", request->TvmTicket);
        const grpc::Slice slice(request->Payload.data(), request->Payload.size());
        request->GrpcRequest = grpc::ByteBuffer(&slice, 1);
        request->Reader = binding.Stub->PrepareUnaryCall(&request->Context, binding.Config.Method, request->GrpcRequest, &Executor->Queue);
        Y_ENSURE(request->Reader, "gRPC request preparation failed");
        auto tag = std::make_unique<TGrpcCompletion>();
        tag->Owner = shared_from_this();
        tag->Request = request;
        std::lock_guard lock(Mutex);
        request->GrpcStarted = true;
        request->Reader->StartCall();
        request->Reader->Finish(&request->GrpcResponse, &request->GrpcStatus, tag.release());
    }

    void AddHttp(TRequest& request) {
        const auto& binding = Bindings[request.Binding].Config;
        // The admission lease already reserves this capacity. Avoid geometric
        // buffer growth beyond the accounted response size as chunks arrive.
        request.Body.reserve(request.MaxResponse);
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
        if (!binding.CaCertificate.empty()) {
            curl_blob roots{const_cast<char*>(binding.CaCertificate.data()), binding.CaCertificate.size(), CURL_BLOB_COPY};
            set(CURLOPT_CAINFO_BLOB, &roots);
        }
        const auto timeout = std::max<ui64>(1, (request.Deadline - TInstant::Now()).MilliSeconds());
        set(CURLOPT_TIMEOUT_MS, static_cast<long>(std::min<ui64>(timeout, std::numeric_limits<long>::max())));
        set(CURLOPT_HEADERFUNCTION, &Header);
        set(CURLOPT_HEADERDATA, &request);
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
        if (!request.TvmTicket.empty()) {
            auto* headers = curl_slist_append(request.HttpHeaders, ("X-Ya-Service-Ticket: " + request.TvmTicket).c_str());
            Y_ENSURE(headers, "curl TVM header allocation failed");
            request.HttpHeaders = headers;
        }
        auto* headers = curl_slist_append(request.HttpHeaders, "Expect:");
        Y_ENSURE(headers, "curl header allocation failed");
        request.HttpHeaders = headers;
        set(CURLOPT_HTTPHEADER, request.HttpHeaders);
        Y_ENSURE(curl_multi_add_handle(Multi, request.Easy) == CURLM_OK, "curl request registration failed");
    }

    void PollHttp() {
        auto& active = ActiveHttp;
        TVector<std::shared_ptr<TRequest>> requests;
        {
            std::lock_guard lock(Mutex);
            for (const auto& [_, request] : Requests) {
                if (request->Kind == EOperationKind::Timer || Bindings[request->Binding].Config.Protocol == EProtocol::Http || !request->GrpcStarted)
                    requests.push_back(request);
            }
        }
        const auto now = TInstant::Now();
        for (const auto& request : requests) {
            if (request->Cancelled || now >= request->Deadline) {
                if (request->Easy) {
                    curl_multi_remove_handle(Multi, request->Easy);
                    active.erase(request->Easy);
                }
                Finish(request, request->Cancelled ? EOperationStatus::Cancelled : EOperationStatus::Failed, 0,
                       request->Cancelled ? EClientError::Cancelled : EClientError::Deadline);
            } else if (request->Kind == EOperationKind::Timer) {
                if (now >= request->TimerDue) {
                    Finish(request, EOperationStatus::Ready, 0, EClientError::None);
                }
            } else if (!request->Easy) {
                try {
                    if (!AcquireTicket(request))
                        continue;
                    // Acquisition is part of this operation's budget, including a ready ticket.
                    if (request->Cancelled || TInstant::Now() >= request->Deadline)
                        continue;
                    if (Bindings[request->Binding].Stub) {
                        StartGrpc(request);
                    } else {
                        AddHttp(*request);
                        active.emplace(request->Easy, request);
                    }
                } catch (...) {
                    if (request->Easy) {
                        curl_multi_remove_handle(Multi, request->Easy);
                    }
                    Finish(request, EOperationStatus::Failed, 0, EClientError::Connection);
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
            EClientError error = EClientError::None;
            if (request->Cancelled) {
                error = EClientError::Cancelled;
            } else if (curlStatus == CURLE_OPERATION_TIMEDOUT) {
                error = EClientError::Deadline;
            } else if (curlStatus == CURLE_PEER_FAILED_VERIFICATION || curlStatus == CURLE_SSL_CONNECT_ERROR ||
                       curlStatus == CURLE_SSL_CERTPROBLEM || curlStatus == CURLE_SSL_CACERT_BADFILE ||
                       curlStatus == CURLE_SSL_ISSUER_ERROR) {
                error = EClientError::Tls;
            } else if (request->ResponseLimitExceeded) {
                error = EClientError::ResourceLimit;
            } else if (curlStatus != CURLE_OK) {
                error = EClientError::Connection;
            } else if (code == 401 || code == 403) {
                error = EClientError::Authentication;
            } else if (code < 200 || code >= 300) {
                error = EClientError::HttpStatus;
            }
            Finish(request, status, code, error, curlStatus, std::move(request->Body));
        }
    }

    void FinishGrpc(const std::shared_ptr<TRequest>& request, bool ok) {
        auto status = request->Cancelled               ? EOperationStatus::Cancelled
                      : ok && request->GrpcStatus.ok() ? EOperationStatus::Ready
                                                       : EOperationStatus::Failed;
        EClientError error = EClientError::None;
        const int nativeCode = request->GrpcStatus.error_code();
        if (request->Cancelled) {
            error = EClientError::Cancelled;
        } else if (!ok || !request->GrpcStatus.ok()) {
            switch (request->GrpcStatus.error_code()) {
            case grpc::StatusCode::CANCELLED:
                error = EClientError::Cancelled;
                break;
            case grpc::StatusCode::DEADLINE_EXCEEDED:
                error = EClientError::Deadline;
                break;
            case grpc::StatusCode::UNAUTHENTICATED:
            case grpc::StatusCode::PERMISSION_DENIED:
                error = EClientError::Authentication;
                break;
            case grpc::StatusCode::RESOURCE_EXHAUSTED:
                error = EClientError::ResourceLimit;
                break;
            case grpc::StatusCode::UNAVAILABLE:
                error = EClientError::Connection;
                break;
            default:
                error = EClientError::GrpcStatus;
                break;
            }
        }
        TString body;
        if (status == EOperationStatus::Ready) {
            std::vector<grpc::Slice> slices;
            if (request->GrpcResponse.Length() > request->MaxResponse || !request->GrpcResponse.Dump(&slices).ok()) {
                status = EOperationStatus::Failed;
                error = EClientError::ResourceLimit;
            } else {
                body.reserve(request->GrpcResponse.Length());
                for (const auto& slice : slices) {
                    body.append(reinterpret_cast<const char*>(slice.begin()), slice.size());
                }
            }
        }
        Finish(request, status, nativeCode, error, nativeCode, std::move(body));
    }

    TCancel Start(THandle handle, EOperationKind kind, TString bytes, TInstant deadline, TCompletion completion) {
        auto request = std::make_shared<TRequest>();
        request->Handle = handle;
        request->Kind = kind;
        request->Deadline = deadline;
        request->Completion = std::move(completion);
        request->MaxResponse = Limits.MaxPayloadBytes;
        ui64 bindingBytes = 0;
        ui64 certificateBytes = 0;
        std::string_view payload;
        if (kind == EOperationKind::Timer) {
            ui64 delay;
            if (bytes.size() != sizeof(delay)) {
                Notify(request->Completion, EOperationStatus::Failed, Response(0, EClientError::InvalidRequest));
                return {};
            }
            std::memcpy(&delay, bytes.data(), sizeof(delay));
            const auto now = TInstant::Now();
            if (delay > (TInstant::Max() - now).MicroSeconds()) {
                Notify(request->Completion, EOperationStatus::Failed, Response(0, EClientError::InvalidRequest));
                return {};
            }
            request->TimerDue = now + TDuration::MicroSeconds(delay);
        } else {
            TRequestHeader header;
            if (bytes.size() > Limits.MaxPayloadBytes + sizeof(header) || !Decode(View(bytes), header, payload) ||
                header.Version != WireVersion || header.Binding >= Bindings.size() || header.PayloadBytes != payload.size()) {
                Notify(request->Completion, EOperationStatus::Failed, Response(0, EClientError::InvalidRequest));
                return {};
            }
            request->Binding = header.Binding;
            bindingBytes = Bindings[header.Binding].Bytes;
            certificateBytes = Bindings[header.Binding].Config.CaCertificate.size();
        }
        // Reserve response capacity before dispatch, including framing and gRPC
        // copies.
        const auto reserve = 2 * (payload.size() + bindingBytes) + 3 * Limits.MaxPayloadBytes +
            sizeof(TResponseHeader) + sizeof(TRequest) + MaxHeaderBytes + certificateBytes;
        {
            std::lock_guard lock(Mutex);
            Y_ENSURE(!Closing && !Requests.contains(handle), "Transport is closed or operation handle is duplicated");
            {
                std::lock_guard accountingLock(Accounting->Mutex);
                if (deadline <= TInstant::Now() || Accounting->Stats.Operations >= Limits.MaxOperations ||
                    reserve > Limits.MaxReservedBytes - Accounting->Stats.ReservedBytes || !Executor->Reserve(reserve)) {
                    // Complete outside the registry lock to allow owner notification.
                    request->Cancelled = true;
                } else {
                    request->Lease.Accounting = Accounting;
                    request->Lease.Executor = Executor;
                    request->Lease.Bytes = reserve;
                    ++Accounting->Stats.Operations;
                    Accounting->Stats.ReservedBytes += reserve;
                }
            }
            if (!request->Cancelled) {
                request->Payload = TString(payload.data(), payload.size());
                // Reserve both budgets before retaining payload or posting work.
                Requests.emplace(handle, request);
            }
        }
        if (request->Cancelled) {
            Notify(request->Completion, EOperationStatus::Failed,
                   Response(0, deadline <= TInstant::Now() ? EClientError::Deadline : EClientError::ResourceLimit));
            return {};
        }
        Wake();
        return [weak = std::weak_ptr<TRequest>(request), impl = weak_from_this()] {
            if (auto owner = impl.lock()) {
                std::lock_guard lock(owner->Mutex);
                if (auto request = weak.lock()) {
                    request->Cancelled = true;
                    request->Context.TryCancel();
                    // The active session is already scheduled on the shared actor.
                }
            }
        };
    }
};

TTransport::TTransport(TVector<TBinding> bindings, std::shared_ptr<TTransportExecutor> executor, TTransportLimits limits)
    : Impl_(std::make_shared<TImpl>(std::move(bindings), std::move(executor), limits))
{}

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
