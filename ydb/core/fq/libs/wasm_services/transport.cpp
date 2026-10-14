#include "grpc_readiness.h"
#include "readiness.h"
#include "testing.h"
#include "transport.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/scheduler_cookie.h>

#include <curl/curl.h>
#include <grpcpp/generic/generic_stub.h>
#include <grpcpp/grpcpp.h>
#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/yexception.h>
#include <util/string/ascii.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <limits>
#include <mutex>
#include <optional>

namespace NFq::NWasmServices {
namespace {

using NAsync::EOperationKind;
using NAsync::EOperationStatus;
using NAsync::THandle;
constexpr ui64 MaxHeaderBytes = 16384;

bool ValidTarget(EProtocol protocol, std::string_view method, std::string_view path) {
    auto printable = [](std::string_view value) {
        return std::all_of(value.begin(), value.end(), [](unsigned char c) {
            return c > 32 && c < 127;
        });
    };
    if (!printable(method) || !printable(path)) {
        return false;
    }
    if (protocol == EProtocol::Grpc) {
        const auto slash = method.find('/', 1);
        return path.empty() && method.front() == '/' && slash != std::string_view::npos && slash > 1 && slash + 1 < method.size() &&
               method.find('/', slash + 1) == std::string_view::npos && method.find_first_of("?#\\") == std::string_view::npos;
    }
    const bool token = std::all_of(method.begin(), method.end(), [](unsigned char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
               std::string_view("!#$%&'*+-.^_`|~").find(c) != std::string_view::npos;
    });
    return token && !path.empty() && path.front() == '/' && !path.starts_with("//") && path.find_first_of("#\\") == std::string_view::npos;
}

std::string_view View(TStringBuf bytes) {
    return { bytes.data(), bytes.size() };
}

TString Response(int code, EClientError error = EClientError::None, int nativeCode = 0, TStringBuf payload = {}) {
    const TResponseHeader header{ WireVersion, code, error, nativeCode, payload.size() };
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

}   // namespace

TString MakeRequest(ui32 binding, TStringBuf payload, TStringBuf method, TStringBuf path, TStringBuf alias) {
    Y_ENSURE(method.size() <= MaxMethodBytes && path.size() <= MaxPathBytes, "Request metadata exceeds limit");
    TString bytes(RequestSize(View(payload), View(method), View(path), View(alias)), '\0');
    WriteRequest(bytes.Detach(), binding, View(payload), View(method), View(path), View(alias));
    return bytes;
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
        virtual bool Run() = 0;
        virtual TInstant NextWakeup() = 0;
        virtual void ForceCancel() = 0;
        virtual void ForceCleanup() = 0;
    };

    struct TEvRun: NActors::TEventLocal<TEvRun, NActors::TEvents::ES_PRIVATE << 16> {};

    struct TEvDeadline: NActors::TEventLocal<TEvDeadline, (NActors::TEvents::ES_PRIVATE << 16) + 1> {
        std::weak_ptr<IJob> Job;
        ui64 Generation;

        TEvDeadline(std::weak_ptr<IJob> job, ui64 generation)
            : Job(std::move(job))
            , Generation(generation)
        {
        }
    };
    class TActor;

    explicit TImpl(NActors::TActorSystem* system, TTransportExecutorLimits limits, std::shared_ptr<TRequestMetrics> metrics)
        : System(system)
        , Limits(limits)
        , Metrics(std::move(metrics))
    {
    }

    NActors::TActorSystem* const System;
    const TTransportExecutorLimits Limits;
    const std::shared_ptr<TRequestMetrics> Metrics;
    NActors::TActorId Actor;
    TSocketReadiness Readiness;
    TGrpcReadiness GrpcReadiness;
    TTransportTestAccess::TCompletionPublished CompletionPublishedHook;
    mutable std::mutex Mutex;
    TTransportStats Stats;
    TDeque<std::shared_ptr<IJob>> Pending;
    bool Closed = false;
    bool WakePosted = false;

    std::optional<EAdmissionReason> Reserve(ui64 bytes) {
        std::lock_guard lock(Mutex);
        if (Closed) {
            return EAdmissionReason::Closed;
        }
        if (Stats.Operations >= Limits.MaxOperations) {
            return EAdmissionReason::SharedOperations;
        }
        if (bytes > Limits.MaxReservedBytes - Stats.ReservedBytes) {
            return EAdmissionReason::SharedBytes;
        }
        ++Stats.Operations;
        Stats.ReservedBytes += bytes;
        return {};
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
                if (!std::exchange(WakePosted, true)) {
                    System->Send(new NActors::IEventHandle(Actor, {}, new TEvRun));
                }
                return;
            }
        }
        // Admission or a native notification raced hard shutdown. Cleanup is
        // idempotent; callback-owned requests retain their physical I/O leases.
        // Never take a session lock under the executor Mutex.
        job->ForceCancel();
        job->ForceCleanup();
    }
};

// Notifications only enqueue jobs. Native request progress and owner completion
// are serialized on this user-pool actor, with bounded work per mailbox turn.
class TTransportExecutor::TImpl::TActor final: public NActors::TActorBootstrapped<TActor> {
public:
    explicit TActor(std::shared_ptr<TImpl> state)
        : State(std::move(state))
    {
    }

    ~TActor() override {
        // Hard teardown only, after user workers stop. Late native callbacks
        // observe Closed under the same lock as Send and never use a dead system.
        {
            std::lock_guard lock(State->Mutex);
            State->Closed = true;
            for (auto& job : State->Pending) {
                Active.try_emplace(job.get()).first->second.Job = std::move(job);
            }
            State->Pending.clear();
        }
        for (auto& [_, active] : Active) {
            active.Job->ForceCancel();
        }
        State->Readiness.Stop();
        State->GrpcReadiness.Stop();
        for (auto& [_, active] : Active) {
            active.Job->ForceCleanup();
        }
        // CQ tags have drained, releasing their request/lease references without
        // owner callbacks. State remains alive until its waiter has joined.
    }

    void Bootstrap() {
        Become(&TActor::StateWork);
    }

private:
    struct TActive {
        std::shared_ptr<IJob> Job;
        TInstant Due = TInstant::Max();
        ui64 Generation = 0;
        NActors::TSchedulerCookieHolder Cookie;
    };
    STRICT_STFUNC(StateWork,
        cFunc(TEvRun::EventType, OnRun);
        hFunc(TEvDeadline, OnDeadline);)

    void Run(const std::shared_ptr<IJob>& job) {
        {
            std::lock_guard lock(State->Mutex);
            ++State->Stats.Dispatches;
        }
        if (!job->Run()) {
            Active.erase(job.get());
            return;
        }
        auto& active = Active.try_emplace(job.get()).first->second;
        active.Job = job;
        const auto due = job->NextWakeup();
        if (due != active.Due) {
            active.Cookie.Reset(nullptr);
            active.Due = due;
            active.Generation = ++Generation;
            if (due != TInstant::Max()) {
                active.Cookie.Reset(NActors::ISchedulerCookie::Make2Way());
                Schedule(due, new TEvDeadline(job, active.Generation), active.Cookie.Get());
            }
        }
    }

    void OnDeadline(TEvDeadline::TPtr& ev) {
        if (auto job = ev->Get()->Job.lock()) {
            auto it = Active.find(job.get());
            if (it != Active.end() && it->second.Generation == ev->Get()->Generation) {
                it->second.Due = TInstant::Max();
                it->second.Cookie.Reset(nullptr);
                Run(job);
            }
        }
    }

    void OnRun() {
        TDeque<std::shared_ptr<IJob>> jobs;
        {
            std::lock_guard lock(State->Mutex);
            for (size_t i = 0; i < 32 && !State->Pending.empty(); ++i) {
                jobs.push_back(std::move(State->Pending.front()));
                State->Pending.pop_front();
            }
            State->WakePosted = !State->Pending.empty();
            if (State->WakePosted) {
                Send(SelfId(), new TEvRun);
            }
        }
        for (auto& job : jobs) {
            Run(job);
        }
    }

    std::shared_ptr<TImpl> State;
    THashMap<IJob*, TActive> Active;
    ui64 Generation = 0;
};

TTransportExecutor::TTransportExecutor(std::shared_ptr<TImpl> impl)
    : Impl_(std::move(impl))
{
}

std::shared_ptr<TTransportExecutor> TTransportExecutor::Create(
    NActors::TActorSystem* system, ui32 userPool, TTransportExecutorLimits limits, std::shared_ptr<TRequestMetrics> metrics) {
    Y_ENSURE(system && limits.MaxOperations && limits.MaxReservedBytes, "Invalid shared transport executor limits");
    auto impl = std::make_shared<TImpl>(system, limits, std::move(metrics));
    impl->Actor = system->Register(new TImpl::TActor(impl), NActors::TMailboxType::HTSwap, userPool);
    return std::shared_ptr<TTransportExecutor>(new TTransportExecutor(std::move(impl)));
}

TTransportStats TTransportExecutor::Stats() const {
    std::lock_guard lock(Impl_->Mutex);
    return Impl_->Stats;
}

void TTransportTestAccess::SetCompletionPublishedHook(TTransportExecutor& executor, TCompletionPublished hook) {
    executor.Impl_->CompletionPublishedHook = std::move(hook);
}

bool TTransportTestAccess::IsClosed(const TTransportExecutor& executor) {
    std::lock_guard lock(executor.Impl_->Mutex);
    return executor.Impl_->Closed;
}

struct TTransport::TImpl: std::enable_shared_from_this<TImpl>, TTransportExecutor::TImpl::IJob {
    struct TAccounting {
        std::mutex Mutex;
        TTransportStats Stats;
    };

    struct TLease {
        std::shared_ptr<TRequestMetrics::TOperation> Metrics;
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
        TString Method;
        TString Path;
        TString Body;
        TString TvmTicket;
        // Protected by MetricsMutex: cancellation may run off the user pool.
        std::shared_ptr<TRequestMetrics::TOperation> TicketMetrics;
        std::shared_ptr<ITvmTicketRequest> TicketRequest;
        bool TicketStarted = false;
        std::mutex TicketMutex;
        std::optional<TTvmTicketResult> TicketResult;
        bool GrpcOk = false;   // published by GrpcDone with release/acquire
        std::atomic<bool> GrpcDone{ false };
        std::atomic<bool> GrpcStarted{ false };
        TInstant Deadline;
        TInstant TimerDue;
        TCompletion Completion;
        std::mutex MetricsMutex;
        std::atomic<bool> Cancelled{ false };
        std::atomic<bool> Finished{ false };
        ui64 MaxResponse = 0;
        bool ResponseLimitExceeded = false;
        ui64 HeaderBytes = 0;
        CURL* Easy = nullptr;
        curl_slist* HttpHeaders = nullptr;
        grpc::ClientContext Context;
        grpc::ByteBuffer GrpcRequest;
        grpc::ByteBuffer GrpcResponse;
        grpc::Status GrpcStatus;
        std::unique_ptr<grpc::ClientAsyncResponseReader<grpc::ByteBuffer>> GrpcReader;

        // Serialize logical completion with cancellation. Native owners retain
        // the lease independently until their buffers and callbacks are drained.
        void CompleteMetricsLocked(EMetricOutcome outcome, std::optional<ui64> payload = {}) {
            if (Lease.Metrics) {
                Lease.Metrics->Complete(outcome, payload);
            }
            if (TicketMetrics) {
                TicketMetrics->Complete(outcome);
            }
        }

        void CompleteMetrics(EMetricOutcome outcome, std::optional<ui64> payload = {}) {
            std::lock_guard lock(MetricsMutex);
            CompleteMetricsLocked(outcome, payload);
        }

        bool StartTicketMetrics(const std::shared_ptr<TRequestMetrics>& metrics) {
            std::lock_guard lock(MetricsMutex);
            // Do not create a new logical dependency after cancellation. Holding
            // this lock across creation also makes concurrent cancellation see it.
            if (Cancelled) {
                return false;
            }
            if (metrics) {
                TicketMetrics = metrics->Start("auth.tvm", EMetricKind::Internal);
            }
            return true;
        }

        std::shared_ptr<TRequestMetrics::TOperation> GetTicketMetrics() {
            std::lock_guard lock(MetricsMutex);
            return TicketMetrics;
        }

        void CompleteTicketMetrics(EMetricOutcome outcome) {
            std::lock_guard lock(MetricsMutex);
            if (TicketMetrics) {
                TicketMetrics->Complete(outcome);
            }
        }

        void ResetTicketMetrics() {
            std::lock_guard lock(MetricsMutex);
            TicketMetrics.reset();
        }

        void Cancel() {
            {
                std::lock_guard lock(MetricsMutex);
                // Runtime and transport share this absolute deadline. Capture its
                // cause now: cleanup after a later deadline cannot reclassify an
                // explicit cancellation that happened before it.
                CompleteMetricsLocked(TInstant::Now() >= Deadline ? EMetricOutcome::Timeout : EMetricOutcome::Cancelled);
                // Publish only after completing metrics, so Progress cannot win
                // with a generic cancelled outcome for an elapsed deadline.
                Cancelled = true;
            }
            Context.TryCancel();
        }

        ~TRequest() {
            CompleteMetrics(EMetricOutcome::Cancelled);
            if (TicketRequest) {
                TicketRequest->Cancel();
            }
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
    bool Cleaned = false;
    CURLM* Multi = nullptr;
    THashMap<CURL*, std::shared_ptr<TRequest>> ActiveHttp;
    bool Scheduled = false;

    struct TSocket {
        ui64 Id;
        int What;
    };

    THashMap<curl_socket_t, TSocket> Sockets;   // actor-owned
    THashMap<curl_socket_t, ui64> ReadySockets;   // published under Mutex
    TInstant CurlDue = TInstant::Max();

    TImpl(TVector<TBinding> bindings, std::shared_ptr<TTransportExecutor> executor, TTransportLimits limits)
        : Executor(executor ? executor->Impl_ : nullptr)
        , Limits(limits)
    {
        Y_ENSURE(Executor, "Shared transport executor is required");
        Y_ENSURE(limits.MaxPayloadBytes && limits.MaxPayloadBytes <= std::numeric_limits<int>::max(), "Invalid transport payload quota");
        static const auto initialized = curl_global_init(CURL_GLOBAL_DEFAULT);
        Y_ENSURE(initialized == CURLE_OK, "curl initialization failed");
        Y_ENSURE(curl_version_info(CURLVERSION_NOW)->features & CURL_VERSION_ASYNCHDNS, "Nonblocking DNS resolver is required");
        Multi = curl_multi_init();
        Y_ENSURE(Multi, "curl multi initialization failed");
        curl_multi_setopt(Multi, CURLMOPT_SOCKETFUNCTION, &SocketCallback);
        curl_multi_setopt(Multi, CURLMOPT_SOCKETDATA, this);
        curl_multi_setopt(Multi, CURLMOPT_TIMERFUNCTION, &TimerCallback);
        curl_multi_setopt(Multi, CURLMOPT_TIMERDATA, this);
        try {
            for (auto& binding : bindings) {
                if (binding.Protocol == EProtocol::Http && binding.Endpoint.EndsWith('/')) {
                    binding.Endpoint.pop_back();
                }
                TResolvedBinding resolved;
                resolved.Bytes = binding.Endpoint.size();
                Y_ENSURE(binding.TvmProvider || (!binding.TvmCredentials.SourceId && !binding.TvmCredentials.DestinationId &&
                                                    binding.TvmCredentials.Secret.empty()), "TVM credentials require a ticket provider");
                if (binding.TvmProvider) {
                    Y_ENSURE(binding.TvmCredentials.SourceId && binding.TvmCredentials.DestinationId && !binding.TvmCredentials.Secret.empty() &&
                                 binding.TvmCredentials.Secret.size() <= 4096, "Invalid TVM credentials");
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
                // Credentials and ticket reservations are independent of the application payload cap.
                // Admission still charges these bytes against the shared transport memory budget.
                Y_ENSURE(resolved.Bytes <= MaxHeaderBytes + 32 * 4 + MaxTvmTicketBytes + 4096 + MaxPathBytes, "Binding metadata exceeds quota");
                if (binding.Protocol == EProtocol::Grpc) {
                    Y_ENSURE(binding.GrpcCredentials && !binding.Endpoint.empty(), "Invalid gRPC binding");
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
                    Y_ENSURE(binding.Endpoint.StartsWith("http://") || binding.Endpoint.StartsWith("https://"), "Invalid HTTP binding scheme");
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
            if (submit) {
                for (auto& [_, request] : Requests) {
                    if (request->Lease.Metrics) {
                        request->Lease.Metrics->Queued();
                    }
                }
            }
        }
        if (submit) {
            Executor->Submit(shared_from_this());
        }
    }

    void ForceCancel() override {
        std::lock_guard lock(Mutex);
        Closing = true;
        for (auto& [_, request] : Requests) {
            request->Cancel();
        }
    }

    void Stop() {
        ForceCancel();
        Wake();   // no join: actor retains the session until physical I/O is drained
    }

    void ForceCleanup() override {
        std::lock_guard lock(Mutex);
        if (std::exchange(Cleaned, true)) {
            return;
        }
        curl_multi_setopt(Multi, CURLMOPT_SOCKETFUNCTION, nullptr);
        curl_multi_setopt(Multi, CURLMOPT_TIMERFUNCTION, nullptr);
        for (const auto& [_, socket] : Sockets) {
            Executor->Readiness.Remove(socket.Id);
        }
        Sockets.clear();
        for (auto& [easy, _] : ActiveHttp) {
            curl_multi_remove_handle(Multi, easy);
        }
        ActiveHttp.clear();
        Requests.clear();
    }

    bool Run() override {
        {
            std::lock_guard lock(Mutex);
            Scheduled = false;   // a concurrent notification must enqueue another turn
            for (auto& [_, request] : Requests) {
                if (request->Lease.Metrics) {
                    request->Lease.Metrics->Dispatched();
                }
            }
        }
        Progress();
        std::lock_guard lock(Mutex);
        return !Requests.empty();
    }

    TInstant NextWakeup() override {
        auto due = CurlDue;
        std::lock_guard lock(Mutex);
        for (const auto& [_, request] : Requests) {
            if (request->GrpcStarted) {
                continue;   // native gRPC deadline produces a callback
            }
            due = std::min(due, request->Deadline);
            if (request->Kind == EOperationKind::Timer) {
                due = std::min(due, request->TimerDue);
            }
        }
        return due;
    }

    static int TimerCallback(CURLM*, long timeoutMs, void* ptr) noexcept {
        auto& self = *static_cast<TImpl*>(ptr);
        self.CurlDue = timeoutMs < 0 ? TInstant::Max() : TInstant::Now() + TDuration::MilliSeconds(timeoutMs);
        return 0;   // zero means a subsequent actor turn/action, never recursion
    }

    static int SocketCallback(CURL*, curl_socket_t fd, int what, void* ptr, void*) noexcept {
        auto& self = *static_cast<TImpl*>(ptr);
        try {
            if (what == CURL_POLL_REMOVE) {
                if (auto it = self.Sockets.find(fd); it != self.Sockets.end()) {
                    self.Executor->Readiness.Remove(it->second.Id);
                    self.Sockets.erase(it);
                }
            } else {
                auto it = self.Sockets.find(fd);
                if (it == self.Sockets.end()) {
                    // ID is captured by a shared cell initialized before Arm.
                    auto id = std::make_shared<ui64>();
                    *id = self.Executor->Readiness.Watch(fd, [weak = self.weak_from_this(), fd, id] {
                        if (auto owner = weak.lock()) {
                            {
                                std::lock_guard lock(owner->Mutex);
                                owner->ReadySockets[fd] = *id;
                            }
                            owner->Wake();
                        }
                    });
                    it = self.Sockets.emplace(fd, TSocket{ *id, what }).first;
                } else {
                    it->second.What = what;
                }
                self.ArmSocket(it->second);
            }
            return 0;
        } catch (...) {
            return -1;   // libcurl fails affected transfers without leaking host diagnostics
        }
    }

    void ArmSocket(const TSocket& socket) {
        Executor->Readiness.Arm(socket.Id, socket.What == CURL_POLL_IN || socket.What == CURL_POLL_INOUT,
            socket.What == CURL_POLL_OUT || socket.What == CURL_POLL_INOUT);
    }

    static size_t Header(char*, size_t size, size_t count, void* context) noexcept {
        auto& request = *static_cast<TRequest*>(context);
        if ((size && count > std::numeric_limits<size_t>::max() / size) || size * count > MaxHeaderBytes - request.HeaderBytes) {
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

    void Finish(
        const std::shared_ptr<TRequest>& request, EOperationStatus status, int code, EClientError error, int nativeCode = 0, TString body = {}) {
        if (request->Finished.exchange(true)) {
            return;
        }
        {
            std::lock_guard lock(Mutex);
            Requests.erase(request->Handle);
        }
        const auto outcome = error == EClientError::Deadline         ? EMetricOutcome::Timeout
                             : status == EOperationStatus::Cancelled ? EMetricOutcome::Cancelled
                             : status == EOperationStatus::Ready     ? EMetricOutcome::Success
                                                                     : EMetricOutcome::Error;
        request->CompleteMetrics(outcome, request->Kind == EOperationKind::Timer ? std::nullopt : std::optional<ui64>(body.size()));
        if (request->TicketRequest) {
            request->TicketRequest->Cancel();
            request->TicketRequest.reset();
        }
        request->ResetTicketMetrics();
        Notify(request->Completion, status, Response(code, error, nativeCode, body));
    }

    bool AcquireTicket(const std::shared_ptr<TRequest>& request) {
        const auto& binding = Bindings[request->Binding].Config;
        if (!binding.TvmProvider) {
            return true;
        }
        try {
            if (!request->TicketStarted) {
                if (!request->StartTicketMetrics(Executor->Metrics)) {
                    return false;
                }
                request->TicketStarted = true;
                request->TicketRequest = binding.TvmProvider->Request(binding.TvmCredentials, request->Deadline,
                    [weak = weak_from_this(), pending = std::weak_ptr<TRequest>(request), metric = request->GetTicketMetrics()](
                        TTvmTicketResult result) {
                        if (metric) {
                            metric->Queued();
                        }
                        if (auto request = pending.lock(); request && !request->Finished && !request->Cancelled) {
                            if (result.Ticket.size() > MaxTvmTicketBytes) {
                                result = { .Status = ETvmTicketStatus::Failed };
                            }
                            {
                                std::lock_guard lock(request->TicketMutex);
                                if (!request->TicketResult) {
                                    request->TicketResult = std::move(result);
                                }
                            }
                            if (auto owner = weak.lock()) {
                                owner->Wake();
                            }
                        }
                    });
                Y_ENSURE(request->TicketRequest, "Missing TVM ticket request");
            }
            if (auto metric = request->GetTicketMetrics()) {
                metric->Dispatched();
            }
            TTvmTicketResult result;
            {
                std::lock_guard lock(request->TicketMutex);
                if (!request->TicketResult) {
                    return false;
                }
                result = std::move(*request->TicketResult);
            }
            const auto& ticket = result.Ticket;
            if (result.Status == ETvmTicketStatus::Ready && !result.Ticket.empty() && result.Ticket.size() <= MaxTvmTicketBytes &&
                std::all_of(ticket.begin(), ticket.end(), [](unsigned char c) {
                    return c > 32 && c < 127;
                })) {
                request->CompleteTicketMetrics(EMetricOutcome::Success);
                request->TvmTicket = std::move(result.Ticket);
                request->TicketRequest.reset();
                request->ResetTicketMetrics();
                return true;
            }
        } catch (...) {
            // Provider errors may contain credentials. Only return the fixed error code.
        }
        Finish(request, EOperationStatus::Failed, 0, EClientError::Authentication);
        return false;
    }

    void StartGrpc(const std::shared_ptr<TRequest>& request) {
        const auto& binding = Bindings[request->Binding];
        const auto maxMicros =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::time_point::max().time_since_epoch()).count();
        request->Context.set_deadline(
            std::chrono::system_clock::time_point(std::chrono::microseconds(std::min<ui64>(request->Deadline.MicroSeconds(), maxMicros))));
        for (const auto& [name, value] : binding.Config.Headers) {
            request->Context.AddMetadata(name, value);
        }
        if (!request->TvmTicket.empty()) {
            request->Context.AddMetadata("x-ya-service-ticket", request->TvmTicket);
        }
        const grpc::Slice slice(request->Payload.data(), request->Payload.size());
        request->GrpcRequest = grpc::ByteBuffer(&slice, 1);
        request->GrpcReader =
            binding.Stub->PrepareUnaryCall(&request->Context, request->Method, request->GrpcRequest, Executor->GrpcReadiness.Queue());
        Y_ENSURE(request->GrpcReader, "Cannot prepare gRPC service call");
        auto tag = std::make_unique<TGrpcReadiness::TTag>();
        tag->Notify = [owner = shared_from_this(), request](bool ok) {
            request->GrpcOk = ok;
            request->GrpcDone.store(true, std::memory_order_release);
            owner->Wake();   // only publish; response processing belongs to the user pool
            if (const auto& hook = owner->Executor->CompletionPublishedHook) {
                hook(owner);
            }
        };
        // The executor owns this CQ, independently of channels. A tag may own
        // the last transport reference without joining the thread running it.
        request->GrpcStarted = true;
        request->GrpcReader->StartCall();
        request->GrpcReader->Finish(&request->GrpcResponse, &request->GrpcStatus, tag.release());
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
        const TString url = binding.Endpoint + request.Path;
        set(CURLOPT_URL, url.c_str());
        set(CURLOPT_PATH_AS_IS, 1L);
        set(CURLOPT_CUSTOMREQUEST, request.Method.c_str());
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
            curl_blob roots{ const_cast<char*>(binding.CaCertificate.data()), binding.CaCertificate.size(), CURL_BLOB_COPY };
            set(CURLOPT_CAINFO_BLOB, &roots);
        }
        const auto timeout = std::max<ui64>(1, (request.Deadline - TInstant::Now()).MilliSeconds());
        set(CURLOPT_TIMEOUT_MS, static_cast<long>(std::min<ui64>(timeout, std::numeric_limits<long>::max())));
        set(CURLOPT_HEADERFUNCTION, &Header);
        set(CURLOPT_HEADERDATA, &request);
        set(CURLOPT_WRITEFUNCTION, &Write);
        set(CURLOPT_WRITEDATA, &request);
        if (request.Method == "POST" || request.Method == "PUT" || !request.Payload.empty()) {
            set(CURLOPT_POSTFIELDS, request.Payload.data());
            set(CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(request.Payload.size()));
        }
        // CUSTOMREQUEST changes only the verb, not curl's response semantics.
        // Set this after POSTFIELDS, which can otherwise reset the request type.
        if (request.Method == "HEAD") {
            set(CURLOPT_NOBODY, 1L);
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

    void Progress() {
        auto& active = ActiveHttp;
        TVector<std::shared_ptr<TRequest>> requests;
        {
            std::lock_guard lock(Mutex);
            for (const auto& [_, request] : Requests) {
                requests.push_back(request);
            }
        }
        const auto now = TInstant::Now();
        for (const auto& request : requests) {
            if (request->GrpcStarted) {
                if (request->GrpcDone.load(std::memory_order_acquire)) {
                    FinishGrpc(request, request->GrpcOk);
                }
                continue;   // retain context/buffers/credits until native completion
            }
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
                    if (!AcquireTicket(request)) {
                        continue;
                    }
                    // Acquisition is part of this operation's budget, including a ready ticket.
                    if (request->Cancelled || TInstant::Now() >= request->Deadline) {
                        continue;
                    }
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
        THashMap<curl_socket_t, ui64> ready;
        {
            std::lock_guard lock(Mutex);
            ready.swap(ReadySockets);
        }
        for (const auto& [fd, id] : ready) {
            if (auto it = Sockets.find(fd); it != Sockets.end() && it->second.Id == id) {
                // TSocketPoller reports the subscription, not the readiness mask.
                // libcurl probes the requested directions without blocking.
                const auto what = it->second.What;
                const int mask = (what == CURL_POLL_IN || what == CURL_POLL_INOUT ? CURL_CSELECT_IN : 0) |
                                 (what == CURL_POLL_OUT || what == CURL_POLL_INOUT ? CURL_CSELECT_OUT : 0);
                curl_multi_socket_action(Multi, fd, mask, &running);
                if (auto current = Sockets.find(fd); current != Sockets.end() && current->second.Id == id) {
                    ArmSocket(current->second);
                }
            }
        }
        if (CurlDue <= TInstant::Now()) {
            CurlDue = TInstant::Max();
            curl_multi_socket_action(Multi, CURL_SOCKET_TIMEOUT, 0, &running);
        }
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
                       curlStatus == CURLE_SSL_CERTPROBLEM || curlStatus == CURLE_SSL_CACERT_BADFILE || curlStatus == CURLE_SSL_ISSUER_ERROR) {
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
        std::string_view payload, method, path, alias;
        EMetricKind metricKind = EMetricKind::Internal;
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
            if (!ReadRequest(View(bytes), header, payload, method, path, alias) || header.Binding >= Bindings.size() ||
                payload.size() > Limits.MaxPayloadBytes || !ValidTarget(Bindings[header.Binding].Config.Protocol, method, path)) {
                Notify(request->Completion, EOperationStatus::Failed, Response(0, EClientError::InvalidRequest));
                return {};
            }
            request->Binding = header.Binding;
            metricKind = Bindings[header.Binding].Config.Protocol == EProtocol::Http ? EMetricKind::Http : EMetricKind::Grpc;
            bindingBytes = Bindings[header.Binding].Bytes + method.size() + path.size() + alias.size();
            certificateBytes = Bindings[header.Binding].Config.CaCertificate.size();
        }
        // Reserve response capacity before dispatch, including framing and gRPC
        // copies.
        // Parent reservation owns both metric handles; auth never double-charges it.
        const ui64 metricBytes = Executor->Metrics
                                     ? sizeof(TRequestMetrics::TOperation) *
                                           (1 + (kind != EOperationKind::Timer && bool(Bindings[request->Binding].Config.TvmProvider)))
                                     : 0;
        const auto reserve = metricBytes + 2 * (payload.size() + bindingBytes) + 3 * Limits.MaxPayloadBytes + sizeof(TResponseHeader) +
                             sizeof(TRequest) + sizeof(TGrpcReadiness::TTag) + MaxHeaderBytes + certificateBytes;
        {
            std::lock_guard lock(Mutex);
            Y_ENSURE(!Closing && !Requests.contains(handle), "Transport is closed or operation handle is duplicated");
            {
                std::lock_guard accountingLock(Accounting->Mutex);
                std::optional<EAdmissionReason> rejected;
                if (deadline <= TInstant::Now()) {
                    rejected = EAdmissionReason::Deadline;
                } else if (Accounting->Stats.Operations >= Limits.MaxOperations) {
                    rejected = EAdmissionReason::LocalOperations;
                } else if (reserve > Limits.MaxReservedBytes - Accounting->Stats.ReservedBytes) {
                    rejected = EAdmissionReason::LocalBytes;
                } else {
                    rejected = Executor->Reserve(reserve);
                }
                if (rejected) {
                    if (Executor->Metrics) {
                        Executor->Metrics->Reject(Executor->Metrics->Get(TStringBuf(alias.data(), alias.size()), metricKind), *rejected);
                    }
                    // Complete outside the registry lock to allow owner notification.
                    request->Cancelled = true;
                } else {
                    request->Lease.Accounting = Accounting;
                    request->Lease.Executor = Executor;
                    request->Lease.Bytes = reserve;
                    ++Accounting->Stats.Operations;
                    Accounting->Stats.ReservedBytes += reserve;
                    if (Executor->Metrics) {
                        request->Lease.Metrics = Executor->Metrics->Start(TStringBuf(alias.data(), alias.size()), metricKind, reserve,
                            kind == EOperationKind::Timer ? std::nullopt : std::optional<ui64>(payload.size()));
                    }
                }
            }
            if (!request->Cancelled) {
                request->Payload = TString(payload.data(), payload.size());
                request->Method = TString(method.data(), method.size());
                request->Path = TString(path.data(), path.size());
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
                if (auto request = weak.lock()) {
                    request->Cancel();
                }
                owner->Wake();
            }
        };
    }
};

TTransport::TTransport(TVector<TBinding> bindings, std::shared_ptr<TTransportExecutor> executor, TTransportLimits limits)
    : Impl_(std::make_shared<TImpl>(std::move(bindings), std::move(executor), limits))
{
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

}   // namespace NFq::NWasmServices
