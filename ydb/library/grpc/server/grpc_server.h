#pragma once

#include "grpc_request_base.h"
#include "logger.h"

#include <ydb/library/grpc/common/constants.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/maybe.h>
#include <util/generic/queue.h>
#include <util/generic/hash_set.h>
#include <util/system/types.h>
#include <util/system/mutex.h>
#include <util/thread/factory.h>

#include <grpcpp/grpcpp.h>

namespace NYdbGrpc {

struct TSslData {
    TString Cert;
    TString Key;
    TString Root;
    bool DoRequestClientCertificate = false;
};

struct IExternalListener
    : public TThrRefBase
{
    using TPtr = TIntrusivePtr<IExternalListener>;
    virtual void Init(std::unique_ptr<grpc::experimental::ExternalConnectionAcceptor> acceptor) = 0;
    virtual void Start() = 0;
    virtual void Stop() = 0;
};

//! Server's options.
struct TServerOptions {
#define DECLARE_FIELD(name, type, default) \
    type name{default}; \
    inline TServerOptions& Set##name(const type& value) { \
        name = value; \
        return *this; \
    }

    //! Hostname of server to bind to.
    DECLARE_FIELD(Host, TString, "[::]");
    //! Service port.
    DECLARE_FIELD(Port, ui16, 0);

    //! Number of worker threads.
    DECLARE_FIELD(WorkerThreads, size_t, 2);

    //! Number of workers per completion queue, i.e. when
    // WorkerThreads=8 and PriorityWorkersPerCompletionQueue=2
    // there will be 4 completion queues. When set to 0 then
    // only UseCompletionQueuePerThread affects number of CQ.
    DECLARE_FIELD(WorkersPerCompletionQueue, size_t, 0);

    //! Obsolete. Create one completion queue per thread.
    // Setting true equals to the WorkersPerCompletionQueue=1
    DECLARE_FIELD(UseCompletionQueuePerThread, bool, false);

    //! Memory quota size for grpc server in bytes. Zero means unlimited.
    DECLARE_FIELD(GRpcMemoryQuotaBytes, size_t, 0);

    //! Enable Grpc memory quota feature.
    DECLARE_FIELD(EnableGRpcMemoryQuota, bool, false);

    //! How long to wait until pending rpcs are forcefully terminated.
    DECLARE_FIELD(GRpcShutdownDeadline, TDuration, TDuration::Seconds(30));

    //! In/Out message size limit
    DECLARE_FIELD(MaxMessageSize, size_t, DEFAULT_GRPC_MESSAGE_SIZE_LIMIT);

    //! Use GRpc keepalive
    DECLARE_FIELD(KeepAliveEnable, TMaybe<bool>, TMaybe<bool>());

    //! GRPC_ARG_KEEPALIVE_TIME_MS setting
    DECLARE_FIELD(KeepAliveIdleTimeoutTriggerSec, int, 0);

    //! Deprecated, ths option ignored. Will be removed soon.
    DECLARE_FIELD(KeepAliveMaxProbeCount, int, 0);

    //! GRPC_ARG_KEEPALIVE_TIMEOUT_MS setting
    DECLARE_FIELD(KeepAliveProbeIntervalSec, int, 0);

    //! Max number of requests processing by services (global limit for grpc server)
    DECLARE_FIELD(MaxGlobalRequestInFlight, size_t, 100000);

    //! SSL server data
    DECLARE_FIELD(SslData, TMaybe<TSslData>, TMaybe<TSslData>());

    //! GRPC auth
    DECLARE_FIELD(UseAuth, bool, false);

    //! Default compression level. Used when no compression options provided by client.
    //  Mapping to particular compression algorithm depends on client.
    DECLARE_FIELD(DefaultCompressionLevel, grpc_compression_level, GRPC_COMPRESS_LEVEL_NONE);

    //! Custom configurator for ServerBuilder.
    DECLARE_FIELD(ServerBuilderMutator, std::function<void(grpc::ServerBuilder&)>, [](grpc::ServerBuilder&){});

    DECLARE_FIELD(ExternalListener, IExternalListener::TPtr, nullptr);

    //! Logger which will be used to write logs about requests handling (iff appropriate log level is enabled).
    DECLARE_FIELD(Logger, TLoggerPtr, nullptr);

#undef DECLARE_FIELD
};

class IQueueEvent {
public:
    virtual ~IQueueEvent() = default;

    //! Execute an action defined by implementation.
    virtual bool Execute(bool ok) = 0;

    //! It is time to perform action requested by AcquireToken server method. It will be called under lock which is also
    //  used in ReturnToken/AcquireToken methods. Default implementation does nothing assuming that request processor does
    //  not implement in flight management.
    virtual void Process() {}

    //! Finish and destroy request.
    virtual void DestroyRequest() = 0;
};

class ICancelableContext {
public:
    virtual void Shutdown() = 0;
    virtual ~ICancelableContext() = default;

private:
    template<class T>
    friend class TGrpcServiceBase;

    // Shard assigned by RegisterRequestCtx. This field is not thread-safe
    // because RegisterRequestCtx may only be called once for a single service,
    // so it's only assigned once.
    size_t ShardIndex = size_t(-1);
};

template <class TLimit>
class TInFlightLimiterImpl {
public:
    explicit TInFlightLimiterImpl(const TLimit& limit)
        : Limit_(limit)
    {}

    bool Inc() {
        i64 newVal;
        i64 prev;
        do {
            prev = AtomicGet(CurInFlightReqs_);
            Y_ABORT_UNLESS(prev >= 0);
            if (Limit_ && prev > Limit_) {
                return false;
            }
            newVal = prev + 1;
        } while (!AtomicCas(&CurInFlightReqs_, newVal, prev));
        return true;
    }

    void Dec() {
        i64 newVal = AtomicDecrement(CurInFlightReqs_);
        Y_ABORT_UNLESS(newVal >= 0);
    }

    i64 GetCurrentInFlight() const {
        return AtomicGet(CurInFlightReqs_);
    }

private:
    const TLimit Limit_;
    TAtomic CurInFlightReqs_ = 0;
};

using TGlobalLimiter = TInFlightLimiterImpl<i64>;


class IGRpcService: public TThrRefBase {
public:
    virtual grpc::Service* GetService() = 0;
    virtual void StopService() noexcept = 0;

    virtual void InitService(grpc::ServerCompletionQueue* cq, TLoggerPtr logger) = 0;

    virtual void InitService(
        const std::vector<std::unique_ptr<grpc::ServerCompletionQueue>>& cqs,
        TLoggerPtr logger,
        size_t index)
    {
        InitService(cqs[index % cqs.size()].get(), logger);
    }

    virtual void SetGlobalLimiterHandle(TGlobalLimiter* limiter) = 0;
    virtual bool IsUnsafeToShutdown() const = 0;
    virtual size_t RequestsInProgress() const = 0;

    /**
     * Called before service is added to the server builder. This allows
     * service to inspect server options and initialize accordingly.
     */
    virtual void SetServerOptions(const TServerOptions& options) = 0;
};

template<typename T>
class TGrpcServiceBase: public IGRpcService {
public:
    class TShutdownGuard {
        using TOwner = TGrpcServiceBase<T>;
        friend class TGrpcServiceBase<T>;

    public:
        TShutdownGuard()
            : Owner(nullptr)
        { }

        ~TShutdownGuard() {
            Release();
        }

        TShutdownGuard(TShutdownGuard&& other)
            : Owner(other.Owner)
        {
            other.Owner = nullptr;
        }

        TShutdownGuard& operator=(TShutdownGuard&& other) {
            if (Y_LIKELY(this != &other)) {
                Release();
                Owner = other.Owner;
                other.Owner = nullptr;
            }
            return *this;
        }

        explicit operator bool() const {
            return bool(Owner);
        }

        void Release() {
            if (Owner) {
                AtomicDecrement(Owner->GuardCount_);
                Owner = nullptr;
            }
        }

        TShutdownGuard(const TShutdownGuard&) = delete;
        TShutdownGuard& operator=(const TShutdownGuard&) = delete;

    private:
        explicit TShutdownGuard(TOwner* owner)
            : Owner(owner)
        { }

    private:
        TOwner* Owner;
    };

public:
    using TCurrentGRpcService = T;

    void StopService() noexcept override {
        AtomicSet(ShuttingDown_, 1);

        for (auto& shard : Shards_) {
            with_lock(shard.Lock_) {
                // Send TryCansel to event (can be send after finishing).
                // Actual dtors will be called from grpc thread, so deadlock impossible
                for (auto* request : shard.Requests_) {
                    request->Shutdown();
                }
            }
        }
    }

    TShutdownGuard ProtectShutdown() noexcept {
        AtomicIncrement(GuardCount_);
        if (IsShuttingDown()) {
            AtomicDecrement(GuardCount_);
            return { };
        }

        return TShutdownGuard(this);
    }

    bool IsUnsafeToShutdown() const override {
        return AtomicGet(GuardCount_) > 0;
    }

    size_t RequestsInProgress() const override {
        size_t c = 0;
        for (auto& shard : Shards_) {
            with_lock(shard.Lock_) {
                c += shard.Requests_.size();
            }
        }
        return c;
    }

    void SetServerOptions(const TServerOptions& options) override {
        SslServer_ = bool(options.SslData);
        NeedAuth_ = options.UseAuth;
    }

    void SetGlobalLimiterHandle(TGlobalLimiter* /*limiter*/) override {}

    //! Check if the server is going to shut down.
    bool IsShuttingDown() const {
       return AtomicGet(ShuttingDown_);
    }

    bool SslServer() const {
        return SslServer_;
    }

    bool NeedAuth() const {
        return NeedAuth_;
    }

    bool RegisterRequestCtx(ICancelableContext* req) {
        if (Y_LIKELY(req->ShardIndex == size_t(-1))) {
            req->ShardIndex = NextShard_.fetch_add(1, std::memory_order_relaxed) % Shards_.size();
        }

        auto& shard = Shards_[req->ShardIndex];
        with_lock(shard.Lock_) {
            if (IsShuttingDown()) {
                return false;
            }

            auto r = shard.Requests_.emplace(req);
            Y_ABORT_UNLESS(r.second, "Ctx already registered");
        }

        return true;
    }

    void DeregisterRequestCtx(ICancelableContext* req) {
        Y_ABORT_UNLESS(req->ShardIndex != size_t(-1), "Ctx does not have an assigned shard index");

        auto& shard = Shards_[req->ShardIndex];
        with_lock(shard.Lock_) {
            Y_ABORT_UNLESS(shard.Requests_.erase(req), "Ctx is not registered");
        }
    }

protected:
    using TGrpcAsyncService = typename TCurrentGRpcService::AsyncService;
    TGrpcAsyncService Service_;

    TGrpcAsyncService* GetService() override {
        return &Service_;
    }

private:
    TAtomic ShuttingDown_ = 0;
    TAtomic GuardCount_ = 0;

    bool SslServer_ = false;
    bool NeedAuth_ = false;

    struct TShard {
        TAdaptiveLock Lock_;
        THashSet<ICancelableContext*> Requests_;
    };

    // Note: benchmarks showed 4 shards is enough to scale to ~30 threads
    TVector<TShard> Shards_{ size_t(4) };
    std::atomic<size_t> NextShard_{ 0 };
};

class TGRpcServer {
public:
    using IGRpcServicePtr = TIntrusivePtr<IGRpcService>;
    TGRpcServer(const TServerOptions& opts);
    ~TGRpcServer();
    void AddService(IGRpcServicePtr service);
    void Start();
    // Send stop to registred services and call Shutdown on grpc server
    // This method MUST be called before destroying TGRpcServer
    void Stop();
    ui16 GetPort() const;
    TString GetHost() const;

    const TVector<IGRpcServicePtr>& GetServices() const;

private:
    using IThreadRef = TAutoPtr<IThreadFactory::IThread>;

    const TServerOptions Options_;
    std::unique_ptr<grpc::Server> Server_;
    std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> CQS_;
    TVector<IThreadRef> Ts;

    TVector<IGRpcServicePtr> Services_;
    TGlobalLimiter Limiter_;
};

} // namespace NYdbGrpc
