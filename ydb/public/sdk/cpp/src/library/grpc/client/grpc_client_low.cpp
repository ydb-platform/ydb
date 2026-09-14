#include "grpc_client_low.h"

#include <grpc/support/log.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/system/thread.h>
#include <util/random/random.h>

#if defined(_WIN32) || defined(_WIN64)
#include <windows.h>
#include <util/system/win_undef.h>
#endif

#if !defined(_WIN32) && !defined(_WIN64)
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#endif

#if !defined(YDB_DISABLE_GRPC_SOCKET_MUTATOR)
#include <contrib/libs/grpc/src/core/lib/iomgr/socket_mutator.h>
#endif

#include <algorithm>
#include <atomic>
#include <format>
#include <thread>

namespace NYdbGrpc::inline Dev {

void EnableGRpcTracing() {
    grpc_tracer_set_enabled("tcp", true);
    grpc_tracer_set_enabled("client_channel", true);
    grpc_tracer_set_enabled("channel", true);
    grpc_tracer_set_enabled("api", true);
    grpc_tracer_set_enabled("connectivity_state", true);
    grpc_tracer_set_enabled("handshaker", true);
    grpc_tracer_set_enabled("http", true);
    grpc_tracer_set_enabled("http2_stream_state", true);
    grpc_tracer_set_enabled("op_failure", true);
    grpc_tracer_set_enabled("timer", true);
    gpr_set_log_verbosity(GPR_LOG_SEVERITY_DEBUG);
}

#if !defined(YDB_DISABLE_GRPC_SOCKET_MUTATOR)
class TGRpcSocketMutator : public grpc_socket_mutator {
public:
    TGRpcSocketMutator(bool isKeepAliveEnabled, int idle, int count, int interval, bool tcpNoDelay)
        : IsKeepAliveEnabled_(isKeepAliveEnabled)
        , Idle_(idle)
        , Count_(count)
        , Interval_(interval)
        , TcpNoDelay_(tcpNoDelay)
    {
        grpc_socket_mutator_init(this, &VTable);
    }
private:
    static TGRpcSocketMutator* Cast(grpc_socket_mutator* mutator) {
        return static_cast<TGRpcSocketMutator*>(mutator);
    }

    template<typename TVal>
    bool SetOption(int fd, int level, int optname, const TVal& value) {
        return setsockopt(fd, level, optname, reinterpret_cast<const char*>(&value), sizeof(value)) == 0;
    }
    bool SetOption(int fd) {
        if (IsKeepAliveEnabled_) {
            if (!SetOption(fd, SOL_SOCKET, SO_KEEPALIVE, 1)) {
                std::cerr << std::format("Failed to set SO_KEEPALIVE option: {}", strerror(errno)) << std::endl;
                return false;
            }
#ifdef _linux_
            if (Idle_ && !SetOption(fd, IPPROTO_TCP, TCP_KEEPIDLE, Idle_)) {
                std::cerr << std::format("Failed to set TCP_KEEPIDLE option: {}", strerror(errno)) << std::endl;
                return false;
            }
            if (Count_ && !SetOption(fd, IPPROTO_TCP, TCP_KEEPCNT, Count_)) {
                std::cerr << std::format("Failed to set TCP_KEEPCNT option: {}", strerror(errno)) << std::endl;
                return false;
            }
            if (Interval_ && !SetOption(fd, IPPROTO_TCP, TCP_KEEPINTVL, Interval_)) {
                std::cerr << std::format("Failed to set TCP_KEEPINTVL option: {}", strerror(errno)) << std::endl;
                return false;
            }
#endif
        }

        if (!SetOption(fd, IPPROTO_TCP, TCP_NODELAY, static_cast<int>(TcpNoDelay_))) {
            std::cerr << std::format("Failed to set TCP_NODELAY option: {}", strerror(errno)) << std::endl;
            return false;
        }

        return true;
    }
    static bool Mutate(int fd, grpc_socket_mutator* mutator) {
        auto self = Cast(mutator);
        return self->SetOption(fd);
    }
    static int Compare(grpc_socket_mutator* a, grpc_socket_mutator* b) {
        const auto* selfA = Cast(a);
        const auto* selfB = Cast(b);
        auto tupleA = std::make_tuple(selfA->IsKeepAliveEnabled_, selfA->Idle_, selfA->Count_, selfA->Interval_, selfA->TcpNoDelay_);
        auto tupleB = std::make_tuple(selfB->IsKeepAliveEnabled_, selfB->Idle_, selfB->Count_, selfB->Interval_, selfB->TcpNoDelay_);
        return tupleA < tupleB ? -1 : tupleA > tupleB ? 1 : 0;
    }
    static void Destroy(grpc_socket_mutator* mutator) {
        delete Cast(mutator);
    }
    static bool Mutate2(const grpc_mutate_socket_info* info, grpc_socket_mutator* mutator) {
        auto self = Cast(mutator);
        return self->SetOption(info->fd);
    }

    static grpc_socket_mutator_vtable VTable;
    const bool IsKeepAliveEnabled_;
    const int Idle_;
    const int Count_;
    const int Interval_;
    const bool TcpNoDelay_;
};

grpc_socket_mutator_vtable TGRpcSocketMutator::VTable =
    {
        &TGRpcSocketMutator::Mutate,
        &TGRpcSocketMutator::Compare,
        &TGRpcSocketMutator::Destroy,
        &TGRpcSocketMutator::Mutate2
    };
#endif

void TGRpcRequestProcessorCommon::ApplyMeta(const TCallMeta& meta) {
    for (const auto& rec : meta.Aux) {
        Context.AddMetadata(NYdb::TStringType{rec.first}, NYdb::TStringType{rec.second});
    }
    if (meta.CallCredentials) {
        Context.set_credentials(meta.CallCredentials);
    }
    if (const NYdb::TDeadline::Duration* timeout = std::get_if<NYdb::TDeadline::Duration>(&meta.Timeout)) {
        Context.set_deadline(NYdb::TDeadline::AfterDuration(*timeout));
    } else if (const NYdb::TDeadline* deadline = std::get_if<NYdb::TDeadline>(&meta.Timeout)) {
        Context.set_deadline(*deadline);
    }
}

void TGRpcRequestProcessorCommon::GetInitialMetadata(std::unordered_multimap<std::string, std::string>* metadata) {
    for (const auto& [key, value] : Context.GetServerInitialMetadata()) {
        metadata->emplace(
            std::string(key.begin(), key.end()),
            std::string(value.begin(), value.end())
        );
    }
}

TChannelPool::TChannelPool(const TTcpKeepAliveSettings& tcpKeepAliveSettings, const TDuration& expireTime, bool tcpNoDelay)
    : TcpKeepAliveSettings_(tcpKeepAliveSettings)
    , TcpNoDelay_(tcpNoDelay)
    , ExpireTime_(expireTime)
    , UpdateReUseTime_(ExpireTime_ * 0.3 < TDuration::Seconds(20) ? ExpireTime_ * 0.3 : TDuration::Seconds(20))
{}

void TChannelPool::GetStubsHolderLocked(
    const std::string& channelId,
    const TGRpcClientConfig& config,
    std::function<void(TStubsHolder&)> cb)
{
    {
        std::shared_lock readGuard(RWMutex_);
        const auto it = Pool_.find(channelId);
        if (it != Pool_.end()) {
            if (!it->second.IsChannelBroken() && !(Now() > it->second.GetLastUseTime() + UpdateReUseTime_)) {
                return cb(it->second);
            }
        }
    }
    {
        std::unique_lock writeGuard(RWMutex_);
        {
            auto it = Pool_.find(channelId);
            if (it != Pool_.end()) {
                if (!it->second.IsChannelBroken()) {
                    EraseFromQueueByTime(it->second.GetLastUseTime(), channelId);
                    auto now = Now();
                    LastUsedQueue_.emplace(now, channelId);
                    it->second.SetLastUseTime(now);
                    return cb(it->second);
                } else {
                    // This channel can't be used. Remove from pool to create new one
                    EraseFromQueueByTime(it->second.GetLastUseTime(), channelId);
                    Pool_.erase(it);
                }
            }
        }
        auto mutator = NImpl::CreateGRpcSocketMutator(TcpKeepAliveSettings_, TcpNoDelay_);
        // will be destroyed inside grpc
        cb(Pool_.emplace(channelId, CreateChannelInterface(config, mutator)).first->second);
        LastUsedQueue_.emplace(Pool_.at(channelId).GetLastUseTime(), channelId);
    }
}

void TChannelPool::DeleteChannel(const std::string& channelId) {
    std::unique_lock writeLock(RWMutex_);
    auto poolIt = Pool_.find(channelId);
    if (poolIt != Pool_.end()) {
        EraseFromQueueByTime(poolIt->second.GetLastUseTime(), channelId);
        Pool_.erase(poolIt);
    }
}

void TChannelPool::DeleteExpiredStubsHolders() {
    std::unique_lock writeLock(RWMutex_);
    auto lastExpired = LastUsedQueue_.lower_bound(Now() - ExpireTime_);
    for (auto i = LastUsedQueue_.begin(); i != lastExpired; ++i){
        Pool_.erase(i->second);
    }
    LastUsedQueue_.erase(LastUsedQueue_.begin(), lastExpired);
}

void TChannelPool::EraseFromQueueByTime(const TInstant& lastUseTime, const std::string& channelId) {
    auto [begin, end] = LastUsedQueue_.equal_range(lastUseTime);
    auto pos = std::find_if(begin, end, [&](auto a){return a.second == channelId;});
    Y_ABORT_UNLESS(pos != LastUsedQueue_.end(), "data corruption at TChannelPool");
    LastUsedQueue_.erase(pos);
}

static void PullEvents(grpc::CompletionQueue* cq) {
#if defined(_WIN32) || defined(_WIN64)
    // Kernel32 outlives SDK workers; no util singleton is needed for naming.
    using TSetThreadDescription = HRESULT (WINAPI*)(HANDLE, PCWSTR);
    static const auto setDescription = reinterpret_cast<TSetThreadDescription>(
        GetProcAddress(GetModuleHandleW(L"kernel32.dll"), "SetThreadDescription"));
    if (setDescription) {
        Y_ABORT_UNLESS(SUCCEEDED(setDescription(GetCurrentThread(), L"grpc_client")),
            "SetThreadDescription failed");
    }
#if defined(_MSC_VER)
    else {
#pragma pack(push, 8)
        const struct {
            DWORD Type;
            LPCSTR Name;
            DWORD ThreadId;
            DWORD Flags;
        } info = {0x1000, "grpc_client", DWORD(-1), 0};
#pragma pack(pop)
        __try {
            RaiseException(0x406D1388, 0, sizeof(info) / sizeof(ULONG_PTR),
                reinterpret_cast<const ULONG_PTR*>(&info));
        } __except (EXCEPTION_EXECUTE_HANDLER) {
        }
    }
#endif
#else
    TThread::SetCurrentThreadName("grpc_client");
#endif
    void* tag;
    bool ok;
    while (cq->Next(&tag, &ok)) {
        if (auto* event = static_cast<IQueueClientEvent*>(tag)) {
            if (!event->Execute(ok)) {
                event->Destroy();
            }
        }
    }
}

class TGRpcClientLow::TContextImpl final
    : public std::enable_shared_from_this<TContextImpl>
    , public IQueueClientContext
{
    using TCallback = std::function<void()>;
    using TContextPtr = std::shared_ptr<TContextImpl>;

public:
    explicit TContextImpl(grpc::CompletionQueue* cq, TContextPtr parent = {})
        : Parent_(std::move(parent))
        , CQ_(cq)
    {
        Y_ABORT_UNLESS(CQ_);
    }

    IQueueClientContextPtr CreateContext() override {
        auto child = std::make_shared<TContextImpl>(CQ_, shared_from_this());
        std::lock_guard guard(Mutex_);
        if (Cancelled_.load(std::memory_order_relaxed)) {
            child->Cancelled_.store(true, std::memory_order_relaxed);
        } else {
            // Amortize pruning across insertions instead of scanning all active
            // children on each request. No destructor needs to unregister.
            if (++AddedSincePrune_ > Children_.size() / 2) {
                Children_.erase(std::remove_if(Children_.begin(), Children_.end(),
                    [](const auto& entry) { return entry.expired(); }), Children_.end());
                AddedSincePrune_ = 0;
            }
            Children_.push_back(child);
        }
        return child;
    }

    grpc::CompletionQueue* CompletionQueue() override {
        return CQ_;
    }

    bool IsCancelled() const override {
        return Cancelled_.load(std::memory_order_acquire);
    }

    bool Cancel() override {
        TStackVec<TCallback, 1> callbacks;
        TStackVec<TContextPtr, 2> children;
        {
            std::lock_guard guard(Mutex_);
            if (Cancelled_.load(std::memory_order_relaxed)) {
                return false;
            }
            callbacks.reserve(Callbacks_.size());
            children.reserve(Children_.size());
            for (auto& callback : Callbacks_) {
                callbacks.push_back(std::move(callback));
            }
            Callbacks_.clear();
            for (auto& entry : Children_) {
                if (auto child = entry.lock()) {
                    children.push_back(std::move(child));
                }
            }
            Children_.clear();
            Cancelled_.store(true, std::memory_order_release);
        }
        RunCallbacks(callbacks);
        for (auto& child : children) {
            child->Cancel();
        }
        return true;
    }

    void SubscribeCancel(TCallback callback) override {
        Y_ABORT_UNLESS(callback, "SubscribeCancel called with an empty callback");
        {
            std::lock_guard guard(Mutex_);
            if (!Cancelled_.load(std::memory_order_relaxed)) {
                Callbacks_.emplace_back().swap(callback);
                return;
            }
        }
        callback();
    }

private:
    static void RunCallbacks(TStackVec<TCallback, 1>& callbacks) noexcept {
        for (auto& callback : callbacks) {
            callback();
            callback = nullptr;
        }
    }

    // Retain the explicit cancellation parent, without any lifetime registry.
    const TContextPtr Parent_;
    grpc::CompletionQueue* const CQ_;
    std::mutex Mutex_;
    TStackVec<std::weak_ptr<TContextImpl>, 2> Children_;
    std::size_t AddedSincePrune_ = 0;
    TStackVec<TCallback, 1> Callbacks_;
    std::atomic<bool> Cancelled_ = false;
};

class TGRpcClientLow::TNetwork final : public IQueueClientContextProvider {
public:
    TNetwork(std::size_t threadCount, bool queuePerThread) {
        if (!threadCount) {
            threadCount = DEFAULT_NUM_THREADS;
        }
        const auto queueCount = queuePerThread ? threadCount : 1;
        CQS_.reserve(queueCount);
        WorkerThreads_.reserve(threadCount);
        for (std::size_t i = 0; i < queueCount; ++i) {
            CQS_.push_back(std::make_unique<grpc::CompletionQueue>());
        }
        try {
            for (std::size_t i = 0; i < threadCount; ++i) {
                AddWorker();
            }
        } catch (...) {
            // Roll back a failed initialization before publishing the singleton.
            for (auto& cq : CQS_) {
                cq->Shutdown();
            }
            for (auto& thread : WorkerThreads_) {
                thread.join();
            }
            throw;
        }
    }

    grpc::CompletionQueue* CompletionQueue() {
        return CQS_.size() == 1 ? CQS_.front().get() : CQS_[RandomNumber(CQS_.size())].get();
    }

    IQueueClientContextPtr CreateContext() override {
        return std::make_shared<TContextImpl>(CompletionQueue());
    }

    void AddWorker() {
        // Reserve before starting a thread so allocation failure cannot leave an
        // unowned worker accessing completion queues during initialization rollback.
        WorkerThreads_.reserve(WorkerThreads_.size() + 1);
        auto* cq = CQS_[WorkerThreads_.size() % CQS_.size()].get();
        WorkerThreads_.emplace_back([cq] {
            PullEvents(cq);
        });
    }

private:
    std::vector<std::unique_ptr<grpc::CompletionQueue>> CQS_;
    std::vector<std::thread> WorkerThreads_;
};

TGRpcClientLow::TNetwork& TGRpcClientLow::GetNetwork(std::size_t threadCount, bool queuePerThread) {
    static auto* network = new TNetwork(threadCount, queuePerThread);
    return *network;
}

TGRpcClientLow::TGRpcClientLow(size_t numWorkerThread, bool useCompletionQueuePerThread)
    : Network_(GetNetwork(numWorkerThread, useCompletionQueuePerThread))
{
}

void TGRpcClientLow::AddWorkerThreadForTest() {
    Network_.AddWorker();
}

IQueueClientContextProvider* TGRpcClientLow::GetContextProvider() {
    return &Network_;
}

grpc::CompletionQueue* TGRpcClientLow::CompletionQueue() {
    return Network_.CompletionQueue();
}

IQueueClientContextPtr TGRpcClientLow::CreateContext() {
    return Network_.CreateContext();
}

grpc_socket_mutator* NImpl::CreateGRpcSocketMutator(const TTcpKeepAliveSettings& TcpKeepAliveSettings_, bool tcpNoDelay) {
#if !defined(YDB_DISABLE_GRPC_SOCKET_MUTATOR)
    return new TGRpcSocketMutator(
        TcpKeepAliveSettings_.Enabled,
        TcpKeepAliveSettings_.Idle,
        TcpKeepAliveSettings_.Count,
        TcpKeepAliveSettings_.Interval,
        tcpNoDelay
    );
#endif
    return nullptr;
}

}

grpc::TimePoint<NYdb::TDeadline>::TimePoint(const NYdb::TDeadline& deadline)
    : time_(DeadlineToTimespec(deadline))
{
}

gpr_timespec grpc::TimePoint<NYdb::TDeadline>::raw_time() const noexcept {
    return time_;
}

gpr_timespec grpc::TimePoint<NYdb::TDeadline>::DurationToTimespec(const NYdb::TDeadline::Duration& duration) noexcept {
    const auto secs = std::chrono::floor<std::chrono::seconds>(duration);
    if (duration == NYdb::TDeadline::Duration::max() || secs.count() >= gpr_inf_future(GPR_CLOCK_MONOTONIC).tv_sec) {
        return gpr_inf_future(GPR_TIMESPAN);
    }
    if (secs.count() < 0) {
        return gpr_inf_past(GPR_TIMESPAN);
    }
    const auto nsecs = std::chrono::duration_cast<std::chrono::nanoseconds>(duration - secs);
    Y_ASSERT(0 <= nsecs.count() && nsecs.count() < GPR_NS_PER_SEC);
    gpr_timespec t;
    t.tv_sec = static_cast<std::int64_t>(secs.count());
    t.tv_nsec = static_cast<std::int32_t>(nsecs.count());
    t.clock_type = GPR_TIMESPAN;
    return t;
}

gpr_timespec grpc::TimePoint<NYdb::TDeadline>::DeadlineToTimespec(const NYdb::TDeadline& deadline) {
    gpr_timespec t =
        DurationToTimespec(deadline.GetTimePoint() != NYdb::TDeadline::TimePoint::max() ? deadline.GetTimePoint() - NYdb::TDeadline::Clock::now() : NYdb::TDeadline::Duration::max());
    return gpr_convert_clock_type(t, GPR_CLOCK_MONOTONIC);
}
