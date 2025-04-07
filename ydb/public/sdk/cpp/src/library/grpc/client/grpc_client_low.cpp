#include "grpc_client_low.h"

#include <grpc/support/log.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/system/thread.h>
#include <util/random/random.h>

#if !defined(_WIN32) && !defined(_WIN64)
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#endif

#if !defined(YDB_DISABLE_GRPC_SOCKET_MUTATOR)
#include <contrib/libs/grpc/src/core/lib/iomgr/socket_mutator.h>
#endif

#include <format>

namespace NYdbGrpc {
inline namespace Dev {

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
class TGRpcKeepAliveSocketMutator : public grpc_socket_mutator {
public:
    TGRpcKeepAliveSocketMutator(int idle, int count, int interval)
        : Idle_(idle)
        , Count_(count)
        , Interval_(interval)
    {
        grpc_socket_mutator_init(this, &VTable);
    }
private:
    static TGRpcKeepAliveSocketMutator* Cast(grpc_socket_mutator* mutator) {
        return static_cast<TGRpcKeepAliveSocketMutator*>(mutator);
    }

    template<typename TVal>
    bool SetOption(int fd, int level, int optname, const TVal& value) {
        return setsockopt(fd, level, optname, reinterpret_cast<const char*>(&value), sizeof(value)) == 0;
    }
    bool SetOption(int fd) {
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
        return true;
    }
    static bool Mutate(int fd, grpc_socket_mutator* mutator) {
        auto self = Cast(mutator);
        return self->SetOption(fd);
    }
    static int Compare(grpc_socket_mutator* a, grpc_socket_mutator* b) {
        const auto* selfA = Cast(a);
        const auto* selfB = Cast(b);
        auto tupleA = std::make_tuple(selfA->Idle_, selfA->Count_, selfA->Interval_);
        auto tupleB = std::make_tuple(selfB->Idle_, selfB->Count_, selfB->Interval_);
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
    const int Idle_;
    const int Count_;
    const int Interval_;
};

grpc_socket_mutator_vtable TGRpcKeepAliveSocketMutator::VTable =
    {
        &TGRpcKeepAliveSocketMutator::Mutate,
        &TGRpcKeepAliveSocketMutator::Compare,
        &TGRpcKeepAliveSocketMutator::Destroy,
        &TGRpcKeepAliveSocketMutator::Mutate2
    };
#endif

TChannelPool::TChannelPool(const TTcpKeepAliveSettings& tcpKeepAliveSettings, const TDuration& expireTime)
    : TcpKeepAliveSettings_(tcpKeepAliveSettings)
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
        auto mutator = NImpl::CreateGRpcKeepAliveSocketMutator(TcpKeepAliveSettings_);
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
    TThread::SetCurrentThreadName("grpc_client");
    while (true) {
        void* tag;
        bool ok;

        if (!cq->Next(&tag, &ok)) {
            break;
        }

        if (auto* ev = static_cast<IQueueClientEvent*>(tag)) {
            if (!ev->Execute(ok)) {
                ev->Destroy();
            }
        }
    }
}

class TGRpcClientLow::TContextImpl final
    : public std::enable_shared_from_this<TContextImpl>
    , public IQueueClientContext
{
    friend class TGRpcClientLow;

    using TCallback = std::function<void()>;
    using TContextPtr = std::shared_ptr<TContextImpl>;

public:
    ~TContextImpl() override {
        Y_ABORT_UNLESS(CountChildren() == 0,
                "Destructor called with non-empty children");

        if (Parent) {
            Parent->ForgetContext(this);
        } else if (Y_LIKELY(Owner)) {
            Owner->ForgetContext(this);
        }
    }

    /**
     * Helper for locking child pointer from a parent container
     */
    static TContextPtr LockChildPtr(TContextImpl* ptr) {
        if (ptr) {
            // N.B. it is safe to do as long as it's done under a mutex and
            // pointer is among valid children. When that's the case we
            // know that TContextImpl destructor has not finished yet, so
            // the object is valid. The lock() method may return nullptr
            // though, if the object is being destructed right now.
            return ptr->weak_from_this().lock();
        } else {
            return nullptr;
        }
    }

    void ForgetContext(TContextImpl* child) {
        std::unique_lock<std::mutex> guard(Mutex);

        auto removed = RemoveChild(child);
        Y_ABORT_UNLESS(removed, "Unexpected ForgetContext(%p)", child);
    }

    IQueueClientContextPtr CreateContext() override {
        auto self = shared_from_this();
        auto child = std::make_shared<TContextImpl>();

        {
            std::unique_lock<std::mutex> guard(Mutex);

            AddChild(child.get());

            // It's now safe to initialize parent and owner
            child->Parent = std::move(self);
            child->Owner = Owner;
            child->CQ = CQ;

            // Propagate cancellation to a child context
            if (Cancelled.load(std::memory_order_relaxed)) {
                child->Cancelled.store(true, std::memory_order_relaxed);
            }
        }

        return child;
    }

    grpc::CompletionQueue* CompletionQueue() override {
        Y_ABORT_UNLESS(Owner, "Uninitialized context");
        return CQ;
    }

    bool IsCancelled() const override {
        return Cancelled.load(std::memory_order_acquire);
    }

    bool Cancel() override {
        TStackVec<TCallback, 1> callbacks;
        TStackVec<TContextPtr, 2> children;

        {
            std::unique_lock<std::mutex> guard(Mutex);

            if (Cancelled.load(std::memory_order_relaxed)) {
                // Already cancelled in another thread
                return false;
            }

            callbacks.reserve(Callbacks.size());
            children.reserve(CountChildren());

            for (auto& callback : Callbacks) {
                callbacks.emplace_back().swap(callback);
            }
            Callbacks.clear();

            // Collect all children we need to cancel
            // N.B. we don't clear children links (cleared by destructors)
            // N.B. some children may be stuck in destructors at the moment
            for (TContextImpl* ptr : InlineChildren) {
                if (auto child = LockChildPtr(ptr)) {
                    children.emplace_back(std::move(child));
                }
            }
            for (auto* ptr : Children) {
                if (auto child = LockChildPtr(ptr)) {
                    children.emplace_back(std::move(child));
                }
            }

            Cancelled.store(true, std::memory_order_release);
        }

        // Call directly subscribed callbacks
        if (!callbacks.empty()) {
            RunCallbacksNoExcept(callbacks);
        }

        // Cancel all children
        for (auto& child : children) {
            child->Cancel();
            child.reset();
        }

        return true;
    }

    void SubscribeCancel(TCallback callback) override {
        Y_ABORT_UNLESS(callback, "SubscribeCancel called with an empty callback");

        {
            std::unique_lock<std::mutex> guard(Mutex);

            if (!Cancelled.load(std::memory_order_relaxed)) {
                Callbacks.emplace_back().swap(callback);
                return;
            }
        }

        // Already cancelled, run immediately
        callback();
    }

private:
    void AddChild(TContextImpl* child) {
        for (TContextImpl*& slot : InlineChildren) {
            if (!slot) {
                slot = child;
                return;
            }
        }

        Children.insert(child);
    }

    bool RemoveChild(TContextImpl* child) {
        for (TContextImpl*& slot : InlineChildren) {
            if (slot == child) {
                slot = nullptr;
                return true;
            }
        }

        return Children.erase(child);
    }

    size_t CountChildren() {
        size_t count = 0;

        for (TContextImpl* ptr : InlineChildren) {
            if (ptr) {
                ++count;
            }
        }

        return count + Children.size();
    }

    template<class TCallbacks>
    static void RunCallbacksNoExcept(TCallbacks& callbacks) noexcept {
        for (auto& callback : callbacks) {
            if (callback) {
                callback();
                callback = nullptr;
            }
        }
    }

private:
    // We want a simple lock here, without extra memory allocations
    std::mutex Mutex;

    // These fields are initialized on successful registration
    TContextPtr Parent;
    TGRpcClientLow* Owner = nullptr;
    grpc::CompletionQueue* CQ = nullptr;

    // Some children are stored inline, others are in a set
    std::array<TContextImpl*, 2> InlineChildren{ { nullptr, nullptr } };
    std::unordered_set<TContextImpl*> Children;

    // Single callback is stored without extra allocations
    TStackVec<TCallback, 1> Callbacks;

    // Atomic flag for a faster IsCancelled() implementation
    std::atomic<bool> Cancelled;
};

TGRpcClientLow::TGRpcClientLow(size_t numWorkerThread, bool useCompletionQueuePerThread)
    : UseCompletionQueuePerThread_(useCompletionQueuePerThread)
{
    Init(numWorkerThread);
}

void TGRpcClientLow::Init(size_t numWorkerThread) {
    SetCqState(WORKING);
    if (UseCompletionQueuePerThread_) {
        for (size_t i = 0; i < numWorkerThread; i++) {
            CQS_.push_back(std::make_unique<grpc::CompletionQueue>());
            auto* cq = CQS_.back().get();
            WorkerThreads_.emplace_back(SystemThreadFactory()->Run([cq]() {
                PullEvents(cq);
            }).Release());
        }
    } else {
        CQS_.push_back(std::make_unique<grpc::CompletionQueue>());
        auto* cq = CQS_.back().get();
        for (size_t i = 0; i < numWorkerThread; i++) {
            WorkerThreads_.emplace_back(SystemThreadFactory()->Run([cq]() {
                PullEvents(cq);
            }).Release());
        }
    }
}

void TGRpcClientLow::AddWorkerThreadForTest() {
    if (UseCompletionQueuePerThread_) {
        CQS_.push_back(std::make_unique<grpc::CompletionQueue>());
        auto* cq = CQS_.back().get();
        WorkerThreads_.emplace_back(SystemThreadFactory()->Run([cq]() {
            PullEvents(cq);
        }).Release());
    } else {
        auto* cq = CQS_.back().get();
        WorkerThreads_.emplace_back(SystemThreadFactory()->Run([cq]() {
            PullEvents(cq);
        }).Release());
    }
}

TGRpcClientLow::~TGRpcClientLow() {
    StopInternal(true);
    WaitInternal();
}

void TGRpcClientLow::Stop(bool wait) {
    StopInternal(false);

    if (wait) {
        WaitInternal();
    }
}

void TGRpcClientLow::StopInternal(bool silent) {
    bool shutdown;

    std::vector<TContextImpl::TContextPtr> cancelQueue;

    {
        std::unique_lock<std::mutex> guard(Mtx_);

        auto allowStateChange = [&]() {
            switch (GetCqState()) {
                case WORKING:
                    return true;
                case STOP_SILENT:
                    return !silent;
                case STOP_EXPLICIT:
                    return false;
            }

            Y_UNREACHABLE();
        };

        if (!allowStateChange()) {
            // Completion queue is already stopping
            return;
        }

        SetCqState(silent ? STOP_SILENT : STOP_EXPLICIT);

        if (!silent && !Contexts_.empty()) {
            cancelQueue.reserve(Contexts_.size());
            for (auto* ptr : Contexts_) {
                // N.B. some contexts may be stuck in destructors
                if (auto context = TContextImpl::LockChildPtr(ptr)) {
                    cancelQueue.emplace_back(std::move(context));
                }
            }
        }

        shutdown = Contexts_.empty();
    }

    for (auto& context : cancelQueue) {
        context->Cancel();
        context.reset();
    }

    if (shutdown) {
        for (auto& cq : CQS_) {
            cq->Shutdown();
        }
    }
}

void TGRpcClientLow::WaitInternal() {
    std::unique_lock<std::mutex> guard(JoinMutex_);

    for (auto& ti : WorkerThreads_) {
        ti->Join();
    }
}

void TGRpcClientLow::WaitIdle() {
    std::unique_lock<std::mutex> guard(Mtx_);

    while (!Contexts_.empty()) {
        ContextsEmpty_.wait(guard);
    }
}

std::shared_ptr<IQueueClientContext> TGRpcClientLow::CreateContext() {
    std::unique_lock<std::mutex> guard(Mtx_);

    auto allowCreateContext = [&]() {
        switch (GetCqState()) {
            case WORKING:
                return true;
            case STOP_SILENT:
            case STOP_EXPLICIT:
                return false;
        }

        Y_UNREACHABLE();
    };

    if (!allowCreateContext()) {
        // New context creation is forbidden
        return nullptr;
    }

    auto context = std::make_shared<TContextImpl>();
    Contexts_.insert(context.get());
    context->Owner = this;
    if (UseCompletionQueuePerThread_) {
        context->CQ = CQS_[RandomNumber(CQS_.size())].get();
    } else {
        context->CQ = CQS_[0].get();
    }
    return context;
}

void TGRpcClientLow::ForgetContext(TContextImpl* context) {
    bool shutdown = false;

    {
        std::unique_lock<std::mutex> guard(Mtx_);

        if (!Contexts_.erase(context)) {
            Y_ABORT("Unexpected ForgetContext(%p)", context);
        }

        if (Contexts_.empty()) {
            if (IsStopping()) {
                shutdown = true;
            }

            ContextsEmpty_.notify_all();
        }
    }

    if (shutdown) {
        // This was the last context, shutdown CQ
        for (auto& cq : CQS_) {
            cq->Shutdown();
        }
    }
}

grpc_socket_mutator* NImpl::CreateGRpcKeepAliveSocketMutator(const TTcpKeepAliveSettings& TcpKeepAliveSettings_) {
#if !defined(YDB_DISABLE_GRPC_SOCKET_MUTATOR)
    TGRpcKeepAliveSocketMutator* mutator = nullptr;
    if (TcpKeepAliveSettings_.Enabled) {
        mutator = new TGRpcKeepAliveSocketMutator(
                TcpKeepAliveSettings_.Idle,
                TcpKeepAliveSettings_.Count,
                TcpKeepAliveSettings_.Interval
                );
    }
    return mutator;
#endif
    return nullptr;
}

}
}
