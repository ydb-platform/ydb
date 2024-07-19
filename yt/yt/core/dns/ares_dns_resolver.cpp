#include "dns_resolver.h"

#include "config.h"
#include "private.h"

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/invoker.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/shutdown_priorities.h>
#include <yt/yt/core/misc/shutdown.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/moody_camel_concurrent_queue.h>

#include <yt/yt/core/threading/thread.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/threading/notification_handle.h>

#include <library/cpp/yt/system/handle_eintr.h>

#include <ares.h>

#include <cmath>

#ifdef _unix_
    #ifdef _linux_
        #define YT_DNS_RESOLVER_USE_EPOLL
    #endif

    #include <arpa/inet.h>
    #include <netdb.h>
    #include <netinet/in.h>
    #include <sys/socket.h>

    #ifdef YT_DNS_RESOLVER_USE_EPOLL
        #include <sys/epoll.h>
    #else
        #include <sys/select.h>
    #endif
#endif

#ifdef _win_
    #include <winsock2.h>
#endif

////////////////////////////////////////////////////////////////////////////////

void FormatValue(NYT::TStringBuilderBase* builder, struct hostent* hostent, TStringBuf /*spec*/)
{
    bool empty = true;

    auto appendIf = [&] (bool condition, auto function) {
        if (condition) {
            if (!empty) {
                builder->AppendString(", ");
            }
            function();
            empty = false;
        }
    };

    builder->AppendString("{");

    appendIf(hostent->h_name, [&] {
        builder->AppendString("Canonical: ");
        builder->AppendString(hostent->h_name);
    });

    appendIf(hostent->h_aliases, [&] {
        builder->AppendString("Aliases: {");
        for (int i = 0; hostent->h_aliases[i]; ++i) {
            if (i > 0) {
                builder->AppendString(", ");
            }
            builder->AppendString(hostent->h_aliases[i]);
        }
        builder->AppendString("}");
    });

    appendIf(hostent->h_addr_list, [&] {
        auto stringSize = 0;
        if (hostent->h_addrtype == AF_INET) {
            stringSize = INET_ADDRSTRLEN;
        }
        if (hostent->h_addrtype == AF_INET6) {
            stringSize = INET6_ADDRSTRLEN;
        }
        builder->AppendString("Addresses: {");
        for (int i = 0; hostent->h_addr_list[i]; ++i) {
            if (i > 0) {
                builder->AppendString(", ");
            }
            auto string = builder->Preallocate(stringSize);
            ares_inet_ntop(
                hostent->h_addrtype,
                hostent->h_addr_list[i],
                string,
                stringSize);
            builder->Advance(strlen(string));
        }
        builder->AppendString("}");
    });

    builder->AppendString("}");
}

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NDns {

using namespace NConcurrency;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = DnsLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

TError MakeCanceledError(TGuid requestId)
{
    return TError(NYT::EErrorCode::Canceled, "Ares DNS resolver is stopped")
        << TErrorAttribute("request_id", requestId);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TAresDnsResolver
    : public IDnsResolver
{
public:
    explicit TAresDnsResolver(TAresDnsResolverConfigPtr config)
        : Config_(std::move(config))
    {
        SetupSocket();

        YT_VERIFY(InitAresGlobals() == ARES_SUCCESS);
        YT_VERIFY(InitAresOptions() == ARES_SUCCESS);

        SetAresSocketCallback();
    }

    void Initialize()
    {
        ResolverThread_ = New<TResolverThread>(this);
        ShutdownCookie_ = RegisterShutdownCallback(
            "Ares DNS Resolver",
            BIND_NO_PROPAGATE(&TAresDnsResolver::Shutdown, MakeWeak(this)),
            ResolverShutdownPriority);
    }

    ~TAresDnsResolver()
    {
        Shutdown();
    }

    TFuture<TNetworkAddress> Resolve(
        const TString& hostName,
        const TDnsResolveOptions& options) override
    {
        EnsureRunning();

        auto request = PrepareRequest(hostName, options);
        auto future = request->Promise.ToFuture();

        EnqueueRequest(std::move(request));

        return future;
    }

private:
    const TAresDnsResolverConfigPtr Config_;

    const NProfiling::TProfiler Profiler_ = DnsProfiler.WithPrefix("/ares_resolver");
    const NProfiling::TCounter RequestCounter_ = Profiler_.Counter("/request_count");
    const NProfiling::TCounter FailureCounter_ = Profiler_.Counter("/failure_count");
    const NProfiling::TCounter TimeoutCounter_ = Profiler_.Counter("/timeout_count");
    const NProfiling::TTimeGauge RequestTimeGauge_ = Profiler_.TimeGauge("/request_time");

    struct TResolveRequest
    {
        TAresDnsResolver* Owner;
        TGuid RequestId;
        TPromise<TNetworkAddress> Promise;
        TString HostName;
        TDnsResolveOptions Options;
        NProfiling::TWallTimer Timer;
        TDelayedExecutorCookie TimeoutCookie;
    };

    TShutdownCookie ShutdownCookie_;

    moodycamel::ConcurrentQueue<std::unique_ptr<TResolveRequest>> Queue_;

    std::atomic<bool> ShuttingDown_ = false;

#ifdef YT_DNS_RESOLVER_USE_EPOLL
    int EpollFD_ = -1;
#endif
    NThreading::TNotificationHandle WakeupHandle_;

    ares_channel Channel_;
    ares_options Options_;


    class TResolverThread
        : public NThreading::TThread
    {
    public:
        explicit TResolverThread(TAresDnsResolver* owner)
            : TThread(
                "AresDnsResolver",
                NThreading::TThreadOptions{
                    .ShutdownPriority = ResolverThreadShutdownPriority,
                })
            , Owner_(owner)
        { }

    private:
        TAresDnsResolver* const Owner_;

        void StopPrologue() override
        {
            Owner_->WakeupHandle_.Raise();
        }

        #ifdef YT_DNS_RESOLVER_USE_EPOLL
        bool DoProcessFDEventsEpoll(TDuration pollTimeout)
        {
            constexpr size_t MaxEventsPerPoll = 10;
            struct epoll_event events[MaxEventsPerPoll];

            int count = HandleEintr(epoll_wait, Owner_->EpollFD_, events, MaxEventsPerPoll, pollTimeout.MilliSeconds());
            YT_VERIFY(count >= 0);

            if (count == 0) {
                ares_process_fd(Owner_->Channel_, ARES_SOCKET_BAD, ARES_SOCKET_BAD);
                return false;
            }

            // According to c-ares implementation this loop would cost O(#dns-servers-total * #dns-servers-active).
            // Hope that we are not creating too many connections!
            bool wakeUpFDTriggered = false;
            for (int i = 0; i < count; ++i) {
                int triggeredFD = events[i].data.fd;
                if (triggeredFD == Owner_->WakeupHandle_.GetFD()) {
                    wakeUpFDTriggered = true;
                    continue;
                }

                // If the error events were returned, process both EPOLLIN and EPOLLOUT.
                int readFD = (events[i].events & (EPOLLIN | EPOLLERR | EPOLLHUP)) ? triggeredFD : ARES_SOCKET_BAD;
                int writeFD = (events[i].events & (EPOLLOUT | EPOLLERR | EPOLLHUP)) ? triggeredFD : ARES_SOCKET_BAD;
                ares_process_fd(Owner_->Channel_, readFD, writeFD);
            }

            return wakeUpFDTriggered;
        }
        #else
        bool DoProcessFDEventsDefault(TDuration pollTimeout)
        {
            fd_set readFDs, writeFDs;
            FD_ZERO(&readFDs);
            FD_ZERO(&writeFDs);

            int wakeupFD = Owner_->WakeupHandle_.GetFD();
            int nFDs = ares_fds(Owner_->Channel_, &readFDs, &writeFDs);
            nFDs = std::max(nFDs, 1 + wakeupFD);
            FD_SET(wakeupFD, &readFDs);

            // Windows has no such limitation since fd_set there implemented as an array, not as bit fields.
            // https://docs.microsoft.com/en-us/windows/win32/api/winsock2/nf-winsock2-select
            #ifndef _win_
                YT_VERIFY(nFDs <= FD_SETSIZE); // This is inherent limitation by select().
            #endif

            timeval timeout;
            timeout.tv_sec = pollTimeout.Seconds();
            timeout.tv_usec = (pollTimeout.MilliSeconds() % 1000) * 1000;

            int result = select(nFDs, &readFDs, &writeFDs, nullptr, &timeout);
            YT_VERIFY(result >= 0);

            ares_process(Owner_->Channel_, &readFDs, &writeFDs);

            return FD_ISSET(wakeupFD, &readFDs);
        }
        #endif

        // Returns |true| if there is a new resolve
        // request in the queue.
        bool ProcessFDEvents(TDuration pollTimeout)
        {
        #ifdef YT_DNS_RESOLVER_USE_EPOLL
            return DoProcessFDEventsEpoll(pollTimeout);
        #else
            return DoProcessFDEventsDefault(pollTimeout);
        #endif
        }

        // Delegate request to c-ares.
        void ExecuteRequest(std::unique_ptr<TResolveRequest> request)
        {
            // Try to reduce number of lookups to save some time.
            int family = AF_UNSPEC;
            if (request->Options.EnableIPv4 && !request->Options.EnableIPv6) {
                family = AF_INET;
            }
            if (request->Options.EnableIPv6 && !request->Options.EnableIPv4) {
                family = AF_INET6;
            }

            ares_gethostbyname(
                Owner_->Channel_,
                request->HostName.c_str(),
                family,
                &OnNamedResolvedThunk,
                request.get());
            Y_UNUSED(request.release());
        }

        // Cancel request's promise.
        void CancelRequest(std::unique_ptr<TResolveRequest> request)
        {
            TDelayedExecutor::CancelAndClear(request->TimeoutCookie);
            request->Promise.Set(MakeCanceledError(request->RequestId));
        }

        // Returns |false| if empty queue was observed.
        bool TryProcessRequests(int batchSize, bool cancelRequests)
        {
            for (int idx = 0; idx < batchSize; ++idx) {
                std::unique_ptr<TResolveRequest> request;
                if (!Owner_->Queue_.try_dequeue(request)) {
                    return false; // Observed empty queue.
                }

                if (cancelRequests) {
                    CancelRequest(std::move(request));
                    continue;
                }

                ExecuteRequest(std::move(request));
            }

            return true; // Full batch processed.
        }

        void Cleanup()
        {
            // Cancel all pending requests.
            // Resolve will not add any more tasks because of short-circuiting.
            while (TryProcessRequests(std::numeric_limits<int>::max(), /*cancelRequests*/ true));

            // Cancel out all pending requests.
            ares_cancel(Owner_->Channel_);
        }

        void ThreadMain() override
        {
            // TODO(arkady-e1ppa): Make batch size and poll timeout configurable?
            constexpr size_t RequestsBatchSize = 100;

            while (!IsStopping()) {
                constexpr auto PollTimeout = TDuration::MilliSeconds(1000);

                bool shouldDequeue = ProcessFDEvents(PollTimeout);

                if (shouldDequeue) {
                    if (bool noRequestsLeft = !TryProcessRequests(RequestsBatchSize, /*cancelRequests*/ false)) {
                        // We make WakeUpHandle triggerable again
                        // and then double check the queue for requests.
                        // This way we ensure that either we observe
                        // request in the queue or we observe the
                        // triggered wakeUpFD in the next iteration of this loop.
                        Owner_->WakeupHandle_.Clear();

                        // NB(arkady-e1ppa): Do not drain the entire queue
                        // because of OOM risk.
                        // (TryProcessRequest allocates data in c-ares internals for each request.)
                        TryProcessRequests(RequestsBatchSize, /*cancelRequests*/ false);
                    }
                }
            }

            // We happen to be here in exactly two cases:
            // 1) Resolver has been destroyed:
            //    1.1) Naturally -- never happens really but equivalent to (2).
            //    1.2) Someone reconfigures resolver by recreating it: cancel all
            //         pending requests and die.
            // 2) Shutdown is happening. Cancel all pending requests and die.
            Cleanup();
        }
    };

    TIntrusivePtr<TResolverThread> ResolverThread_;


    static int OnSocketCreated(ares_socket_t socket, int /*type*/, void* opaque)
    {
        int result = 0;
    #ifdef YT_DNS_RESOLVER_USE_EPOLL
        auto this_ = static_cast<TAresDnsResolver*>(opaque);
        struct epoll_event event{0, {0}};
        event.data.fd = socket;
        result = epoll_ctl(this_->EpollFD_, EPOLL_CTL_ADD, socket, &event);
        if (result != 0) {
            YT_LOG_WARNING(TError::FromSystem(), "epoll_ctl() failed in Ares DNS resolver");
            result = -1;
        }
    #else
        Y_UNUSED(opaque);
        #ifndef _win_
            if (socket >= FD_SETSIZE) {
                YT_LOG_WARNING(
                    "File descriptor is out of valid range (FD: %v, Limit: %v)",
                    socket,
                    FD_SETSIZE);
                result = -1;
            }
        #endif
    #endif
        return result;
    }

    static void OnSocketStateChanged(void* opaque, ares_socket_t socket, int readable, int writable)
    {
    #ifdef YT_DNS_RESOLVER_USE_EPOLL
        auto this_ = static_cast<TAresDnsResolver*>(opaque);
        struct epoll_event event{0, {0}};
        event.data.fd = socket;
        int op = EPOLL_CTL_MOD;
        if (readable) {
            event.events |= EPOLLIN;
        }
        if (writable) {
            event.events |= EPOLLOUT;
        }
        if (!readable && !writable) {
            op = EPOLL_CTL_DEL;
        }
        YT_VERIFY(epoll_ctl(this_->EpollFD_, op, socket, &event) == 0);
    #else
        Y_UNUSED(opaque, readable, writable);
        #ifndef _win_
            YT_VERIFY(socket < FD_SETSIZE);
        #endif
    #endif
    }

    void OnRequestTimeout(TPromise<TNetworkAddress> promise, TGuid requestId)
    {
        auto timeoutError
            = TError(NNet::EErrorCode::ResolveTimedOut, "Ares DNS resolve timed out");
        if (promise.TrySet(std::move(timeoutError))) {
            TimeoutCounter_.Increment();
            YT_LOG_WARNING(
                "Ares DNS resolve timed out (RequestId: %v)",
                requestId);
        }
    }

    static void OnNamedResolvedThunk(
        void* opaque,
        int status,
        int timeouts,
        struct hostent* hostent)
    {
        auto* request = static_cast<TResolveRequest*>(opaque);
        request->Owner->OnNamedResolved(
            std::unique_ptr<TResolveRequest>(request),
            status,
            timeouts,
            hostent);
    }

    void CleanupAres() noexcept
    {
        ares_destroy(Channel_);

        // Cleanup library globals.
        // c-ares 1.10+ provides recursive behaviour of init/cleanup.
        ares_library_cleanup();
    }

    void CleanupSocket()
    {
    #ifdef YT_DNS_RESOLVER_USE_EPOLL
        YT_VERIFY(HandleEintr(::close, EpollFD_) == 0);
    #endif
    }

    void Shutdown()
    {
        if (Y_UNLIKELY(ShuttingDown_.exchange(true, std::memory_order::relaxed))) { // (d)
            return;
        }
        std::atomic_thread_fence(std::memory_order::seq_cst); // (e)

        // Thread drains the queue.
        ResolverThread_->Stop();
        // ^- (f) (Actually, Drain which has this call in sihb relation is (f).)

        CleanupAres();
        CleanupSocket();
    }

    void SetupSocket()
    {
    #ifdef YT_DNS_RESOLVER_USE_EPOLL
        EpollFD_ = HandleEintr(epoll_create1, EPOLL_CLOEXEC);
        YT_VERIFY(EpollFD_ >= 0);
    #endif

        int wakeupFD = WakeupHandle_.GetFD();
        OnSocketCreated(wakeupFD, AF_UNSPEC, this);
        OnSocketStateChanged(this, wakeupFD, 1, 0);
    }

    int InitAresGlobals()
    {
        // Init library globals.
        // c-ares 1.10+ provides recursive behaviour of init/cleanup.
        return ares_library_init(ARES_LIB_INIT_ALL);
    }

    int InitAresOptions()
    {
        Zero(Channel_);
        Zero(Options_);

        // See https://c-ares.haxx.se/ares_init_options.html for full details.
        int mask = 0;

        if (Config_->ForceTcp) {
            Options_.flags |= ARES_FLAG_USEVC;
        }
        if (Config_->KeepSocket) {
            Options_.flags |= ARES_FLAG_STAYOPEN;
        }
        mask |= ARES_OPT_FLAGS;

        Options_.timeout = static_cast<int>(Config_->ResolveTimeout.MilliSeconds());
        mask |= ARES_OPT_TIMEOUTMS;

        // ARES_OPT_MAXTIMEOUTMS and ARES_OPT_JITTER are options from
        // yandex privately patched (05-ttl, 07-timeouts) version of c-ares.
    #ifdef ARES_OPT_MAXTIMEOUTMS
        Options_.maxtimeout = static_cast<int>(Config_->MaxResolveTimeout.MilliSeconds());
        mask |= ARES_OPT_MAXTIMEOUTMS;
    #endif

    #ifdef ARES_OPT_JITTER
        if (Config_->Jitter) {
            Options_.jitter = llround(*Config_->Jitter * 1000.0);
            Options_.jitter_rand_seed = TGuid::Create().Parts32[0];
            mask |= ARES_OPT_JITTER;
        }
    #endif

        Options_.tries = Config_->Retries;
        mask |= ARES_OPT_TRIES;

        Options_.sock_state_cb = &TAresDnsResolver::OnSocketStateChanged;
        Options_.sock_state_cb_data = this;
        mask |= ARES_OPT_SOCK_STATE_CB;

        // Disable lookups from /etc/hosts file. Otherwise, cares re-reads file on every dns query.
        // That causes issues with memory management, because c-areas uses fopen(), and fopen() calls mmap().
        Options_.lookups = const_cast<char*>("b");

        return ares_init_options(&Channel_, &Options_, mask);
    }

    void SetAresSocketCallback()
    {
        ares_set_socket_callback(Channel_, &TAresDnsResolver::OnSocketCreated, this);
    }

    void EnsureRunning()
    {
        // ResolverThread has its own short-circuit mechanism
        // thus no need for a second one.
        ResolverThread_->Start();
    }

    std::unique_ptr<TResolveRequest> PrepareRequest(
        const TString& hostName,
        const TDnsResolveOptions& options)
    {
        auto requestId = TGuid::Create();

        auto promise = NewPromise<TNetworkAddress>();

        auto timeoutCookie = TDelayedExecutor::Submit(
            BIND(&TAresDnsResolver::OnRequestTimeout, MakeStrong(this), promise, requestId),
            Config_->MaxResolveTimeout);

        return std::make_unique<TResolveRequest>(TResolveRequest{
            .Owner = this,
            .RequestId = requestId,
            .Promise = std::move(promise),
            .HostName = hostName,
            .Options = options,
            .Timer = {},
            .TimeoutCookie = std::move(timeoutCookie),
        });
    }

    void DrainQueue()
    {
        // Moody camel is a mpmc queue so it should be
        // safe to drain it from producer side.
        std::unique_ptr<TResolveRequest> request;
        while (Queue_.try_dequeue(request)) {
            YT_LOG_DEBUG(
                "Canceling request because Ares DNS resolver is shutting down (RequestId: %v)",
                request->RequestId);
            TDelayedExecutor::CancelAndClear(request->TimeoutCookie);
            request->Promise.Set(MakeCanceledError(request->RequestId));
        }
    }

    // (arkady-e1ppa): Memory orders explanation:
    /*
        There are operations (a) to (f) marked in the code below.
        (Guarantee): We want to make sure that in TryEnqueue we either observe
        ShuttingDown_ == true in (c) or Drain in Shutdown (f) observes
        callback placed in enqueue (a).
        Moody-camel enqueue/dequeue can be modeled as W and RMW respectively
        (it can be rmw for either side -- it doesn't matter if they are atomic).
        Now, we shall prove that (Guarantee) holds true. Suppose it doesn't and
        (c) reads false while (f) doesn't read result of (a). For the sake
        of simplicity, assume enqueue if RMW^rel(Queue_, 0, 1) (reads 0 and writes 1)
        and dequeue is RMW^acq(Queue_, 0, 0) (reads 0 and writes 0)
        Then we have execution:

            T1(TryEnqueue)                    T2(Shutdown)
        W^rlx(Queue_, 0, 1)         (a)    RMW^rlx(ShuttingDown_, false, true) (d)
        F^sc                        (b)    F^sc                                (e)
        R^rlx(ShuttingDown_, false) (c)    RMW^rlx(Queue_, 0, 0)               (f)

        Since (c) reads false it reads it from some other modification which
        precedes (d) in modification order (in this case it is ctor). Thus
        by (https://eel.is/c++draft/atomics.order#3.3) we have (c) -cob-> (d).
        Like-wise (f) must have read some modification of Queue_ X which
        preceedes (a) in modification order (like previous dequeue)
        thus establishing (f) -cob-> (a).
        Now, (b) -sb-> (c) -cob-> (d) -sb-> (e)
        (Here sb means sequenced-before) thus we have (b) -S-> (e)
        (https://eel.is/c++draft/atomics.order#4.4). We also have
        (e) -sihb*-> (f) -cob-> (a) -sb-> (b) thus again (e) -S->(b)
        and so the loop in S found. Thus we can't have both
        (c) reading false and (f) not reading (a)'s result.
        *sihb --- simply-happens before is established via
        TThread::Stop call.
    */
    bool TryEnqueue(std::unique_ptr<TResolveRequest> request)
    {
        if (ShuttingDown_.load(std::memory_order::relaxed)) {
            YT_LOG_DEBUG(
                "Canceling request because Ares DNS resolver is shutting down (RequestId: %v)",
                request->RequestId);
            TDelayedExecutor::CancelAndClear(request->TimeoutCookie);
            request->Promise.Set(MakeCanceledError(request->RequestId));
            return false;
        }
        // <- Concurrent shutdown compelition will cause request to be lost.
        // Thus we double check after enqueue.

        Queue_.enqueue(std::move(request)); // (a)

        std::atomic_thread_fence(std::memory_order::seq_cst); // (b)
        if (ShuttingDown_.load(std::memory_order::relaxed)) { // (c)
            DrainQueue();
            return false;
        }

        return true;
    }

    void EnqueueRequest(std::unique_ptr<TResolveRequest> request)
    {
        RequestCounter_.Increment();
        YT_LOG_DEBUG(
            "Started Ares DNS resolve (RequestId: %v, HostName: %v, Options: %v)",
            request->RequestId,
            request->HostName,
            request->Options);

        if (TryEnqueue(std::move(request))) {
            WakeupHandle_.Raise();
        }
    }

    TDuration ProcessRequestTimeStatistics(
        const std::unique_ptr<TResolveRequest>& request,
        int timeouts)
    {
        auto elapsed = request->Timer.GetElapsedTime();
        RequestTimeGauge_.Update(elapsed);

        if (elapsed > Config_->WarningTimeout || timeouts > 0) {
            YT_LOG_WARNING(
                "Ares DNS resolve took too long (RequestId: %v, HostName: %v, Timeouts: %v, Elapsed: %v)",
                request->RequestId,
                request->HostName,
                timeouts,
                elapsed);
        }

        return elapsed;
    }

    TError MakeFailedRequestError(
        const std::unique_ptr<TResolveRequest>& request,
        int status,
        bool isShuttingDown)
    {
        if (isShuttingDown) {
            return TError(NYT::EErrorCode::Canceled, "Ares DNS resolver is stopped");
        }

        FailureCounter_.Increment();

        return TError(
            "Ares DNS resolve failed for %Qv",
            request->HostName)
            << TErrorAttribute("enable_ipv4", request->Options.EnableIPv4)
            << TErrorAttribute("enable_ipv6", request->Options.EnableIPv6)
            << TError(ares_strerror(status));
    }

    void FailRequest(
        std::unique_ptr<TResolveRequest> request,
        int status)
    {
        // If it happens concurrently with Shutdown call, we don't care what we read
        // cause it is racy anyway.
        // If it happens as a result of Shutdown of a ResolverThread
        // then we have ShuttingDown_.store(true) in happens-before relation
        // (more precisely, at least simply-happens-before) because of
        // TThread::Stop synchronisation.
        bool isShuttingDown = ShuttingDown_.load(std::memory_order::relaxed);

        if (request->Promise.TrySet(MakeFailedRequestError(request, status, isShuttingDown))) {
            YT_LOG_WARNING(
                "Ares DNS resolve failed (RequestId: %v, HostName: %v, IsShuttingDown: %v)",
                request->RequestId,
                request->HostName,
                isShuttingDown);
        }
    }

    void CompleteRequest(
        std::unique_ptr<TResolveRequest> request,
        TDuration elapsed,
        struct hostent* hostent)
    {
        YT_VERIFY(hostent->h_addrtype == AF_INET || hostent->h_addrtype == AF_INET6);
        YT_VERIFY(hostent->h_addr_list && hostent->h_addr_list[0]);

        TNetworkAddress result(hostent->h_addrtype, hostent->h_addr, hostent->h_length);

        if (request->Promise.TrySet(result)) {
            YT_LOG_DEBUG(
                "Ares DNS resolve completed (RequestId: %v, HostName: %v, Result: %v, Hostent: %v, Elapsed: %v)",
                request->RequestId,
                request->HostName,
                result,
                hostent,
                elapsed);
        }
    }

    void OnNamedResolved(
        std::unique_ptr<TResolveRequest> request,
        int status,
        int timeouts,
        struct hostent* hostent)
    {
        TDelayedExecutor::CancelAndClear(request->TimeoutCookie);

        auto elapsed = ProcessRequestTimeStatistics(request, timeouts);

        if (status != ARES_SUCCESS) {
            FailRequest(std::move(request), status);
            return;
        }

        CompleteRequest(std::move(request), elapsed, hostent);
    }
};

////////////////////////////////////////////////////////////////////////////////

IDnsResolverPtr CreateAresDnsResolver(TAresDnsResolverConfigPtr config)
{
    auto resolver =  New<TAresDnsResolver>(std::move(config));
    resolver->Initialize();
    return resolver;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDns
