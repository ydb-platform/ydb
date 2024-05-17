#include "dns_resolver.h"

#include "config.h"
#include "private.h"

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/invoker.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/misc/proc.h>

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

class TAresDnsResolver
    : public IDnsResolver
{
public:
    explicit TAresDnsResolver(TAresDnsResolverConfigPtr config)
        : Config_(std::move(config))
        , ResolverThread_(New<TResolverThread>(this))
    {
    #ifdef YT_DNS_RESOLVER_USE_EPOLL
        EpollFD_ = HandleEintr(epoll_create1, EPOLL_CLOEXEC);
        YT_VERIFY(EpollFD_ >= 0);
    #endif

        int wakeupFD = WakeupHandle_.GetFD();
        OnSocketCreated(wakeupFD, AF_UNSPEC, this);
        OnSocketStateChanged(this, wakeupFD, 1, 0);

        // Init library globals.
        // c-ares 1.10+ provides recursive behaviour of init/cleanup.
        YT_VERIFY(ares_library_init(ARES_LIB_INIT_ALL) == ARES_SUCCESS);

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

        YT_VERIFY(ares_init_options(&Channel_, &Options_, mask) == ARES_SUCCESS);

        ares_set_socket_callback(Channel_, &TAresDnsResolver::OnSocketCreated, this);
    }

    ~TAresDnsResolver()
    {
        ResolverThread_->Stop();

        ares_destroy(Channel_);

        // Cleanup library globals.
        // c-ares 1.10+ provides recursive behaviour of init/cleanup.
        ares_library_cleanup();

    #ifdef YT_DNS_RESOLVER_USE_EPOLL
        YT_VERIFY(HandleEintr(close, EpollFD_) == 0);
    #endif
    }

    TFuture<TNetworkAddress> Resolve(
        const TString& hostName,
        const TDnsResolveOptions& options) override
    {
        auto promise = NewPromise<TNetworkAddress>();
        auto future = promise.ToFuture();

        auto requestId = TGuid::Create();
        auto timeoutCookie = TDelayedExecutor::Submit(
            BIND([promise, requestId] {
                YT_LOG_WARNING("Ares DNS resolve timed out (RequestId: %v)",
                    requestId);
                promise.TrySet(TError(NNet::EErrorCode::ResolveTimedOut, "Ares DNS resolve timed out"));
            }),
            Config_->MaxResolveTimeout);

        RequestCounter_.Increment();

        YT_LOG_DEBUG("Started Ares DNS resolve (RequestId: %v, HostName: %v, Options: %v)",
            requestId,
            hostName,
            options);

        auto request = std::unique_ptr<TResolveRequest>{new TResolveRequest{
            this,
            requestId,
            std::move(promise),
            hostName,
            options,
            {},
            std::move(timeoutCookie)
        }};

        Queue_.enqueue(std::move(request));

        WakeupHandle_.Raise();

        if (!ResolverThread_->Start()) {
            std::unique_ptr<TResolveRequest> request;
            while (Queue_.try_dequeue(request)) {
                request->Promise.Set(TError(NYT::EErrorCode::Canceled, "Ares DNS resolver is stopped"));
            }
        }

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

    moodycamel::ConcurrentQueue<std::unique_ptr<TResolveRequest>> Queue_;

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
            : TThread("AresDnsResolver")
            , Owner_(owner)
        { }

    private:
        TAresDnsResolver* const Owner_;

        void StopPrologue() override
        {
            Owner_->WakeupHandle_.Raise();
        }

        void ThreadMain() override
        {
            constexpr size_t MaxRequestsPerDrain = 100;

            auto drainQueue = [&] {
                for (size_t iteration = 0; iteration < MaxRequestsPerDrain; ++iteration) {
                    std::unique_ptr<TResolveRequest> request;
                    if (!Owner_->Queue_.try_dequeue(request)) {
                        return true;
                    }

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

                    // Releasing unique_ptr on a separate line,
                    // because argument evaluation order is not specified.
                    Y_UNUSED(request.release());
                }
                return false;
            };

            while (!IsStopping()) {
                bool drain = false;
                constexpr int PollTimeoutMs = 1000;

            #ifdef YT_DNS_RESOLVER_USE_EPOLL
                constexpr size_t MaxEventsPerPoll = 10;
                struct epoll_event events[MaxEventsPerPoll];
                int count = HandleEintr(epoll_wait, Owner_->EpollFD_, events, MaxEventsPerPoll, PollTimeoutMs);
                YT_VERIFY(count >= 0);

                if (count == 0) {
                    ares_process_fd(Owner_->Channel_, ARES_SOCKET_BAD, ARES_SOCKET_BAD);
                } else {
                    // According to c-ares implementation this loop would cost O(#dns-servers-total * #dns-servers-active).
                    // Hope that we are not creating too many connections!
                    for (int i = 0; i < count; ++i) {
                        int triggeredFD = events[i].data.fd;
                        if (triggeredFD == Owner_->WakeupHandle_.GetFD()) {
                            drain = true;
                        } else {
                            // If the error events were returned, process both EPOLLIN and EPOLLOUT.
                            int readFD = (events[i].events & (EPOLLIN | EPOLLERR | EPOLLHUP)) ? triggeredFD : ARES_SOCKET_BAD;
                            int writeFD = (events[i].events & (EPOLLOUT | EPOLLERR | EPOLLHUP)) ? triggeredFD : ARES_SOCKET_BAD;
                            ares_process_fd(Owner_->Channel_, readFD, writeFD);
                        }
                    }
                }
            #else
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
                timeout.tv_sec = PollTimeoutMs / 1000;
                timeout.tv_usec = (PollTimeoutMs % 1000) * 1000;

                int result = select(nFDs, &readFDs, &writeFDs, nullptr, &timeout);
                YT_VERIFY(result >= 0);

                ares_process(Owner_->Channel_, &readFDs, &writeFDs);

                if (FD_ISSET(wakeupFD, &readFDs)) {
                    drain = true;
                }
            #endif

                if (drain && drainQueue()) {
                    Owner_->WakeupHandle_.Clear();
                    while (!drainQueue());
                }
            }

            // Make sure that there are no more enqueued requests.
            // We have observed `IsStopping() == true` previously,
            // which implies that producers no longer pushing to the queue.
            while (!drainQueue());
            // Cancel out all pending requests.
            ares_cancel(Owner_->Channel_);
        }
    };

    using TResolverThreadPtr = TIntrusivePtr<TResolverThread>;

    const TResolverThreadPtr ResolverThread_;


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
                YT_LOG_WARNING("File descriptor is out of valid range (FD: %v, Limit: %v)",
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

    void OnNamedResolved(
        std::unique_ptr<TResolveRequest> request,
        int status,
        int timeouts,
        struct hostent* hostent)
    {
        TDelayedExecutor::CancelAndClear(request->TimeoutCookie);

        auto elapsed = request->Timer.GetElapsedTime();
        RequestTimeGauge_.Update(elapsed);

        if (elapsed > Config_->WarningTimeout || timeouts > 0) {
            YT_LOG_WARNING("Ares DNS resolve took too long (RequestId: %v, HostName: %v, Timeouts: %v, Elapsed: %v)",
                request->RequestId,
                request->HostName,
                timeouts,
                elapsed);
        }

        if (status != ARES_SUCCESS) {
            YT_LOG_WARNING("Ares DNS resolve failed (RequestId: %v, HostName: %v)",
                request->RequestId,
                request->HostName);
            request->Promise.TrySet(TError("Ares DNS resolve failed for %Qv",
                request->HostName)
                << TErrorAttribute("enable_ipv4", request->Options.EnableIPv4)
                << TErrorAttribute("enable_ipv6", request->Options.EnableIPv6)
                << TError(ares_strerror(status)));
            FailureCounter_.Increment();
            return;
        }

        YT_VERIFY(hostent->h_addrtype == AF_INET || hostent->h_addrtype == AF_INET6);
        YT_VERIFY(hostent->h_addr_list && hostent->h_addr_list[0]);

        TNetworkAddress result(hostent->h_addrtype, hostent->h_addr, hostent->h_length);
        YT_LOG_DEBUG("Ares DNS resolve completed (RequestId: %v, HostName: %v, Result: %v, Hostent: %v, Elapsed: %v)",
            request->RequestId,
            request->HostName,
            result,
            hostent,
            elapsed);

        request->Promise.TrySet(result);
    }
};

////////////////////////////////////////////////////////////////////////////////

IDnsResolverPtr CreateAresDnsResolver(TAresDnsResolverConfigPtr config)
{
    return New<TAresDnsResolver>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDns
