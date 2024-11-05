#include "dnsresolver.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/threading/queue/mpsc_htswap.h>
#include <util/network/pair.h>
#include <util/network/socket.h>
#include <util/string/builder.h>
#include <util/system/thread.h>

#include <ares.h>

#include <atomic>

namespace NActors {
namespace NDnsResolver {

    class TAresLibraryInitBase {
    protected:
        TAresLibraryInitBase() noexcept {
            int status = ares_library_init(ARES_LIB_INIT_ALL);
            Y_ABORT_UNLESS(status == ARES_SUCCESS, "Unexpected failure to initialize c-ares library");
        }

        ~TAresLibraryInitBase() noexcept {
            ares_library_cleanup();
        }
    };

    class TCallbackQueueBase {
    protected:
        TCallbackQueueBase() noexcept {
            int err = SocketPair(Sockets, false, true);
            Y_ABORT_UNLESS(err == 0, "Unexpected failure to create a socket pair");
            SetNonBlock(Sockets[0]);
            SetNonBlock(Sockets[1]);
        }

        ~TCallbackQueueBase() noexcept {
            closesocket(Sockets[0]);
            closesocket(Sockets[1]);
        }

    protected:
        using TCallback = std::function<void()>;
        using TCallbackQueue = NThreading::THTSwapQueue<TCallback>;

        void PushCallback(TCallback callback) {
            Y_ABORT_UNLESS(callback, "Cannot push an empty callback");
            CallbackQueue.Push(std::move(callback)); // this is a lockfree queue

            // Wake up worker thread on the first activation
            if (Activations.fetch_add(1, std::memory_order_acq_rel) == 0) {
                char ch = 'x';
                ssize_t ret;
#ifdef _win_
                ret = send(SignalSock(), &ch, 1, 0);
                if (ret == -1) {
                    Y_ABORT_UNLESS(WSAGetLastError() == WSAEWOULDBLOCK, "Unexpected send error");
                    return;
                }
#else
                do {
                    ret = send(SignalSock(), &ch, 1, 0);
                } while (ret == -1 && errno == EINTR);
                if (ret == -1) {
                    Y_ABORT_UNLESS(errno == EAGAIN || errno == EWOULDBLOCK, "Unexpected send error");
                    return;
                }
#endif
                Y_ABORT_UNLESS(ret == 1, "Unexpected send result");
            }
        }

        void RunCallbacks() noexcept {
            char ch[32];
            ssize_t ret;
            bool signalled = false;
            for (;;) {
                ret = recv(WaitSock(), ch, sizeof(ch), 0);
                if (ret > 0) {
                    signalled = true;
                }
                if (ret == sizeof(ch)) {
                    continue;
                }
                if (ret != -1) {
                    break;
                }
#ifdef _win_
                if (WSAGetLastError() == WSAEWOULDBLOCK) {
                    break;
                }
                Y_ABORT("Unexpected recv error");
#else
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break;
                }
                Y_ABORT_UNLESS(errno == EINTR, "Unexpected recv error");
#endif
            }

            if (signalled) {
                // There's exactly one write to SignalSock while Activations != 0
                // It's impossible to get signalled while Activations == 0
                // We must set Activations = 0 to receive new signals
                size_t count = Activations.exchange(0, std::memory_order_acq_rel);
                Y_ABORT_UNLESS(count != 0);

                // N.B. due to the way HTSwap works we may not be able to pop
                // all callbacks on this activation, however we expect a new
                // delayed activation to happen at a later time.
                while (auto callback = CallbackQueue.Pop()) {
                    callback();
                }
            }
        }

        SOCKET SignalSock() {
            return Sockets[0];
        }

        SOCKET WaitSock() {
            return Sockets[1];
        }

    private:
        SOCKET Sockets[2];
        TCallbackQueue CallbackQueue;
        std::atomic<size_t> Activations{ 0 };
    };

    class TSimpleDnsResolver
        : public TActor<TSimpleDnsResolver>
        , private TAresLibraryInitBase
        , private TCallbackQueueBase
    {
    public:
        TSimpleDnsResolver(TSimpleDnsResolverOptions options) noexcept
            : TActor(&TThis::StateWork)
            , Options(std::move(options))
            , WorkerThread(&TThis::WorkerThreadStart, this)
        {
            InitAres();

            WorkerThread.Start();
        }

        ~TSimpleDnsResolver() noexcept override {
            if (!Stopped) {
                PushCallback([this] {
                    // Mark as stopped first
                    Stopped = true;

                    // Cancel all current ares requests (will not send replies)
                    ares_cancel(AresChannel);
                });

                WorkerThread.Join();
            }

            StopAres();
        }

        static constexpr EActivityType ActorActivityType() {
            return EActivityType::DNS_RESOLVER;
        }

    private:
        void InitAres() noexcept {
            struct ares_options options;
            memset(&options, 0, sizeof(options));
            int optmask = 0;

            if (Options.ForceTcp) {
                options.flags |= ARES_FLAG_USEVC;
            }
            if (Options.KeepSocket) {
                options.flags |= ARES_FLAG_STAYOPEN;
            }

            optmask |= ARES_OPT_FLAGS;

            options.sock_state_cb = &TThis::SockStateCallback;
            options.sock_state_cb_data = this;
            optmask |= ARES_OPT_SOCK_STATE_CB;

            options.timeout = Options.Timeout.MilliSeconds();
            if (options.timeout > 0) {
                optmask |= ARES_OPT_TIMEOUTMS;
            }

            options.tries = Options.Attempts;
            if (options.tries > 0) {
                optmask |= ARES_OPT_TRIES;
            }

            int err = ares_init_options(&AresChannel, &options, optmask);
            Y_ABORT_UNLESS(err == 0, "Unexpected failure to initialize c-ares channel");

            if (Options.Servers) {
                TStringBuilder csv;
                for (const TString& server : Options.Servers) {
                    if (csv) {
                        csv << ',';
                    }
                    csv << server;
                }
                err = ares_set_servers_ports_csv(AresChannel, csv.c_str());
                Y_ABORT_UNLESS(err == 0, "Unexpected failure to set a list of dns servers: %s", ares_strerror(err));
            }
        }

        void StopAres() noexcept {
            // Destroy the ares channel
            ares_destroy(AresChannel);
            AresChannel = nullptr;
        }

    private:
        STRICT_STFUNC(StateWork, {
            hFunc(TEvents::TEvPoison, Handle);
            hFunc(TEvDns::TEvGetHostByName, Handle);
            hFunc(TEvDns::TEvGetAddr, Handle);
        })

        void Handle(TEvents::TEvPoison::TPtr&) {
            Y_ABORT_UNLESS(!Stopped);

            PushCallback([this] {
                // Cancel all current ares requests (will send notifications)
                ares_cancel(AresChannel);

                // Mark as stopped last
                Stopped = true;
            });

            WorkerThread.Join();
            PassAway();
        }

    private:
        enum class ERequestType {
            GetHostByName,
            GetAddr,
        };

        struct TRequestContext : public TThrRefBase {
            using TPtr = TIntrusivePtr<TRequestContext>;

            TThis* Self;
            TActorSystem* ActorSystem;
            TActorId SelfId;
            TActorId Sender;
            ui64 Cookie;
            ERequestType Type;

            TRequestContext(TThis* self, TActorSystem* as, TActorId selfId, TActorId sender, ui64 cookie, ERequestType type)
                : Self(self)
                , ActorSystem(as)
                , SelfId(selfId)
                , Sender(sender)
                , Cookie(cookie)
                , Type(type)
            { }
        };

    private:
        void Handle(TEvDns::TEvGetHostByName::TPtr& ev) {
            auto* msg = ev->Get();
            auto reqCtx = MakeIntrusive<TRequestContext>(
                this, TActivationContext::ActorSystem(), SelfId(), ev->Sender, ev->Cookie, ERequestType::GetHostByName);
            PushCallback([this, reqCtx = std::move(reqCtx), name = std::move(msg->Name), family = msg->Family] () mutable {
                StartGetAddrInfo(std::move(reqCtx), std::move(name), family);
            });
        }

        void Handle(TEvDns::TEvGetAddr::TPtr& ev) {
            auto* msg = ev->Get();
            auto reqCtx = MakeIntrusive<TRequestContext>(
                this, TActivationContext::ActorSystem(), SelfId(), ev->Sender, ev->Cookie, ERequestType::GetAddr);
            PushCallback([this, reqCtx = std::move(reqCtx), name = std::move(msg->Name), family = msg->Family] () mutable {
                StartGetAddrInfo(std::move(reqCtx), std::move(name), family);
            });
        }

        void StartGetAddrInfo(TRequestContext::TPtr reqCtx, TString name, int family) noexcept {
            reqCtx->Ref();
            ares_addrinfo_hints hints;
            memset(&hints, 0, sizeof(hints));
            hints.ai_flags = ARES_AI_NOSORT;
            hints.ai_family = family;
            ares_getaddrinfo(AresChannel, name.c_str(), nullptr, &hints, &TThis::GetAddrInfoAresCallback, reqCtx.Get());
        }

    private:
        static void GetAddrInfoAresCallback(void* arg, int status, int timeouts, ares_addrinfo *result) {
            struct TDeleter {
                void operator ()(ares_addrinfo *ptr) const {
                    ares_freeaddrinfo(ptr);
                }
            };
            std::unique_ptr<ares_addrinfo, TDeleter> ptr(result);

            Y_UNUSED(timeouts);
            TRequestContext::TPtr reqCtx(static_cast<TRequestContext*>(arg));
            reqCtx->UnRef();

            if (reqCtx->Self->Stopped) {
                // Don't send any replies after destruction
                return;
            }

            switch (reqCtx->Type) {
                case ERequestType::GetHostByName: {
                    auto result = MakeHolder<TEvDns::TEvGetHostByNameResult>();
                    if (status == ARES_SUCCESS) {
                        for (auto *node = ptr->nodes; node; node = node->ai_next) {
                            switch (node->ai_family) {
                                case AF_INET: {
                                    result->AddrsV4.emplace_back(((sockaddr_in*)node->ai_addr)->sin_addr);
                                    break;
                                }
                                case AF_INET6: {
                                    result->AddrsV6.emplace_back(((sockaddr_in6*)node->ai_addr)->sin6_addr);
                                    break;
                                }
                                default:
                                    Y_ABORT("unknown address family in ares callback");
                            }
                        }
                    } else {
                        result->ErrorText = ares_strerror(status);
                    }
                    result->Status = status;

                    reqCtx->ActorSystem->Send(new IEventHandle(reqCtx->Sender, reqCtx->SelfId, result.Release(), 0, reqCtx->Cookie));
                    break;
                }

                case ERequestType::GetAddr: {
                    auto result = MakeHolder<TEvDns::TEvGetAddrResult>();
                    if (status == ARES_SUCCESS && Y_UNLIKELY(ptr->nodes == nullptr)) {
                        status = ARES_ENODATA;
                    }
                    if (status == ARES_SUCCESS) {
                        auto *node = ptr->nodes;
                        switch (node->ai_family) {
                            case AF_INET: {
                                result->Addr = ((sockaddr_in*)node->ai_addr)->sin_addr;
                                break;
                            }
                            case AF_INET6: {
                                result->Addr = ((sockaddr_in6*)node->ai_addr)->sin6_addr;
                                break;
                            }
                            default:
                                Y_ABORT("unknown address family in ares callback");
                        }
                    } else {
                        result->ErrorText = ares_strerror(status);
                    }
                    result->Status = status;

                    reqCtx->ActorSystem->Send(new IEventHandle(reqCtx->Sender, reqCtx->SelfId, result.Release(), 0, reqCtx->Cookie));
                    break;
                }
            }
        }

    private:
        static void SockStateCallback(void* data, ares_socket_t socket_fd, int readable, int writable) {
            static_cast<TThis*>(data)->DoSockStateCallback(socket_fd, readable, writable);
        }

        void DoSockStateCallback(ares_socket_t socket_fd, int readable, int writable) noexcept {
            int events = (readable ? (POLLRDNORM | POLLIN) : 0) | (writable ? (POLLWRNORM | POLLOUT) : 0);
            if (events == 0) {
                AresSockStates.erase(socket_fd);
            } else {
                AresSockStates[socket_fd].NeededEvents = events;
            }
        }

    private:
        static void* WorkerThreadStart(void* arg) noexcept {
            static_cast<TSimpleDnsResolver*>(arg)->WorkerThreadLoop();
            return nullptr;
        }

        void WorkerThreadLoop() noexcept {
            TThread::SetCurrentThreadName("DnsResolver");

            TVector<struct pollfd> fds;
            while (!Stopped) {
                fds.clear();
                fds.reserve(1 + AresSockStates.size());
                {
                    auto& entry = fds.emplace_back();
                    entry.fd = WaitSock();
                    entry.events = POLLRDNORM | POLLIN;
                }
                for (auto& kv : AresSockStates) {
                    auto& entry = fds.emplace_back();
                    entry.fd = kv.first;
                    entry.events = kv.second.NeededEvents;
                }

                int timeout = -1;
                struct timeval tv;
                if (ares_timeout(AresChannel, nullptr, &tv)) {
                    timeout = tv.tv_sec * 1000 + tv.tv_usec / 1000;
                }

                int ret = poll(fds.data(), fds.size(), timeout);
                if (ret == -1) {
                    if (errno == EINTR) {
                        continue;
                    }
                    // we cannot handle failures, run callbacks and pretend everything is ok
                    RunCallbacks();
                    if (Stopped) {
                        break;
                    }
                    ret = 0;
                }

                bool ares_called = false;
                if (ret > 0) {
                    for (size_t i = 0; i < fds.size(); ++i) {
                        auto& entry = fds[i];

                        // Handle WaitSock activation and run callbacks
                        if (i == 0) {
                            if (entry.revents & (POLLRDNORM | POLLIN)) {
                                RunCallbacks();
                                if (Stopped) {
                                    break;
                                }
                            }
                            continue;
                        }

                        // All other sockets belong to ares
                        if (entry.revents == 0) {
                            continue;
                        }
                        // Previous invocation of aress_process_fd might have removed some sockets
                        if (Y_UNLIKELY(!AresSockStates.contains(entry.fd))) {
                            continue;
                        }
                        ares_process_fd(
                                AresChannel,
                                entry.revents & (POLLRDNORM | POLLIN) ? entry.fd : ARES_SOCKET_BAD,
                                entry.revents & (POLLWRNORM | POLLOUT) ? entry.fd : ARES_SOCKET_BAD);
                        ares_called = true;
                    }

                    if (Stopped) {
                        break;
                    }
                }

                if (!ares_called) {
                    // Let ares handle timeouts
                    ares_process_fd(AresChannel, ARES_SOCKET_BAD, ARES_SOCKET_BAD);
                }
            }
        }

    private:
        struct TSockState {
            short NeededEvents = 0; // poll events
        };

    private:
        TSimpleDnsResolverOptions Options;
        TThread WorkerThread;

        ares_channel AresChannel;
        THashMap<SOCKET, TSockState> AresSockStates;

        bool Stopped = false;
    };

    IActor* CreateSimpleDnsResolver(TSimpleDnsResolverOptions options) {
        return new TSimpleDnsResolver(std::move(options));
    }

} // namespace NDnsResolver
} // namespace NActors
