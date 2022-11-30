#include "client.h"
#include "request.h"

#include <library/cpp/coroutine/dns/cache.h>
#include <library/cpp/coroutine/dns/coro.h>
#include <library/cpp/coroutine/dns/helpers.h>
#include <library/cpp/coroutine/engine/condvar.h>
#include <library/cpp/coroutine/engine/impl.h>
#include <library/cpp/coroutine/engine/network.h>
#include <library/cpp/coroutine/util/pipeque.h>

#include <library/cpp/http/client/ssl/sslsock.h>
#include <library/cpp/http/client/fetch/coctx.h>
#include <library/cpp/http/client/fetch/codes.h>
#include <library/cpp/http/client/fetch/cosocket.h>
#include <library/cpp/http/client/fetch/fetch_single.h>

#include <util/stream/output.h>
#include <util/thread/factory.h>
#include <util/system/event.h>
#include <util/system/spinlock.h>
#include <util/system/thread.h>

namespace NHttp {
    namespace {
        using namespace NHttpFetcher;

        class TFetcher: private IThreadFactory::IThreadAble {
        public:
            TFetcher(const TClientOptions& options)
                : Options_(options)
                , FetchCoroutines(Max<size_t>(Options_.FetchCoroutines, 1))
                , RequestsQueue_(true, false)
                , Done_(false)
            {
            }

            void Start() {
                T_ = SystemThreadFactory()->Run(this);
            }

            void Stop() {
                if (T_) {
                    for (size_t i = 0; i < FetchCoroutines; ++i) {
                        RequestsQueue_.Push(nullptr);
                    }

                    T_->Join();
                    T_.Reset();
                }
            }

            ~TFetcher() override {
                Stop();
            }

            TFetchState FetchAsync(const TFetchRequestRef& req, NHttpFetcher::TCallBack cb) {
                req->SetCallback(cb);
                RequestsQueue_.Push(req);
                return TFetchState(req);
            }

            static TFetcher* Instance() {
                static struct TFetcherHolder {
                    TFetcherHolder() {
                        Fetcher.Start();
                    }

                    TFetcher Fetcher{{}};
                } holder;

                return &holder.Fetcher;
            }

        private:
            void DoDrainLoop(TCont* c) {
                TInstant nextDrain = TInstant::Now() + Options_.KeepAliveTimeout;

                while (true) {
                    DrainMutex_.LockI(c);

                    while (true) {
                        if (Done_) {
                            DrainMutex_.UnLock();
                            // All sockets in the connection pool should be cleared
                            // on some active couroutine.
                            SocketPool_.Clear();
                            return;
                        }
                        if (DrainCond_.WaitD(c, &DrainMutex_, nextDrain) != 0) {
                            // In case of timeout the mutex will be in unlocked state.
                            break;
                        }
                    }

                    SocketPool_.Drain(Options_.KeepAliveTimeout);
                    nextDrain = TInstant::Now() + Options_.KeepAliveTimeout;
                }
            }

            void DoFetchLoop(TCont* c) {
                while (true) {
                    if (NCoro::PollI(c, RequestsQueue_.PopFd(), CONT_POLL_READ) == 0) {
                        TFetchRequestRef req;

                        if (RequestsQueue_.Pop(&req)) {
                            if (!req) {
                                DrainMutex_.LockI(c);
                                const auto wasDone = Done_;
                                Done_ = true;
                                DrainMutex_.UnLock();
                                if (!wasDone) {
                                    DrainCond_.Signal();
                                }
                                break;
                            }

                            if (req->IsCancelled()) {
                                auto result = MakeIntrusive<TResult>(req->GetRequestImpl()->Url, FETCH_CANCELLED);
                                req->OnResponse(result);
                                continue;
                            }

                            try {
                                while (true) {
                                    auto getConnectionPool = [&] () -> TSocketPool* {
                                        if (!Options_.KeepAlive || req->GetForceReconnect()) {
                                            return nullptr;
                                        }
                                        return &SocketPool_;
                                    };

                                    auto sleep = req->OnResponse(
                                        FetchSingleImpl(req->GetRequestImpl(), getConnectionPool()));

                                    if (!req->IsValid()) {
                                        break;
                                    }

                                    if (sleep != TDuration::Zero()) {
                                        c->SleepT(sleep);
                                    }
                                }
                            } catch (...) {
                                req->SetException(std::current_exception());
                            }
                        }
                    }
                }
            }

            void DoExecute() override {
                // Executor must be initialized in the same thread that will use it
                // for fibers to work correctly on windows
                TContExecutor executor(Options_.ExecutorStackSize);

                TThread::SetCurrentThreadName(Options_.Name.c_str());
                NAsyncDns::TOptions dnsOpts;
                dnsOpts.SetMaxRequests(200);
                NAsyncDns::TContResolver resolver(&executor, dnsOpts);

                THolder<NAsyncDns::TContDnsCache> dnsCache;
                if (Options_.DnsCacheLifetime != TDuration::Zero()) {
                    NAsyncDns::TCacheOptions cacheOptions;
                    cacheOptions.SetEntryLifetime(Options_.DnsCacheLifetime);
                    dnsCache = MakeHolder<NAsyncDns::TContDnsCache>(&executor, cacheOptions);
                }

                TCoCtxSetter ctxSetter(&executor, &resolver, dnsCache.Get());

                for (size_t i = 0; i < FetchCoroutines; ++i) {
                    executor.Create<TFetcher, &TFetcher::DoFetchLoop>(this, "fetch_loop");
                }

                if (Options_.KeepAlive) {
                    executor.Create<TFetcher, &TFetcher::DoDrainLoop>(this, "drain_loop");
                }

                executor.Execute();
                executor.Abort();
            }

        private:
            using IThreadRef = THolder<IThreadFactory::IThread>;

            const TClientOptions Options_;
            const size_t FetchCoroutines;

            TContCondVar DrainCond_;
            TContMutex DrainMutex_;
            TSocketPool SocketPool_;

            /// Queue of incoming requests.
            TPipeQueue<TFetchRequestRef> RequestsQueue_;

            bool Done_;
            IThreadRef T_;
        };

    }

    class TFetchClient::TImpl: public TFetcher {
    public:
        inline TImpl(const TClientOptions& options)
            : TFetcher(options)
        {
        }
    };

    TFetchClient::TFetchClient(const TClientOptions& options)
        : Impl_(new TImpl(options))
    {
        Impl_->Start();
    }

    TFetchClient::~TFetchClient() {
        Impl_->Stop();
    }

    TFetchState TFetchClient::Fetch(const TFetchQuery& query, NHttpFetcher::TCallBack cb) {
        return Impl_->FetchAsync(TFetchRequest::FromQuery(query), cb);
    }

    TResultRef Fetch(const TFetchQuery& query) {
        return FetchAsync(query, NHttpFetcher::TCallBack()).Get();
    }

    TFetchState FetchAsync(const TFetchQuery& query, NHttpFetcher::TCallBack cb) {
        return TFetcher::Instance()->FetchAsync(TFetchRequest::FromQuery(query), cb);
    }

}
