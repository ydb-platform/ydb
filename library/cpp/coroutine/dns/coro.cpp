#include "coro.h"

#include "async.h"

#include <library/cpp/coroutine/engine/events.h>
#include <library/cpp/coroutine/engine/impl.h>

#include <util/generic/vector.h>
#include <util/memory/smallobj.h>

using namespace NAsyncDns;

class TContResolver::TImpl: public IPoller {
    struct TPollEvent:
        public NCoro::IPollEvent,
        public TIntrusiveListItem<TPollEvent>,
        public TObjectFromPool<TPollEvent>
    {
        inline TPollEvent(TImpl* parent, SOCKET s, int what)
            : IPollEvent(s, what)
            , P(parent)
        {
            P->E_->Poller()->Schedule(this);
        }

        inline ~TPollEvent() override {
            P->E_->Poller()->Remove(this);
        }

        void OnPollEvent(int) noexcept override {
            P->D_.ProcessSocket(Fd());
        }

        TImpl* P;
    };

    typedef TAutoPtr<TPollEvent> TEventRef;

    struct TOneShotEvent {
        inline TOneShotEvent(TContExecutor* e) noexcept
            : E(e)
            , S(false)
        {
        }

        inline void Signal() noexcept {
            S = true;
            E.Signal();
        }

        inline void Wait() noexcept {
            if (S) {
                return;
            }

            E.WaitI();
        }

        TContSimpleEvent E;
        bool S;
    };

    struct TContSemaphore {
        inline TContSemaphore(TContExecutor* e, size_t num) noexcept
            : E(e)
            , N(num)
        {
        }

        inline void Acquire() noexcept {
            while (!N) {
                E.WaitI();
            }

            --N;
        }

        inline void Release() noexcept {
            ++N;
            E.Signal();
        }

        TContSimpleEvent E;
        size_t N;
    };

    struct TRequest: public TIntrusiveListItem<TRequest>, public TOneShotEvent, public IHostResult {
        inline TRequest(const TNameRequest* req, TContExecutor* e)
            : TOneShotEvent(e)
            , Req(req)
        {
        }

        void OnComplete(const TResult& res) override {
            Req->CB->OnComplete(res);
            Signal();
        }

        const TNameRequest* Req;
    };

public:
    inline TImpl(TContExecutor* e, const TOptions& opts)
        : P_(TDefaultAllocator::Instance())
        , E_(e)
        , RateLimit_(E_, opts.MaxRequests)
        , D_(this, opts)
        , DC_(nullptr)
    {
    }

    inline ~TImpl() {
        Y_VERIFY(DC_ == nullptr, "shit happens");
    }

    inline void Resolve(const TNameRequest& hreq) {
        TGuard<TContSemaphore> g(RateLimit_);

        if (hreq.Family == AF_UNSPEC) {
            const TNameRequest hreq1(hreq.Copy(AF_INET));
            TRequest req1(&hreq1, E_);

            const TNameRequest hreq2(hreq.Copy(AF_INET6));
            TRequest req2(&hreq2, E_);

            Schedule(&req1);
            Schedule(&req2);

            req1.Wait();
            req2.Wait();
        } else {
            TRequest req(&hreq, E_);

            Schedule(&req);

            req.Wait();
        }

        if (A_.Empty() && DC_) {
            DC_->ReSchedule();
        }
    }

private:
    inline void Schedule(TRequest* req) {
        D_.AsyncResolve(req->Req->Copy(req));
        A_.PushBack(req);
        ResolveCont()->ReSchedule();
    }

    inline TCont* ResolveCont() {
        if (!DC_) {
            DC_ = E_->Create<TImpl, &TImpl::RunAsyncDns>(this, "async_dns");
        }

        return DC_;
    }

    void RunAsyncDns(TCont*) {
        while (!A_.Empty()) {
            Y_VERIFY(!I_.Empty(), "shit happens");

            const TDuration tout = D_.Timeout();

            if (E_->Running()->SleepT(Max(tout, TDuration::MilliSeconds(10))) == ETIMEDOUT) {
                D_.ProcessNone();
            }

            DC_->Yield();
        }

        DC_ = nullptr;
    }

    void OnStateChange(SOCKET s, bool read, bool write) noexcept override {
        static const ui16 WHAT[] = {
            0, CONT_POLL_READ, CONT_POLL_WRITE, CONT_POLL_READ | CONT_POLL_WRITE};

        const int what = WHAT[((size_t)read) | (((size_t)write) << 1)];

        TEventRef& ev = S_.Get(s);

        if (!ev) {
            ev.Reset(new (&P_) TPollEvent(this, s, what));
            I_.PushBack(ev.Get());
        } else {
            if (what) {
                if (ev->What() != what) {
                    //may optimize
                    ev.Reset(new (&P_) TPollEvent(this, s, what));
                    I_.PushBack(ev.Get());
                }
            } else {
                ev.Destroy();
                //auto unlink
            }
        }

        if (DC_) {
            DC_->ReSchedule();
        }
    }

private:
    TSocketMap<TEventRef> S_;
    TPollEvent::TPool P_;
    TContExecutor* E_;
    TContSemaphore RateLimit_;
    TAsyncDns D_;
    TIntrusiveList<TPollEvent> I_;
    TIntrusiveList<TRequest> A_;
    TCont* DC_;
};

TContResolver::TContResolver(TContExecutor* e, const TOptions& opts)
    : I_(new TImpl(e, opts))
{
}

TContResolver::~TContResolver() {
}

void TContResolver::Resolve(const TNameRequest& req) {
    I_->Resolve(req);
}
