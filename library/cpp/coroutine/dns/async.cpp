#include "async.h"

#include <util/generic/singleton.h>
#include <util/generic/vector.h>

#include <contrib/libs/c-ares/include/ares.h>

using namespace NAsyncDns;

namespace {
    struct TAresError: public TDnsError {
        inline TAresError(int code) {
            (*this) << ares_strerror(code);
        }
    };

    struct TAresInit {
        inline TAresInit() {
            const int code = ares_library_init(ARES_LIB_INIT_ALL);

            if (code) {
                ythrow TAresError(code) << "can not init ares engine";
            }
        }

        inline ~TAresInit() {
            ares_library_cleanup();
        }

        static inline void InitOnce() {
            Singleton<TAresInit>();
        }
    };
}

class TAsyncDns::TImpl {
public:
    inline TImpl(IPoller* poller, const TOptions& o)
        : P_(poller)
    {
        TAresInit::InitOnce();

        ares_options opts;

        Zero(opts);

        int optflags = 0;

        optflags |= ARES_OPT_FLAGS;
        opts.flags = ARES_FLAG_STAYOPEN;

        optflags |= ARES_OPT_TIMEOUTMS;
        opts.timeout = o.TimeOut.MilliSeconds();

        optflags |= ARES_OPT_TRIES;
        opts.tries = o.Retries;

        optflags |= ARES_OPT_SOCK_STATE_CB;
        opts.sock_state_cb = (decltype(opts.sock_state_cb))StateCb;
        static_assert(sizeof(opts.sock_state_cb) == sizeof(&StateCb), "Inconsistent socket state size");
        opts.sock_state_cb_data = this;

        const int code = ares_init_options(&H_, &opts, optflags);

        if (code) {
            ythrow TAresError(code) << "can not init ares channel";
        }
    }

    inline ~TImpl() {
        ares_destroy(H_);
    }

    inline TDuration Timeout() {
        struct timeval tv;
        Zero(tv);

        ares_timeout(H_, nullptr, &tv);

        return TDuration(tv);
    }

    template <class T>
    inline void ProcessSocket(T s) {
        ares_process_fd(H_, s, s);
    }

    inline void ProcessNone() {
        ProcessSocket(ARES_SOCKET_BAD);
    }

    inline void AsyncResolve(const TNameRequest& req) {
        ares_gethostbyname(H_, req.Addr, req.Family, AsyncResolveHostCb, req.CB);
    }

private:
    static void StateCb(void* arg, int s, int read, int write) {
        ((TImpl*)arg)->P_->OnStateChange((SOCKET)s, (bool)read, (bool)write);
    }

    static void AsyncResolveHostCb(void* arg, int status, int timeouts, hostent* he) {
        const IHostResult::TResult res = {
            status, timeouts, he};

        ((IHostResult*)arg)->OnComplete(res);
    }

private:
    IPoller* P_;
    ares_channel H_;
};

void NAsyncDns::CheckAsyncStatus(int status) {
    if (status) {
        ythrow TAresError(status);
    }
}

void NAsyncDns::CheckPartialAsyncStatus(int status) {
    if (status == ARES_ENODATA) {
        return;
    }

    CheckAsyncStatus(status);
}

TAsyncDns::TAsyncDns(IPoller* poller, const TOptions& opts)
    : I_(new TImpl(poller, opts))
{
}

TAsyncDns::~TAsyncDns() {
}

void TAsyncDns::AsyncResolve(const TNameRequest& req) {
    I_->AsyncResolve(req);
}

TDuration TAsyncDns::Timeout() {
    return I_->Timeout();
}

void TAsyncDns::ProcessSocket(SOCKET s) {
    I_->ProcessSocket(s);
}

void TAsyncDns::ProcessNone() {
    I_->ProcessNone();
}
