#include "scheduler.h"

namespace NHttp {
    namespace {
        class TDefaultHostsPolicy: public IHostsPolicy {
        public:
            size_t GetMaxHostConnections(const TStringBuf&) const override {
                return 20;
            }
        };

    }

    TScheduler::TScheduler()
        : HostsPolicy_(new TDefaultHostsPolicy)
    {
    }

    TFetchRequestRef TScheduler::Extract() {
        {
            auto g(Guard(Lock_));

            if (!RequestQueue_.empty()) {
                TFetchRequestRef result(RequestQueue_.front());
                RequestQueue_.pop();
                return result;
            }
        }
        return TFetchRequestRef();
    }

    void TScheduler::Schedule(TFetchRequestRef req) {
        auto g(Guard(Lock_));
        RequestQueue_.push(req);
    }

}
