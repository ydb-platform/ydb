#pragma once

#include <library/cpp/tvmauth/client/logger.h>

#include <util/generic/vector.h>
#include <util/thread/lfqueue.h>

namespace NTvmAuthPy {
    class TPyLogger: public NTvmAuth::ILogger {
    public:
        using TMessage = std::pair<int, TString>;
        using TPyLoggerPtr = TIntrusivePtr<TPyLogger>;

        static TPyLoggerPtr Create() {
            return MakeIntrusive<TPyLogger>();
        }

        void Log(int lvl, const TString& msg) override {
            queue_.Enqueue(TMessage{lvl, msg});
        }

        static TVector<TMessage> FetchMessages(TPyLoggerPtr ptr) {
            TVector<TMessage> res;
            ptr->queue_.DequeueAll(&res);
            return res;
        }

    private:
        TLockFreeQueue<TMessage> queue_;
    };
}
