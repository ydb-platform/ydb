#pragma once

#include "async_updater.h"
#include "settings.h"

#include <library/cpp/tvmauth/client/logger.h>

#include <library/cpp/http/simple/http_client.h>

#include <util/datetime/base.h>
#include <util/generic/ptr.h>
#include <util/system/event.h>
#include <util/system/thread.h>

class TKeepAliveHttpClient;

namespace NTvmAuth::NInternal {
    class TClientCaningKnife;
}
namespace NTvmAuth {
    class TThreadedUpdaterBase: public TAsyncUpdaterBase {
    public:
        TThreadedUpdaterBase(TDuration workerAwakingPeriod, 
                             TLoggerPtr logger, 
                             const TString& url, 
                             ui16 port, 
                             TDuration socketTimeout, 
                             TDuration connectTimeout); 
        virtual ~TThreadedUpdaterBase();

    protected:
        void StartWorker();
        void StopWorker();

        virtual void Worker() {
        }

        TKeepAliveHttpClient& GetClient() const;

        void LogDebug(const TString& msg) const;
        void LogInfo(const TString& msg) const;
        void LogWarning(const TString& msg) const;
        void LogError(const TString& msg) const;

    protected:
        TDuration WorkerAwakingPeriod_;

        const TLoggerPtr Logger_;

    protected:
        const TString TvmUrl_;

    private:
        static void* WorkerWrap(void* arg);

        void StartTvmClientStopping() const override {
            Event_.Signal();
        }

        bool IsTvmClientStopped() const override {
            return IsStopped_;
        }

    private:
        mutable THolder<TKeepAliveHttpClient> HttpClient_;

        const ui32 TvmPort_;
        const TDuration TvmSocketTimeout_; 
        const TDuration TvmConnectTimeout_; 

        mutable TAutoEvent Event_;
        mutable TAutoEvent Started_;
        std::atomic_bool IsStopped_;
        THolder<TThread> Thread_;
    };
}
