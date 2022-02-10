#include "threaded_updater.h" 
 
#include <library/cpp/tvmauth/client/exception.h> 
 
#include <util/string/builder.h> 
#include <util/system/spin_wait.h> 
#include <util/system/thread.h>
 
namespace NTvmAuth { 
    TThreadedUpdaterBase::TThreadedUpdaterBase(TDuration workerAwakingPeriod, 
                                               TLoggerPtr logger, 
                                               const TString& url, 
                                               ui16 port,
                                               TDuration socketTimeout,
                                               TDuration connectTimeout)
        : WorkerAwakingPeriod_(workerAwakingPeriod) 
        , Logger_(std::move(logger)) 
        , TvmUrl_(url) 
        , TvmPort_(port) 
        , TvmSocketTimeout_(socketTimeout)
        , TvmConnectTimeout_(connectTimeout)
        , IsStopped_(true) 
    { 
        Y_ENSURE_EX(Logger_, TNonRetriableException() << "Logger is required"); 
 
        ServiceTicketsDurations_.RefreshPeriod = TDuration::Hours(1); 
        ServiceTicketsDurations_.Expiring = TDuration::Hours(2); 
        ServiceTicketsDurations_.Invalid = TDuration::Hours(11); 
 
        PublicKeysDurations_.RefreshPeriod = TDuration::Days(1); 
        PublicKeysDurations_.Expiring = TDuration::Days(2); 
        PublicKeysDurations_.Invalid = TDuration::Days(6); 
    } 
 
    TThreadedUpdaterBase::~TThreadedUpdaterBase() { 
        StopWorker(); 
    } 
 
    void TThreadedUpdaterBase::StartWorker() { 
        if (HttpClient_) { 
            HttpClient_->ResetConnection(); 
        } 
        Thread_ = MakeHolder<TThread>(WorkerWrap, this); 
        Thread_->Start(); 
        Started_.Wait(); 
        IsStopped_ = false; 
    } 
 
    void TThreadedUpdaterBase::StopWorker() { 
        Event_.Signal(); 
        if (Thread_) { 
            Thread_.Reset(); 
        } 
    } 
 
    TKeepAliveHttpClient& TThreadedUpdaterBase::GetClient() const { 
        if (!HttpClient_) { 
            HttpClient_ = MakeHolder<TKeepAliveHttpClient>(TvmUrl_, TvmPort_, TvmSocketTimeout_, TvmConnectTimeout_);
        } 
 
        return *HttpClient_; 
    } 
 
    void TThreadedUpdaterBase::LogDebug(const TString& msg) const { 
        if (Logger_) { 
            Logger_->Debug(msg); 
        } 
    } 
 
    void TThreadedUpdaterBase::LogInfo(const TString& msg) const { 
        if (Logger_) { 
            Logger_->Info(msg); 
        } 
    } 
 
    void TThreadedUpdaterBase::LogWarning(const TString& msg) const { 
        if (Logger_) { 
            Logger_->Warning(msg); 
        } 
    } 
 
    void TThreadedUpdaterBase::LogError(const TString& msg) const { 
        if (Logger_) { 
            Logger_->Error(msg); 
        } 
    } 
 
    void* TThreadedUpdaterBase::WorkerWrap(void* arg) { 
        TThread::SetCurrentThreadName("TicketParserUpd");
        TThreadedUpdaterBase& this_ = *reinterpret_cast<TThreadedUpdaterBase*>(arg); 
        this_.Started_.Signal(); 
        this_.LogDebug("Thread-worker started"); 
 
        while (true) { 
            if (this_.Event_.WaitT(this_.WorkerAwakingPeriod_)) { 
                break; 
            } 
 
            try { 
                this_.Worker(); 
                this_.GetClient().ResetConnection(); 
            } catch (const std::exception& e) { // impossible now 
                this_.LogError(TStringBuilder() << "Failed to generate new cache: " << e.what()); 
            } 
        } 
 
        this_.LogDebug("Thread-worker stopped"); 
        this_.IsStopped_ = true; 
        return nullptr; 
    } 
} 
