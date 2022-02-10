#pragma once 
 
#include <library/cpp/tvmauth/client/misc/api/threaded_updater.h> 
 
#include <library/cpp/threading/future/future.h>
 
#include <util/generic/map.h> 
#include <util/thread/lfqueue.h> 
 
#include <optional> 
 
namespace NTvmAuth::NDynamicClient { 
    enum class EDstStatus { 
        Success, 
        Fail, 
    }; 
 
    struct TDstResponse { 
        EDstStatus Status = EDstStatus::Fail; 
        TString Error; 
 
        bool operator==(const TDstResponse& o) const { 
            return Status == o.Status && Error == o.Error; 
        } 
    }; 
 
    using TDsts = NTvmApi::TDstSet; 
    using TAddResponse = TMap<NTvmApi::TClientSettings::TDst, TDstResponse>; 
 
    class TTvmClient: public NTvmApi::TThreadedUpdater { 
    public: 
        static TAsyncUpdaterPtr Create(const NTvmApi::TClientSettings& settings, TLoggerPtr logger); 
        virtual ~TTvmClient();
 
        NThreading::TFuture<TAddResponse> Add(TDsts&& dsts); 
        std::optional<TString> GetOptionalServiceTicketFor(const TTvmId dst); 
 
    protected: // for tests 
        struct TTask { 
            ui64 Id = 0; 
            NThreading::TPromise<TAddResponse> Promise; 
            TDsts Dsts; 
        }; 
 
        using TBase = NTvmApi::TThreadedUpdater; 
 
    protected: // for tests 
        TTvmClient(const NTvmApi::TClientSettings& settings, TLoggerPtr logger); 
 
        void Worker() override; 
        void ProcessTasks(); 
 
        void SetResponseForTask(TTask& task, const TServiceTickets& cache); 
 
    private: 
        std::atomic<ui64> TaskIds_ = {0}; 
        TLockFreeQueue<TTask> TaskQueue_; 
        TVector<TTask> Tasks_; 
    }; 
} 
