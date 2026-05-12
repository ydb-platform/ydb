#include "yql_yt_file_metadata_impl.h"
#include <queue>
#include <util/thread/pool.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_client.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NFmr {

namespace {

struct TYtFileMetadataServiceState {
    NYT::IClientPtr Client;
    const TString RemotePath;
    const TMaybe<TDuration> ExpirationInterval;
};

class TYtFileMetadataService: public IFileMetadataService {
public:
    TYtFileMetadataService(const TYtFileMetadataServiceOptions& options)
        : RemotePath_(options.RemotePath)
        , MaxExistRequestBatchSize_(options.MaxExistRequestBatchSize)
        , TimeToSleepBetweenExistRequests_(options.TimeToSleepBetweenExistRequests)
    {
        YQL_ENSURE(MaxExistRequestBatchSize_ > 0);
        Client_ = CreateClient(TClusterConnection{.YtServerName = options.YtServerName, .Token = options.YtToken});
        YQL_ENSURE(Client_->Exists(RemotePath_), "Remote path " << RemotePath_ << " for distributed file cache should already exist on yt cluster " + options.YtServerName);
        ThreadPool_ = CreateThreadPool(options.ThreadsNum, 0, TThreadPool::TParams().SetBlocking(true).SetCatching(true));
        ProcessBatchExistRequests();
    }

    ~TYtFileMetadataService() {
        StopMetadataService_ = true;
        ThreadPool_->Stop();
    }

    NThreading::TFuture<bool> GetFileUploadStatus(const TString& md5Key) override {
        TGuard<TMutex> guard(Mutex_);
        auto promise = NThreading::NewPromise<bool>();
        auto future = promise.GetFuture();
        if (ExistRequestBatches_.empty() || ExistRequestBatches_.back().size() == MaxExistRequestBatchSize_) {
            ExistRequestBatches_.emplace(std::vector<TFileExistRequest>{});
        }
        ExistRequestBatches_.back().emplace_back(TFileExistRequest{.Promise = std::move(promise), .Md5Key = md5Key});
        return future;
    }

private:
    struct TFileExistRequest {
        NThreading::TPromise<bool> Promise;
        TString Md5Key;
    };

    void ProcessBatchExistRequests() {
        auto getBatchExistRequestFunc = [&] () {
            while (!StopMetadataService_) {
                std::vector<TFileExistRequest> batchExistRequest;
                with_lock(Mutex_) {
                    if (!ExistRequestBatches_.empty()) {
                        batchExistRequest = ExistRequestBatches_.front();
                        ExistRequestBatches_.pop();
                    }
                }

                if (!batchExistRequest.empty()) {
                    auto batchClient = Client_->CreateBatchRequest();
                    std::vector<NThreading::TFuture<bool>> existRequests;
                    for (auto& req: batchExistRequest) {
                        existRequests.emplace_back(batchClient->Exists(RemotePath_ + "/" + req.Md5Key).Apply(
                            [promise = std::move(req.Promise)] (const auto& future) mutable {
                                try {
                                    bool exists = future.GetValue();
                                    promise.SetValue(exists);
                                } catch (...) {
                                    promise.SetException(CurrentExceptionMessage());
                                }
                                return future;
                            }
                        ));
                    }
                    batchClient->ExecuteBatch();
                }
                Sleep(TimeToSleepBetweenExistRequests_);
            }
        };
        ThreadPool_->SafeAddFunc(getBatchExistRequestFunc);
    }

private:
    TMutex Mutex_;
    THolder<IThreadPool> ThreadPool_;
    NYT::IClientPtr Client_;
    std::atomic<bool> StopMetadataService_ = false;
    const TString RemotePath_;
    const ui64 MaxExistRequestBatchSize_;
    const TDuration TimeToSleepBetweenExistRequests_;
    std::queue<std::vector<TFileExistRequest>> ExistRequestBatches_;
};

} // namespace

TYtFileMetadataService::TPtr MakeYtFileMetadataService(const TYtFileMetadataServiceOptions& options) {
    return MakeIntrusive<TYtFileMetadataService>(options);
}

} // namespace NYql::NFmr
