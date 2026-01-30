#include "yql_yt_file_upload_impl.h"
#include <util/thread/pool.h>
#include <yql/essentials/utils/log/context.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_client.h>
#include <yt/yql/providers/yt/gateway/lib/yt_helpers.h>

namespace NYql::NFmr {

namespace {

struct TYtFileUploadServiceState {
    NYT::IClientPtr Client;
    const TString RemotePath;
    const TDuration ExpirationInterval;
};

class TYtFileUploadService: public IFileUploadService {
public:
    TYtFileUploadService(const TYtFileUploadServiceOptions& options)
    {
        TString remotePath = options.RemotePath;
        auto client = CreateClient(TClusterConnection{.YtServerName = options.YtServerName, .Token = options.YtToken});
        YQL_ENSURE(client->Exists(remotePath), "Remote path " << remotePath << " for distributed file cache should already exist on yt cluster " + options.YtServerName);
        FileUploadState_ = std::make_shared<TYtFileUploadServiceState>(client, remotePath, options.ExpirationInterval);
        ThreadPool_ = CreateThreadPool(options.ThreadsNum, 0, TThreadPool::TParams().SetBlocking(true).SetCatching(true));
    }

    ~TYtFileUploadService() {
        StopUploadService_ = true;
        ThreadPool_->Stop();
    }

    NThreading::TFuture<void> UploadObject(const TString& md5Key, const TString& fileName) override {
        auto promise = NThreading::NewPromise();
        auto future = promise.GetFuture();

        auto uploadFileToDistCacheFunc = [promise = std::move(promise), weakState = std::weak_ptr(FileUploadState_), md5Key, fileName] () mutable {
            YQL_LOG_CTX_ROOT_SCOPE("UploadFileToDistCache");
            std::shared_ptr<TYtFileUploadServiceState> state = weakState.lock();
            if (state) {
                try {
                    TString ytRemoteFilePath = state->RemotePath + "/" + md5Key;
                    auto client = state->Client;
                    auto snapshotTx = client->StartTransaction();
                    auto expirationInterval = state->ExpirationInterval;

                    UploadBinarySnapshotToYt(ytRemoteFilePath, client, snapshotTx, fileName, expirationInterval);
                    promise.SetValue();
                } catch (...) {
                    promise.SetException(CurrentExceptionMessage());
                }
            }
        };
        ThreadPool_->SafeAddFunc(uploadFileToDistCacheFunc);
        return future;
    }

private:
    THolder<IThreadPool> ThreadPool_;
    std::atomic<bool> StopUploadService_ = false;
    std::shared_ptr<TYtFileUploadServiceState> FileUploadState_;
};

} // namespace

IFileUploadService::TPtr MakeYtFileUploadService(const TYtFileUploadServiceOptions& options) {
    return MakeIntrusive<TYtFileUploadService>(options);
}

} // namespace NYql::NFmr
