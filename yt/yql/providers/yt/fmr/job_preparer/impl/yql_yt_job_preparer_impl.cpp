#include <library/cpp/random_provider/random_provider.h>
#include <util/system/fs.h>
#include <util/stream/file.h>
#include <yql/essentials/core/file_storage/proto/file_storage.pb.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>

#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_reader.h>
#include <yt/yql/providers/yt/fmr/job_preparer/impl/yql_yt_job_preparer_impl.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/table_data_service/client/impl/yql_yt_table_data_service_client_impl.h>
#include <yt/yql/providers/yt/fmr/table_data_service/discovery/file/yql_yt_file_service_discovery.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/impl/yql_yt_job_service_impl.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_client.h>

namespace NYql::NFmr {

namespace {

class TFmrJobPreparer: public IFmrJobPreparer {
public:
    TFmrJobPreparer(
        TFileStoragePtr fileStorage,
        const TString& tableDataServiceDiscoveryFilePath,
        const TFmrJobPreparerSettings& settings,
        IFmrTvmClient::TPtr tvmClient,
        TTvmId destinationTvmId
    )
        : FileStorage_(fileStorage)
        , NumThreads_(settings.NumThreads)
    {
        YQL_ENSURE(FileStorage_ && FileStorage_->GetConfig().GetThreads() != 1, " File storage in job preparer should be asynchonous");
        ThreadPool_ = CreateThreadPool(NumThreads_, 0, TThreadPool::TParams().SetBlocking(true).SetCatching(true));
        YtJobService_ = MakeYtJobSerivce();
        auto tableDataServiceDiscovery = MakeFileTableDataServiceDiscovery({.Path = tableDataServiceDiscoveryFilePath});
        TableDataService_ = MakeTableDataServiceClient(tableDataServiceDiscovery, tvmClient, destinationTvmId);
    }

    ~TFmrJobPreparer() {
        ThreadPool_->Stop();
    }

    TString GenerateJobEnvironmentDir(const TString& taskId) override {
        auto tempDir = FileStorage_->GetTemp() / taskId;
        NFs::MakeDirectory(tempDir);
        YQL_CLOG(TRACE, FastMapReduce) << " Setting job environment directory " << tempDir << " For task with id " << taskId;
        return tempDir;
    }

    void InitalizeDistributedCache(const TString& distributedCacheUrl, const TString& distributedCacheToken = TString()) override {
        DistributedCacheUrl_ = distributedCacheUrl;
        DistributedCacheToken_ = distributedCacheToken;
    }

    NThreading::TFuture<TFileLinkPtr> DownloadFileFromDistributedCache(const TString& md5Key) override {
        YQL_ENSURE(!DistributedCacheUrl_.empty(), " Distributed cache url is not set");
        TString remoteFileUrl = DistributedCacheUrl_ + "/" + md5Key;
        YQL_CLOG(DEBUG, FastMapReduce) << "Downloading file with key " << md5Key << " from distributed cache " << DistributedCacheUrl_;
        return FileStorage_->PutUrlAsync(remoteFileUrl, DistributedCacheToken_);
    }

    NThreading::TFuture<TFileLinkPtr> DownloadYtResource(const NYT::TRichYPath& path, const TString& ytServerName, const TString& token) override {
        YQL_CLOG(DEBUG, FastMapReduce) << "Downloading remote yt resource from cypress path " << SerializeRichPath(path) << " and cluster " << path.Cluster_;

        TYtTableRef remoteYtTable(path); // Remote yt table, needed to download as file, for example for mapJoin
        YQL_ENSURE(path.FileName_.Defined());

        TClusterConnection clusterConnection{.YtServerName = ytServerName};
        if (!token.empty()) {
            clusterConnection.Token = token;
        }
        if (path.TransactionId_.Defined()) {
            clusterConnection.TransactionId = GetGuidAsString(*path.TransactionId_);
        }

        NYT::IClientBasePtr clientBase;

        auto client = CreateClient(clusterConnection);
        if (clusterConnection.TransactionId) {
            // If tx is passed to richPath (for example for table in MapJoin), attach to it, otherwise use client.
            clientBase = client->AttachTransaction(*path.TransactionId_);
        } else {
            clientBase = client;
        }

        auto ytResourceType = clientBase->Get(path.Path_ + "/@type").AsString();

        YQL_ENSURE(ytResourceType == "table" || ytResourceType == "file", "Remote yt resource should be either table or file");

        if (ytResourceType == "file") {
            TString remoteFileUrl = "yt://" + ytServerName + "?path=" + path.Path_;
            YQL_CLOG(DEBUG, FastMapReduce) << "Downloading yt file with path " << path.Path_ ;
            return FileStorage_->PutUrlAsync(remoteFileUrl, token);
        }

        auto promise = NThreading::NewPromise<TFileLinkPtr>();
        auto future = promise.GetFuture();
        auto downloadRemoteFileFunc = [path, clusterConnection, promise = std::move(promise), this] () mutable {
            try {
                TYtTableRef remoteYtTable(path);
                auto reader = YtJobService_->MakeReader(remoteYtTable, clusterConnection);
                TFileLinkPtr downloadedFile = TransferDataFromReadersToFile({reader});
                promise.SetValue(std::move(downloadedFile));
            } catch (...) {
                promise.SetException(CurrentExceptionMessage());
            }
        };
        ThreadPool_->SafeAddFunc(downloadRemoteFileFunc);
        return future;
    }

    NThreading::TFuture<TFileLinkPtr> DownloadFmrResource(const TFmrResourceTaskInfo& fmrResource) override {
        auto promise = NThreading::NewPromise<TFileLinkPtr>();
        auto future = promise.GetFuture();

        auto downloadFmrResourceFunc = [this, promise = std::move(promise), fmrResource] () mutable {
            try {
                std::vector<NYT::TRawTableReaderPtr> fmrReaders; // readers for all existing parts of table with fixed fmr tableId.
                for (auto& fmrTablePart: fmrResource.FmrResourceTasks) {
                    fmrReaders.emplace_back(MakeIntrusive<TFmrTableDataServiceReader>(fmrTablePart.TableId, fmrTablePart.TableRanges, TableDataService_, fmrTablePart.Columns, fmrTablePart.SerializedColumnGroups));
                }

                TFileLinkPtr downloadedFile = TransferDataFromReadersToFile(fmrReaders);
                promise.SetValue(std::move(downloadedFile));
            } catch (...) {
                promise.SetException(CurrentExceptionMessage());
            }
        };
        ThreadPool_->SafeAddFunc(downloadFmrResourceFunc);
        return future;
    }

private:
    TString DistributedCacheUrl_;
    TString DistributedCacheToken_;
    TFileStoragePtr FileStorage_;

    THolder<IThreadPool> ThreadPool_; // Thread pool for downloading yt and fmr tables to files.
    const ui64 NumThreads_;
    IYtJobService::TPtr YtJobService_;
    ITableDataService::TPtr TableDataService_;

    TFileLinkPtr TransferDataFromReadersToFile(const std::vector<NYT::TRawTableReaderPtr>& readers) {
        TTempFileHandle file;
        TFileOutput fileOutput(file.Name());
        for (auto& reader: readers) {
            TransferData(reader.Get(), &fileOutput);
        }

        // move resulting file with table content to file storage
        return FileStorage_->PutFile(file.Name());
    }
};

} // namespace

IFmrJobPreparer::TPtr MakeFmrJobPreparer(
    TFileStoragePtr fileStorage,
    const TString& tableDataServiceDiscoveryFilePath,
    const TFmrJobPreparerSettings& settings,
    IFmrTvmClient::TPtr tvmClient,
    TTvmId destinationTvmId
) {
    return MakeIntrusive<TFmrJobPreparer>(fileStorage, tableDataServiceDiscoveryFilePath, settings, tvmClient, destinationTvmId);
}

} // namespace NYql::NFmr
