
#include "yt_wrapper.h"

#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/utils/log/log.h>

#include <ydb/library/yql/providers/dq/global_worker_manager/coordination_helper.h>
#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>
#include <ydb/library/yql/providers/dq/actors/yt/resource_manager.h>
#include <ydb/library/yql/providers/dq/actors/events/events.h>

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/system/fs.h>

#include <yt/cpp/mapreduce/interface/fluent.h>

namespace NYql {

using namespace NActors;

class TYtResourceDownloader: public TRichActor<TYtResourceDownloader> {
public:
    static constexpr char ActorName[] = "DOWNLOADER";

    TYtResourceDownloader(
        const TResourceManagerOptions& options,
        const ICoordinationHelper::TPtr& coordinator)
        : TRichActor<TYtResourceDownloader>(&TYtResourceDownloader::Handler)
        , Options(options)
        , Coordinator(coordinator)
    {
    }

private:

    void DoPassAway() override {
        auto ev = MakeHolder<TEvDownloadComplete>();
        for (auto it = Options.Files.begin() + CurrentFileId; it != Options.Files.end(); ++it) {
            ev->FailedObjectIds.push_back(it->LocalFileName);
        }
        Send(ParentId, ev.Release());
    }

    STRICT_STFUNC(Handler, {
        HFunc(TEvReadFileResponse, OnFileDownloaded)
        CFunc(TEvents::TEvBootstrap::EventType, Bootstrap)
        cFunc(TEvTick::EventType, DownloadFile)
    })

    TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parentId) override {
        ParentId = parentId;
        return new IEventHandle(self, parentId, new TEvents::TEvBootstrap, 0);
    }

    void Bootstrap(const NActors::TActorContext& ctx) {
        YtWrapper = Coordinator->GetWrapper(
            ctx.ActorSystem(),
            Options.YtBackend.GetClusterName(),
            Options.YtBackend.GetUser(),
            Options.YtBackend.GetToken());
        DownloadFile();
    }

    void DownloadFile() {
        TString remotePath = Options.UploadPrefix + "/";
        NYT::NApi::TFileReaderOptions options;

        auto& file = Options.Files[CurrentFileId];
        remotePath += file.GetRemoteFileName(); /* md5 */
        TString localFileName = Options.TmpDir + "/" + file.GetRemoteFileName();

        YQL_CLOG(DEBUG, ProviderDq) << "Downloading file " << CurrentFileId+1 << "/" << Options.Files.size() << " " << remotePath << "->" << localFileName;
        auto message = MakeHolder<TEvReadFile>(NYT::NYPath::TYPath(remotePath), localFileName, options);
        Send(YtWrapper, message.Release());
    }

    void Tick(const NActors::TActorContext& ctx) {
        ctx.Schedule(TDuration::Seconds(5), new TEvTick());
    }

    void SaveToCache() {
        auto& file = Options.Files[CurrentFileId];
        Options.FileCache->AddFile(
            Options.TmpDir + "/" + file.GetRemoteFileName(), /* /tmp/md5 */
            file.LocalFileName  /* md5 */);
    }

    void OnFileDownloaded(TEvReadFileResponse::TPtr& ev, const NActors::TActorContext& ctx) {
        auto result = std::get<0>(*ev->Get());
        if (result.IsOK()) {
            SaveToCache();

            Retry = 0;
            CurrentFileId++;
            YQL_CLOG(DEBUG, ProviderDq) << "File downloaded " << CurrentFileId << "/" << Options.Files.size() << " " << Options.Files[CurrentFileId-1].GetRemoteFileName();
            if (CurrentFileId == Options.Files.size()) {
                // save to cache
                YQL_CLOG(DEBUG, ProviderDq) << "Download complete";
                PassAway();
            } else {
                DownloadFile();
            }
        } else if (Options.MaxRetries == -1 || ++Retry < Options.MaxRetries) {
            YQL_CLOG(DEBUG, ProviderDq) << "Retry " << ToString(result);
            std::random_shuffle(Options.Files.begin() + CurrentFileId, Options.Files.end());
            Tick(ctx);
        } else {
            PassAway();
        }
    }

    TResourceManagerOptions Options;
    const ICoordinationHelper::TPtr Coordinator;

    TActorId YtWrapper;

    ui32 CurrentFileId = 0;
    int Retry = 0;
    TActorId ParentId;
};

IActor* CreateYtResourceDownloader(
    const TResourceManagerOptions& options,
    const ICoordinationHelper::TPtr& coordinator)
{
    Y_ABORT_UNLESS(!options.YtBackend.GetClusterName().empty());
    Y_ABORT_UNLESS(!options.YtBackend.GetUser().empty());
    Y_ABORT_UNLESS(!options.Files.empty());
    Y_ABORT_UNLESS(!options.UploadPrefix.empty());
    Y_ABORT_UNLESS(!options.TmpDir.empty());
    Y_ABORT_UNLESS(options.FileCache);

    NFs::MakeDirectoryRecursive(options.TmpDir, NFs::FP_NONSECRET_FILE, false);

    return new TYtResourceDownloader(options, coordinator);
}

} // namespace NYql
