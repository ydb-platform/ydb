
#include "yt_wrapper.h"

#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/utils/log/log.h>

#include <ydb/library/yql/providers/dq/global_worker_manager/coordination_helper.h>
#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>
#include <ydb/library/yql/providers/dq/actors/yt/resource_manager.h>
#include <ydb/library/yql/providers/dq/actors/events/events.h>
#include <ydb/library/yql/providers/dq/common/attrs.h>

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/yson/node/node_io.h>

#include <yt/cpp/mapreduce/interface/fluent.h>

namespace NYql {

using namespace NActors;
using namespace NMonitoring;

class TYtResourceUploader: public TRichActor<TYtResourceUploader> {
public:
    static constexpr char ActorName[] = "UPLOADER";

    TYtResourceUploader(
        const TResourceManagerOptions& options,
        const ICoordinationHelper::TPtr& coordinator)
        : TRichActor<TYtResourceUploader>(&TYtResourceUploader::Follower)
        , Options(options)
        , Coordinator(coordinator)
        , Config(Coordinator->GetConfig())
    {
        if (Options.Counters) {
            FileSize = Options.Counters->GetHistogram("FileSize", ExponentialHistogram(10, 4, 10));
            FileUploadTime = Options.Counters->GetHistogram("UploadTime", ExponentialHistogram(10, 3, 1));
            Errors = Options.Counters->GetCounter("Errors");
        }
    }

private:
    // States: Follower -> Upload

    void StartFollower(TEvBecomeFollower::TPtr& ev, const TActorContext& ctx) {
        YQL_LOG_CTX_ROOT_SCOPE(CurrentLockName);
        YQL_CLOG(DEBUG, ProviderDq) << "Current lock '" << CurrentLockName << "'";
        Y_UNUSED(ctx);
        auto leaderAttributes = NYT::NodeFromYsonString(ev->Get()->Attributes).AsMap();

        if (leaderAttributes.contains(NCommonAttrs::ACTOR_NODEID_ATTR)) {
            YQL_CLOG(INFO, ProviderDq) << " Following leader: "
                << leaderAttributes.at(NCommonAttrs::ACTOR_NODEID_ATTR).AsUint64();
        }
        if (leaderAttributes.contains(NCommonAttrs::HOSTNAME_ATTR)) {
            YQL_CLOG(INFO, ProviderDq) << " Leader hostname: "
                << leaderAttributes.at(NCommonAttrs::HOSTNAME_ATTR).AsString();
        }
        Become(&TYtResourceUploader::Follower);
    }

    void StartLeader(TEvBecomeLeader::TPtr& ev, const TActorContext& ctx) {
        YQL_LOG_CTX_ROOT_SCOPE(CurrentLockName);
        YQL_CLOG(DEBUG, ProviderDq) << "Current lock '" << CurrentLockName << "'";
        Y_UNUSED(ctx);
        YQL_CLOG(INFO, ProviderDq) << "Become leader, epoch=" << ev->Get()->LeaderEpoch;
        Become(&TYtResourceUploader::UploadState);
        UploadFile();
    }

    void Follower(STFUNC_SIG) {
        switch (const ui32 etype = ev->GetTypeRewrite()) {
            HFunc(TEvBecomeFollower, StartFollower)
            HFunc(TEvBecomeLeader, StartLeader)

            case TEvents::TEvBootstrap::EventType: {
                Bootstrap(TActivationContext::ActorContextFor(this->SelfId()));
                break;
            }

            default:
                break;
        }
    }

    void DoPassAway() override {
        Send(ParentId, new TEvUploadComplete());

        if (Options.Uploaded) {
            YQL_CLOG(DEBUG, ProviderDq) << "Promise SetValue";
            auto promise = *Options.Uploaded;
            promise.TrySetValue();
        }
    }

    void UploadState(STFUNC_SIG) {
        switch (const ui32 etype = ev->GetTypeRewrite()) {
            HFunc(TEvBecomeFollower, StartFollower)
            HFunc(TEvWriteFileResponse, OnFileUploaded)

            case TEvTick::EventType: {
                // retry
                UploadFile();
                break;
            }
            default:
                break;
        }
    }

    TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parentId) override {
        ParentId = parentId;
        return new IEventHandle(self, parentId, new TEvents::TEvBootstrap, 0);
    }

    TString GetLockName() {
        Y_ABORT_UNLESS(!Options.Files[CurrentFileId].ObjectId.empty());
        return Options.Files[CurrentFileId].ObjectId;
    }

    void Lock() {
        if (LockId) {
            UnregisterChild(LockId);
        }
        CurrentLockName = Options.LockName.empty()
            ? GetLockName()
            : Options.LockName;
        LockId = RegisterChild(Coordinator->CreateLockOnCluster(YtWrapper, Options.YtBackend.GetPrefix(), CurrentLockName));
        Become(&TYtResourceUploader::Follower);
    }

    void Bootstrap(const NActors::TActorContext& ctx) {
        YtWrapper = Coordinator->GetWrapper(
            ctx.ActorSystem(),
            Options.YtBackend.GetClusterName(),
            Options.YtBackend.GetUser(),
            Options.YtBackend.GetToken());
        Lock();
    }

    void UploadFile() {
        TString remotePath = Options.UploadPrefix + "/";
        NYT::NApi::TFileWriterOptions options;
        auto& file = Options.Files[CurrentFileId];
        THashMap<TString, NYT::TNode> attributes;
        for (const auto& [k, v] : file.Attributes) {
            attributes[k] = NYT::TNode(v);
        }
        if (Options.YtBackend.HasUploadReplicationFactor()) {
            attributes["replication_factor"] = NYT::TNode(Options.YtBackend.GetUploadReplicationFactor());
        }
        options.ComputeMD5 = true;

        if (FileSize && file.File.IsOpen()) {
            FileSize->Collect(file.File.GetLength() / 1024 / 1024); // megabytes
        }

        UploadStart = TInstant::Now();

        remotePath += file.GetRemoteFileName();
        auto message = MakeHolder<TEvWriteFile>(file.File, NYT::NYPath::TYPath(remotePath), attributes, options);
        Send(YtWrapper, message.Release());
    }

    void Tick(const NActors::TActorContext& ctx) {
        ctx.Schedule(TDuration::Seconds(5), new TEvTick());
    }

    void OnFileUploaded(TEvWriteFileResponse::TPtr& ev, const NActors::TActorContext& ctx) {
        YQL_LOG_CTX_ROOT_SCOPE(CurrentLockName);
        YQL_CLOG(DEBUG, ProviderDq) << "Current lock '" << CurrentLockName << "'";
        auto result = std::get<0>(*ev->Get());
        if (result.IsOK()) {
            if (FileUploadTime) {
                FileUploadTime->Collect((TInstant::Now() - UploadStart).Seconds());
            }
            CurrentFileId++;
            if (CurrentFileId == Options.Files.size()) {
                YQL_CLOG(DEBUG, ProviderDq) << "Upload complete";
                PassAway();
            } else {
                if (Options.LockName.empty()) {
                    Lock();
                    YQL_CLOG(DEBUG, ProviderDq) << "Lock next " << CurrentFileId << "/" << Options.Files.size();
                } else {
                    UploadFile();
                    YQL_CLOG(DEBUG, ProviderDq) << "Upload next " << CurrentFileId << "/" << Options.Files.size();
                }
            }
        } else {
            YQL_CLOG(DEBUG, ProviderDq) << "Retry " << ToString(result);

            if (Errors) {
                *Errors += 1;
            }

            std::random_shuffle(Options.Files.begin() + CurrentFileId, Options.Files.end());
            Tick(ctx);
        }
    }

    const NYT::NApi::IClientPtr Client;
    TResourceManagerOptions Options;

    const ICoordinationHelper::TPtr Coordinator;

    const NProto::TDqConfig::TYtCoordinator Config;

    TActorId YtWrapper;

    ui32 CurrentFileId = 0;
    TActorId ParentId;
    TActorId LockId;

    THistogramPtr FileSize;
    THistogramPtr FileUploadTime;
    TDynamicCounters::TCounterPtr Errors;
    TInstant UploadStart;
    TString CurrentLockName;
};

IActor* CreateYtResourceUploader(
    const TResourceManagerOptions& options,
    const ICoordinationHelper::TPtr& coordinator)
{
    Y_ABORT_UNLESS(!options.YtBackend.GetClusterName().empty());
    Y_ABORT_UNLESS(!options.YtBackend.GetUser().empty());
    Y_ABORT_UNLESS(options.YtBackend.HasPrefix());
    Y_ABORT_UNLESS(!options.Files.empty());
    Y_ABORT_UNLESS(!options.UploadPrefix.empty());

    return new TYtResourceUploader(options, coordinator);
}

}
