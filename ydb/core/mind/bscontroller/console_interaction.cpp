#include "impl.h"
#include <ydb/library/yaml_config/yaml_config.h>
#include <ydb/core/blobstorage/base/utility.h>

namespace NKikimr::NBsController {

void TBlobStorageController::StartConsoleConnectionService() {
    NTabletPipe::TClientConfig pipeConfig;
    pipeConfig.RetryPolicy = {
        .RetryLimitCount = 10,
    };
    auto pipe = NTabletPipe::CreateClient(IActor::SelfId(), MakeConsoleID(), pipeConfig);
    ConsolePipe = IActor::Register(pipe);
    STLOG(PRI_DEBUG, BS_CONTROLLER, BSC18, "Console connection service started");

    auto ev = MakeHolder<TEvBlobStorage::TEvControllerProposeConfigRequest>();
    auto configHash = std::hash<TString>{}(YamlConfig);
    ev->Record.SetConfigHash(ToString(configHash));
    ev->Record.SetConfigVersion(ConfigVersion);

    NTabletPipe::SendData(IActor::SelfId(), ConsolePipe, ev.Release());
}

void TBlobStorageController::MakeCommitToConsole() {
    IsOngoingCommit = true;
    db.Table<Schema::State>().Key(true).Update<T::IsOngoingCommit>(true);
    auto ev = MakeHolder<TEvBlobStorage::TEvControllerCommitConfigRequest>();
    ev->Record.SetYAML(YamlConfig);
    NTabletPipe::SendData(IActor::SelfId(), ConsolePipe, ev.Release());
}

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerProposeConfigResponse::TPtr ev) {
    STLOG(PRI_DEBUG, BS_CONTROLLER, BSC19, "Console proposed config response", (Response, ev->Get()->Record));
    auto& record = ev->Get()->Record;
    if (record.GetStatus() == NKikimrBlobStorage::TEvControllerProposeConfigResponse::HashMismatch) {
        LOG_ALERT_S(ev->Sender, NKikimrServices::BS_CONTROLLER, "Config hash mismatch.");
        return;
    }
    if (record.GetStatus() == NKikimrBlobStorage::TEvControllerProposeConfigResponse::UnexpectedConfig) {
        // Tx to local base
    }
    if (record.GetStatus() == NKikimrBlobStorage::TEvControllerProposeConfigResponse::CommitIsNotNeeded) {
        return;
    }
    auto ev = MakeHolder<TEvBlobStorage::TEvControllerCommitConfigRequest>();
    ev->Record.SetYAML(YamlConfig);
    NTabletPipe::SendData(IActor::SelfId(), ConsolePipe, ev.Release());
}

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerCommitConfigResponse::TPtr ev) {
    STLOG(PRI_DEBUG, BS_CONTROLLER, BSC20, "Console commit config response", (Response, ev->Get()->Record));
    auto& record = ev->Get()->Record;
    IsOngoingCommit = false;
    db.Table<Schema::State>().Key(true).Update<T::IsOngoingCommit>(false);
    if (record.GetStatus() == NKikimrBlobStorage::TEvControllerCommitConfigResponse::SessionMismatch) {
        return;
    }
    if (record.GetStatus() == NKikimrBlobStorage::TEvControllerCommitConfigResponse::NotCommited) {
        // Tx to local base and backoff
        auto retryCommitConfig = std::make_unique<TEvBlobStorage::TEvControllerRetryCommitConfig>();
        retryCommitConfig->Record.SetConfigVersion(ConfigVersion);
        Schedule(TDuration::MilliSeconds(CommitConfigBackoffTimer.NextBackoffMs()), new IEventHandle(SelfId(), SelfId(), retryCommitConfig.release()));
        return;
    }
    CommitConfigBackoffTimer.Reset();
}

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerRetryCommitConfig::TPtr ev) {
    STLOG(PRI_DEBUG, BS_CONTROLLER, BSC21, "Console retry commit config", (Request, ev->Get()->Record));
    if (ev->Get()->Record.GetConfigVersion() != ConfigVersion) {
        LOG_ALERT_S(ev->Sender, NKikimrServices::BS_CONTROLLER, "Config version mismatch in retry commit config.");
        return;
    }
    auto commitConfigEv = std::make_unique<TEvBlobStorage::TEvControllerCommitConfigRequest>();
    commitConfigEv->Record.SetYAML(YamlConfig);
    NTabletPipe::SendData(IActor::SelfId(), ConsolePipe, commitConfigEv.release());
}

void TBlobStorageController::Handle(TEvBlobStorage::TEvReplaceConfigRequest::TPtr ev) {
    STLOG(PRI_DEBUG, BS_CONTROLLER, BSC22, "Console replace config request", (Request, ev->Get()->Record));
    auto& record = ev->Get()->Record;
    if (!record.GetOverWriteFlag()) {
        MakeCommitToConsole();
    }
}

}
