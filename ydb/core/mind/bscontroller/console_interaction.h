#pragma once

#include <ydb/core/cms/console/console.h>
#include <ydb/core/blobstorage/base/blobstorage_console_events.h>
#include <ydb/core/util/backoff.h>
#include <ydb/core/base/tablet_pipe.h>
#include <contrib/libs/xxhash/xxhash.h>
#include <ydb/core/protos/blobstorage_distributed_config.pb.h>
#include "impl.h"

namespace NKikimr::NBsController {

    class TBlobStorageController::TConsoleInteraction {
    public:
        struct TEvPrivate {
            enum EEv {
                EvValidationTimeout,
            };
        };

        TConsoleInteraction(TBlobStorageController& controller);
        void Start();
        // storageApplyCookie: the cookie of the application of the storage config the committed replacement waits for
        void OnConfigCommit(bool mainConfigChanged, std::optional<ui64> storageApplyCookie = std::nullopt);
        void Stop();
        void ProcessDryRunResponse(bool success, TString errorReason = {});
        void ProcessApplyResponse(ui64 cookie, bool success, TString errorReason);

        void Handle(TEvBlobStorage::TEvControllerProposeConfigResponse::TPtr& ev);
        void Handle(TEvBlobStorage::TEvControllerConsoleCommitResponse::TPtr& ev);
        void Handle(TEvBlobStorage::TEvControllerReplaceConfigRequest::TPtr& ev);
        void Handle(TEvBlobStorage::TEvControllerFetchConfigRequest::TPtr& ev);
        void HandleValidationTimeout(TAutoPtr<IEventHandle>& ev);
        void Handle(TEvBlobStorage::TEvControllerValidateConfigResponse::TPtr& ev);
        void Handle(TEvTabletPipe::TEvClientConnected::TPtr& /*ev*/);
        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& /*ev*/);
        void Handle(TEvBlobStorage::TEvGetBlockResult::TPtr& ev);

        bool RequestIsBeingProcessed() const { return static_cast<bool>(ClientId); }

    private:
        TBlobStorageController& Self;
        TActorId ConsolePipe;
        TActorId ClientId;
        TBackoffTimer MakeSessionBackoffTimer{1, 1000};
        TInstant ValidationTimeout;
        ui64 ExpectedValidationTimeoutCookie = 0;
        ui64 ValidationRequestCookie = Max<ui64>(); // the cookie the validation of the replacement going on was asked with
        TBackoffTimer GetBlockBackoff{1, 1000};
        ui32 BlockedGeneration = 0;
        bool Working = false;
        bool CommitInProgress = false;
        // the committed replacement is answered once Console has committed it and the storage config has applied
        bool ConsoleCommitted = false;
        std::optional<ui64> StorageApplyCookie; // the application of the committed replacement, if it is going on
        std::optional<TString> StorageApplyError;
        std::optional<bool> SwitchEnableConfigV2;
        TEvBlobStorage::TEvControllerReplaceConfigRequest::TPtr PendingReplaceRequest;
        std::optional<TAuditLogInfo> AuditLogInfo;

        std::optional<TString> PendingYamlConfig;
        bool AllowUnknownFields = false;
        bool IsDryRun = false;
        std::optional<std::optional<TString>> PendingStorageYamlConfig;
        std::optional<ui64> ExpectedYamlConfigVersion;

        struct TPendingCommitState {
            std::optional<TYamlConfig> YamlConfig;
            std::optional<NKikimrBlobStorage::TStorageConfig> StorageConfig;
            std::optional<ui64> ExpectedStorageYamlConfigVersion;
        };
        std::optional<TPendingCommitState> PendingCommitState;

        void MakeCommitToConsole(TString& config, ui32 configVersion);
        void MakeGetBlock();
        void MaybeDisableConfigV2();
        void OnConsoleCommit();

        void CommitConfig();
        void MaybeFinishCommittedReplacement();
        void IssueGRpcResponse(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::EStatus status,
            std::optional<TString> errorReason = std::nullopt, bool disabledConfigV2 = false);
    };
}
