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
        void OnConfigCommit();
        void Stop();

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
        TBackoffTimer GetBlockBackoff{1, 1000};
        ui32 BlockedGeneration = 0;
        bool NeedRetrySession = false;
        bool Working = false;
        bool CommitInProgress = false;

        std::optional<TString> PendingYamlConfig;
        bool AllowUnknownFields = false;
        bool BypassMetadataChecks = false;

        std::optional<std::optional<TString>> PendingStorageYamlConfig;

        void MakeCommitToConsole(TString& config, ui32 configVersion);
        void MakeGetBlock();
        void MakeRetrySession();

        void IssueGRpcResponse(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::EStatus status,
            std::optional<TString> errorReason = std::nullopt);
    };

}
