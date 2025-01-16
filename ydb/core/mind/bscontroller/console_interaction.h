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
        struct TCommitConfigResult {
            enum class EStatus {
                ParseError,
                ValidationError,
                Success,
            };
            EStatus Status = EStatus::Success;
        };
        struct TEvPrivate {
            enum EEv {
                EvValidationTimeout,
            };

            struct TEvValidationTimeout : public TEventLocal<TEvValidationTimeout, EvValidationTimeout> {};
        };

        TConsoleInteraction(TBlobStorageController& controller);
        void Start();
        void OnConfigCommit(const TCommitConfigResult& result);
        void OnPassAway();
        bool ParseConfig(const TString& config, ui32& configVersion, NKikimrBlobStorage::TStorageConfig& storageConfig);
        TCommitConfigResult::EStatus CheckConfig(const TString& config, ui32& configVersion, bool skipBSCValidation, NKikimrBlobStorage::TStorageConfig& storageConfig);

        void Handle(TEvBlobStorage::TEvControllerProposeConfigResponse::TPtr& ev);
        void Handle(TEvBlobStorage::TEvControllerConsoleCommitResponse::TPtr& ev);
        void Handle(TEvBlobStorage::TEvControllerReplaceConfigRequest::TPtr& ev);
        void Handle(TEvPrivate::TEvValidationTimeout::TPtr& ev);
        void Handle(TEvBlobStorage::TEvControllerValidateConfigResponse::TPtr& ev);
        void Handle(TEvTabletPipe::TEvClientConnected::TPtr& /*ev*/);
        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& /*ev*/);
        void Handle(TEvBlobStorage::TEvGetBlockResult::TPtr& ev);

    private:
        TBlobStorageController& Self;
        TActorId ConsolePipe;
        TActorId GRPCSenderId;
        TBackoffTimer MakeSessionBackoffTimer{1, 1000};
        TInstant ValidationTimeout;
        TBackoffTimer GetBlockBackoff{1, 1000};
        ui32 BlockedGeneration = 0;
        bool NeedRetrySession = false;

        void MakeCommitToConsole(TString& config, ui32 configVersion);
        void MakeGetBlock();
        void MakeRetrySession();
    };

}
