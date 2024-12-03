#include "impl.h"
#include <ydb/library/yaml_config/yaml_config.h>
#include <ydb/library/yaml_config/yaml_config_parser.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_impl.h>
#include <ydb/core/blobstorage/base/utility.h>

namespace NKikimr::NBsController {

    void TBlobStorageController::StartConsoleInteraction() {
        ConsoleInteraction = std::make_unique<TConsoleInteraction>(*this);
        ConsoleInteraction->Start();
    }

    TConsoleInteraction::TConsoleInteraction(TBlobStorageController& controller)
        : Self(controller)
    {}

    void TConsoleInteraction::Start() {
        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = {
            .RetryLimitCount = 10,
        };
        auto pipe = NTabletPipe::CreateClient(Self.SelfId(), MakeConsoleID(), pipeConfig);
        ConsolePipe = Self.Register(pipe);
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC18, "Console connection service started");

        auto ev = MakeHolder<TEvBlobStorage::TEvControllerProposeConfigRequest>();
        auto configHash = std::hash<TString>{}(Self.YamlConfig);
        ev->Record.SetConfigHash(configHash);
        ev->Record.SetConfigVersion(Self.ConfigVersion);

        NTabletPipe::SendData(Self.SelfId(), ConsolePipe, ev.Release());
    }

    void TConsoleInteraction::MakeCommitToConsole(TString& config, ui32 configVersion) {
        Self.IsOngoingCommit = true;
        auto ev = MakeHolder<TEvBlobStorage::TEvControllerConsoleCommitRequest>();
        ev->Record.SetYAML(Self.YamlConfig);
        NTabletPipe::SendData(Self.SelfId(), ConsolePipe, ev.Release());
    }

    void TConsoleInteraction::Handle(TEvBlobStorage::TEvControllerProposeConfigResponse::TPtr ev) {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC19, "Console proposed config response", (Response, ev->Get()->Record));
        auto& record = ev->Get()->Record;
        auto status = record.GetStatus();
        auto commitConfigEv = MakeHolder<TEvBlobStorage::TEvControllerCommitConfigRequest>();
        switch(status) {
            case NKikimrBlobStorage::TEvControllerProposeConfigResponse::HashMismatch:
                STLOG(PRI_ALERT, BS_CONTROLLER, BSC25, "Config hash mismatch.");
                return;
            case NKikimrBlobStorage::TEvControllerProposeConfigResponse::UnexpectedConfig:
                // TODO: Tx to local base
                break;
            case NKikimrBlobStorage::TEvControllerProposeConfigResponse::CommitIsNotNeeded:
                return;
            case NKikimrBlobStorage::TEvControllerProposeConfigResponse::CommitIsNeeded:
                commitConfigEv->Record.SetYAML(Self.YamlConfig);
                commitConfigEv->Record.SetConfigVersion(Self.ConfigVersion);
                Self.Send(Self.SelfId(), commitConfigEv.Release());
                return;
            default:
                Y_FAIL("Unexpected config propose response status: %d", status);
        }
    }

    void TConsoleInteraction::Handle(TEvBlobStorage::TEvControllerCommitConfigResponse::TPtr ev) {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC20, "Console commit config response", (Response, ev->Get()->Record));
        auto& record = ev->Get()->Record;
        if (record.GetStatus() == NKikimrBlobStorage::TEvControllerCommitConfigResponse::ParseError ||
            record.GetStatus() == NKikimrBlobStorage::TEvControllerCommitConfigResponse::ValidationError) {
            response->Record.SetStatus(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::BSCInvalidConfig);
            Self.Send(GRPCSenderId, response.Release());
            return;
        }
        if (record.GetStatus() == NKikimrBlobStorage::TEvControllerCommitConfigResponse::Success) {
            return;
        }
    }

    void TConsoleInteraction::Handle(TEvBlobStorage::TEvControllerConsoleCommitResponse::TPtr ev) {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC20, "Console commit config response", (Response, ev->Get()->Record));
        auto& record = ev->Get()->Record;
        Self.IsOngoingCommit = false;
        if (record.GetStatus() == NKikimrBlobStorage::TEvControllerConsoleCommitResponse::SessionMismatch) {
            return;
        }
        if (record.GetStatus() == NKikimrBlobStorage::TEvControllerConsoleCommitResponse::NotCommited) {
            // TODO: Tx to local base
            auto RetryConsoleCommit = std::make_unique<TEvBlobStorage::TEvControllerRetryConsoleCommit>();
            RetryConsoleCommit->Record.SetConfigVersion(ConfigVersion);
            Self.Schedule(TDuration::MilliSeconds(CommitConfigBackoffTimer.NextBackoffMs()), new IEventHandle(SelfId(), SelfId(), RetryConsoleCommit.release()));
            return;
        }
        CommitConfigBackoffTimer.Reset();
    }

    void TConsoleInteraction::Handle(TEvBlobStorage::TEvControllerRetryConsoleCommit::TPtr ev) {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC21, "Console retry commit config", (Request, ev->Get()->Record));
        if (ev->Get()->Record.GetConfigVersion() != ConfigVersion) {
            STLOG(PRI_ALERT, BS_CONTROLLER, BSC23, "Config version mismatch in retry commit config.");
            return;
        }
        auto commitConfigEv = std::make_unique<TEvBlobStorage::TEvControllerConsoleCommitRequest>();
        commitConfigEv->Record.SetYAML(Self.YamlConfig);
        NTabletPipe::SendData(Self.SelfId(), ConsolePipe, commitConfigEv.release());
    }

    void TConsoleInteraction::Handle(TEvBlobStorage::TEvControllerReplaceConfigRequest::TPtr ev) {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC24, "Console replace config request", (Request, ev->Get()->Record));
        auto& record = ev->Get()->Record;
        auto response = MakeHolder<TEvBlobStorage::TEvControllerReplaceConfigResponse>();
        if (!record.GetOverWriteFlag()) {
            if (Self.IsOngoingCommit) {
                response->Record.SetStatus(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::OnGoingCommit);
                Self.Send(ev->Sender, response.Release());
                return;
            }
        }
        if (!record.GetSkipConsoleValidation()) {
            ValidationTimeout = false;
            Schedule(TDuration::Minutes(2), new IEventHandle(SelfId(), SelfId(), new TEvBlobStorage::TEvControllerValidationTimeout()));
            auto validateConfigEv = std::make_unique<TEvBlobStorage::TEvControllerValidateConfigRequest>();
            validateConfigEv->Record.SetYAML(Self.YamlConfig);
            NTabletPipe::SendData(Self.SelfId(), ConsolePipe, validateConfigEv.release());
        }
    }

    void TConsoleInteraction::Handle(TEvBlobStorage::TEvControllerValidationTimeout::TPtr ev) {
        ValidationTimeout = true;
        auto response = MakeHolder<TEvBlobStorage::TEvControllerReplaceConfigResponse>();
        response->Record.SetStatus(NKikimrBlobStorage::TEvControllerReplaceConfigResponse::ValidationTimeout);
        Self.Send(ev->Get()->Record.GetGRPCSenderId(), response.Release());
    }

}
