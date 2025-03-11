#include "distconf.h"

#include <ydb/library/yaml_config/yaml_config.h>
#include <library/cpp/streams/zstd/zstd.h>

namespace NKikimr::NStorage {

    void TDistributedConfigKeeper::ConnectToConsole(bool enablingDistconf) {
        if (ConsolePipeId) {
            return; // connection is already established
        } else if (!Scepter) {
            return; // this is not the root node
        } else if (enablingDistconf) {
            // NO RETURN HERE -> right now we are enabling distconf, so we can skip rest of the checks
        } else if (!SelfManagementEnabled || !StorageConfig->HasStateStorageConfig()) {
            return; // no self-management config enabled or no way to find Console (no statestorage configured yet)
        }

        STLOG(PRI_DEBUG, BS_NODE, NWDC66, "ConnectToConsole: creating pipe to the Console");
        ConsolePipeId = Register(NTabletPipe::CreateClient(SelfId(), MakeConsoleID(),
            NTabletPipe::TClientRetryPolicy::WithRetries()));
    }

    void TDistributedConfigKeeper::DisconnectFromConsole() {
        NTabletPipe::CloseAndForgetClient(SelfId(), ConsolePipeId);
        ConsoleConnected = false;
    }

    void TDistributedConfigKeeper::SendConfigProposeRequest() {
        if (!ConsoleConnected) {
            return;
        }

        if (ProposeRequestInFlight) {
            return; // still waiting for previous one
        }

        ProposeRequestInFlight = true;

        if (!StorageConfig || !StorageConfig->HasConfigComposite()) {
            // send empty proposition just to connect to console
            auto ev = std::make_unique<TEvBlobStorage::TEvControllerProposeConfigRequest>();
            ev->Record.SetDistconf(true);
            NTabletPipe::SendData(SelfId(), ConsolePipeId, ev.release(), ++ProposeRequestCookie);
            return;
        }

        Y_ABORT_UNLESS(MainConfigYamlVersion);

        STLOG(PRI_DEBUG, BS_NODE, NWDC67, "SendConfigProposeRequest: sending propose request to the Console",
            (MainConfigFetchYamlHash, MainConfigFetchYamlHash),
            (MainConfigYamlVersion, MainConfigYamlVersion),
            (ProposedConfigHashVersion, ProposedConfigHashVersion),
            (ProposeRequestCookie, ProposeRequestCookie + 1));

        Y_DEBUG_ABORT_UNLESS(!ProposedConfigHashVersion || ProposedConfigHashVersion == std::make_tuple(
            MainConfigFetchYamlHash, *MainConfigYamlVersion));
        ProposedConfigHashVersion.emplace(MainConfigFetchYamlHash, *MainConfigYamlVersion);
        NTabletPipe::SendData(SelfId(), ConsolePipeId, new TEvBlobStorage::TEvControllerProposeConfigRequest(
            MainConfigFetchYamlHash, *MainConfigYamlVersion, true), ++ProposeRequestCookie);
    }

    void TDistributedConfigKeeper::Handle(TEvBlobStorage::TEvControllerValidateConfigResponse::TPtr ev) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC10, "received TEvControllerValidateConfigResponse",
            (Sender, ev->Sender), (Cookie, ev->Cookie), (Record, ev->Get()->Record),
            (ConsoleConfigValidationQ.size, ConsoleConfigValidationQ.size()));

        auto& q = ConsoleConfigValidationQ;
        auto pred = [&](const auto& item) {
            const auto& [actorId, yaml, cookie] = item;
            const bool match = cookie == ev->Cookie;
            if (match) {
                TActivationContext::Send(ev->Forward(actorId));
            }
            return match;
        };
        q.erase(std::remove_if(q.begin(), q.end(), pred), q.end());
    }

    void TDistributedConfigKeeper::Handle(TEvBlobStorage::TEvControllerProposeConfigResponse::TPtr ev) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC68, "received TEvControllerProposeConfigResponse",
            (ConsoleConnected, ConsoleConnected),
            (ProposeRequestInFlight, ProposeRequestInFlight),
            (Cookie, ev->Cookie),
            (ProposeRequestCookie, ProposeRequestCookie),
            (ProposedConfigHashVersion, ProposedConfigHashVersion),
            (Record, ev->Get()->Record));

        if (!ConsoleConnected || !ProposeRequestInFlight || ev->Cookie != ProposeRequestCookie) {
            return;
        }
        ProposeRequestInFlight = false;

        const auto& record = ev->Get()->Record;
        switch (record.GetStatus()) {
            case NKikimrBlobStorage::TEvControllerProposeConfigResponse::HashMismatch:
            case NKikimrBlobStorage::TEvControllerProposeConfigResponse::UnexpectedConfig:
                // TODO: error condition; restart?
                ProposedConfigHashVersion.reset();
                break;

            case NKikimrBlobStorage::TEvControllerProposeConfigResponse::CommitIsNeeded: {
                if (!StorageConfig || !StorageConfig->HasConfigComposite() || ProposedConfigHashVersion !=
                        std::make_tuple(MainConfigFetchYamlHash, *MainConfigYamlVersion)) {
                    const char *err = "proposed config, but something has gone awfully wrong";
                    STLOG(PRI_CRIT, BS_NODE, NWDC69, err, (StorageConfig, StorageConfig),
                        (ProposedConfigHashVersion, ProposedConfigHashVersion),
                        (MainConfigFetchYamlHash, MainConfigFetchYamlHash),
                        (MainConfigYamlVersion, MainConfigYamlVersion));
                    Y_DEBUG_ABORT("%s", err);
                    return;
                }

                NTabletPipe::SendData(SelfId(), ConsolePipeId, new TEvBlobStorage::TEvControllerConsoleCommitRequest(
                    MainConfigYaml), // FIXME: probably should propagate force here
                        ++CommitRequestCookie);
                break;
            }

            case NKikimrBlobStorage::TEvControllerProposeConfigResponse::CommitIsNotNeeded:
                // it's okay, just wait for another configuration change or something like that
                ConfigCommittedToConsole = true;
                break;

            case NKikimrBlobStorage::TEvControllerProposeConfigResponse::ReverseCommit:
                // just do nothing, we didn't have the config in distconf, possibly it is being enabled
                break;
        }
    }

    void TDistributedConfigKeeper::Handle(TEvBlobStorage::TEvControllerConsoleCommitResponse::TPtr ev) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC70, "received TEvControllerConsoleCommitResponse",
            (ConsoleConnected, ConsoleConnected),
            (Cookie, ev->Cookie),
            (CommitRequestCookie, CommitRequestCookie),
            (Record, ev->Get()->Record));

        if (!ConsoleConnected || ev->Cookie != CommitRequestCookie) {
            return;
        }

        const auto& record = ev->Get()->Record;
        switch (record.GetStatus()) {
            case NKikimrBlobStorage::TEvControllerConsoleCommitResponse::SessionMismatch:
                DisconnectFromConsole();
                ConnectToConsole();
                break;

            case NKikimrBlobStorage::TEvControllerConsoleCommitResponse::NotCommitted:
                break;

            case NKikimrBlobStorage::TEvControllerConsoleCommitResponse::Committed:
                ConfigCommittedToConsole = true;
                break;
        }

        ProposedConfigHashVersion.reset();
    }

    void TDistributedConfigKeeper::Handle(TEvTabletPipe::TEvClientConnected::TPtr ev) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC71, "received TEvClientConnected", (ConsolePipeId, ConsolePipeId),
            (TabletId, ev->Get()->TabletId), (Status, ev->Get()->Status), (ClientId, ev->Get()->ClientId),
            (ServerId, ev->Get()->ServerId));
        if (ev->Get()->ClientId == ConsolePipeId) {
            if (ev->Get()->Status == NKikimrProto::OK) {
                Y_ABORT_UNLESS(!ConsoleConnected);
                ConsoleConnected = true;
                SendConfigProposeRequest();
                for (auto& [actorId, yaml, cookie] : ConsoleConfigValidationQ) {
                    Y_ABORT_UNLESS(!cookie);
                    cookie = ++ValidateRequestCookie;
                    NTabletPipe::SendData(SelfId(), ConsolePipeId, new TEvBlobStorage::TEvControllerValidateConfigRequest(
                        yaml), cookie);
                }
            } else {
                OnConsolePipeError();
            }
        }
    }

    void TDistributedConfigKeeper::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr ev) {
        STLOG(PRI_DEBUG, BS_NODE, NWDC72, "received TEvClientDestroyed", (ConsolePipeId, ConsolePipeId),
            (TabletId, ev->Get()->TabletId), (ClientId, ev->Get()->ClientId), (ServerId, ev->Get()->ServerId));
        if (ev->Get()->ClientId == ConsolePipeId) {
            OnConsolePipeError();
        }
    }

    void TDistributedConfigKeeper::OnConsolePipeError() {
        ConsolePipeId = {};
        ConsoleConnected = false;
        ConfigCommittedToConsole = false;
        ProposedConfigHashVersion.reset();
        ProposeRequestInFlight = false;
        ++CommitRequestCookie; // to prevent processing any messages

        // cancel any pending requests
        for (const auto& [actorId, yaml, cookie] : ConsoleConfigValidationQ) {
            auto ev = std::make_unique<TEvBlobStorage::TEvControllerValidateConfigResponse>();
            ev->InternalError = "pipe disconnected";
            Send(actorId, ev.release());
        }
        ConsoleConfigValidationQ.clear();

        ConnectToConsole();
    }

    std::optional<TString> TDistributedConfigKeeper::UpdateConfigComposite(NKikimrBlobStorage::TStorageConfig& config,
            const TString& yaml, const std::optional<TString>& fetched) {
        TString temp;
        const TString *finalFetchedConfig = fetched ? &fetched.value() : &temp;

        if (!fetched) { // fill in 'to-be-fetched' version of config with version incremented by one
            try {
                auto metadata = NYamlConfig::GetMainMetadata(yaml);
                metadata.Cluster = metadata.Cluster.value_or("unknown"); // TODO: fix this
                metadata.Version = metadata.Version.value_or(0) + 1;
                temp = NYamlConfig::ReplaceMetadata(yaml, metadata);
            } catch (const std::exception& ex) {
                return ex.what();
            }
        }

        TStringStream ss;
        {
            TZstdCompress zstd(&ss);
            SaveSize(&zstd, yaml.size());
            zstd.Write(yaml);
            SaveSize(&zstd, finalFetchedConfig->size());
            zstd.Write(*finalFetchedConfig);
        }
        config.SetConfigComposite(ss.Str());

        return {};
    }

    bool TDistributedConfigKeeper::EnqueueConsoleConfigValidation(TActorId actorId, bool enablingDistconf, TString yaml) {
        if (!ConsolePipeId) {
            ConnectToConsole(enablingDistconf);
            if (!ConsolePipeId) {
                return false;
            }
        }

        auto& [qActorId, qYaml, qCookie] = ConsoleConfigValidationQ.emplace_back(actorId, std::move(yaml), 0);

        if (ConsoleConnected) {
            qCookie = ++ValidateRequestCookie;
            NTabletPipe::SendData(SelfId(), ConsolePipeId, new TEvBlobStorage::TEvControllerValidateConfigRequest(qYaml),
                qCookie);
        }

        return true;
    }

}
