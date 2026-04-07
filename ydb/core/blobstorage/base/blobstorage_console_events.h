#pragma once
#include "defs.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/blobstorage.pb.h>

namespace NKikimr {

    struct TEvBlobStorage::TEvControllerProposeConfigRequest : TEventPB<TEvControllerProposeConfigRequest,
            NKikimrBlobStorage::TEvControllerProposeConfigRequest, EvControllerProposeConfigRequest> {
        TEvControllerProposeConfigRequest() = default;

        TEvControllerProposeConfigRequest(ui64 configHash, ui64 configVersion, bool distconf) {
            Record.SetConfigHash(configHash);
            Record.SetConfigVersion(configVersion);
            Record.SetDistconf(distconf);
        }

        TString ToString() const override {
            TStringStream str;
            str << "{TEvControllerProposeConfigRequest Record# " << Record.DebugString();
            str << "}";
            return str.Str();
        }
    };

    struct TEvBlobStorage::TEvControllerProposeConfigResponse : TEventPB<TEvControllerProposeConfigResponse,
            NKikimrBlobStorage::TEvControllerProposeConfigResponse, EvControllerProposeConfigResponse> {
        TEvControllerProposeConfigResponse() = default;
    };

    struct TEvBlobStorage::TEvControllerConsoleCommitRequest : TEventPB<TEvControllerConsoleCommitRequest,
            NKikimrBlobStorage::TEvControllerConsoleCommitRequest, EvControllerConsoleCommitRequest> {
        TEvControllerConsoleCommitRequest() = default;

        TEvControllerConsoleCommitRequest(const TString& yamlConfig) {
            Record.SetYAML(yamlConfig);
        }

        TString ToString() const override {
            TStringStream str;
            str << "{TEvControllerConsoleCommitRequest Record# " << Record.DebugString();
            str << "}";
            return str.Str();
        }
    };

    struct TEvBlobStorage::TEvControllerConsoleCommitResponse : TEventPB<TEvControllerConsoleCommitResponse,
            NKikimrBlobStorage::TEvControllerConsoleCommitResponse, EvControllerConsoleCommitResponse> {
        TEvControllerConsoleCommitResponse() = default;
    };

    struct TEvBlobStorage::TEvControllerValidateConfigRequest : TEventPB<TEvControllerValidateConfigRequest,
            NKikimrBlobStorage::TEvControllerValidateConfigRequest, EvControllerValidateConfigRequest> {
        TEvControllerValidateConfigRequest() = default;

        TEvControllerValidateConfigRequest(const TString& yamlConfig) {
            Record.SetYAML(yamlConfig);
        }

        TString ToString() const override {
            TStringStream str;
            str << "{TEvControllerValidateConfigRequest Record# " << Record.DebugString();
            str << "}";
            return str.Str();
        }
    };

    struct TEvBlobStorage::TEvControllerValidateConfigResponse : TEventPB<TEvControllerValidateConfigResponse,
            NKikimrBlobStorage::TEvControllerValidateConfigResponse, EvControllerValidateConfigResponse> {
        TEvControllerValidateConfigResponse() = default;

        std::optional<TString> InternalError;
    };

    struct TEvBlobStorage::TEvControllerReplaceConfigRequest : TEventPB<TEvControllerReplaceConfigRequest,
            NKikimrBlobStorage::TEvControllerReplaceConfigRequest, EvControllerReplaceConfigRequest> {
        TEvControllerReplaceConfigRequest() = default;

        struct TArgs {
            std::optional<TString> ClusterYaml;
            std::optional<TString> StorageYaml;
            std::optional<bool> SwitchDedicatedStorageSection;
            bool DedicatedConfigMode;
            bool AllowUnknownFields;
            bool BypassMetadataChecks;
            bool DryRun;
            bool EnableConfigV2;
            bool DisableConfigV2;
            TString PeerName;
            TString UserToken;
        };

        TEvControllerReplaceConfigRequest(const TArgs& args) {
            if (args.ClusterYaml) {
                Record.SetClusterYaml(*args.ClusterYaml);
            }
            if (args.StorageYaml) {
                Record.SetStorageYaml(*args.StorageYaml);
            }
            if (args.SwitchDedicatedStorageSection) {
                Record.SetSwitchDedicatedStorageSection(*args.SwitchDedicatedStorageSection);
            }
            Record.SetDedicatedConfigMode(args.DedicatedConfigMode);
            Record.SetAllowUnknownFields(args.AllowUnknownFields);
            Record.SetBypassMetadataChecks(args.BypassMetadataChecks);
            if (args.EnableConfigV2) {
                Record.SetSwitchEnableConfigV2(true);
            } else if (args.DisableConfigV2) {
                Record.SetSwitchEnableConfigV2(false);
            }
            Record.SetPeerName(args.PeerName);
            Record.SetUserToken(args.UserToken);
            Record.SetDryRun(args.DryRun);
        }

        TString ToString() const override {
            TStringStream str;
            str << "{TEvControllerReplaceConfigRequest Record# " << Record.DebugString();
            str << "}";
            return str.Str();
        }
    };

    struct TEvBlobStorage::TEvControllerReplaceConfigResponse : TEventPB<TEvControllerReplaceConfigResponse,
            NKikimrBlobStorage::TEvControllerReplaceConfigResponse, EvControllerReplaceConfigResponse> {
        TEvControllerReplaceConfigResponse() = default;

        TEvControllerReplaceConfigResponse(ProtoRecordType::EStatus status, std::optional<TString> errorReason = std::nullopt) {
            Record.SetStatus(status);
            if (errorReason) {
                Record.SetErrorReason(*errorReason);
            }
        }
    };

    struct TEvBlobStorage::TEvControllerFetchConfigRequest : TEventPB<TEvControllerFetchConfigRequest,
        NKikimrBlobStorage::TEvControllerFetchConfigRequest, EvControllerFetchConfigRequest> {};

    struct TEvBlobStorage::TEvControllerFetchConfigResponse : TEventPB<TEvControllerFetchConfigResponse,
        NKikimrBlobStorage::TEvControllerFetchConfigResponse, EvControllerFetchConfigResponse> {};

    struct TEvBlobStorage::TEvControllerDistconfRequest : TEventPB<TEvControllerDistconfRequest,
        NKikimrBlobStorage::TEvControllerDistconfRequest, EvControllerDistconfRequest> {};

    struct TEvBlobStorage::TEvControllerDistconfResponse : TEventPB<TEvControllerDistconfResponse,
        NKikimrBlobStorage::TEvControllerDistconfResponse, EvControllerDistconfResponse> {};

}
