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

        TEvControllerConsoleCommitRequest(
            const TString& yamlConfig,
            bool allowUnknownFields = false,
            bool bypassMetadataChecks = false) {

            Record.SetYAML(yamlConfig);
            Record.SetAllowUnknownFields(allowUnknownFields);
            Record.SetBypassMetadataChecks(bypassMetadataChecks);
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

        TEvControllerReplaceConfigRequest(
            std::optional<TString> clusterYaml,
            std::optional<TString> storageYaml,
            std::optional<bool> switchDedicatedStorageSection,
            bool dedicatedConfigMode,
            bool allowUnknownFields,
            bool bypassMetadataChecks) {

            if (clusterYaml) {
                Record.SetClusterYaml(*clusterYaml);
            }
            if (storageYaml) {
                Record.SetStorageYaml(*storageYaml);
            }
            if (switchDedicatedStorageSection) {
                Record.SetSwitchDedicatedStorageSection(*switchDedicatedStorageSection);
            }
            Record.SetDedicatedConfigMode(dedicatedConfigMode);
            Record.SetAllowUnknownFields(allowUnknownFields);
            Record.SetBypassMetadataChecks(bypassMetadataChecks);
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

}
