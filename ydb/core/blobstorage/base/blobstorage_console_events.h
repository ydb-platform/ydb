#pragma once
#include "defs.h"

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/protos/blobstorage.pb.h>

namespace NKikimr {

    struct TEvBlobStorage::TEvControllerProposeConfigRequest : TEventPB<TEvBlobStorage::TEvControllerProposeConfigRequest,
        NKikimrBlobStorage::TEvControllerProposeConfigRequest, TEvBlobStorage::EvControllerProposeConfigRequest> {
        TEvControllerProposeConfigRequest() = default;

        TEvControllerProposeConfigRequest(const ui32 configHash, const ui32 configVersion) {
            Record.SetConfigHash(configHash);
            Record.SetConfigVersion(configVersion);
        }
        
        TString ToString() const override {
            TStringStream str;
            str << "{TEvControllerProposeConfigRequest Record# " << Record.DebugString();
            str << "}";
            return str.Str();
        }
    };

    struct TEvBlobStorage::TEvControllerProposeConfigResponse : TEventPB<TEvBlobStorage::TEvControllerProposeConfigResponse,
        NKikimrBlobStorage::TEvControllerProposeConfigResponse, TEvBlobStorage::EvControllerProposeConfigResponse> {
        TEvControllerProposeConfigResponse() = default;
    };

    struct TEvBlobStorage::TEvControllerConsoleCommitRequest : TEventPB<TEvBlobStorage::TEvControllerConsoleCommitRequest,
        NKikimrBlobStorage::TEvControllerConsoleCommitRequest, TEvBlobStorage::EvControllerConsoleCommitRequest> {
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

    struct TEvBlobStorage::TEvControllerConsoleCommitResponse : TEventPB<TEvBlobStorage::TEvControllerConsoleCommitResponse,
        NKikimrBlobStorage::TEvControllerConsoleCommitResponse, TEvBlobStorage::EvControllerConsoleCommitResponse> {
        TEvControllerConsoleCommitResponse() = default;
    };

    struct TEvBlobStorage::TEvControllerValidateConfigRequest : TEventPB<TEvBlobStorage::TEvControllerValidateConfigRequest,
        NKikimrBlobStorage::TEvControllerValidateConfigRequest, TEvBlobStorage::EvControllerValidateConfigRequest> {
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

    struct TEvBlobStorage::TEvControllerValidateConfigResponse : TEventPB<TEvBlobStorage::TEvControllerValidateConfigResponse,
        NKikimrBlobStorage::TEvControllerValidateConfigResponse, TEvBlobStorage::EvControllerValidateConfigResponse> {
        TEvControllerValidateConfigResponse() = default;
    };

    struct TEvBlobStorage::TEvControllerReplaceConfigRequest : TEventPB<TEvBlobStorage::TEvControllerReplaceConfigRequest,
        NKikimrBlobStorage::TEvControllerReplaceConfigRequest, TEvBlobStorage::EvControllerReplaceConfigRequest> {
        TEvControllerReplaceConfigRequest() = default;

        TEvControllerReplaceConfigRequest(const TString& yamlConfig) {
            Record.SetYAML(yamlConfig);
        }

        TString ToString() const override {
            TStringStream str;
            str << "{TEvControllerReplaceConfigRequest Record# " << Record.DebugString();
            str << "}";
            return str.Str();
        }
    };

    struct TEvBlobStorage::TEvControllerReplaceConfigResponse : TEventPB<TEvBlobStorage::TEvControllerReplaceConfigResponse,
        NKikimrBlobStorage::TEvControllerReplaceConfigResponse, TEvBlobStorage::EvControllerReplaceConfigResponse> {
        TEvControllerReplaceConfigResponse() = default;
    };

}
