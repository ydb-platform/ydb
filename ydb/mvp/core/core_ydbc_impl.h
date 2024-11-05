#pragma once
#include <contrib/libs/googleapis-common-protos/google/rpc/status.pb.h>
#include <ydb/public/api/client/yc_private/quota/quota.pb.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <ydb/core/viewer/json/json.h>
#include "core_ydb_impl.h"
#include <ydb/public/api/client/yc_private/ydb/v1/database_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/ydb/v1/backup_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/ydb/v1/storage_type_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/ydb/v1/resource_preset_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/ydb/v1/quota_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/resourcemanager/cloud.pb.h>
#include <ydb/public/api/client/yc_private/ydb/v1/console_service.grpc.pb.h>

namespace NMVP {

struct THandlerActorYdbc : THandlerActorYdb {
    struct TEvPrivate : THandlerActorYdb::TEvPrivate {
        struct TEvGetDatabaseResponse : NActors::TEventLocal<TEvGetDatabaseResponse, EvGetDatabaseResponse> {
            yandex::cloud::priv::ydb::v1::Database Database;

            TEvGetDatabaseResponse(yandex::cloud::priv::ydb::v1::Database&& database)
                : Database(std::move(database))
            {}
        };

        struct TEvListDatabaseResponse : NActors::TEventLocal<TEvListDatabaseResponse, EvListDatabaseResponse> {
            yandex::cloud::priv::ydb::v1::ListDatabasesResponse Databases;

            TEvListDatabaseResponse(yandex::cloud::priv::ydb::v1::ListDatabasesResponse&& databases)
                : Databases(std::move(databases))
            {}
        };

        struct TEvListAllDatabaseResponse : NActors::TEventLocal<TEvListAllDatabaseResponse, EvListAllDatabaseResponse> {
            yandex::cloud::priv::ydb::v1::ListAllDatabasesResponse Databases;

            TEvListAllDatabaseResponse(yandex::cloud::priv::ydb::v1::ListAllDatabasesResponse&& databases)
                : Databases(std::move(databases))
            {}
        };

        struct TEvOperationResponse : NActors::TEventLocal<TEvOperationResponse, EvOperationResponse> {
            ydb::yc::priv::operation::Operation Operation;

            TEvOperationResponse(ydb::yc::priv::operation::Operation&& operation)
                : Operation(operation)
            {}
        };

        struct TEvDatabaseResponse : NActors::TEventLocal<TEvDatabaseResponse, EvDatabaseResponse> {
            yandex::cloud::priv::ydb::v1::Database Database;

            TEvDatabaseResponse(yandex::cloud::priv::ydb::v1::Database&& database)
                : Database(std::move(database))
            {}
        };

        struct TEvBackupResponse : NActors::TEventLocal<TEvBackupResponse, EvBackupResponse> {
            yandex::cloud::priv::ydb::v1::Backup Backup;

            TEvBackupResponse(yandex::cloud::priv::ydb::v1::Backup&& backup)
                : Backup(std::move(backup))
            {}
        };

        struct TEvListBackupsResponse : NActors::TEventLocal<TEvListBackupsResponse, EvListBackupsResponse> {
            yandex::cloud::priv::ydb::v1::ListBackupsResponse ListBackups;

            TEvListBackupsResponse(yandex::cloud::priv::ydb::v1::ListBackupsResponse&& listBackups)
                : ListBackups(std::move(listBackups))
            {}
        };

        struct TEvListStorageTypesResponse : NActors::TEventLocal<TEvListStorageTypesResponse, EvListStorageTypesResponse> {
            yandex::cloud::priv::ydb::v1::ListStorageTypesResponse Response;

            TEvListStorageTypesResponse(yandex::cloud::priv::ydb::v1::ListStorageTypesResponse&& response)
                : Response(std::move(response))
            {}
        };

        struct TEvListResourcePresetsResponse : NActors::TEventLocal<TEvListResourcePresetsResponse, EvListResourcePresetsResponse> {
            yandex::cloud::priv::ydb::v1::ListResourcePresetsResponse Response;

            TEvListResourcePresetsResponse(yandex::cloud::priv::ydb::v1::ListResourcePresetsResponse&& response)
                : Response(std::move(response))
            {}
        };

        struct TEvGetConfigResponse : NActors::TEventLocal<TEvGetConfigResponse, EvGetConfigResponse> {
            yandex::cloud::priv::ydb::v1::GetConfigResponse Response;

            TEvGetConfigResponse(yandex::cloud::priv::ydb::v1::GetConfigResponse&& response)
                : Response(std::move(response))
            {}
        };

        struct TEvListOperationsResponse : NActors::TEventLocal<TEvListOperationsResponse, EvListOperationsResponse> {
            yandex::cloud::priv::ydb::v1::ListOperationsResponse Response;

            TEvListOperationsResponse(yandex::cloud::priv::ydb::v1::ListOperationsResponse&& response)
                : Response(std::move(response))
            {}
        };

        struct TEvSimulateResponse : NActors::TEventLocal<TEvSimulateResponse, EvSimulateResponse> {
            yandex::cloud::priv::ydb::v1::SimulateResponse Response;

            TEvSimulateResponse(yandex::cloud::priv::ydb::v1::SimulateResponse&& response)
                : Response(response)
            {}
        };

        struct TEvGetCloudResponse : NActors::TEventLocal<TEvGetCloudResponse, EvGetCloudResponse> {
            yandex::cloud::priv::resourcemanager::v1::Cloud Response;

            TEvGetCloudResponse(yandex::cloud::priv::resourcemanager::v1::Cloud&& response)
                : Response(std::move(response))
            {}
        };

        struct TEvQuota : NActors::TEventLocal<TEvQuota, EvQuota> {
            yandex::cloud::priv::quota::Quota Response;

            TEvQuota(yandex::cloud::priv::quota::Quota&& response)
                : Response(std::move(response))
            {}
        };

        struct TEvQuotaDefaultResponse : NActors::TEventLocal<TEvQuotaDefaultResponse, EvQuotaDefaultResponse> {
            yandex::cloud::priv::quota::GetQuotaDefaultResponse Response;

            TEvQuotaDefaultResponse(yandex::cloud::priv::quota::GetQuotaDefaultResponse&& response)
                : Response(std::move(response))
            {}
        };

        struct TEvEmpty : NActors::TEventLocal<TEvEmpty, EvEmpty> {
            google::protobuf::Empty Response;

            TEvEmpty(google::protobuf::Empty&& response)
                : Response(std::move(response))
            {}
        };
    };

    static TJsonSettings JsonSettings;
    static NProtobufJson::TJson2ProtoConfig Json2ProtoConfig;
    static NProtobufJson::TJson2ProtoConfig Json2ProtoConfig2;
    static NProtobufJson::TProto2JsonConfig Proto2JsonConfig;

    static NHttp::THttpOutgoingResponsePtr CreateErrorResponse(NHttp::THttpIncomingRequestPtr request, const TEvPrivate::TEvErrorResponse* error) {
        NJson::TJsonValue json;
        json["message"] = error->Message;
        if (error->Details) {
            ::google::rpc::Status status;
            if (status.ParseFromString(error->Details)) {
                json["code"] = status.code();
                NJson::TJsonValue& jsonDetails = json["details"];
                jsonDetails.SetType(NJson::JSON_ARRAY);
                for (const ::google::protobuf::Any& detail : status.details()) {
                    NJson::TJsonValue& jsonDetail = jsonDetails.AppendValue({});
                    const TString& typeUrl = detail.type_url();
                    TString type;
                    if (typeUrl.StartsWith("type.googleapis.com/")) {
                        type = "." + typeUrl.substr(20);
                    }
                    if (type) {
                        jsonDetail["@type"] = type;
                        if (type == ".yandex.cloud.priv.quota.QuotaFailure") {
                            ::yandex::cloud::priv::quota::QuotaFailure quotaFailure;
                            if (detail.UnpackTo(&quotaFailure)) {
                                NProtobufJson::Proto2Json(quotaFailure, jsonDetail, Proto2JsonConfig);
                            }
                        }
                    } else {
                        jsonDetail["@type"] = typeUrl;
                    }
                }
            }
        }
        TString body = NJson::WriteJson(json, false);
        return request->CreateResponse(error->Status, error->Message, "application/json", body);
    }

    static NHttp::THttpOutgoingResponsePtr CreateErrorResponse(NHttp::THttpIncomingRequestPtr request, const TString& error) {
        return THandlerActorYdb::CreateErrorResponse(request, error);
    }
};

}
