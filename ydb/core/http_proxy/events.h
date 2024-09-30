#pragma once

#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>

#include <ydb/core/base/events.h>

#include <ydb/library/grpc/client/grpc_client_low.h>

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>



namespace NKikimr::NHttpProxy {

    struct TDatabase {
        TString Endpoint;
        TString Id;
        TString Path;
        TString CloudId;
        TString FolderId;

        TDatabase(const TString& endpoint, const TString& id, const TString& path, const TString& cloudId, const TString& folderId)
        : Endpoint(endpoint)
        , Id(id)
        , Path(path)
        , CloudId(cloudId)
        , FolderId(folderId)
        {}

        TDatabase() {}
    };

    struct TEvServerlessProxy {
        enum TEv {
            EvDiscoverDatabaseEndpointRequest = EventSpaceBegin(TKikimrEvents::ES_HTTP_PROXY),
            EvDiscoverDatabaseEndpointResult,
            EvReportMetricsRequest,
            EvGrpcRequestResult,
            EvUpdateDatabasesEvent,
            EvListEndpointsRequest,
            EvListEndpointsResponse,
            EvErrorWithIssue,
            EvCounter,
            EvHistCounter,
            EvToken,
            EvClientReady
        };

        struct TEvGrpcRequestResult : public TEventLocal<TEvGrpcRequestResult, EvGrpcRequestResult> {
            THolder<google::protobuf::Message> Message;
            THolder<NYdb::TStatus> Status;
        };

        struct TEvDiscoverDatabaseEndpointRequest : public TEventLocal<TEvDiscoverDatabaseEndpointRequest, EvDiscoverDatabaseEndpointRequest> {
            TString DatabasePath;
            TString DatabaseId;
        };

        struct TEvUpdateDatabasesEvent : public TEventLocal<TEvUpdateDatabasesEvent, EvUpdateDatabasesEvent> {
            std::vector<TDatabase> Databases;
            std::unique_ptr<NYdbGrpc::TGrpcStatus> Status;
        };

        struct TEvDiscoverDatabaseEndpointResult : public TEventLocal<TEvDiscoverDatabaseEndpointResult, EvDiscoverDatabaseEndpointResult> {
            THolder<TDatabase> DatabaseInfo;
            NYdb::EStatus Status;
            TString Message;
        };

        struct TEvReportMetricsRequest : public TEventLocal<TEvReportMetricsRequest, EvReportMetricsRequest> {
        };

        struct TEvListEndpointsRequest : public TEventLocal<TEvListEndpointsRequest, EvListEndpointsRequest> {
            TEvListEndpointsRequest(const TString& endpoint, const TString& database)
                : Endpoint(endpoint)
                , Database(database)
            {}

            TString Endpoint;
            TString Database;
        };

        struct TEvListEndpointsResponse : public TEventLocal<TEvListEndpointsResponse, EvListEndpointsResponse> {
             std::unique_ptr<Ydb::Discovery::ListEndpointsResponse> Record;
             std::shared_ptr<NYdbGrpc::TGrpcStatus> Status;
        };

        struct TEvCounter : public TEventLocal<TEvCounter, EvCounter> {
            i64 Delta;
            bool Additive;
            bool Derivative;
            std::vector<std::pair<TString, TString>> Labels;

            TEvCounter(const i64 delta, const bool additive, const bool derivative, const TVector<std::pair<TString, TString>> labels)
            : Delta(delta)
            , Additive(additive)
            , Derivative(derivative)
            , Labels(labels)
            {}
        };

        struct TEvHistCounter : public TEventLocal<TEvHistCounter, EvHistCounter> {
            i64 Value;
            ui64 Count;
            std::vector<std::pair<TString, TString>> Labels;

            TEvHistCounter(const i64 value, const ui64 count, const TVector<std::pair<TString, TString>> labels)
            : Value(value)
            , Count(count)
            , Labels(labels)
            {}
        };

        struct TEvToken : public TEventLocal<TEvToken, EvToken> {
            TString ServiceAccountId;
            TString IamToken;

            TString SerializedUserToken;

            TDatabase Database;

            TEvToken(const TString& serviceAccountId, const TString& iamToken, const TString& serializedUserToken, const TDatabase& database)
            : ServiceAccountId(serviceAccountId)
            , IamToken(iamToken)
            , SerializedUserToken(serializedUserToken)
            , Database(database)
            {}
        };

        struct TEvClientReady : public TEventLocal<TEvClientReady, EvClientReady> {
            TEvClientReady() {}
        };

        struct TEvErrorWithIssue : public TEventLocal<TEvErrorWithIssue, EvErrorWithIssue> {
            NYdb::EStatus Status;
            size_t IssueCode;
            TString Response;
            TDatabase Database;

            TEvErrorWithIssue(const NYdb::EStatus status, const TString& response, const TDatabase& database, size_t issueCode)
            : Status(status)
            , IssueCode(issueCode)
            , Response(response)
            , Database(database)
            {}
        };
    };

    enum TEv {
        EvYmqCloudAuthResponse
    };

    struct TEvYmqCloudAuthResponse: public TEventLocal<
            TEvYmqCloudAuthResponse,
            EvYmqCloudAuthResponse> {
        struct TError {
            TString ErrorCode;
            ui32 HttpStatusCode;
            TString Message;
        };

        bool IsSuccess;

        TString CloudId;
        TString FolderId;
        TString Sid;

        TMaybe<TError> Error;

        TEvYmqCloudAuthResponse(const TString& cloudId, const TString& folderId, const TString& sid)
            : IsSuccess(true)
            , CloudId(cloudId)
            , FolderId(folderId)
            , Sid(sid)
            , Error(Nothing())
            {}

        TEvYmqCloudAuthResponse(TError& error)
            : IsSuccess(false)
            , Error(error)
            {}
    };

    inline TActorId MakeAccessServiceID() {
        static const char x[12] = "accss_srvce";
        return TActorId(0, TStringBuf(x, 12));
    }

    inline TActorId MakeTenantDiscoveryID() {
        static const char x[12] = "tenant_disc";
        return TActorId(0, TStringBuf(x, 12));
    }

    inline TActorId MakeIamTokenServiceID() {
        static const char x[12] = "iamtokensvc";
        return TActorId(0, TStringBuf(x, 12));
    }

    inline TActorId MakeDiscoveryProxyID() {
        static const char x[12] = "dscoveryprx";
        return TActorId(0, TStringBuf(x, 12));
    }

    inline TActorId MakeMetricsServiceID() {
        static const char x[12] = "monitoring ";
        return TActorId(0, TStringBuf(x, 12));
    }

    inline TActorId MakeHttpServerServiceID() {
        static const char x[12] = "http_server";
        return TActorId(0, TStringBuf(x, 12));
    }

    inline TActorId MakeHttpProxyID() {
        static const char x[12] = "http_proxy ";
        return TActorId(0, TStringBuf(x, 12));
    }

    inline TActorId MakeFolderServiceID() {
        static const char x[12] = "folder_svc";
        return TActorId(0, TStringBuf(x, 12));
    }

#define LOG_SP_ERROR_S(actorCtxOrSystem, component, stream) LOG_ERROR_S(actorCtxOrSystem, component, LogPrefix() << " " << stream)
#define LOG_SP_WARN_S(actorCtxOrSystem, component, stream) LOG_WARN_S(actorCtxOrSystem, component, LogPrefix() << " " << stream)
#define LOG_SP_INFO_S(actorCtxOrSystem, component, stream) LOG_INFO_S(actorCtxOrSystem, component, LogPrefix() << " " << stream)
#define LOG_SP_DEBUG_S(actorCtxOrSystem, component, stream) LOG_DEBUG_S(actorCtxOrSystem, component, LogPrefix() << " " << stream)


} // namespace NKikimr::NHttpProxy
