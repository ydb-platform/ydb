#pragma once

#include <ydb/core/protos/grpc.pb.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/core/protos/msgbus_kv.pb.h>

#include <util/datetime/base.h>
#include <ydb/library/grpc/client/grpc_common.h>

namespace NKikimr {
    namespace NGRpcProxy {

        using TGRpcClientConfig = NYdbGrpc::TGRpcClientConfig;

        using TGrpcError = std::pair<TString, int>;

        template<typename T>
        using TSimpleCallback = std::function<void (const T&)>;

        template<typename T>
        using TCallback = std::function<void (const TGrpcError*, const T&)>;

        using TResponseCallback = TCallback<NKikimrClient::TResponse>;
        using TJSONCallback = TCallback<NKikimrClient::TJSON>;
        using TNodeRegistrationResponseCallback = TCallback<NKikimrClient::TNodeRegistrationResponse>;
        using TCmsResponseCallback = TCallback<NKikimrClient::TCmsResponse>;
        using TSqsResponseCallback = TCallback<NKikimrClient::TSqsResponse>;
        using TConsoleResponseCallback = TCallback<NKikimrClient::TConsoleResponse>;

        using TFinishCallback = std::function<void (const TGrpcError*)>;

        class TGRpcClient {
            TGRpcClientConfig Config;
            class TImpl;
            THolder<TImpl> Impl;

        public:
            TGRpcClient(const TGRpcClientConfig& config);
            ~TGRpcClient();
            const TGRpcClientConfig& GetConfig() const;
            grpc_connectivity_state GetNetworkStatus() const;

            // MiniKQL request, TResponseCallback callback (const NKikimrClient::DML& request)
            void Request(const NKikimrClient::TRequest& request, TResponseCallback callback);
            // ChooseProxy request
            void ChooseProxy(const NKikimrClient::TChooseProxyRequest& request, TResponseCallback callback);

            // Stream request
            void StreamRequest(const NKikimrClient::TRequest& request, TSimpleCallback<NKikimrClient::TResponse> process, TFinishCallback finish);

            // DML transactions
            void SchemeOperation(const NKikimrClient::TSchemeOperation& request, TResponseCallback callback);
            // status polling for scheme transactions
            void SchemeOperationStatus(const NKikimrClient::TSchemeOperationStatus& request, TResponseCallback callback);
            // describe
            void SchemeDescribe(const NKikimrClient::TSchemeDescribe& request, TResponseCallback callback);

            /////////////////////////////////////////////////////////////////////////////////////////////////
            // PERSISTENT QUEUE CLIENT INTERFACE
            /////////////////////////////////////////////////////////////////////////////////////////////////
            void PersQueueRequest(const NKikimrClient::TPersQueueRequest& request, TResponseCallback callback);

            /////////////////////////////////////////////////////////////////////////////////////////////////
            // ADMIN INTERNAL INTERFACE
            /////////////////////////////////////////////////////////////////////////////////////////////////
            void SchemeInitRoot(const NKikimrClient::TSchemeInitRoot& request, TResponseCallback callback);
            void BlobStorageConfig(const NKikimrClient::TBlobStorageConfigRequest& request, TResponseCallback callback);

            void ResolveNode(const NKikimrClient::TResolveNodeRequest& request, TResponseCallback callback);

            /////////////////////////////////////////////////////////////////////////////////////////////////
            // KV-TABLET INTERNAL INTERFACE
            /////////////////////////////////////////////////////////////////////////////////////////////////
            void HiveCreateTablet(const NKikimrClient::THiveCreateTablet& request, TResponseCallback callback);
            void LocalEnumerateTablets(const NKikimrClient::TLocalEnumerateTablets& request, TResponseCallback callback);
            void KeyValue(const NKikimrClient::TKeyValueRequest& request, TResponseCallback callback);

            /////////////////////////////////////////////////////////////////////////////////////////////////
            // DYNAMIC NODES INTERNAL INTERFACE
            /////////////////////////////////////////////////////////////////////////////////////////////////
            void RegisterNode(const NKikimrClient::TNodeRegistrationRequest& request, TNodeRegistrationResponseCallback callback);

            /////////////////////////////////////////////////////////////////////////////////////////////////
            // CMS INTERFACE
            /////////////////////////////////////////////////////////////////////////////////////////////////
            void CmsRequest(const NKikimrClient::TCmsRequest& request, TCmsResponseCallback callback);

            /////////////////////////////////////////////////////////////////////////////////////////////////
            // SQS INTERFACE
            /////////////////////////////////////////////////////////////////////////////////////////////////
            void SqsRequest(const NKikimrClient::TSqsRequest& request, TSqsResponseCallback callback);

            /////////////////////////////////////////////////////////////////////////////////////////////////
            // CONSOLE INTERFACE
            /////////////////////////////////////////////////////////////////////////////////////////////////
            void ConsoleRequest(const NKikimrClient::TConsoleRequest& request, TConsoleResponseCallback callback);

            /////////////////////////////////////////////////////////////////////////////////////////////////
            // INTROSPECTION
            /////////////////////////////////////////////////////////////////////////////////////////////////
            void LocalMKQL(const NKikimrClient::TLocalMKQL& request, TResponseCallback callback);
            void LocalSchemeTx(const NKikimrClient::TLocalSchemeTx& request, TResponseCallback callback);
            void TabletKillRequest(const NKikimrClient::TTabletKillRequest& request, TResponseCallback callback);
            void InterconnectDebug(const NKikimrClient::TInterconnectDebug& request, TResponseCallback callback);

            /////////////////////////////////////////////////////////////////////////////////////////////////
            // MONITORING
            /////////////////////////////////////////////////////////////////////////////////////////////////
            void TabletStateRequest(const NKikimrClient::TTabletStateRequest& request, TResponseCallback callback);

            void FillNode(const NKikimrClient::TFillNodeRequest& request, TResponseCallback callback);
            void DrainNode(const NKikimrClient::TDrainNodeRequest& request, TResponseCallback callback);
        };

    } // NGRpcProxy
} // NKikimr
