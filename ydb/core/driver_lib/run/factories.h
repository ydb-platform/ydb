#pragma once
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_util_devicemode.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/tx/datashard/export_iface.h>
#include <ydb/core/persqueue/actor_persqueue_client_iface.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/base/grpc_service_factory.h>
#include <ydb/core/security/ticket_parser_settings.h>

#include <ydb/core/ymq/actor/auth_factory.h>
#include <ydb/core/http_proxy/auth_factory.h>

#include <ydb/library/folder_service/folder_service.h>
#include <ydb/library/folder_service/proto/config.pb.h>
#include <ydb/library/pdisk_io/aio.h>
#include <ydb/core/fq/libs/config/protos/audit.pb.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/providers/pq/cm_client/client.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/wilson/wilson_uploader.h>

#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace NKikimr {

// A way to parameterize YDB binary, we do it via a set of factories
struct TModuleFactories {
    // A backend factory for Query Replay
    std::shared_ptr<NKqp::IQueryReplayBackendFactory> QueryReplayBackendFactory;
    //
    std::shared_ptr<NMsgBusProxy::IPersQueueGetReadSessionsInfoWorkerFactory> PQReadSessionsInfoWorkerFactory;
    // Can be nullptr. In that case there would be no ability to work with internal configuration manager.
    NPq::NConfigurationManager::IConnections::TPtr PqCmConnections;
    // Export implementation for Data Shards
    std::shared_ptr<NDataShard::IExportFactory> DataShardExportFactory;
    // Factory for Simple queue services implementation details
    std::shared_ptr<NSQS::IEventsWriterFactory> SqsEventsWriterFactory;

    IActor*(*CreateTicketParser)(const TTicketParserSettings&);
    IActor*(*FolderServiceFactory)(const NKikimrProto::NFolderService::TFolderServiceConfig&);

    // Factory for grpc services
    TGrpcServiceFactory GrpcServiceFactory;

    std::shared_ptr<NPQ::IPersQueueMirrorReaderFactory> PersQueueMirrorReaderFactory;
    /// Factory for pdisk's aio engines
    std::shared_ptr<NPDisk::IIoContextFactory> IoContextFactory;

    std::function<NActors::TMon* (NActors::TMon::TConfig, const NKikimrConfig::TAppConfig& appConfig)> MonitoringFactory;
    std::shared_ptr<NSQS::IAuthFactory> SqsAuthFactory;

    std::shared_ptr<NHttpProxy::IAuthFactory> DataStreamsAuthFactory;
    std::vector<NKikimr::NMiniKQL::TComputationNodeFactory> AdditionalComputationNodeFactories;

    std::unique_ptr<NWilson::IGrpcSigner>(*WilsonGrpcSignerFactory)(const NKikimrConfig::TTracingConfig::TBackendConfig::TAuthConfig&);

    ~TModuleFactories();
};

} // NKikimr
