#pragma once

#include <ydb/core/fq/libs/protos/dq_effects.pb.h>
#include <ydb/library/yql/providers/s3/proto/retry_config.pb.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_default_retry_policy.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <util/generic/size_literals.h>


namespace NYql::NDq {

    struct TS3ReadActorFactoryConfig {
        ui64 RowsInBatch = 1000;
        ui64 MaxInflight = 20;
        ui64 DataInflight = 200_MB;
        ui64 FileSizeLimit = 2_GB;
        ui64 BlockFileSizeLimit = 50_GB;
        std::unordered_map<TString, ui64> FormatSizeLimits;
    };

    class IS3ActorsFactory {
    public:
        virtual ~IS3ActorsFactory() = default;
        
        virtual THolder<NActors::IActor> CreateS3ApplicatorActor(
            NActors::TActorId parentId,
            IHTTPGateway::TPtr gateway,
            const TString& queryId,
            const TString& jobId,
            std::optional<ui32> restartNumber,
            bool commit,
            const THashMap<TString, TString>& secureParams,
            ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
            const NYql::NDqProto::TExternalEffect& externalEffect) = 0;

        virtual void RegisterS3WriteActorFactory(
            TDqAsyncIoFactory& factory,
            ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
            IHTTPGateway::TPtr gateway,
            const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy) = 0;

        virtual void RegisterS3ReadActorFactory(
            TDqAsyncIoFactory& factory,
            ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
            IHTTPGateway::TPtr gateway,
            const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
            const TS3ReadActorFactoryConfig& cfg = {},
            ::NMonitoring::TDynamicCounterPtr counters = nullptr) = 0;
    };

    std::shared_ptr<IS3ActorsFactory> CreateDefaultS3ActorsFactory();

    TS3ReadActorFactoryConfig CreateReadActorFactoryConfig(const ::NYql::TS3GatewayConfig& s3Config);
}
