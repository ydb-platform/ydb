#include "yql_s3_source_factory.h"

#include <util/system/platform.h>
#if defined(_linux_) || defined(_darwin_)
#include "yql_s3_read_actor.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/udfs/common/clickhouse/client/src/Formats/registerFormats.h>
#endif

namespace NYql::NDq {

void RegisterS3ReadActorFactory(
        TDqAsyncIoFactory& factory,
        ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
        IHTTPGateway::TPtr gateway,
        const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
        const TS3ReadActorFactoryConfig& cfg,
        ::NMonitoring::TDynamicCounterPtr counters) {
#if defined(_linux_) || defined(_darwin_)
    NDB::registerFormats();
    factory.RegisterSource<NS3::TSource>("S3Source",
        [credentialsFactory, gateway, retryPolicy, cfg, counters](NS3::TSource&& settings, IDqAsyncIoFactory::TSourceArguments&& args) {
            return CreateS3ReadActor(args.TypeEnv, args.HolderFactory, gateway,
                std::move(settings), args.InputIndex, args.StatsLevel, args.TxId, args.SecureParams,
                args.TaskParams, args.ReadRanges, args.ComputeActorId, credentialsFactory, retryPolicy, cfg,
                counters, args.TaskCounters, args.MemoryQuotaManager);
        });
#else
    Y_UNUSED(factory);
    Y_UNUSED(credentialsFactory);
    Y_UNUSED(gateway);
    Y_UNUSED(counters);
#endif
}

TS3ReadActorFactoryConfig CreateReadActorFactoryConfig(const ::NYql::TS3GatewayConfig& s3Config) {
    TS3ReadActorFactoryConfig s3ReadActoryConfig;
    if (const ui64 rowsInBatch = s3Config.GetRowsInBatch()) {
        s3ReadActoryConfig.RowsInBatch = rowsInBatch;
    }
    if (const ui64 maxInflight = s3Config.GetMaxInflight()) {
        s3ReadActoryConfig.MaxInflight = maxInflight;
    }
    if (const ui64 dataInflight = s3Config.GetDataInflight()) {
        s3ReadActoryConfig.DataInflight = dataInflight;
    }
    for (auto& formatSizeLimit: s3Config.GetFormatSizeLimit()) {
        if (formatSizeLimit.GetName()) { // ignore unnamed limits
            s3ReadActoryConfig.FormatSizeLimits.emplace(
                formatSizeLimit.GetName(), formatSizeLimit.GetFileSizeLimit());
        }
    }
    if (s3Config.HasFileSizeLimit()) {
        s3ReadActoryConfig.FileSizeLimit = s3Config.GetFileSizeLimit();
    }
    if (s3Config.HasBlockFileSizeLimit()) {
        s3ReadActoryConfig.BlockFileSizeLimit = s3Config.GetBlockFileSizeLimit();
    }
    return s3ReadActoryConfig;
}

}
