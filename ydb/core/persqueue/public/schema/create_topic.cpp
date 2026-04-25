#include "create_topic_operation.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/ydb_convert/topic_description.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

namespace NKikimr::NPQ::NSchema {

namespace {

TResult ApplyChangesInt(
    const TString& database,
    const Ydb::Topic::CreateTopicRequest& request,
    NKikimrSchemeOp::TModifyScheme& modifyScheme,
    NKikimrSchemeOp::TPersQueueGroupDescription& targetConfig,
    const TString& localDc
) {
    const auto* appData = AppData();
    const auto& pqConfig = appData->PQConfig;

    const auto path = modifyScheme.GetWorkingDir();

    TString error;

    const auto& name = targetConfig.GetName();
    ui32 minParts = 1;

    auto pqTabletConfig = targetConfig.MutablePQTabletConfig();
    auto partConfig = pqTabletConfig->MutablePartitionConfig();

    if (request.retention_storage_mb())
        partConfig->SetStorageLimitBytes(request.retention_storage_mb() * 1024 * 1024);

    if (request.has_partitioning_settings()) {
        const auto& settings = request.partitioning_settings();
        if (settings.min_active_partitions() < 0) {
            error = TStringBuilder() << "Partitions count must be positive, provided " << settings.min_active_partitions();
            return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
        }
        if (settings.min_active_partitions() >= Max<ui32>()) {
            error = TStringBuilder() << "Partitions count must be less than " << Max<ui32>() << ", provided " << settings.min_active_partitions();
            return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
        }
        minParts = std::max<ui32>(1, settings.min_active_partitions());
        if (request.partitioning_settings().has_auto_partitioning_settings() &&
            request.partitioning_settings().auto_partitioning_settings().strategy() != ::Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_DISABLED) {

            auto pqTabletConfigPartStrategy = pqTabletConfig->MutablePartitionStrategy();
            auto autoscaleSettings = settings.auto_partitioning_settings();
            pqTabletConfigPartStrategy->SetMinPartitionCount(minParts);
            pqTabletConfigPartStrategy->SetMaxPartitionCount(IfEqualThenDefault<int64_t>(settings.max_active_partitions(),0L,minParts));
            pqTabletConfigPartStrategy->SetScaleUpPartitionWriteSpeedThresholdPercent(IfEqualThenDefault(autoscaleSettings.partition_write_speed().up_utilization_percent(), 0, 90));
            pqTabletConfigPartStrategy->SetScaleDownPartitionWriteSpeedThresholdPercent(IfEqualThenDefault(autoscaleSettings.partition_write_speed().down_utilization_percent(), 0, 30));
            pqTabletConfigPartStrategy->SetScaleThresholdSeconds(IfEqualThenDefault<int64_t>(autoscaleSettings.partition_write_speed().stabilization_window().seconds(), 0L, 300L));
            switch (autoscaleSettings.strategy()) {
                case ::Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP:
                    pqTabletConfigPartStrategy->SetPartitionStrategyType(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT);
                    break;
                case ::Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN:
                    pqTabletConfigPartStrategy->SetPartitionStrategyType(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT_AND_MERGE);
                    break;
                case ::Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_PAUSED:
                    pqTabletConfigPartStrategy->SetPartitionStrategyType(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_PAUSED);
                    break;
                default:
                    pqTabletConfigPartStrategy->SetPartitionStrategyType(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_DISABLED);
                    break;
            }
            if (auto result = ValidatePartitionStrategy(*pqTabletConfig); !result) {
                return result;
            }
        }
    }
    targetConfig.SetTotalGroupCount(minParts);
    pqTabletConfig->SetRequireAuthWrite(true);
    pqTabletConfig->SetRequireAuthRead(true);
    targetConfig.SetPartitionPerTablet(1);

    partConfig->SetMaxCountInPartition(Max<i32>());

    partConfig->SetSourceIdLifetimeSeconds(NKikimrPQ::TPartitionConfig().GetSourceIdLifetimeSeconds());
    partConfig->SetSourceIdMaxCounts(NKikimrPQ::TPartitionConfig().GetSourceIdMaxCounts());

    NGRpcProxy::V1::TConsumersAdvancedMonitoringSettings consumersAdvancedMonitoringSettings;
    auto res = ProcessTopicAttributes(request.attributes(), &targetConfig, EOperation::Create, pqConfig.GetTopicsAreFirstClassCitizen(), consumersAdvancedMonitoringSettings);
    if (!res) {
        return res;
    }

    bool local = true; // TODO: check here cluster;

    if (!pqConfig.GetTopicsAreFirstClassCitizen()) {
        auto converter = NPersQueue::TTopicNameConverter::ForFederation(
                pqConfig.GetRoot(),
                pqConfig.GetTestDatabaseRoot(),
                name,
                path,
                database,
                local,
                localDc,
                pqTabletConfig->GetFederationAccount()
        );

        if (!converter->IsValid()) {
            error = TStringBuilder() << "Bad topic: " << converter->GetReason();
            return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
        }
        pqTabletConfig->SetLocalDC(local);
        pqTabletConfig->SetDC(converter->GetCluster());
        pqTabletConfig->SetProducer(converter->GetLegacyProducer());
        pqTabletConfig->SetTopic(converter->GetLegacyLogtype());
    }

    const auto& channelProfiles = pqConfig.GetChannelProfiles();
    if (channelProfiles.size() > 2) {
        partConfig->MutableExplicitChannelProfiles()->CopyFrom(channelProfiles);
    }
    if (request.has_retention_period()) {
        if (auto retentionPeriodSeconds = CheckRetentionPeriod(request.retention_period().seconds())) {
            partConfig->SetLifetimeSeconds(retentionPeriodSeconds.value());
        } else {
            error = TStringBuilder() << retentionPeriodSeconds.error() << ", provided " <<
                    request.retention_period().DebugString();
            return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
        }
    } else {
        partConfig->SetLifetimeSeconds(TDuration::Days(1).Seconds());
    }
    if (local) {
        auto partSpeed = request.partition_write_speed_bytes_per_second();
        if (partSpeed == 0) {
            partSpeed = DEFAULT_PARTITION_SPEED;
        }
        partConfig->SetWriteSpeedInBytesPerSecond(partSpeed);

        const auto& burstSpeed = request.partition_write_burst_bytes();
        if (burstSpeed == 0) {
            partConfig->SetBurstSize(partSpeed);
        } else {
            partConfig->SetBurstSize(burstSpeed);
        }
    }
    pqTabletConfig->SetFormatVersion(0);
    pqTabletConfig->SetContentBasedDeduplication(request.content_based_deduplication());

    auto ct = pqTabletConfig->MutableCodecs();
    for(const auto& codec : request.supported_codecs().codecs()) {
        if ((!Ydb::Topic::Codec_IsValid(codec) && codec < Ydb::Topic::CODEC_CUSTOM) || codec == 0) {
            error = TStringBuilder() << "Unknown codec with value " << codec;
            return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
        }
        ct->AddIds(codec - 1);
        ct->AddCodecs(Ydb::Topic::Codec_IsValid(codec) ? LegacySubstr(to_lower(Ydb::Topic::Codec_Name((Ydb::Topic::Codec)codec)), 6) : "CUSTOM");
    }

    if (request.consumers_size() > MAX_READ_RULES_COUNT) {
        error = TStringBuilder() << "consumers count cannot be more than "
                                 << MAX_READ_RULES_COUNT << ", provided " << request.consumers_size();
        return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
    }

    auto result = FillMeteringMode(*pqTabletConfig, request.metering_mode(), EOperation::Create);
    if (!result) {
        return result;
    }

    const auto supportedClientServiceTypes = GetSupportedClientServiceTypes();
    for (const auto& consumer : request.consumers()) {
        auto result = AddConsumer(
            pqTabletConfig,
            consumer,
            supportedClientServiceTypes,
            true,
            &consumersAdvancedMonitoringSettings
        );
        if (!result) {
            return result;
        }
    }
    if (auto errorCode = consumersAdvancedMonitoringSettings.CheckForUnknownConsumers(error); errorCode != Ydb::StatusIds::SUCCESS) {
        return {errorCode, std::move(error)};
    }

    if (request.has_metrics_level()) {
        pqTabletConfig->SetMetricsLevel(request.metrics_level());
    }

    return {};
}


struct TCreateTopicStrategy: public ICreateTopicStrategy {
    TCreateTopicStrategy(Ydb::Topic::CreateTopicRequest&& request)
        : Request(std::move(request))
    {
    }

    const TString& GetTopicName() const override {
        return Request.path();
    }

    TResult ApplyChanges(
        const TString localCluster,
        const TString& database,
        NKikimrSchemeOp::TModifyScheme& modifyScheme,
        NKikimrSchemeOp::TPersQueueGroupDescription& targetConfig
    ) override {
        return ApplyChangesInt(database, Request, modifyScheme, targetConfig, localCluster);
    }

    Ydb::Topic::CreateTopicRequest Request;
};

} // namespace

NActors::IActor* CreateCreateTopicActor(const NActors::TActorId& parentId, TCreateTopicSettings&& settings) {
    return CreateCreateTopicOperationActor(parentId, {
        .Database = std::move(settings.Database),
        .PeerName = std::move(settings.PeerName),
        .UserToken = std::move(settings.UserToken),
        .Strategy = std::make_unique<TCreateTopicStrategy>(std::move(settings.Request)),
        .Cookie = settings.Cookie,
    });
}

} // namespace NKikimr::NPQ::NSchema
