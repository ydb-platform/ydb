#include "common.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

namespace NKikimr::NPQ::NSchema {

TResult ValidatePartitionStrategy(const ::NKikimrPQ::TPQTabletConfig& config) {
    if (!config.HasPartitionStrategy()) {
        return TResult();
    }
    auto strategy = config.GetPartitionStrategy();
    if (strategy.GetMinPartitionCount() < 0) {
        return {Ydb::StatusIds::BAD_REQUEST,
            TStringBuilder() << "Partitions count must be non-negative, provided " << strategy.GetMinPartitionCount()};
    }
    if (strategy.GetMaxPartitionCount() < 0) {
        return {Ydb::StatusIds::BAD_REQUEST,
            TStringBuilder() << "Partitions count must be non-negative, provided " << strategy.GetMaxPartitionCount()};
    }
    if (strategy.GetMaxPartitionCount() != 0 && strategy.GetMaxPartitionCount() < strategy.GetMinPartitionCount()) {
        return {Ydb::StatusIds::BAD_REQUEST,
            TStringBuilder() << "Max active partitions must be greater than or equal to partitions count or equals zero (unlimited), provided "
            << strategy.GetMaxPartitionCount() << " and " << strategy.GetMinPartitionCount()};
    }
    if (strategy.GetScaleUpPartitionWriteSpeedThresholdPercent() < 0 || strategy.GetScaleUpPartitionWriteSpeedThresholdPercent() > 100) {
        return {Ydb::StatusIds::BAD_REQUEST,
            TStringBuilder() << "Partition scale up threshold percent must be between 0 and 100, provided "
            << strategy.GetScaleUpPartitionWriteSpeedThresholdPercent()};
    }
    if (strategy.GetScaleDownPartitionWriteSpeedThresholdPercent() < 0 || strategy.GetScaleDownPartitionWriteSpeedThresholdPercent() > 100) {
        return {Ydb::StatusIds::BAD_REQUEST,
            TStringBuilder() << "Partition scale down threshold percent must be between 0 and 100, provided "
            << strategy.GetScaleDownPartitionWriteSpeedThresholdPercent()};
    }
    if (strategy.GetScaleThresholdSeconds() <= 0) {
        return {Ydb::StatusIds::BAD_REQUEST, 
            TStringBuilder() << "Partition scale threshold time must be greater then 1 second, provided "
            << strategy.GetScaleThresholdSeconds() << " seconds"};
    }
    if (config.GetPartitionConfig().HasStorageLimitBytes()) {
        return {Ydb::StatusIds::BAD_REQUEST,
            "Auto partitioning is incompatible with retention storage bytes option"};
    }

    return TResult();
}

TResult ValidateConfig(
    const NKikimrPQ::TPQTabletConfig& config,
    const TClientServiceTypes& supportedClientServiceTypes,
    const EOperation operation
) {
    const auto& pqConfig = AppData()->PQConfig;

    if (config.GetPartitionConfig().HasStorageLimitBytes() && config.GetPartitionConfig().GetStorageLimitBytes() > 0) {
        auto hasMLP = AnyOf(config.GetConsumers(), [](const auto& consumer) {
            return consumer.GetType() == ::NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP;
        });
        if (hasMLP) {
            return {Ydb::StatusIds::BAD_REQUEST, "Retention by storage size is not supported for shared consumers"};
        }
    }

    ui32 speed = config.GetPartitionConfig().GetWriteSpeedInBytesPerSecond();
    ui32 burst = config.GetPartitionConfig().GetBurstSize();

    std::set<ui32> validLimits {};
    if (pqConfig.ValidWriteSpeedLimitsKbPerSecSize() == 0) {
        validLimits.insert(speed);
    } else {
        const auto& limits = AppData()->PQConfig.GetValidWriteSpeedLimitsKbPerSec();
        for (auto& limit : limits) {
            validLimits.insert(limit * 1_KB);
        }
    }
    if (validLimits.find(speed) == validLimits.end()) {
        return {Ydb::StatusIds::BAD_REQUEST,
            TStringBuilder() << "write_speed per second in partition must have values from set {" << JoinSeq(",", validLimits) << "}, got " << speed};
    }

    if (burst > speed * 2 && burst > 1_MB) {
        return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder()
            << "Invalid write burst in partition specified: " << burst
            << " vs " << Max(speed * 2, (ui32)1_MB)};
    }

    ui32 lifeTimeSeconds = config.GetPartitionConfig().GetLifetimeSeconds();
    ui64 storageBytes = config.GetPartitionConfig().GetStorageLimitBytes();


    auto retentionLimits = AppData()->PQConfig.GetValidRetentionLimits();
    if (retentionLimits.size() == 0) {
        auto* limit = retentionLimits.Add();
        limit->SetMinPeriodSeconds(lifeTimeSeconds);
        limit->SetMaxPeriodSeconds(lifeTimeSeconds);
        limit->SetMinStorageMegabytes(storageBytes / 1_MB);
        limit->SetMaxStorageMegabytes(storageBytes / 1_MB + 1);
    }

    TStringBuilder errStr;
    errStr << "retention hours and storage megabytes must fit one of:";
    bool found = false;
    for (auto& limit : retentionLimits) {
        errStr << " { hours : [" << limit.GetMinPeriodSeconds() / 3600 << ", " << limit.GetMaxPeriodSeconds() / 3600 << "], "
                << " storage : [" << limit.GetMinStorageMegabytes() << ", " << limit.GetMaxStorageMegabytes() << "]},";
        found = found || (lifeTimeSeconds >= limit.GetMinPeriodSeconds() && lifeTimeSeconds <= limit.GetMaxPeriodSeconds() &&
                            storageBytes >= limit.GetMinStorageMegabytes() * 1_MB && storageBytes <= limit.GetMaxStorageMegabytes() * 1_MB);
    }
    if (!found) {
        errStr << " provided values: hours " << lifeTimeSeconds / 3600 << ", storage " << storageBytes / 1_MB;
        return {Ydb::StatusIds::BAD_REQUEST, std::move(errStr)};
    }

    return ValidateConsumersConfig(config, supportedClientServiceTypes, operation);
}

TResult ValidateConsumersConfig(
    const NKikimrPQ::TPQTabletConfig& config,
    const TClientServiceTypes& supportedClientServiceTypes,
    const EOperation operation
) {
    const auto& pqConfig = AppData()->PQConfig;

    size_t consumerCount = NPQ::ConsumerCount(config);
    if (consumerCount > MAX_READ_RULES_COUNT) {
        return {Ydb::StatusIds::BAD_REQUEST,
            TStringBuilder() << "read rules count cannot be more than " << MAX_READ_RULES_COUNT << ", provided " << consumerCount};
    }

    THashSet<TString> consumers;
    for (auto consumer : config.GetConsumers()) {
        if (consumers.find(consumer.GetName()) != consumers.end()) {
            return {operation == EOperation::Alter ? Ydb::StatusIds::ALREADY_EXISTS : Ydb::StatusIds::BAD_REQUEST,
                TStringBuilder() << "Duplicate consumer name " << consumer.GetName()};
        }
        consumers.insert(consumer.GetName());

        if (consumer.GetImportant() && consumer.HasAvailabilityPeriodMs()) {
            return {Ydb::StatusIds::BAD_REQUEST,
                TStringBuilder() << "Consumer '" << consumer.GetName()
                << "' has both an important flag and a limited availability_period, which are mutually exclusive"};
        }
    }

    for (const auto& t : supportedClientServiceTypes) {

        auto type = t.first;
        auto count = std::count_if(config.GetConsumers().begin(), config.GetConsumers().end(),
                    [type](const auto& c){
                        return type == c.GetServiceType();
                    });
        auto limit = t.second.MaxCount;
        if (count > limit) {
            return {Ydb::StatusIds::BAD_REQUEST,
                TStringBuilder() << "Count of consumers with service type '" << type << "' is limited for " << limit << " for stream\n"};
        }
    }
    if (config.GetCodecs().IdsSize() > 0) {
        for (const auto& consumer : config.GetConsumers()) {
            TString name = NPersQueue::ConvertOldConsumerName(consumer.GetName(), pqConfig);

            if (consumer.GetCodec().IdsSize() > 0) {
                THashSet<i64> codecs;
                for (auto& cc : consumer.GetCodec().GetIds()) {
                    codecs.insert(cc);
                }
                for (auto& cc : config.GetCodecs().GetIds()) {
                    if (codecs.find(cc) == codecs.end()) {
                        return {Ydb::StatusIds::BAD_REQUEST,
                            TStringBuilder() << "for consumer '" << name << "' got unsupported codec " << (cc+1) << " which is suppored by topic"};
                    }
                }
            }
        }
    }

    return TResult();
}
    
} // namespace NKikimr::NPQ::NSchema
