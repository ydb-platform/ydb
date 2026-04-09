#include "scheme_int.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/kafka_proxy/kafka_constants.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/util/proto_duration.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <library/cpp/digest/md5/md5.h>

namespace NKikimr::NPQ::NScheme {

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
    auto pqConfig = AppData()->PQConfig;

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
    auto pqConfig = AppData()->PQConfig;

    size_t consumerCount = NPQ::ConsumerCount(config);
    if (consumerCount > MAX_READ_RULES_COUNT) {
        return {Ydb::StatusIds::BAD_REQUEST,
            TStringBuilder() << "read rules count cannot be more than " << MAX_READ_RULES_COUNT << ", provided " << consumerCount};
    }

    THashSet<TString> readRuleConsumers;
    for (auto consumer : config.GetConsumers()) {
        if (readRuleConsumers.find(consumer.GetName()) != readRuleConsumers.end()) {
            return {operation == EOperation::Alter ? Ydb::StatusIds::ALREADY_EXISTS : Ydb::StatusIds::BAD_REQUEST,
                TStringBuilder() << "Duplicate consumer name " << consumer.GetName()};
        }
        readRuleConsumers.insert(consumer.GetName());

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


std::expected<TDuration, TString> ConvertPositiveDuration(const google::protobuf::Duration& duration) {
    if (duration.seconds() < 0) {
        return std::unexpected(TStringBuilder() << "duration seconds cannot be negative, provided " << duration.seconds());
    }
    return NKikimr::GetDuration(duration);
}

std::expected<i32, TString> CheckRetentionPeriod(auto seconds) {
    if (std::cmp_greater(seconds, Max<i32>())) {
        return std::unexpected{"retention_period must be less than " + ToString(ui64(Max<i32>()) + 1)};
    } else if (std::cmp_less_equal(seconds, 0)) {
        return std::unexpected{"retention_period must be positive"};
    }
    return seconds;
}

std::expected<std::optional<TDuration>, TResult> ConvertConsumerAvailabilityPeriod(const google::protobuf::Duration& duration, std::string_view consumerName) {
    if (auto val = ConvertPositiveDuration(duration); val.has_value()) {
        if (val.value() == TDuration::Zero()) {
            return std::nullopt;
        } else {
            return val.value();
        }
    } else {
        return std::unexpected(TResult{Ydb::StatusIds::BAD_REQUEST,
            TStringBuilder() << "Invalid availability_period for consumer '" << consumerName << "': " << val.error()
        });
    }
}

TResult FillMeteringMode(
    NKikimrPQ::TPQTabletConfig& config,
    Ydb::Topic::MeteringMode mode, 
    EOperation operation)
{
    bool meteringEnabled = AppData()->PQConfig.GetBillingMeteringConfig().GetEnabled();
    if (meteringEnabled) {
        switch (mode) {
            case Ydb::Topic::METERING_MODE_UNSPECIFIED:
                if (operation == EOperation::Create) {
                    config.SetMeteringMode(NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS);
                }
                break;
            case Ydb::Topic::METERING_MODE_REQUEST_UNITS:
                config.SetMeteringMode(NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS);
                break;
            case Ydb::Topic::METERING_MODE_RESERVED_CAPACITY:
                config.SetMeteringMode(NKikimrPQ::TPQTabletConfig::METERING_MODE_RESERVED_CAPACITY);
                break;
            default:
                return {Ydb::StatusIds::BAD_REQUEST, "Unknown metering mode"};
        }
    } else {
        switch (mode) {
            case Ydb::Topic::METERING_MODE_UNSPECIFIED:
                break;
            default:
                return {Ydb::StatusIds::PRECONDITION_FAILED, "Metering mode can only be specified in a serverless database"};
        }
    }

    return TResult();
}

TResult ProcessTopicAttributes(
    const ::google::protobuf::Map<TProtoStringType, TProtoStringType>& attributes,
    NKikimrSchemeOp::TPersQueueGroupDescription* config,
    const EOperation operation,
    const bool topicsAreFirstClassCitizen,
    NGRpcProxy::V1::TConsumersAdvancedMonitoringSettings& consumersAdvancedMonitoringSettings // out parameter
) {

    auto tabletConfig = config->MutablePQTabletConfig();
    auto partConfig = tabletConfig->MutablePartitionConfig();

    for (const auto& [attrName, attrValue] : attributes) {
        if (attrName == "_partitions_per_tablet") {
            try {
                if (operation == EOperation::Create) {
                    config->SetPartitionPerTablet(FromString<ui32>(attrValue));
                }
                if (config->GetPartitionPerTablet() > 20) {
                    return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Attribute partitions_per_tablet is " << attrValue << ", which is greater than 20"};
                }
            } catch(...) {
                return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Attribute partitions_per_tablet is " << attrValue << ", which is not ui32"};
            }
        } else if (attrName == "_allow_unauthenticated_read") {
            if (attrValue.empty()) {
                tabletConfig->SetRequireAuthRead(true);
            } else  {
                try {
                    tabletConfig->SetRequireAuthRead(!FromString<bool>(attrValue));
                } catch(...) {
                    return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Attribute allow_unauthenticated_read is " << attrValue << ", which is not bool"};
                }
            }
        } else if (attrName == "_allow_unauthenticated_write") {
            if (attrValue.empty()) {
                tabletConfig->SetRequireAuthWrite(true);
            } else  {
                try {
                    tabletConfig->SetRequireAuthWrite(!FromString<bool>(attrValue));
                } catch(...) {
                    return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Attribute allow_unauthenticated_write is " << attrValue << ", which is not bool"};
                }
            }
        } else if (attrName == "_abc_slug") {
            tabletConfig->SetAbcSlug(attrValue);
        }  else if (attrName == "_federation_account") {
            tabletConfig->SetFederationAccount(attrValue);
        } else if (attrName == "_abc_id") {
            if (attrValue.empty()) {
                tabletConfig->SetAbcId(0);
            } else {
                try {
                    tabletConfig->SetAbcId(FromString<ui32>(attrValue));
                } catch(...) {
                    return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Attribute abc_id is " << attrValue << ", which is not integer"};
                }
            }
        } else if (attrName == "_max_partition_storage_size") {
            if (attrValue.empty()) {
                partConfig->SetMaxSizeInPartition(Max<i64>());
            } else {
                try {
                    i64 size = FromString<i64>(attrValue);
                    if (size < 0) {
                        return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "_max_partiton_strorage_size can't be negative, provided " << size};
                    }

                    partConfig->SetMaxSizeInPartition(size ? size : Max<i64>());
                } catch(...) {
                    return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Attribute _max_partition_storage_size is " << attrValue << ", which is not ui64"};
                }
            }
        }  else if (attrName == "_message_group_seqno_retention_period_ms") {
            partConfig->SetSourceIdLifetimeSeconds(NKikimrPQ::TPartitionConfig().GetSourceIdLifetimeSeconds());
            if (!attrValue.empty()) {
                try {
                    i64 ms = FromString<i64>(attrValue);
                    if (ms < 0) {
                        return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "_message_group_seqno_retention_period_ms can't be negative, provided " << ms};
                    }

                    if (ms > DEFAULT_MAX_DATABASE_MESSAGEGROUP_SEQNO_RETENTION_PERIOD_MS) {
                        return {Ydb::StatusIds::BAD_REQUEST,
                            TStringBuilder() << "message_group_seqno_retention_period_ms (provided " << ms <<
                            ") must be less then default limit for database " <<
                            DEFAULT_MAX_DATABASE_MESSAGEGROUP_SEQNO_RETENTION_PERIOD_MS};
                    }
                    if (ms > 0) {
                        partConfig->SetSourceIdLifetimeSeconds(ms > 999 ? ms / 1000 : 1);
                    }
                } catch(...) {
                    return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Attribute " << attrName << " is " << attrValue << ", which is not ui64"};
                }
            }

        } else if (attrName == "_max_partition_message_groups_seqno_stored") {
            partConfig->SetSourceIdMaxCounts(NKikimrPQ::TPartitionConfig().GetSourceIdMaxCounts());
            if (!attrValue.empty()) {
                try {
                    i64 count = FromString<i64>(attrValue);
                    if (count < 0) {
                        return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << attrName << " can't be negative, provided " << count};
                    }
                    if (count > 0) {
                        partConfig->SetSourceIdMaxCounts(count);
                    }
                } catch(...) {
                    return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Attribute " << attrName << " is " << attrValue << ", which is not ui64"};
                }
            }
        } else if (attrName == "_cleanup_policy") {
            tabletConfig->SetEnableCompactification(attrValue == "compact");
        } else if (attrName == "_timestamp_type") {
            if (!attrValue || attrValue == NKafka::MESSAGE_TIMESTAMP_CREATE_TIME || attrValue == NKafka::MESSAGE_TIMESTAMP_LOG_APPEND) {
                tabletConfig->SetTimestampType(attrValue ? attrValue :  NKafka::MESSAGE_TIMESTAMP_CREATE_TIME);
            } else {
                return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Attribute " << attrName << " is " << attrValue << ", which is an incorrect value."};
            }
        } else if (attrName == "_advanced_monitoring") {
            if (topicsAreFirstClassCitizen) {
                return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Attribute " << attrName << " is not supported in non-federation"};
            }
            if (std::expected m = NGRpcProxy::V1::TConsumersAdvancedMonitoringSettings::FromJson(attrValue); m.has_value()) {
                consumersAdvancedMonitoringSettings = std::move(m).value();
            } else {
                return {Ydb::StatusIds::BAD_REQUEST, std::move(m).error()};
            }
        } else {
            return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Attribute " << attrName << " is not supported"};
        }
    }

    return TResult();
}

TClientServiceTypes GetSupportedClientServiceTypes() {
    auto pqConfig = AppData()->PQConfig;

    TClientServiceTypes serviceTypes;
    ui32 count = pqConfig.GetDefaultClientServiceType().GetMaxReadRulesCountPerTopic();
    if (count == 0) count = Max<ui32>();
    TString name = pqConfig.GetDefaultClientServiceType().GetName();
    TVector<TString> passwordHashes;
    for (auto ph : pqConfig.GetDefaultClientServiceType().GetPasswordHashes()) {
        passwordHashes.push_back(ph);
    }

    serviceTypes.insert({name, {name, count, passwordHashes}});

    for (const auto& serviceType : pqConfig.GetClientServiceType()) {
        ui32 count = serviceType.GetMaxReadRulesCountPerTopic();
        if (count == 0) count = Max<ui32>();
        TString name = serviceType.GetName();
        TVector<TString> passwordHashes;
        for (auto ph : serviceType.GetPasswordHashes()) {
            passwordHashes.push_back(ph);
        }

        serviceTypes.insert({name, {name, count, passwordHashes}});
    }

    return serviceTypes;
}

TResult ProcessAddConsumer(
    NKikimrPQ::TPQTabletConfig* config,
    const Ydb::Topic::Consumer& consumerConfig,
    const TClientServiceTypes& supportedClientServiceTypes,
    const bool checkServiceType,
    NGRpcProxy::V1::TConsumersAdvancedMonitoringSettings* consumersAdvancedMonitoringSettings
) {
    auto enableTopicDiskSubDomainQuota = AppData()->FeatureFlags.GetEnableTopicDiskSubDomainQuota();
    auto pqConfig = AppData()->PQConfig;

    auto consumerName = NPersQueue::ConvertNewConsumerName(consumerConfig.name(), pqConfig);
    if (consumerName.find("/") != TString::npos || consumerName.find("|") != TString::npos) {
        return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "consumer '" << consumerConfig.name() << "' has illegal symbols"};
    }
    if (consumerName.empty()) {
        return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "consumer with empty name is forbidden"};
    }

    ::NKikimrPQ::TPQTabletConfig_TConsumer* consumer = config->AddConsumers();

    consumer->SetName(consumerName);

    if (consumerConfig.has_shared_consumer_type()) {
        if (!AppData()->FeatureFlags.GetEnableTopicMessageLevelParallelism()) {
            return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "shared consumers is disabled"};
        }
        consumer->SetType(::NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);

        consumer->SetKeepMessageOrder(consumerConfig.shared_consumer_type().keep_messages_order());
        consumer->SetDefaultProcessingTimeoutSeconds(consumerConfig.shared_consumer_type().default_processing_timeout().seconds());

        consumer->SetDeadLetterPolicyEnabled(consumerConfig.shared_consumer_type().dead_letter_policy().enabled());
        consumer->SetMaxProcessingAttempts(consumerConfig.shared_consumer_type().dead_letter_policy().condition().max_processing_attempts());

        consumer->SetDefaultDelayMessageTimeMs(consumerConfig.shared_consumer_type().receive_message_delay().seconds() * 1'000 + consumerConfig.shared_consumer_type().receive_message_delay().nanos() / 1'000'000);
        consumer->SetDefaultReceiveMessageWaitTimeMs(consumerConfig.shared_consumer_type().receive_message_wait_time().seconds() * 1'000 + consumerConfig.shared_consumer_type().receive_message_wait_time().nanos() / 1'000'000);

        if (consumerConfig.shared_consumer_type().dead_letter_policy().has_move_action()) {
            consumer->SetDeadLetterPolicy(::NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
            consumer->SetDeadLetterQueue(consumerConfig.shared_consumer_type().dead_letter_policy().move_action().dead_letter_queue());
        } else if (consumerConfig.shared_consumer_type().dead_letter_policy().has_delete_action()) {
            consumer->SetDeadLetterPolicy(::NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_DELETE);
        } else {
            consumer->SetDeadLetterPolicy(::NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_UNSPECIFIED);
        }
    } else {
        consumer->SetType(::NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING);
    }

    if (consumerConfig.read_from().seconds() < 0) {
        return {Ydb::StatusIds::BAD_REQUEST,
            TStringBuilder() << "starting_message_timestamp_ms in read_rule can't be negative, provided " << consumerConfig.read_from().seconds()};
    }
    consumer->SetReadFromTimestampsMs(consumerConfig.read_from().seconds() * 1000);
    consumer->SetFormatVersion(0);

    const auto& defaultClientServiceType = pqConfig.GetDefaultClientServiceType().GetName();
    TString serviceType = defaultClientServiceType;

    TString passwordHash = "";
    bool hasPassword = false;

    ui32 version = 0;
    for (const auto& [attrName, attrValue] : consumerConfig.attributes()) {
        if (attrName == "_version") {
            try {
                if (!attrValue.empty())
                    version = FromString<ui32>(attrValue);
            } catch(...) {
                return {Ydb::StatusIds::BAD_REQUEST,
                    TStringBuilder() << "Attribute for consumer '" << consumerConfig.name() << "' _version is " << attrValue << ", which is not ui32"};
            }
        } else if (attrName == "_service_type") {
            if (!attrValue.empty()) {
                if (!supportedClientServiceTypes.contains(attrValue)) {
                    return {Ydb::StatusIds::BAD_REQUEST, 
                        TStringBuilder() << "Unknown _service_type '" << attrValue << "' for consumer '" << consumerConfig.name() << "'"};
                }
                serviceType = attrValue;
            }
        } else if (attrName == "_service_type_password") {
            passwordHash = MD5::Data(attrValue);
            passwordHash.to_lower();
            hasPassword = true;
        }
    }
    if (serviceType.empty()) {
        return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "service type cannot be empty for consumer '" << consumerConfig.name() << "'"};
    }

    Y_ABORT_UNLESS(supportedClientServiceTypes.find(serviceType) != supportedClientServiceTypes.end());

    const TClientServiceType& clientServiceType = supportedClientServiceTypes.find(serviceType)->second;

    if (checkServiceType) {
        bool found = clientServiceType.PasswordHashes.empty() && !hasPassword;
        for (auto ph : clientServiceType.PasswordHashes) {
            if (ph == passwordHash) {
                found = true;
            }
        }
        if (!found) {
            if (hasPassword) {
                return {Ydb::StatusIds::BAD_REQUEST, "incorrect client service type password"};
            }
            if (pqConfig.GetForceClientServiceTypePasswordCheck()) { // no password and check is required
                return {Ydb::StatusIds::BAD_REQUEST, "no client service type password provided"};
            }
        }
    }

    consumer->SetServiceType(serviceType);
    consumer->SetVersion(version);

    auto* cct = consumer->MutableCodec();

    for(const auto& codec : consumerConfig.supported_codecs().codecs()) {
        if ((!Ydb::Topic::Codec_IsValid(codec) && codec < Ydb::Topic::CODEC_CUSTOM) || codec == 0) {
            return {Ydb::StatusIds::BAD_REQUEST,
                TStringBuilder() << "Unknown codec for consumer '" << consumerConfig.name() << "' with value " << codec};
        }
        cct->AddIds(codec - 1);
        cct->AddCodecs(Ydb::Topic::Codec_IsValid(codec) ? LegacySubstr(to_lower(Ydb::Topic::Codec_Name((Ydb::Topic::Codec)codec)), 6) : "CUSTOM");
    }

    if (consumerConfig.important()) {
        if (pqConfig.GetTopicsAreFirstClassCitizen() && !enableTopicDiskSubDomainQuota) {
            return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "important flag is forbiden for consumer " << consumerConfig.name()};
        }
        consumer->SetImportant(true);
    }
    if (auto period = ConvertConsumerAvailabilityPeriod(consumerConfig.availability_period(), consumerConfig.name()); period.has_value()) {
        if (period.value().has_value()) {
            consumer->SetAvailabilityPeriodMs(period.value()->MilliSeconds());
        } else {
            consumer->ClearAvailabilityPeriodMs();
        }
    } else {
        return period.error();
    }

    if (consumersAdvancedMonitoringSettings) {
        consumersAdvancedMonitoringSettings->UpdateConsumerConfig(consumerConfig.name(), *consumer);
    }

    return TResult();
}


    
} // namespace NKikimr::NPQ::NScheme
