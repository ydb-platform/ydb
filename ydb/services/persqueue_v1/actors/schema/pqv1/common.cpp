#include "common.h"

#include <ydb/core/base/feature_flags.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/jwt/jwt.h>
#include <ydb/public/sdk/cpp/src/library/persqueue/obfuscate/obfuscate.h>

#include <util/string/vector.h>

namespace NKikimr::NGRpcProxy::V1::NPQv1 {

namespace {

using namespace NPQ::NSchema;

constexpr TStringBuf GRPCS_ENDPOINT_PREFIX = "grpcs://";
constexpr TStringBuf GRPC_ENDPOINT_PREFIX = "grpc://";

TResult AddConsumerImpl(
    NKikimrPQ::TPQTabletConfig* config,
    const Ydb::PersQueue::V1::TopicSettings::ReadRule& rr,
    const TConsumersAdvancedMonitoringSettings* consumersAdvancedMonitoringSettings
) {
    const auto& pqConfig = AppData()->PQConfig;
    auto consumerName = NPersQueue::ConvertNewConsumerName(rr.consumer_name(), pqConfig);
    if (consumerName.empty()) {
        return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "consumer with empty name is forbidden"};
    }
    if(consumerName.find("/") != TString::npos || consumerName.find("|") != TString::npos) {
        return {Ydb::StatusIds::BAD_REQUEST, 
            TStringBuilder() << "consumer '" << rr.consumer_name() << "' has illegal symbols"};
    }
    if (consumerName == NPQ::CLIENTID_COMPACTION_CONSUMER && !config->GetEnableCompactification()) {
        return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "cannot add service consumer '" << consumerName 
            << " to a topic without compactification enabled"};
    }

    auto* consumer = config->AddConsumers();

    consumer->SetName(consumerName);

    if (rr.starting_message_timestamp_ms() < 0) {
        return {Ydb::StatusIds::BAD_REQUEST, 
            TStringBuilder() << "starting_message_timestamp_ms in read_rule can't be negative, provided " << rr.starting_message_timestamp_ms()};
    }
    consumer->SetReadFromTimestampsMs(rr.starting_message_timestamp_ms());

    if (!Ydb::PersQueue::V1::TopicSettings::Format_IsValid((int)rr.supported_format()) || rr.supported_format() == 0) {
        return {Ydb::StatusIds::BAD_REQUEST, 
            TStringBuilder() << "Unknown format version with value " << (int)rr.supported_format()  << " for " << rr.consumer_name()};
    }
    consumer->SetFormatVersion(rr.supported_format() - 1);

    if (rr.version() < 0) {
        return {Ydb::StatusIds::BAD_REQUEST, 
            TStringBuilder() << "version in read_rule can't be negative, provided " << rr.version()};
    }
    consumer->SetVersion(rr.version());

    auto* cct = consumer->MutableCodec();
    if (rr.supported_codecs().size() > NPQ::MAX_SUPPORTED_CODECS_COUNT) {
        return {Ydb::StatusIds::BAD_REQUEST, 
            TStringBuilder() << "supported_codecs count cannot be more than "
                                << NPQ::MAX_SUPPORTED_CODECS_COUNT << ", provided " << rr.supported_codecs().size()
        };
    }
    for (const auto& codec : rr.supported_codecs()) {
        if (!Ydb::PersQueue::V1::Codec_IsValid(codec) || codec == 0)
            return {Ydb::StatusIds::BAD_REQUEST, 
                TStringBuilder() << "Unknown codec with value " << codec  << " for " << rr.consumer_name()};

        auto codecName = to_lower(Ydb::PersQueue::V1::Codec_Name((Ydb::PersQueue::V1::Codec)codec)).substr(6);

        cct->AddIds(codec - 1);
        cct->AddCodecs(codecName);
    }

    if (rr.important()) {
        consumer->SetImportant(true);
    }
    if (auto period = ConvertConsumerAvailabilityPeriod(rr.availability_period(), rr.consumer_name()); period.has_value()) {
        if (period.value().has_value()) {
            consumer->SetAvailabilityPeriodMs(period.value()->MilliSeconds());
        } else {
            consumer->ClearAvailabilityPeriodMs();
        }
    } else {
        return period.error();
    }

    if (!rr.service_type().empty()) {
        const TClientServiceTypes supportedClientServiceTypes = GetSupportedClientServiceTypes();
        if (!supportedClientServiceTypes.contains(rr.service_type())) {
            return {Ydb::StatusIds::BAD_REQUEST, 
                TStringBuilder() << "Unknown read rule service type '" << rr.service_type()
                                    << "' for consumer '" << rr.consumer_name() << "'"};
        }
        consumer->SetServiceType(rr.service_type());
    } else {
        if (pqConfig.GetDisallowDefaultClientServiceType()) {
            return {Ydb::StatusIds::BAD_REQUEST, 
                TStringBuilder() << "service type cannot be empty for consumer '" << rr.consumer_name() << "'"};
        }
        const auto& defaultCientServiceType = pqConfig.GetDefaultClientServiceType().GetName();
        consumer->SetServiceType(defaultCientServiceType);
    }

    if (rr.has_shared_read_rule_type()) {
        if (!AppData()->FeatureFlags.GetEnableTopicMessageLevelParallelism()) {
            return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "shared consumers are disabled"};
        }
        consumer->SetType(::NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);

        consumer->SetKeepMessageOrder(rr.shared_read_rule_type().keep_messages_order());
        auto defaultProcessingTimeout = rr.shared_read_rule_type().default_processing_timeout();
        if (defaultProcessingTimeout.seconds() < 0 || defaultProcessingTimeout.nanos() < 0) {
            return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "default_processing_timeout in shared_read_rule_type can't be negative, provided "
                << defaultProcessingTimeout.seconds() << " seconds and " << defaultProcessingTimeout.nanos() << " nanos"};
        }
        consumer->SetDefaultProcessingTimeoutSeconds(defaultProcessingTimeout.seconds());

        consumer->SetDeadLetterPolicyEnabled(rr.shared_read_rule_type().dead_letter_policy().enabled());
        consumer->SetMaxProcessingAttempts(rr.shared_read_rule_type().dead_letter_policy().condition().max_processing_attempts());

        auto delayMessageTime = rr.shared_read_rule_type().receive_message_delay();
        if (delayMessageTime.seconds() < 0 || delayMessageTime.nanos() < 0) {
            return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "receive_message_delay in shared_read_rule_type can't be negative, provided "
                << delayMessageTime.seconds() << " seconds and " << delayMessageTime.nanos() << " nanos"};
        }
        consumer->SetDefaultDelayMessageTimeMs(rr.shared_read_rule_type().receive_message_delay().seconds() * 1'000 + rr.shared_read_rule_type().receive_message_delay().nanos() / 1'000'000);

        auto receiveMessageWaitTime = rr.shared_read_rule_type().receive_message_wait_time();
        if (receiveMessageWaitTime.seconds() < 0 || receiveMessageWaitTime.nanos() < 0) {
            return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "receive_message_wait_time in shared_read_rule_type can't be negative, provided "
                << receiveMessageWaitTime.seconds() << " seconds and " << receiveMessageWaitTime.nanos() << " nanos"};
        }
        consumer->SetDefaultReceiveMessageWaitTimeMs(rr.shared_read_rule_type().receive_message_wait_time().seconds() * 1'000 + rr.shared_read_rule_type().receive_message_wait_time().nanos() / 1'000'000);

        if (rr.shared_read_rule_type().dead_letter_policy().has_move_action()) {
            consumer->SetDeadLetterPolicy(::NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
            consumer->SetDeadLetterQueue(rr.shared_read_rule_type().dead_letter_policy().move_action().dead_letter_queue());
        } else if (rr.shared_read_rule_type().dead_letter_policy().has_delete_action()) {
            consumer->SetDeadLetterPolicy(::NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_DELETE);
        } else {
            consumer->SetDeadLetterPolicy(::NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_UNSPECIFIED);
        }
    } else {
        consumer->SetType(::NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING);
    }

    if (consumersAdvancedMonitoringSettings) {
        consumersAdvancedMonitoringSettings->UpdateConsumerConfig(rr.consumer_name(), *consumer);
    }

    return {};
}


TResult ApplyChangesInt( // create and alter
    const TString& database,
    const TString& name,
    const Ydb::PersQueue::V1::TopicSettings& settings,
    NKikimrSchemeOp::TModifyScheme& modifyScheme,
    NKikimrSchemeOp::TPersQueueGroupDescription* pqDescr,
    EOperation operation,
    const TString& localDc
) {
    const auto& pqConfig = AppData()->PQConfig;

    pqDescr->SetName(name);

    auto minParts = 1;
    auto* pqTabletConfig = pqDescr->MutablePQTabletConfig();
    auto partConfig = pqTabletConfig->MutablePartitionConfig();

    TString error;
    switch (settings.retention_case()) {
        case Ydb::PersQueue::V1::TopicSettings::kRetentionPeriodMs: {
            if (auto retentionPeriodSeconds = CheckRetentionPeriod(Max(settings.retention_period_ms() / 1000ll, 1ll))) {
                partConfig->SetLifetimeSeconds(retentionPeriodSeconds.value());
            } else {
                error = TStringBuilder() << retentionPeriodSeconds.error() << ", provided " << settings.retention_period_ms() << " ms";
                return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
            }
        }
        break;

        case Ydb::PersQueue::V1::TopicSettings::kRetentionStorageBytes: {
            if (settings.retention_storage_bytes() <= 0) {
                error = TStringBuilder() << "retention_storage_bytes must be positive, provided " <<
                    settings.retention_storage_bytes();
                    return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
                }
            partConfig->SetStorageLimitBytes(settings.retention_storage_bytes());
        }
        break;

        default: {
            error = TStringBuilder() << "retention_storage_bytes or retention_period_ms should be set";
            return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
        }
    }

    if (!settings.has_auto_partitioning_settings()) {
        minParts = settings.partitions_count();
    } else {
        const auto& autoPartitioningSettings = settings.auto_partitioning_settings();
        if (autoPartitioningSettings.min_active_partitions() > 0) {
            minParts = autoPartitioningSettings.min_active_partitions();
        }

        auto pqTabletConfigPartStrategy = pqTabletConfig->MutablePartitionStrategy();

        pqTabletConfigPartStrategy->SetMinPartitionCount(minParts);
        pqTabletConfigPartStrategy->SetMaxPartitionCount(IfEqualThenDefault<int64_t>(autoPartitioningSettings.max_active_partitions(), 0L, minParts));
        pqTabletConfigPartStrategy->SetScaleUpPartitionWriteSpeedThresholdPercent(IfEqualThenDefault(autoPartitioningSettings.partition_write_speed().up_utilization_percent(), 0 ,30));
        pqTabletConfigPartStrategy->SetScaleDownPartitionWriteSpeedThresholdPercent(IfEqualThenDefault(autoPartitioningSettings.partition_write_speed().down_utilization_percent(), 0, 90));
        pqTabletConfigPartStrategy->SetScaleThresholdSeconds(IfEqualThenDefault<int64_t>(autoPartitioningSettings.partition_write_speed().stabilization_window().seconds(), 0L, 300L));
        switch (autoPartitioningSettings.strategy()) {
            case ::Ydb::PersQueue::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP:
                pqTabletConfigPartStrategy->SetPartitionStrategyType(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT);
                break;
            case ::Ydb::PersQueue::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN:
                pqTabletConfigPartStrategy->SetPartitionStrategyType(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT_AND_MERGE);
                break;
            case ::Ydb::PersQueue::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_PAUSED:
                pqTabletConfigPartStrategy->SetPartitionStrategyType(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_PAUSED);
                break;
            default:
                pqTabletConfigPartStrategy->SetPartitionStrategyType(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_DISABLED);
                break;
        }
        if (auto r = ValidatePartitionStrategy(*pqTabletConfig); !r) {
            return r;
        }
    }
    if (minParts <= 0) {
        error = TStringBuilder() << "Partitions count must be positive, provided " << settings.partitions_count();
        return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
    }
    pqDescr->SetTotalGroupCount(minParts);
    pqTabletConfig->SetRequireAuthWrite(true);
    pqTabletConfig->SetRequireAuthRead(true);

    TConsumersAdvancedMonitoringSettings consumersAdvancedMonitoringSettings;
    if (auto r = ProcessTopicAttributes(settings.attributes(), pqDescr, operation, pqConfig.GetTopicsAreFirstClassCitizen(), consumersAdvancedMonitoringSettings); !r) {
        return r;
    }

    bool local = !settings.client_write_disabled();

    if (operation == EOperation::Create && !pqConfig.GetTopicsAreFirstClassCitizen()) {
        auto converter = NPersQueue::TTopicNameConverter::ForFederation(
                pqConfig.GetRoot(),
                pqConfig.GetTestDatabaseRoot(),
                name,
                modifyScheme.GetWorkingDir(),
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

    //Sets legacy 'logtype'.

    const auto& channelProfiles = pqConfig.GetChannelProfiles();
    if (channelProfiles.size() > 2) {
        partConfig->MutableExplicitChannelProfiles()->CopyFrom(channelProfiles);
    }
    if (settings.max_partition_storage_size() < 0) {
        error = TStringBuilder() << "Max_partiton_strorage_size must can't be negative, provided " << settings.max_partition_storage_size();
        return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
    }
    partConfig->SetMaxSizeInPartition(settings.max_partition_storage_size() ? settings.max_partition_storage_size() : Max<i64>());
    partConfig->SetMaxCountInPartition(Max<i32>());

    if (settings.message_group_seqno_retention_period_ms() > 0 && settings.message_group_seqno_retention_period_ms() < settings.retention_period_ms()) {
        error = TStringBuilder() << "message_group_seqno_retention_period_ms (provided " << settings.message_group_seqno_retention_period_ms() << ") must be more than retention_period_ms (provided " << settings.retention_period_ms() << ")";
        return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
    }
    if (settings.message_group_seqno_retention_period_ms() >
        NPQ::DEFAULT_MAX_DATABASE_MESSAGEGROUP_SEQNO_RETENTION_PERIOD_MS) {
        error = TStringBuilder() <<
            "message_group_seqno_retention_period_ms (provided " <<
            settings.message_group_seqno_retention_period_ms() <<
            ") must be less than default limit for database " <<
            NPQ::DEFAULT_MAX_DATABASE_MESSAGEGROUP_SEQNO_RETENTION_PERIOD_MS;
            return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
        }
    if (settings.message_group_seqno_retention_period_ms() < 0) {
        error = TStringBuilder() << "message_group_seqno_retention_period_ms can't be negative, provided " << settings.message_group_seqno_retention_period_ms();
        return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
    }
    if (settings.message_group_seqno_retention_period_ms() > 0) {
        partConfig->SetSourceIdLifetimeSeconds(settings.message_group_seqno_retention_period_ms() > 999 ? settings.message_group_seqno_retention_period_ms() / 1000 :1);
    } else {
        // default value
        partConfig->SetSourceIdLifetimeSeconds(NKikimrPQ::TPartitionConfig().GetSourceIdLifetimeSeconds());
    }

    if (settings.max_partition_message_groups_seqno_stored() < 0) {
        error = TStringBuilder() << "max_partition_message_groups_seqno_stored can't be negative, provided " << settings.max_partition_message_groups_seqno_stored();
        return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
    }
    if (settings.max_partition_message_groups_seqno_stored() > 0) {
        partConfig->SetSourceIdMaxCounts(settings.max_partition_message_groups_seqno_stored());
    } else {
        // default value
        partConfig->SetSourceIdMaxCounts(NKikimrPQ::TPartitionConfig().GetSourceIdMaxCounts());
    }

    if (local) {
        auto partSpeed = settings.max_partition_write_speed();
        if (partSpeed < 0) {
            error = TStringBuilder() << "max_partition_write_speed can't be negative, provided " << partSpeed;
            return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
        } else if (partSpeed == 0) {
            partSpeed = NPQ::DEFAULT_PARTITION_SPEED;
        }
        partConfig->SetWriteSpeedInBytesPerSecond(partSpeed);

        const auto& burstSpeed = settings.max_partition_write_burst();
        if (burstSpeed < 0) {
            error = TStringBuilder() << "max_partition_write_burst can't be negative, provided " << burstSpeed;
            return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
        } else if (burstSpeed == 0) {
            partConfig->SetBurstSize(partSpeed);
        } else {
            partConfig->SetBurstSize(burstSpeed);
        }
    }

    if (!Ydb::PersQueue::V1::TopicSettings::Format_IsValid((int)settings.supported_format()) || settings.supported_format() == 0) {
        error = TStringBuilder() << "Unknown format version with value " << (int)settings.supported_format();
        return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
    }
    pqTabletConfig->SetFormatVersion(settings.supported_format() - 1);

    auto ct = pqTabletConfig->MutableCodecs();
    if (settings.supported_codecs().size() > NPQ::MAX_SUPPORTED_CODECS_COUNT) {
        error = TStringBuilder() << "supported_codecs count cannot be more than "
                                 << NPQ::MAX_SUPPORTED_CODECS_COUNT << ", provided " << settings.supported_codecs().size();
        return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
    }
    for(const auto& codec : settings.supported_codecs()) {
        if (!Ydb::PersQueue::V1::Codec_IsValid(codec) || codec == 0) {
            error = TStringBuilder() << "Unknown codec with value " << codec;
            return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
        }
        ct->AddIds(codec - 1);
        ct->AddCodecs(LegacySubstr(to_lower(Ydb::PersQueue::V1::Codec_Name((Ydb::PersQueue::V1::Codec)codec)), 6));
    }

    //TODO: check all values with defaults
    if (settings.read_rules().size() > NPQ::MAX_READ_RULES_COUNT) {
        error = TStringBuilder() << "read rules count cannot be more than "
                                 << NPQ::MAX_READ_RULES_COUNT << ", provided " << settings.read_rules().size();
        return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
    }

    for (const auto& rr : settings.read_rules()) {
        auto r = AddConsumerImpl(pqTabletConfig, rr, &consumersAdvancedMonitoringSettings);
        if (!r) {
            return r;
        }
    }

    if (auto errorCode = consumersAdvancedMonitoringSettings.CheckForUnknownConsumers(error); errorCode != Ydb::StatusIds::SUCCESS) {
        return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
    }

    if (settings.has_remote_mirror_rule()) {
        auto mirrorFrom = partConfig->MutableMirrorFrom();
        if (!local) {
            mirrorFrom->SetSyncWriteTime(true);
        }
        {
            TString endpoint = settings.remote_mirror_rule().endpoint();
            if (endpoint.StartsWith(GRPCS_ENDPOINT_PREFIX)) {
                mirrorFrom->SetUseSecureConnection(true);
                endpoint = TString(endpoint.begin() + GRPCS_ENDPOINT_PREFIX.size(), endpoint.end());
            } else if (endpoint.StartsWith(GRPC_ENDPOINT_PREFIX)) {
                endpoint = TString(endpoint.begin() + GRPC_ENDPOINT_PREFIX.size(), endpoint.end());
            }
            auto parts = SplitString(endpoint, ":");
            if (parts.size() != 2) {
                error = TStringBuilder() << "endpoint in remote mirror rule must be in format [grpcs://]server:port or [grpc://]server:port, but got '"
                                         << settings.remote_mirror_rule().endpoint() << "'";
                return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
            }
            ui16 port;
            if (!TryFromString(parts[1], port)) {
                error = TStringBuilder() << "cannot parse port from endpoint ('" << settings.remote_mirror_rule().endpoint() << "') for remote mirror rule";
                return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
            }
            mirrorFrom->SetEndpoint(parts[0]);
            mirrorFrom->SetEndpointPort(port);
        }
        mirrorFrom->SetTopic(settings.remote_mirror_rule().topic_path());
        mirrorFrom->SetConsumer(settings.remote_mirror_rule().consumer_name());
        if (settings.remote_mirror_rule().starting_message_timestamp_ms() < 0) {
            error = TStringBuilder() << "starting_message_timestamp_ms in remote_mirror_rule can't be negative, provided "
                                     << settings.remote_mirror_rule().starting_message_timestamp_ms();
            return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
        }
        mirrorFrom->SetReadFromTimestampsMs(settings.remote_mirror_rule().starting_message_timestamp_ms());
        if (!settings.remote_mirror_rule().has_credentials()) {
            error = "credentials for remote mirror rule must be set";
            return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
        }
        const auto& credentials = settings.remote_mirror_rule().credentials();
        switch (credentials.credentials_case()) {
            case Ydb::PersQueue::V1::Credentials::kOauthToken: {
                mirrorFrom->MutableCredentials()->SetOauthToken(credentials.oauth_token());
                break;
            }
            case Ydb::PersQueue::V1::Credentials::kJwtParams: {
                try {
                    auto res = NYdb::ParseJwtParams(credentials.jwt_params());
                    NYdb::MakeSignedJwt(res);
                } catch (...) {
                    error = TStringBuilder() << "incorrect jwt params in remote mirror rule: " << CurrentExceptionMessage();
                    return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
                }
                mirrorFrom->MutableCredentials()->SetJwtParams(credentials.jwt_params());
                break;
            }
            case Ydb::PersQueue::V1::Credentials::kIam: {
                try {
                    auto res = NYdb::ParseJwtParams(credentials.iam().service_account_key());
                    NYdb::MakeSignedJwt(res);
                } catch (...) {
                    error = TStringBuilder() << "incorrect service account key for iam in remote mirror rule: " << CurrentExceptionMessage();
                    return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
                }
                if (credentials.iam().endpoint().empty()) {
                    error = "iam endpoint must be set in remote mirror rule";
                    return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
                }
                mirrorFrom->MutableCredentials()->MutableIam()->SetEndpoint(credentials.iam().endpoint());
                mirrorFrom->MutableCredentials()->MutableIam()->SetServiceAccountKey(credentials.iam().service_account_key());
                break;
            }
            case Ydb::PersQueue::V1::Credentials::CREDENTIALS_NOT_SET: {
                error = "one of the credential fields must be filled for remote mirror rule";
                return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
            }
            default: {
                error = TStringBuilder() << "unsupported credentials type " << ::NPersQueue::ObfuscateString(ToString(credentials));
                return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
            }
        }
        if (settings.remote_mirror_rule().database()) {
            mirrorFrom->SetDatabase(settings.remote_mirror_rule().database());
        }
    }

    if (settings.has_metrics_level()) {
        pqTabletConfig->SetMetricsLevel(settings.metrics_level());
    } else {
        pqTabletConfig->ClearMetricsLevel();
    }

    return {};
}

} // namespace

NPQ::NSchema::TResult ApplyChangesInt(
    const TString& database,
    const TString& name,
    const Ydb::PersQueue::V1::AlterTopicRequest& request,
    NKikimrSchemeOp::TModifyScheme& modifyScheme,
    NKikimrSchemeOp::TPersQueueGroupDescription& targetConfig,
    const TString& localDc
) {
    return ApplyChangesInt(
        database,
        name,
        request.settings(),
        modifyScheme,
        &targetConfig,
        EOperation::Create,
        localDc);
}

NPQ::NSchema::TResult ApplyChangesInt(
    const TString& database,
    const Ydb::PersQueue::V1::CreateTopicRequest& request,
    NKikimrSchemeOp::TModifyScheme& modifyScheme,
    NKikimrSchemeOp::TPersQueueGroupDescription& targetConfig,
    const TString& localDc
) {
    targetConfig.SetPartitionPerTablet(1);

    return ApplyChangesInt(
        database,
        targetConfig.GetName(),
        request.settings(),
        modifyScheme,
        &targetConfig,
        EOperation::Create,
        localDc);
}

NPQ::NSchema::TResult AddConsumer(
    NKikimrPQ::TPQTabletConfig* config,
    const Ydb::PersQueue::V1::TopicSettings::ReadRule& readRule,
    const TConsumersAdvancedMonitoringSettings* consumersAdvancedMonitoringSettings
) {
    return AddConsumerImpl(config, readRule, consumersAdvancedMonitoringSettings);
}

} // namespace NKikimr::NGRpcProxy::V1::NPQv1
