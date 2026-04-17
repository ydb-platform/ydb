#include "common.h"

#include <ydb/services/lib/actors/pq_schema_actor.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/schema/common.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/jwt/jwt.h>
#include <ydb/public/sdk/cpp/src/library/persqueue/obfuscate/obfuscate.h>

#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/string/cast.h>
#include <util/string/vector.h>

namespace NKikimr::NGRpcProxy::V1::NPQv1 {

namespace {

using namespace NPQ::NSchema;

constexpr TStringBuf GRPCS_ENDPOINT_PREFIX = "grpcs://";
constexpr TStringBuf GRPC_ENDPOINT_PREFIX = "grpc://";

TResult FillProposeRequestImpl( // create and alter
        NKikimrSchemeOp::TModifyScheme& modifyScheme,
        NKikimrSchemeOp::TPersQueueGroupDescription* pqDescr,
        const Ydb::PersQueue::V1::TopicSettings& settings,
        const TString& path,
        const TString& name,
        const TString& database,
        const TString& localDc,
        bool alter
) {
    const auto& pqConfig = AppData()->PQConfig;

    pqDescr->SetName(name);

    auto minParts = 1;
    auto* pqTabletConfig = pqDescr->MutablePQTabletConfig();
    auto partConfig = pqTabletConfig->MutablePartitionConfig();

    auto topicPath = NKikimr::JoinPath({modifyScheme.GetWorkingDir(), name});

    switch (settings.retention_case()) {
        case Ydb::PersQueue::V1::TopicSettings::kRetentionPeriodMs: {
            if (auto retentionPeriodSeconds = CheckRetentionPeriod(Max(settings.retention_period_ms() / 1000ll, 1ll))) {
                partConfig->SetLifetimeSeconds(retentionPeriodSeconds.value());
            } else {
                return {Ydb::StatusIds::BAD_REQUEST,
                    TStringBuilder() << retentionPeriodSeconds.error() << ", provided " << settings.retention_period_ms() << " ms" };
            }
        }
        break;

        case Ydb::PersQueue::V1::TopicSettings::kRetentionStorageBytes: {
            if (settings.retention_storage_bytes() <= 0) {
                return {Ydb::StatusIds::BAD_REQUEST,
                    TStringBuilder() << "retention_storage_bytes must be positive, provided " << settings.retention_storage_bytes()};
            }
            partConfig->SetStorageLimitBytes(settings.retention_storage_bytes());
        }
        break;

        default: {
            return {Ydb::StatusIds::BAD_REQUEST,
                TStringBuilder() << "retention_storage_bytes or retention_period_ms should be set"};
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
        if (auto result = ValidatePartitionStrategy(*pqTabletConfig); result) {
            return result;
        }
    }
    if (minParts <= 0) {
        return {Ydb::StatusIds::BAD_REQUEST,
            TStringBuilder() << "Partitions count must be positive, provided " << settings.partitions_count()};
    }
    pqDescr->SetTotalGroupCount(minParts);
    pqTabletConfig->SetRequireAuthWrite(true);
    pqTabletConfig->SetRequireAuthRead(true);
    if (!alter)
        pqDescr->SetPartitionPerTablet(1);

    TConsumersAdvancedMonitoringSettings consumersAdvancedMonitoringSettings;
    auto res = ProcessTopicAttributes(
        settings.attributes(),
        pqDescr,
        alter ? EOperation::Alter : EOperation::Create,
        pqConfig.GetTopicsAreFirstClassCitizen(),
        consumersAdvancedMonitoringSettings);
    if (!res) {
        return res;
    }

    bool local = !settings.client_write_disabled();

    if (!pqConfig.GetTopicsAreFirstClassCitizen()) {
        auto converter = NPersQueue::TTopicNameConverter::ForFederation(
                pqConfig.GetRoot(), pqConfig.GetTestDatabaseRoot(), name, path, database, local, localDc,
                pqTabletConfig->GetFederationAccount()
        );

        if (!converter->IsValid()) {
            return {Ydb::StatusIds::BAD_REQUEST,
                TStringBuilder() << "Bad topic: " << converter->GetReason()};
        }
        pqTabletConfig->SetLocalDC(local);
        pqTabletConfig->SetDC(converter->GetCluster());
        pqTabletConfig->SetProducer(converter->GetLegacyProducer());
        pqTabletConfig->SetTopic(converter->GetLegacyLogtype());
        pqTabletConfig->SetIdent(converter->GetLegacyProducer());
    }

    //config->SetTopicName(name);
    //config->SetTopicPath(topicPath);

    //Sets legacy 'logtype'.

    const auto& channelProfiles = pqConfig.GetChannelProfiles();
    if (channelProfiles.size() > 2) {
        partConfig->MutableExplicitChannelProfiles()->CopyFrom(channelProfiles);
    }
    if (settings.max_partition_storage_size() < 0) {
        return {Ydb::StatusIds::BAD_REQUEST,
            TStringBuilder() << "Max_partiton_strorage_size must can't be negative, provided " << settings.max_partition_storage_size()};
    }
    partConfig->SetMaxSizeInPartition(settings.max_partition_storage_size() ? settings.max_partition_storage_size() : Max<i64>());
    partConfig->SetMaxCountInPartition(Max<i32>());

    if (settings.message_group_seqno_retention_period_ms() > 0 && settings.message_group_seqno_retention_period_ms() < settings.retention_period_ms()) {
        return {Ydb::StatusIds::BAD_REQUEST,
            TStringBuilder() << "message_group_seqno_retention_period_ms (provided " << settings.message_group_seqno_retention_period_ms() << ") must be more then retention_period_ms (provided " << settings.retention_period_ms() << ")"};
    }
    if (settings.message_group_seqno_retention_period_ms() >
        DEFAULT_MAX_DATABASE_MESSAGEGROUP_SEQNO_RETENTION_PERIOD_MS) {
        auto error = TStringBuilder() <<
            "message_group_seqno_retention_period_ms (provided " <<
            settings.message_group_seqno_retention_period_ms() <<
            ") must be less then default limit for database " <<
            DEFAULT_MAX_DATABASE_MESSAGEGROUP_SEQNO_RETENTION_PERIOD_MS;
        return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
    }
    if (settings.message_group_seqno_retention_period_ms() < 0) {
        auto error = TStringBuilder() << "message_group_seqno_retention_period_ms can't be negative, provided " << settings.message_group_seqno_retention_period_ms();
        return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
    }
    if (settings.message_group_seqno_retention_period_ms() > 0) {
        partConfig->SetSourceIdLifetimeSeconds(settings.message_group_seqno_retention_period_ms() > 999 ? settings.message_group_seqno_retention_period_ms() / 1000 :1);
    } else {
        // default value
        partConfig->SetSourceIdLifetimeSeconds(NKikimrPQ::TPartitionConfig().GetSourceIdLifetimeSeconds());
    }

    if (settings.max_partition_message_groups_seqno_stored() < 0) {
        auto error = TStringBuilder() << "max_partition_message_groups_seqno_stored can't be negative, provided " << settings.max_partition_message_groups_seqno_stored();
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
            auto error = TStringBuilder() << "max_partition_write_speed can't be negative, provided " << partSpeed;
            return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
        } else if (partSpeed == 0) {
            partSpeed = DEFAULT_PARTITION_SPEED;
        }
        partConfig->SetWriteSpeedInBytesPerSecond(partSpeed);

        const auto& burstSpeed = settings.max_partition_write_burst();
        if (burstSpeed < 0) {
            auto error = TStringBuilder() << "max_partition_write_burst can't be negative, provided " << burstSpeed;
            return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
        } else if (burstSpeed == 0) {
            partConfig->SetBurstSize(partSpeed);
        } else {
            partConfig->SetBurstSize(burstSpeed);
        }
    }

    if (!Ydb::PersQueue::V1::TopicSettings::Format_IsValid((int)settings.supported_format()) || settings.supported_format() == 0) {
        auto error = TStringBuilder() << "Unknown format version with value " << (int)settings.supported_format();
        return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
    }
    pqTabletConfig->SetFormatVersion(settings.supported_format() - 1);

    auto ct = pqTabletConfig->MutableCodecs();
    if (settings.supported_codecs().size() > MAX_SUPPORTED_CODECS_COUNT) {
        auto error = TStringBuilder() << "supported_codecs count cannot be more than "
                                 << MAX_SUPPORTED_CODECS_COUNT << ", provided " << settings.supported_codecs().size();
        return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};

    }
    for(const auto& codec : settings.supported_codecs()) {
        if (!Ydb::PersQueue::V1::Codec_IsValid(codec) || codec == 0) {
            auto error = TStringBuilder() << "Unknown codec with value " << codec;
            return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
        }
        ct->AddIds(codec - 1);
        ct->AddCodecs(LegacySubstr(to_lower(Ydb::PersQueue::V1::Codec_Name((Ydb::PersQueue::V1::Codec)codec)), 6));
    }

    //TODO: check all values with defaults
    if (settings.read_rules().size() > MAX_READ_RULES_COUNT) {
        auto error = TStringBuilder() << "read rules count cannot be more than "
                                 << MAX_READ_RULES_COUNT << ", provided " << settings.read_rules().size();
        return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
    }

    const auto& supportedClientServiceTypes = GetSupportedClientServiceTypes(pqConfig);
    for (const auto& rr : settings.read_rules()) {
        auto messageAndCode = AddReadRuleToConfig(pqTabletConfig, rr, supportedClientServiceTypes, pqConfig, &consumersAdvancedMonitoringSettings);
        if (messageAndCode.PQCode != Ydb::PersQueue::ErrorCode::OK) {
            auto error = messageAndCode.Message;
            return {Ydb::StatusIds::BAD_REQUEST, std::move(error)};
        }
    }

    TString error;
    if (auto errorCode = consumersAdvancedMonitoringSettings.CheckForUnknownConsumers(error); errorCode != Ydb::StatusIds::SUCCESS) {
        return {errorCode, std::move(error)};
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

    return ValidateConfig(*pqTabletConfig, supportedClientServiceTypes, EOperation::Alter);
}

}

TResult ApplyCreate(
    NKikimrSchemeOp::TModifyScheme& modifyScheme,
    const Ydb::PersQueue::V1::TopicSettings& settings,
    const TString& path,
    const TString& name,
    const TString& database,
    const TString& localDc
) {
    modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup);
    auto* description = modifyScheme.MutableCreatePersQueueGroup();

    return FillProposeRequestImpl(modifyScheme, description, settings, path, name, database, localDc, false);
}

TResult ApplyAlter(
    NKikimrSchemeOp::TModifyScheme& modifyScheme,
    const Ydb::PersQueue::V1::TopicSettings& settings,
    const TString& path,
    const TString& name,
    const TString& database,
    const TString& localDc
) {
    modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup);
    auto* description = modifyScheme.MutableAlterPersQueueGroup();

    return FillProposeRequestImpl(modifyScheme, description, settings, path, name, database, localDc, true);
}

} // namespace NKikimr::NGRpcProxy::V1::NPQv1