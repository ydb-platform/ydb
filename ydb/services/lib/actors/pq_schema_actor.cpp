#include "pq_schema_actor.h"

#include <ydb/core/ydb_convert/topic_description.h>
#include <ydb/public/sdk/cpp/src/library/persqueue/obfuscate/obfuscate.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/kafka_proxy/kafka_constants.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/util/proto_duration.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/jwt/jwt.h>

#include <ydb/public/api/protos/ydb_topic.pb.h>

#include <yql/essentials/public/decimal/yql_decimal.h>

#include <util/string/vector.h>

#include <library/cpp/digest/md5/md5.h>

#include <expected>


namespace NKikimr::NGRpcProxy::V1 {

    constexpr TStringBuf GRPCS_ENDPOINT_PREFIX = "grpcs://";
    constexpr TStringBuf GRPC_ENDPOINT_PREFIX = "grpc://";

    TClientServiceTypes GetSupportedClientServiceTypes(const NKikimrPQ::TPQConfig& /*pqConfig*/) {
        return NPQ::NSchema::GetSupportedClientServiceTypes();
    }

    static std::expected<std::optional<TDuration>, TMsgPqCodes> ConvertConsumerAvailabilityPeriod(const google::protobuf::Duration& duration, std::string_view consumerName) {
        auto val = NPQ::NSchema::ConvertConsumerAvailabilityPeriod(duration, consumerName);
        if (val.has_value()) {
            return std::expected<std::optional<TDuration>, TMsgPqCodes>(val.value());
        } else {
            return std::unexpected(TMsgPqCodes(val.error().GetErrorMessage(), Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT));
        }
    }

    TMsgPqCodes AddReadRuleToConfig(
        NKikimrPQ::TPQTabletConfig* config,
        const Ydb::PersQueue::V1::TopicSettings::ReadRule& rr,
        const TClientServiceTypes& supportedClientServiceTypes,
        const NKikimrPQ::TPQConfig& pqConfig,
        const TConsumersAdvancedMonitoringSettings* consumersAdvancedMonitoringSettings
    ) {
        // TODO remove this function
        auto consumerName = NPersQueue::ConvertNewConsumerName(rr.consumer_name(), pqConfig);
        if (consumerName.empty()) {
            return TMsgPqCodes(TStringBuilder() << "consumer with empty name is forbidden", Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
        }
        if(consumerName.find("/") != TString::npos || consumerName.find("|") != TString::npos) {
            return TMsgPqCodes(
                TStringBuilder() << "consumer '" << rr.consumer_name() << "' has illegal symbols",
                Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT
            );
        }
        if (consumerName == NPQ::CLIENTID_COMPACTION_CONSUMER && !config->GetEnableCompactification()) {
            return TMsgPqCodes(TStringBuilder() << "cannot add service consumer '" << consumerName << " to a topic without compactification enabled", Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
        }

        auto* consumer = config->AddConsumers();

        consumer->SetName(consumerName);

        if (rr.starting_message_timestamp_ms() < 0) {
            return TMsgPqCodes(
                TStringBuilder() << "starting_message_timestamp_ms in read_rule can't be negative, provided " << rr.starting_message_timestamp_ms(),
                Ydb::PersQueue::ErrorCode::VALIDATION_ERROR
            );
        }
        consumer->SetReadFromTimestampsMs(rr.starting_message_timestamp_ms());

        if (!Ydb::PersQueue::V1::TopicSettings::Format_IsValid((int)rr.supported_format()) || rr.supported_format() == 0) {
            return TMsgPqCodes(
                TStringBuilder() << "Unknown format version with value " << (int)rr.supported_format()  << " for " << rr.consumer_name(),
                Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT
            );
        }
        consumer->SetFormatVersion(rr.supported_format() - 1);

        if (rr.version() < 0) {
            return TMsgPqCodes(
                TStringBuilder() << "version in read_rule can't be negative, provided " << rr.version(),
                Ydb::PersQueue::ErrorCode::VALIDATION_ERROR
            );
        }
        consumer->SetVersion(rr.version());

        auto* cct = consumer->MutableCodec();
        if (rr.supported_codecs().size() > MAX_SUPPORTED_CODECS_COUNT) {
            return TMsgPqCodes(
                TStringBuilder() << "supported_codecs count cannot be more than "
                                    << MAX_SUPPORTED_CODECS_COUNT << ", provided " << rr.supported_codecs().size(),
                Ydb::PersQueue::ErrorCode::VALIDATION_ERROR
            );
        }
        for (const auto& codec : rr.supported_codecs()) {
            if (!Ydb::PersQueue::V1::Codec_IsValid(codec) || codec == 0)
                return TMsgPqCodes(
                    TStringBuilder() << "Unknown codec with value " << codec  << " for " << rr.consumer_name(),
                    Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT
                );

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
            if (!supportedClientServiceTypes.contains(rr.service_type())) {
                return TMsgPqCodes(
                    TStringBuilder() << "Unknown read rule service type '" << rr.service_type()
                                        << "' for consumer '" << rr.consumer_name() << "'",
                    Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT
                );
            }
            consumer->SetServiceType(rr.service_type());
        } else {
            if (pqConfig.GetDisallowDefaultClientServiceType()) {
                return TMsgPqCodes(
                    TStringBuilder() << "service type cannot be empty for consumer '" << rr.consumer_name() << "'",
                    Ydb::PersQueue::ErrorCode::VALIDATION_ERROR
                );
            }
            const auto& defaultCientServiceType = pqConfig.GetDefaultClientServiceType().GetName();
            consumer->SetServiceType(defaultCientServiceType);
        }

        if (consumersAdvancedMonitoringSettings) {
            consumersAdvancedMonitoringSettings->UpdateConsumerConfig(rr.consumer_name(), *consumer);
        }

        return TMsgPqCodes("", Ydb::PersQueue::ErrorCode::OK);
    }

    TString ProcessAlterConsumer(Ydb::Topic::Consumer& consumer, const Ydb::Topic::AlterConsumer& alter) {
        if (alter.has_set_important()) {
            consumer.set_important(alter.set_important());
        }
        if (alter.has_set_read_from()) {
            consumer.mutable_read_from()->CopyFrom(alter.set_read_from());
        }
        if (alter.has_set_supported_codecs()) {
            consumer.mutable_supported_codecs()->CopyFrom(alter.set_supported_codecs());
        }
        for (const auto& [attrName, attrValue] : alter.alter_attributes()) {
            (*consumer.mutable_attributes())[attrName] = attrValue;
        }
        if (alter.has_set_availability_period()) {
            consumer.mutable_availability_period()->CopyFrom(alter.set_availability_period());
        }
        if (alter.has_reset_availability_period()) {
            consumer.clear_availability_period();
        }

        if (alter.has_alter_streaming_consumer_type()) {
            if (!consumer.has_streaming_consumer_type()) {
                return "Cannot alter consumer type";
            }
        } else if (alter.has_alter_shared_consumer_type()) {
            if (!consumer.has_shared_consumer_type()) {
                return "Cannot alter consumer type";
            }

            auto* type = consumer.mutable_shared_consumer_type();
            auto& alterType = alter.alter_shared_consumer_type();

            if (alterType.has_set_default_processing_timeout()) {
                type->mutable_default_processing_timeout()->CopyFrom(alterType.set_default_processing_timeout());
            }

            if (alterType.has_set_receive_message_delay()) {
                type->mutable_receive_message_delay()->CopyFrom(alterType.set_receive_message_delay());
            }

            if (alterType.has_set_receive_message_wait_time()) {
                type->mutable_receive_message_wait_time()->CopyFrom(alterType.set_receive_message_wait_time());
            }

            if (alterType.has_alter_dead_letter_policy()) {
                auto& alterPolicy = alterType.alter_dead_letter_policy();
                auto* policy = type->mutable_dead_letter_policy();
                if (alterPolicy.has_set_enabled()) {
                    policy->set_enabled(alterPolicy.set_enabled());
                }

                if (alterPolicy.has_alter_condition()) {
                    policy->mutable_condition()->set_max_processing_attempts(alterPolicy.alter_condition().set_max_processing_attempts());
                }

                if (alterPolicy.has_alter_move_action()) {
                    if (!policy->has_move_action()) {
                        return "Cannot alter move action";
                    }
                    if (alterPolicy.alter_move_action().has_set_dead_letter_queue()) {
                        if (alterPolicy.alter_move_action().set_dead_letter_queue().empty()) {
                            return "Dead letter queue cannot be empty";
                        }
                        policy->mutable_move_action()->set_dead_letter_queue(alterPolicy.alter_move_action().set_dead_letter_queue());
                    }
                } else if (alterPolicy.has_set_move_action()) {
                    if (alterPolicy.set_move_action().dead_letter_queue().empty()) {
                        return "Dead letter queue cannot be empty";
                    }
                    policy->clear_action();
                    policy->mutable_move_action()->set_dead_letter_queue(alterPolicy.set_move_action().dead_letter_queue());
                } else if (alterPolicy.has_set_delete_action()) {
                    policy->clear_action();
                    policy->mutable_delete_action();
                }
            }
        }

        return {};
    }

    TMsgPqCodes AddReadRuleToConfig(
        NKikimrPQ::TPQTabletConfig* config,
        const Ydb::Topic::Consumer& rr,
        const TClientServiceTypes& supportedClientServiceTypes,
        const bool checkServiceType,
        const NKikimrPQ::TPQConfig& /*pqConfig*/,
        bool /*enableTopicDiskSubDomainQuota*/,
        const TAppData* /*appData*/,
        TConsumersAdvancedMonitoringSettings* consumersAdvancedMonitoringSettings
    ) {
        auto result = NPQ::NSchema::ProcessAddConsumer(
            config,
            rr,
            supportedClientServiceTypes,
            checkServiceType,
            consumersAdvancedMonitoringSettings
        );
        if (!result) {
            return TMsgPqCodes(result.GetErrorMessage(), Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
        }
        return TMsgPqCodes("", Ydb::PersQueue::ErrorCode::OK);
    }

    TString RemoveReadRuleFromConfig(
        NKikimrPQ::TPQTabletConfig* config,
        const NKikimrPQ::TPQTabletConfig& originalConfig,
        const TString& consumerName,
        const NKikimrPQ::TPQConfig& /*pqConfig*/
    ) {
        config->ClearConsumers();

        bool removed = false;

        for (auto& consumer : originalConfig.GetConsumers()) {
            if (consumerName == consumer.GetName()) {
                removed = true;
                continue;
            }

            auto* dst = config->AddConsumers();
            dst->CopyFrom(consumer);
        }

        if (!removed) {
            return TStringBuilder() << "Rule for consumer " << consumerName << " doesn't exist";
        }

        return "";
    }

    Ydb::StatusIds::StatusCode CheckConfig(const NKikimrPQ::TPQTabletConfig& config,
                              const TClientServiceTypes& supportedClientServiceTypes,
                              TString& error, const NKikimrPQ::TPQConfig& /*pqConfig*/,
                              EOperation operation)
    {
        auto result = NPQ::NSchema::ValidateConfig(config, supportedClientServiceTypes, operation);
        if (!result) {
            error = result.GetErrorMessage();
        }

        return result.GetStatus();
    }

    NYql::TIssue FillIssue(const TString& errorReason, const Ydb::PersQueue::ErrorCode::ErrorCode errorCode) {
        NYql::TIssue res(NYql::TPosition(), errorReason);
        res.SetCode(errorCode, NYql::ESeverity::TSeverityIds_ESeverityId_S_ERROR);
        return res;
    }

    NYql::TIssue FillIssue(const TString& errorReason, const size_t errorCode) {
        NYql::TIssue res(NYql::TPosition(), errorReason);
        res.SetCode(errorCode, NYql::ESeverity::TSeverityIds_ESeverityId_S_ERROR);
        return res;
    }

    Ydb::StatusIds::StatusCode ProcessAttributes(
        const ::google::protobuf::Map<TProtoStringType, TProtoStringType>& attributes,
        const bool topicsAreFirstClassCitizen,
        NKikimrSchemeOp::TPersQueueGroupDescription* pqDescr,
        TConsumersAdvancedMonitoringSettings& consumersAdvancedMonitoringSettings,
        TString& error,
        const bool alter) {

        auto [status, error_] = NPQ::NSchema::ProcessTopicAttributes(
            attributes,
            pqDescr,
            alter ? NPQ::NSchema::EOperation::Alter : NPQ::NSchema::EOperation::Create,
            topicsAreFirstClassCitizen,
            consumersAdvancedMonitoringSettings);

        if (status != Ydb::StatusIds::SUCCESS) {
            error = error_;
        }
        return status;
    }

    std::optional<TYdbPqCodes> ValidatePartitionStrategy(const ::NKikimrPQ::TPQTabletConfig& config, TString& error) {
        auto [status, error_] = NPQ::NSchema::ValidatePartitionStrategy(config);

        if (status != Ydb::StatusIds::SUCCESS) {
            error = error_;
            return TYdbPqCodes(status, Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
        }

        return std::nullopt;
    }

    Ydb::StatusIds::StatusCode FillProposeRequestImpl( // create and alter
            const TString& name, const Ydb::PersQueue::V1::TopicSettings& settings,
            NKikimrSchemeOp::TModifyScheme& modifyScheme, const TActorContext& ctx,
            bool alter, TString& error, const TString& path, const TString& database, const TString& localDc
    ) {
        const auto& pqConfig = AppData(ctx)->PQConfig;

        modifyScheme.SetOperationType(alter ? NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup : NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup);

        auto pqDescr = alter ? modifyScheme.MutableAlterPersQueueGroup() : modifyScheme.MutableCreatePersQueueGroup();
        pqDescr->SetName(name);

        auto minParts = 1;
        auto* pqTabletConfig = pqDescr->MutablePQTabletConfig();
        auto partConfig = pqTabletConfig->MutablePartitionConfig();

        switch (settings.retention_case()) {
            case Ydb::PersQueue::V1::TopicSettings::kRetentionPeriodMs: {
                if (auto retentionPeriodSeconds = CheckRetentionPeriod(Max(settings.retention_period_ms() / 1000ll, 1ll))) {
                    partConfig->SetLifetimeSeconds(retentionPeriodSeconds.value());
                } else {
                    error = TStringBuilder() << retentionPeriodSeconds.error() << ", provided " << settings.retention_period_ms() << " ms";
                    return Ydb::StatusIds::BAD_REQUEST;
                }
            }
            break;

            case Ydb::PersQueue::V1::TopicSettings::kRetentionStorageBytes: {
                if (settings.retention_storage_bytes() <= 0) {
                    error = TStringBuilder() << "retention_storage_bytes must be positive, provided " <<
                        settings.retention_storage_bytes();
                    return Ydb::StatusIds::BAD_REQUEST;
                }
                partConfig->SetStorageLimitBytes(settings.retention_storage_bytes());
            }
            break;

            default: {
                error = TStringBuilder() << "retention_storage_bytes or retention_period_ms should be set";
                return Ydb::StatusIds::BAD_REQUEST;
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
            if (auto code = ValidatePartitionStrategy(*pqTabletConfig, error); code) {
                return code->YdbCode;
            }
        }
        if (minParts <= 0) {
            error = TStringBuilder() << "Partitions count must be positive, provided " << settings.partitions_count();
            return Ydb::StatusIds::BAD_REQUEST;
        }
        pqDescr->SetTotalGroupCount(minParts);
        pqTabletConfig->SetRequireAuthWrite(true);
        pqTabletConfig->SetRequireAuthRead(true);
        if (!alter)
            pqDescr->SetPartitionPerTablet(1);

        TConsumersAdvancedMonitoringSettings consumersAdvancedMonitoringSettings;
        auto res = ProcessAttributes(settings.attributes(), pqConfig.GetTopicsAreFirstClassCitizen(), pqDescr, consumersAdvancedMonitoringSettings, error, alter);
        if (res != Ydb::StatusIds::SUCCESS) {
            return res;
        }

        bool local = !settings.client_write_disabled();

        auto topicPath = NKikimr::JoinPath({modifyScheme.GetWorkingDir(), name});
        if (!pqConfig.GetTopicsAreFirstClassCitizen()) {
            auto converter = NPersQueue::TTopicNameConverter::ForFederation(
                    pqConfig.GetRoot(), pqConfig.GetTestDatabaseRoot(), name, path, database, local, localDc,
                    pqTabletConfig->GetFederationAccount()
            );

            if (!converter->IsValid()) {
                error = TStringBuilder() << "Bad topic: " << converter->GetReason();
                return Ydb::StatusIds::BAD_REQUEST;
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
            error = TStringBuilder() << "Max_partiton_strorage_size must can't be negative, provided " << settings.max_partition_storage_size();
            return Ydb::StatusIds::BAD_REQUEST;
        }
        partConfig->SetMaxSizeInPartition(settings.max_partition_storage_size() ? settings.max_partition_storage_size() : Max<i64>());
        partConfig->SetMaxCountInPartition(Max<i32>());

        if (settings.message_group_seqno_retention_period_ms() > 0 && settings.message_group_seqno_retention_period_ms() < settings.retention_period_ms()) {
            error = TStringBuilder() << "message_group_seqno_retention_period_ms (provided " << settings.message_group_seqno_retention_period_ms() << ") must be more then retention_period_ms (provided " << settings.retention_period_ms() << ")";
            return Ydb::StatusIds::BAD_REQUEST;
        }
        if (settings.message_group_seqno_retention_period_ms() >
            DEFAULT_MAX_DATABASE_MESSAGEGROUP_SEQNO_RETENTION_PERIOD_MS) {
            error = TStringBuilder() <<
                "message_group_seqno_retention_period_ms (provided " <<
                settings.message_group_seqno_retention_period_ms() <<
                ") must be less then default limit for database " <<
                DEFAULT_MAX_DATABASE_MESSAGEGROUP_SEQNO_RETENTION_PERIOD_MS;
            return Ydb::StatusIds::BAD_REQUEST;
        }
        if (settings.message_group_seqno_retention_period_ms() < 0) {
            error = TStringBuilder() << "message_group_seqno_retention_period_ms can't be negative, provided " << settings.message_group_seqno_retention_period_ms();
            return Ydb::StatusIds::BAD_REQUEST;
        }
        if (settings.message_group_seqno_retention_period_ms() > 0) {
            partConfig->SetSourceIdLifetimeSeconds(settings.message_group_seqno_retention_period_ms() > 999 ? settings.message_group_seqno_retention_period_ms() / 1000 :1);
        } else {
            // default value
            partConfig->SetSourceIdLifetimeSeconds(NKikimrPQ::TPartitionConfig().GetSourceIdLifetimeSeconds());
        }

        if (settings.max_partition_message_groups_seqno_stored() < 0) {
            error = TStringBuilder() << "max_partition_message_groups_seqno_stored can't be negative, provided " << settings.max_partition_message_groups_seqno_stored();
            return Ydb::StatusIds::BAD_REQUEST;
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
                return Ydb::StatusIds::BAD_REQUEST;
            } else if (partSpeed == 0) {
                partSpeed = DEFAULT_PARTITION_SPEED;
            }
            partConfig->SetWriteSpeedInBytesPerSecond(partSpeed);

            const auto& burstSpeed = settings.max_partition_write_burst();
            if (burstSpeed < 0) {
                error = TStringBuilder() << "max_partition_write_burst can't be negative, provided " << burstSpeed;
                return Ydb::StatusIds::BAD_REQUEST;
            } else if (burstSpeed == 0) {
                partConfig->SetBurstSize(partSpeed);
            } else {
                partConfig->SetBurstSize(burstSpeed);
            }
        }

        if (!Ydb::PersQueue::V1::TopicSettings::Format_IsValid((int)settings.supported_format()) || settings.supported_format() == 0) {
            error = TStringBuilder() << "Unknown format version with value " << (int)settings.supported_format();
            return Ydb::StatusIds::BAD_REQUEST;
        }
        pqTabletConfig->SetFormatVersion(settings.supported_format() - 1);

        auto ct = pqTabletConfig->MutableCodecs();
        if (settings.supported_codecs().size() > MAX_SUPPORTED_CODECS_COUNT) {
            error = TStringBuilder() << "supported_codecs count cannot be more than "
                                     << MAX_SUPPORTED_CODECS_COUNT << ", provided " << settings.supported_codecs().size();
            return Ydb::StatusIds::BAD_REQUEST;

        }
        for(const auto& codec : settings.supported_codecs()) {
            if (!Ydb::PersQueue::V1::Codec_IsValid(codec) || codec == 0) {
                error = TStringBuilder() << "Unknown codec with value " << codec;
                return Ydb::StatusIds::BAD_REQUEST;
            }
            ct->AddIds(codec - 1);
            ct->AddCodecs(LegacySubstr(to_lower(Ydb::PersQueue::V1::Codec_Name((Ydb::PersQueue::V1::Codec)codec)), 6));
        }

        //TODO: check all values with defaults
        if (settings.read_rules().size() > MAX_READ_RULES_COUNT) {
            error = TStringBuilder() << "read rules count cannot be more than "
                                     << MAX_READ_RULES_COUNT << ", provided " << settings.read_rules().size();
            return Ydb::StatusIds::BAD_REQUEST;
        }

        const auto& supportedClientServiceTypes = GetSupportedClientServiceTypes(pqConfig);
        for (const auto& rr : settings.read_rules()) {
            auto messageAndCode = AddReadRuleToConfig(pqTabletConfig, rr, supportedClientServiceTypes, pqConfig, &consumersAdvancedMonitoringSettings);
            if (messageAndCode.PQCode != Ydb::PersQueue::ErrorCode::OK) {
                error = messageAndCode.Message;
                return Ydb::StatusIds::BAD_REQUEST;
            }
        }
        if (auto errorCode = consumersAdvancedMonitoringSettings.CheckForUnknownConsumers(error); errorCode != Ydb::StatusIds::SUCCESS) {
            return errorCode;
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
                    return Ydb::StatusIds::BAD_REQUEST;
                }
                ui16 port;
                if (!TryFromString(parts[1], port)) {
                    error = TStringBuilder() << "cannot parse port from endpoint ('" << settings.remote_mirror_rule().endpoint() << "') for remote mirror rule";
                    return Ydb::StatusIds::BAD_REQUEST;
                }
                mirrorFrom->SetEndpoint(parts[0]);
                mirrorFrom->SetEndpointPort(port);
            }
            mirrorFrom->SetTopic(settings.remote_mirror_rule().topic_path());
            mirrorFrom->SetConsumer(settings.remote_mirror_rule().consumer_name());
            if (settings.remote_mirror_rule().starting_message_timestamp_ms() < 0) {
                error = TStringBuilder() << "starting_message_timestamp_ms in remote_mirror_rule can't be negative, provided "
                                         << settings.remote_mirror_rule().starting_message_timestamp_ms();
                return Ydb::StatusIds::BAD_REQUEST;
            }
            mirrorFrom->SetReadFromTimestampsMs(settings.remote_mirror_rule().starting_message_timestamp_ms());
            if (!settings.remote_mirror_rule().has_credentials()) {
                error = "credentials for remote mirror rule must be set";
                return Ydb::StatusIds::BAD_REQUEST;
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
                        return Ydb::StatusIds::BAD_REQUEST;
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
                        return Ydb::StatusIds::BAD_REQUEST;
                    }
                    if (credentials.iam().endpoint().empty()) {
                        error = "iam endpoint must be set in remote mirror rule";
                        return Ydb::StatusIds::BAD_REQUEST;
                    }
                    mirrorFrom->MutableCredentials()->MutableIam()->SetEndpoint(credentials.iam().endpoint());
                    mirrorFrom->MutableCredentials()->MutableIam()->SetServiceAccountKey(credentials.iam().service_account_key());
                    break;
                }
                case Ydb::PersQueue::V1::Credentials::CREDENTIALS_NOT_SET: {
                    error = "one of the credential fields must be filled for remote mirror rule";
                    return Ydb::StatusIds::BAD_REQUEST;
                }
                default: {
                    error = TStringBuilder() << "unsupported credentials type " << ::NPersQueue::ObfuscateString(ToString(credentials));
                    return Ydb::StatusIds::BAD_REQUEST;
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

        return CheckConfig(*pqTabletConfig, supportedClientServiceTypes, error, pqConfig, EOperation::Create);
    }

    static bool FillMeteringMode(Ydb::Topic::MeteringMode mode, NKikimrPQ::TPQTabletConfig& config,
            bool meteringEnabled, bool isAlter, Ydb::StatusIds::StatusCode& code, TString& error)
    {
        Y_UNUSED(meteringEnabled);

        auto res = NPQ::NSchema::FillMeteringMode(config, mode, isAlter ? NPQ::NSchema::EOperation::Alter : NPQ::NSchema::EOperation::Create);
        if (!res) {
            error = res.GetErrorMessage();
            code = res.GetStatus();
            return false;
        }
        return true;
    }

    TYdbPqCodes FillProposeRequestImpl(
            const TString& name, const Ydb::Topic::CreateTopicRequest& request,
            NKikimrSchemeOp::TModifyScheme& modifyScheme, TAppData* appData,
            TString& error, const TString& path, const TString& database, const TString& localDc
    ) {
        const auto& pqConfig = appData->PQConfig;

        modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup);
        auto pqDescr = modifyScheme.MutableCreatePersQueueGroup();

        pqDescr->SetName(name);
        ui32 minParts = 1;

        auto pqTabletConfig = pqDescr->MutablePQTabletConfig();
        auto partConfig = pqTabletConfig->MutablePartitionConfig();

        if (request.retention_storage_mb())
            partConfig->SetStorageLimitBytes(request.retention_storage_mb() * 1024 * 1024);

        if (request.has_partitioning_settings()) {
            const auto& settings = request.partitioning_settings();
            if (settings.min_active_partitions() < 0) {
                error = TStringBuilder() << "Partitions count must be positive, provided " << settings.min_active_partitions();
                return TYdbPqCodes(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
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
                if (auto code = ValidatePartitionStrategy(*pqTabletConfig, error); code) {
                    return *code;
                }
            }
        }
        pqDescr->SetTotalGroupCount(minParts);
        pqTabletConfig->SetRequireAuthWrite(true);
        pqTabletConfig->SetRequireAuthRead(true);
        pqDescr->SetPartitionPerTablet(1);

        partConfig->SetMaxCountInPartition(Max<i32>());

        partConfig->SetSourceIdLifetimeSeconds(NKikimrPQ::TPartitionConfig().GetSourceIdLifetimeSeconds());
        partConfig->SetSourceIdMaxCounts(NKikimrPQ::TPartitionConfig().GetSourceIdMaxCounts());

        TConsumersAdvancedMonitoringSettings consumersAdvancedMonitoringSettings;
        auto res = ProcessAttributes(request.attributes(), pqConfig.GetTopicsAreFirstClassCitizen(), pqDescr, consumersAdvancedMonitoringSettings, error, false);
        if (res != Ydb::StatusIds::SUCCESS) {
            return TYdbPqCodes(res, Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
        }

        bool local = true; // TODO: check here cluster;

        auto topicPath = NKikimr::JoinPath({modifyScheme.GetWorkingDir(), name});
        if (!pqConfig.GetTopicsAreFirstClassCitizen()) {
            auto converter = NPersQueue::TTopicNameConverter::ForFederation(
                    pqConfig.GetRoot(), pqConfig.GetTestDatabaseRoot(), name, path, database, local, localDc,
                    pqTabletConfig->GetFederationAccount()
            );

            if (!converter->IsValid()) {
                error = TStringBuilder() << "Bad topic: " << converter->GetReason();
                return TYdbPqCodes(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT);
            }
            pqTabletConfig->SetLocalDC(local);
            pqTabletConfig->SetDC(converter->GetCluster());
            pqTabletConfig->SetProducer(converter->GetLegacyProducer());
            pqTabletConfig->SetTopic(converter->GetLegacyLogtype());
            pqTabletConfig->SetIdent(converter->GetLegacyProducer());
        }

//        config->SetTopicName(name);
//        config->SetTopicPath(topicPath);

        //Sets legacy 'logtype'.


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
                return TYdbPqCodes(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
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
                return TYdbPqCodes(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT);
            }
            ct->AddIds(codec - 1);
            ct->AddCodecs(Ydb::Topic::Codec_IsValid(codec) ? LegacySubstr(to_lower(Ydb::Topic::Codec_Name((Ydb::Topic::Codec)codec)), 6) : "CUSTOM");
        }

        if (request.consumers_size() > MAX_READ_RULES_COUNT) {
            error = TStringBuilder() << "consumers count cannot be more than "
                                     << MAX_READ_RULES_COUNT << ", provided " << request.consumers_size();
            return TYdbPqCodes(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
        }

        if (Ydb::StatusIds::StatusCode code; !FillMeteringMode(request.metering_mode(), *pqTabletConfig, pqConfig.GetBillingMeteringConfig().GetEnabled(), false, code, error)) {
            return TYdbPqCodes(code, Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT);
        }

        const auto& supportedClientServiceTypes = GetSupportedClientServiceTypes(pqConfig);


        for (const auto& consumer : request.consumers()) {
            auto messageAndCode = AddReadRuleToConfig(pqTabletConfig, consumer, supportedClientServiceTypes, true, pqConfig,
                                                      appData->FeatureFlags.GetEnableTopicDiskSubDomainQuota(),
                                                      appData,
                                                      &consumersAdvancedMonitoringSettings);
            if (messageAndCode.PQCode != Ydb::PersQueue::ErrorCode::OK) {
                error = messageAndCode.Message;
                return TYdbPqCodes(Ydb::StatusIds::BAD_REQUEST, messageAndCode.PQCode);
            }
        }
        if (auto errorCode = consumersAdvancedMonitoringSettings.CheckForUnknownConsumers(error); errorCode != Ydb::StatusIds::SUCCESS) {
            return TYdbPqCodes(errorCode, Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT);
        }

        if (request.has_metrics_level()) {
            pqTabletConfig->SetMetricsLevel(request.metrics_level());
        }

        return TYdbPqCodes(CheckConfig(*pqTabletConfig, supportedClientServiceTypes, error, pqConfig, EOperation::Create),
                           Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
    }

    Ydb::StatusIds::StatusCode FillProposeRequestImpl(
            const Ydb::Topic::AlterTopicRequest& request,
            NKikimrSchemeOp::TPersQueueGroupDescription& pqDescr, TAppData* /*appData*/,
            TString& error, bool isCdcStream
    ) {
        auto result = ApplyChangesInt(request, pqDescr, isCdcStream);
        if (result.GetStatus() != Ydb::StatusIds::SUCCESS) {
            error = result.GetErrorMessage();
        }
        return result.GetStatus();
    }
}
