#include "alter_topic_operation.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/ydb_convert/topic_description.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

namespace NKikimr::NPQ::NSchema {

TResult ApplyChangesInt(
    const Ydb::Topic::AlterTopicRequest& request,
    NKikimrSchemeOp::TPersQueueGroupDescription& config,
    bool isCdcStream
) {
    #define CHECK_CDC  if (isCdcStream) {\
            return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Full alter of cdc stream is forbidden #" << __LINE__};\
        }

    auto appData = AppData();
    TString error;

    const auto& pqConfig = appData->PQConfig;
    auto pqTabletConfig = config.MutablePQTabletConfig();
    NPQ::Migrate(*pqTabletConfig);
    auto partConfig = pqTabletConfig->MutablePartitionConfig();

    auto needHandleAutoPartitioning = false;

    auto reqHasAutoPartitioningStrategyChange = request.has_alter_partitioning_settings() &&
        request.alter_partitioning_settings().has_alter_auto_partitioning_settings() &&
        request.alter_partitioning_settings().alter_auto_partitioning_settings().has_set_strategy();

    auto pqConfigHasAutoPartitioningStrategy = pqTabletConfig->HasPartitionStrategy() &&
        pqTabletConfig->GetPartitionStrategy().HasPartitionStrategyType() &&
        pqTabletConfig->GetPartitionStrategy().GetPartitionStrategyType();

    if (pqConfigHasAutoPartitioningStrategy && pqTabletConfig->GetPartitionStrategy().GetPartitionStrategyType() != ::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_DISABLED) {
        needHandleAutoPartitioning = true;
    } else if (reqHasAutoPartitioningStrategyChange) {
        auto strategy = request.alter_partitioning_settings().alter_auto_partitioning_settings().set_strategy();
        needHandleAutoPartitioning = strategy == ::Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_PAUSED ||
                                    strategy == ::Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP ||
                                    strategy == ::Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN;
    }

    if (request.has_set_retention_storage_mb()) {
        CHECK_CDC;
        partConfig->ClearStorageLimitBytes();
        if (request.set_retention_storage_mb())
            partConfig->SetStorageLimitBytes(request.set_retention_storage_mb() * 1024 * 1024);
    }

    if (request.has_set_content_based_deduplication()) {
        pqTabletConfig->SetContentBasedDeduplication(request.set_content_based_deduplication());
    }

    if (request.has_alter_partitioning_settings()) {
        const auto& settings = request.alter_partitioning_settings();
        if (settings.has_set_min_active_partitions()) {
            auto minParts = IfEqualThenDefault<i64>(settings.set_min_active_partitions(), 0L, 1L);
            config.SetTotalGroupCount(minParts);
            if (needHandleAutoPartitioning) {
                pqTabletConfig->MutablePartitionStrategy()->SetMinPartitionCount(minParts);
            }
        }

        if (needHandleAutoPartitioning) {
            if (settings.has_set_max_active_partitions()) {
                pqTabletConfig->MutablePartitionStrategy()->SetMaxPartitionCount(settings.set_max_active_partitions());
            }
            if (settings.has_alter_auto_partitioning_settings()) {
                if (settings.alter_auto_partitioning_settings().has_set_partition_write_speed()) {
                    if (settings.alter_auto_partitioning_settings().set_partition_write_speed().has_set_up_utilization_percent()) {
                        pqTabletConfig->MutablePartitionStrategy()->SetScaleUpPartitionWriteSpeedThresholdPercent(settings.alter_auto_partitioning_settings().set_partition_write_speed().set_up_utilization_percent());
                    }
                    if (settings.alter_auto_partitioning_settings().set_partition_write_speed().has_set_down_utilization_percent()) {
                        pqTabletConfig->MutablePartitionStrategy()->SetScaleDownPartitionWriteSpeedThresholdPercent(settings.alter_auto_partitioning_settings().set_partition_write_speed().set_down_utilization_percent());
                    }
                    if (settings.alter_auto_partitioning_settings().set_partition_write_speed().has_set_stabilization_window()) {
                        pqTabletConfig->MutablePartitionStrategy()->SetScaleThresholdSeconds(settings.alter_auto_partitioning_settings().set_partition_write_speed().set_stabilization_window().seconds());
                    }
                }

                auto oldStrategy = pqTabletConfig->GetPartitionStrategy().GetPartitionStrategyType();

                if (settings.alter_auto_partitioning_settings().has_set_strategy()) {
                    switch (settings.alter_auto_partitioning_settings().set_strategy()) {
                        case ::Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP:
                            pqTabletConfig->MutablePartitionStrategy()->SetPartitionStrategyType(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT);
                            break;
                        case ::Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN:
                            pqTabletConfig->MutablePartitionStrategy()->SetPartitionStrategyType(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT_AND_MERGE);
                            break;
                        case ::Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_PAUSED:
                            pqTabletConfig->MutablePartitionStrategy()->SetPartitionStrategyType(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_PAUSED);
                            break;
                        default:
                            pqTabletConfig->MutablePartitionStrategy()->SetPartitionStrategyType(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_DISABLED);
                            break;
                    }
                }

                if (oldStrategy == ::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_DISABLED &&
                    pqTabletConfig->GetPartitionStrategy().GetPartitionStrategyType() != ::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_DISABLED) {
                    CHECK_CDC;
                }
            }
        }
    }

    if (needHandleAutoPartitioning) {
        auto code = ValidatePartitionStrategy(*pqTabletConfig);
        if (!code) return code;
    }

    if (request.alter_attributes().size()) {
        CHECK_CDC;
    }

    NGRpcProxy::V1::TConsumersAdvancedMonitoringSettings consumersAdvancedMonitoringSettings;
    auto res = ProcessTopicAttributes(request.alter_attributes(), &config, EOperation::Alter, pqConfig.GetTopicsAreFirstClassCitizen(), consumersAdvancedMonitoringSettings);
    if (!res) {
        return res;
    }

    if (request.has_set_retention_period()) {
        if (auto retentionPeriodSeconds = CheckRetentionPeriod(request.set_retention_period().seconds())) {
            partConfig->SetLifetimeSeconds(retentionPeriodSeconds.value());
        } else {
            return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << retentionPeriodSeconds.error() << ", provided " << request.set_retention_period().ShortDebugString()};
        }
    }

    bool local = true; //todo: check locality
    if (local || pqConfig.GetTopicsAreFirstClassCitizen()) {
        if (request.has_set_partition_write_speed_bytes_per_second()) {
            CHECK_CDC;
            auto partSpeed = request.set_partition_write_speed_bytes_per_second();
            if (partSpeed == 0) {
                partSpeed = DEFAULT_PARTITION_SPEED;
            }
            partConfig->SetWriteSpeedInBytesPerSecond(partSpeed);
        }

        if (request.has_set_partition_write_burst_bytes()) {
            CHECK_CDC;
            const auto& burstSpeed = request.set_partition_write_burst_bytes();
            if (burstSpeed == 0) {
                partConfig->SetBurstSize(partConfig->GetWriteSpeedInBytesPerSecond());
            } else {
                partConfig->SetBurstSize(burstSpeed);
            }
        }
    }

    if (request.has_set_supported_codecs()) {
        CHECK_CDC;
        pqTabletConfig->ClearCodecs();
        auto ct = pqTabletConfig->MutableCodecs();
        for(const auto& codec : request.set_supported_codecs().codecs()) {
            if ((!Ydb::Topic::Codec_IsValid(codec) && codec < Ydb::Topic::CODEC_CUSTOM) || codec == 0) {
                return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Unknown codec with value " << codec};
            }
            ct->AddIds(codec - 1);
            ct->AddCodecs(Ydb::Topic::Codec_IsValid(codec) ? LegacySubstr(to_lower(Ydb::Topic::Codec_Name((Ydb::Topic::Codec)codec)), 6) : "CUSTOM");
        }
    }

    auto result = FillMeteringMode(*pqTabletConfig, request.set_metering_mode(), EOperation::Create);
    if (!result) {
        return result;
    }

    const auto& supportedClientServiceTypes = GetSupportedClientServiceTypes();

    std::vector<std::pair<bool, Ydb::Topic::Consumer>> consumers;

    i32 dropped = 0;
    for (const auto& c : pqTabletConfig->GetConsumers()) {
        auto& oldName = c.GetName();
        auto name = NPersQueue::ConvertOldConsumerName(oldName, pqConfig);
        bool erase = false;
        for (auto consumer: request.drop_consumers()) {
            if (consumer == name || consumer == oldName) {
                erase = true;
                ++dropped;
                break;
            }
            if (pqTabletConfig->GetEnableCompactification() && consumer == NPQ::CLIENTID_COMPACTION_CONSUMER) {
                return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Cannot drop service consumer '" << consumer << "'from topic with compactification enabled"};
            }
        }
        if (erase) continue;
        if (!pqTabletConfig->GetEnableCompactification() && (oldName == NPQ::CLIENTID_COMPACTION_CONSUMER || name == NPQ::CLIENTID_COMPACTION_CONSUMER))
            continue;

        consumers.push_back({false, Ydb::Topic::Consumer{}}); // do not check service type for presented consumers
        auto& consumer = consumers.back().second;

        Ydb::StatusIds_StatusCode status;
        TString error;
        FillConsumer(consumer, c, status, error, false);
    }


    for (auto& cons : request.add_consumers()) {
        consumers.push_back({true, cons}); // check service type for added consumers is true
    }

    if (dropped != request.drop_consumers_size()) {
        return {Ydb::StatusIds::NOT_FOUND, "some consumers in drop_consumers are missing already"};
    }

    for (const auto& alter : request.alter_consumers()) {
        auto name = alter.name();
        auto oldName = NPersQueue::ConvertOldConsumerName(name, pqConfig);
        if (name == NPQ::CLIENTID_COMPACTION_CONSUMER || oldName == NPQ::CLIENTID_COMPACTION_CONSUMER) {
            return {Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Cannot alter service consumer '" << name};
        }
        bool found = false;
        for (auto& consumer : consumers) {
            if (consumer.second.name() == name || consumer.second.name() == oldName) {
                found = true;
                if (auto error_ = ProcessAlterConsumer(consumer.second, alter); !error_) {
                    return error_;
                }
                consumer.first = true; // check service type
                break;
            }
        }
        if (!found) {
            return {Ydb::StatusIds::NOT_FOUND, TStringBuilder() << "consumer '" << name << "' in alter_consumers is missing"};
        }
    }

    pqTabletConfig->ClearConsumers();

    for (const auto& rr : consumers) {
        auto result = ProcessAddConsumer(
            pqTabletConfig,
            rr.second,
            supportedClientServiceTypes,
            rr.first,
            &consumersAdvancedMonitoringSettings
        );
        if (!result) {
            return result;
        }
    }
    if (auto errorCode = consumersAdvancedMonitoringSettings.CheckForUnknownConsumers(error); errorCode != Ydb::StatusIds::SUCCESS) {
        return {errorCode, std::move(error)};
    }

    if (request.has_set_metrics_level()) {
        pqTabletConfig->SetMetricsLevel(request.set_metrics_level());
    } else if (request.has_reset_metrics_level()) {
        pqTabletConfig->ClearMetricsLevel();
    }

    return ValidateConfig(*pqTabletConfig, supportedClientServiceTypes, EOperation::Alter);
}

TResult ProcessAlterConsumer(Ydb::Topic::Consumer& consumer, const Ydb::Topic::AlterConsumer& alter) {
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
            return {Ydb::StatusIds::BAD_REQUEST, "Cannot alter consumer type"};
        }
    } else if (alter.has_alter_shared_consumer_type()) {
        if (!consumer.has_shared_consumer_type()) {
            return {Ydb::StatusIds::BAD_REQUEST, "Cannot alter consumer type"};
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
                    return {Ydb::StatusIds::BAD_REQUEST, "Cannot alter move action"};
                }
                if (alterPolicy.alter_move_action().has_set_dead_letter_queue()) {
                    if (alterPolicy.alter_move_action().set_dead_letter_queue().empty()) {
                        return {Ydb::StatusIds::BAD_REQUEST, "Dead letter queue cannot be empty"};
                    }
                    policy->mutable_move_action()->set_dead_letter_queue(alterPolicy.alter_move_action().set_dead_letter_queue());
                }
            } else if (alterPolicy.has_set_move_action()) {
                if (alterPolicy.set_move_action().dead_letter_queue().empty()) {
                    return {Ydb::StatusIds::BAD_REQUEST, "Dead letter queue cannot be empty"};
                }
                policy->clear_action();
                policy->mutable_move_action()->set_dead_letter_queue(alterPolicy.set_move_action().dead_letter_queue());
            } else if (alterPolicy.has_set_delete_action()) {
                policy->clear_action();
                policy->mutable_delete_action();
            }
        }
    }

    return TResult();
}

namespace {

struct TAlterTopicStrategy: public IAlterTopicStrategy {
    TAlterTopicStrategy(Ydb::Topic::AlterTopicRequest&& request)
        : Request(std::move(request))
    {
    }

    const TString& GetTopicName() const override {
        return Request.path();
    }

    TResult ApplyChanges(
        NKikimrSchemeOp::TModifyScheme& /*modifyScheme*/,
        NKikimrSchemeOp::TPersQueueGroupDescription& targetConfig,
        const NKikimrSchemeOp::TPersQueueGroupDescription& /*sourceConfig*/,
        const bool isCdcStream
    ) override {
        return ApplyChangesInt(Request, targetConfig, isCdcStream);
    }

    Ydb::Topic::AlterTopicRequest Request;
};

} // namespace

NActors::IActor* CreateAlterTopicActor(const NActors::TActorId& parentId, TAlterTopicSettings&& settings) {
    return CreateAlterTopicOperationActor(parentId, {
        .Database = std::move(settings.Database),
        .PeerName = std::move(settings.PeerName),
        .UserToken = settings.UserToken,
        .Strategy = std::make_unique<TAlterTopicStrategy>(std::move(settings.Request)),
        .IfExists = settings.IfExists,
        .Cookie = settings.Cookie,
    });
}

} // namespace NKikimr::NPQ::NSchema
