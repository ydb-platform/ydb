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
                              const TClientServiceTypes& /*supportedClientServiceTypes*/,
                              TString& error, const NKikimrPQ::TPQConfig& /*pqConfig*/,
                              EOperation operation)
    {
        auto result = NPQ::NSchema::ValidateConfig(config, operation);
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
}
