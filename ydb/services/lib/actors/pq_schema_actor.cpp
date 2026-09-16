#include "pq_schema_actor.h"

#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/core/persqueue/public/constants.h>

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

    TString RemoveReadRuleFromConfig(
        NKikimrPQ::TPQTabletConfig* config,
        const NKikimrPQ::TPQTabletConfig& originalConfig,
        const TString& consumerName,
        const NKikimrPQ::TPQConfig& /*pqConfig*/
    ) {
        config->ClearConsumers();
        NPQ::ClearReadQuotaExceptWithoutConsumer(*config);

        bool removed = false;

        for (auto& consumer : originalConfig.GetConsumers()) {
            if (consumerName == consumer.GetName()) {
                removed = true;
                continue;
            }

            auto* dst = config->AddConsumers();
            dst->CopyFrom(consumer);
            auto* srcReadQuota = NPQ::GetReadQuota(originalConfig, consumer.GetName());
            if (srcReadQuota) {
                auto* dstReadQuota = NPQ::GetOrAddReadQuota(*config, consumer.GetName());
                dstReadQuota->CopyFrom(*srcReadQuota);
            }
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
