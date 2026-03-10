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
    constexpr i64 DEFAULT_MAX_DATABASE_MESSAGEGROUP_SEQNO_RETENTION_PERIOD_MS =
        TDuration::Days(16).MilliSeconds();
    constexpr ui64 DEFAULT_PARTITION_SPEED = 1_MB;
    constexpr i32 MAX_READ_RULES_COUNT = 3000;
    constexpr i32 MAX_SUPPORTED_CODECS_COUNT = 100;

    template<typename T>
    T IfEqualThenDefault(const T& value, const T& compareTo, const T& defaultValue) {
        return value == compareTo ? defaultValue : value;
    }

    TClientServiceTypes GetSupportedClientServiceTypes(const NKikimrPQ::TPQConfig& pqConfig) {
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

    static std::expected<TDuration, TString> ConvertPositiveDuration(const google::protobuf::Duration& duration) {
        if (duration.seconds() < 0) {
            return std::unexpected(TStringBuilder() << "duration seconds cannot be negative, provided " << duration.seconds());
        }
        return NKikimr::GetDuration(duration);
    }

    static std::expected<TMaybe<TDuration>, TMsgPqCodes> ConvertConsumerAvailabilityPeriod(const google::protobuf::Duration& duration, std::string_view consumerName) {
        if (auto val = ConvertPositiveDuration(duration); val.has_value()) {
            if (val.value() == TDuration::Zero()) {
                return Nothing();
            } else {
                return val.value();
            }
        } else {
            return std::unexpected(TMsgPqCodes(
                TStringBuilder() << "Invalid availability_period for consumer '" << consumerName << "': " << val.error(),
                Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT
            ));
        }
    }

    TMsgPqCodes AddReadRuleToConfig(
        NKikimrPQ::TPQTabletConfig* config,
        const Ydb::PersQueue::V1::TopicSettings::ReadRule& rr,
        const TClientServiceTypes& supportedClientServiceTypes,
        const NKikimrPQ::TPQConfig& pqConfig,
        const TConsumersAdvancedMonitoringSettings* consumersAdvancedMonitoringSettings
    ) {

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
            if (period.value().Defined()) {
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
        const NKikimrPQ::TPQConfig& pqConfig,
        bool enableTopicDiskSubDomainQuota,
        const TAppData* appData,
        TConsumersAdvancedMonitoringSettings* consumersAdvancedMonitoringSettings
    ) {
        auto consumerName = NPersQueue::ConvertNewConsumerName(rr.name(), pqConfig);
        if (consumerName.find("/") != TString::npos || consumerName.find("|") != TString::npos) {
            return TMsgPqCodes(TStringBuilder() << "consumer '" << rr.name() << "' has illegal symbols", Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT);
        }
        if (consumerName.empty()) {
            return TMsgPqCodes(TStringBuilder() << "consumer with empty name is forbidden", Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
        }

        ::NKikimrPQ::TPQTabletConfig_TConsumer* consumer = config->AddConsumers();

        consumer->SetName(consumerName);

        if (rr.has_shared_consumer_type()) {
                if (!appData->FeatureFlags.GetEnableTopicMessageLevelParallelism()) {
                    return TMsgPqCodes(TStringBuilder() << "shared consumers is disabled", Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
                }
                consumer->SetType(::NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);

                consumer->SetKeepMessageOrder(rr.shared_consumer_type().keep_messages_order());
                consumer->SetDefaultProcessingTimeoutSeconds(rr.shared_consumer_type().default_processing_timeout().seconds());

                consumer->SetDeadLetterPolicyEnabled(rr.shared_consumer_type().dead_letter_policy().enabled());
                consumer->SetMaxProcessingAttempts(rr.shared_consumer_type().dead_letter_policy().condition().max_processing_attempts());

                if (rr.shared_consumer_type().dead_letter_policy().has_move_action()) {
                    consumer->SetDeadLetterPolicy(::NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
                    consumer->SetDeadLetterQueue(rr.shared_consumer_type().dead_letter_policy().move_action().dead_letter_queue());
                } else if (rr.shared_consumer_type().dead_letter_policy().has_delete_action()) {
                    consumer->SetDeadLetterPolicy(::NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_DELETE);
                } else {
                    consumer->SetDeadLetterPolicy(::NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_UNSPECIFIED);
                }
        } else {
            consumer->SetType(::NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING);
        }

        if (rr.read_from().seconds() < 0) {
            return TMsgPqCodes(
                TStringBuilder() << "starting_message_timestamp_ms in read_rule can't be negative, provided " << rr.read_from().seconds(),
                Ydb::PersQueue::ErrorCode::VALIDATION_ERROR
            );
        }
        consumer->SetReadFromTimestampsMs(rr.read_from().seconds() * 1000);
        consumer->SetFormatVersion(0);

        const auto& defaultClientServiceType = pqConfig.GetDefaultClientServiceType().GetName();
        TString serviceType = defaultClientServiceType;

        TString passwordHash = "";
        bool hasPassword = false;

        ui32 version = 0;
        for (const auto& [attrName, attrValue] : rr.attributes()) {
            if (attrName == "_version") {
                try {
                    if (!attrValue.empty())
                        version = FromString<ui32>(attrValue);
                } catch(...) {
                    return TMsgPqCodes(
                        TStringBuilder() << "Attribute for consumer '" << rr.name() << "' _version is " << attrValue << ", which is not ui32",
                        Ydb::PersQueue::ErrorCode::VALIDATION_ERROR
                    );
                }
            } else if (attrName == "_service_type") {
                if (!attrValue.empty()) {
                    if (!supportedClientServiceTypes.contains(attrValue)) {
                        return TMsgPqCodes(TStringBuilder() << "Unknown _service_type '" << attrValue
                                                << "' for consumer '" << rr.name() << "'", Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT);
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
            return TMsgPqCodes(TStringBuilder() << "service type cannot be empty for consumer '" << rr.name() << "'", Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
        }

        Y_ABORT_UNLESS(supportedClientServiceTypes.find(serviceType) != supportedClientServiceTypes.end());

        const NKikimr::NGRpcProxy::V1::TClientServiceType& clientServiceType = supportedClientServiceTypes.find(serviceType)->second;

        if (checkServiceType) {
            bool found = clientServiceType.PasswordHashes.empty() && !hasPassword;
            for (auto ph : clientServiceType.PasswordHashes) {
                if (ph == passwordHash) {
                    found = true;
                }
            }
            if (!found) {
                if (hasPassword) {
                    return TMsgPqCodes("incorrect client service type password", Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT);
                }
                if (pqConfig.GetForceClientServiceTypePasswordCheck()) { // no password and check is required
                    return TMsgPqCodes("no client service type password provided", Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
                }
            }
        }

        consumer->SetServiceType(serviceType);
        consumer->SetVersion(version);

        auto* cct = consumer->MutableCodec();

        for(const auto& codec : rr.supported_codecs().codecs()) {
            if ((!Ydb::Topic::Codec_IsValid(codec) && codec < Ydb::Topic::CODEC_CUSTOM) || codec == 0) {
                return TMsgPqCodes(
                    TStringBuilder() << "Unknown codec for consumer '" << rr.name() << "' with value " << codec,
                    Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT
                );
            }
            cct->AddIds(codec - 1);
            cct->AddCodecs(Ydb::Topic::Codec_IsValid(codec) ? LegacySubstr(to_lower(Ydb::Topic::Codec_Name((Ydb::Topic::Codec)codec)), 6) : "CUSTOM");
        }

        if (rr.important()) {
            if (pqConfig.GetTopicsAreFirstClassCitizen() && !enableTopicDiskSubDomainQuota) {
                return TMsgPqCodes(TStringBuilder() << "important flag is forbiden for consumer " << rr.name(), Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT);
            }
            consumer->SetImportant(true);
        }
        if (auto period = ConvertConsumerAvailabilityPeriod(rr.availability_period(), rr.name()); period.has_value()) {
            if (period.value().Defined()) {
                consumer->SetAvailabilityPeriodMs(period.value()->MilliSeconds());
            } else {
                consumer->ClearAvailabilityPeriodMs();
            }
        } else {
            return period.error();
        }

        if (consumersAdvancedMonitoringSettings) {
            consumersAdvancedMonitoringSettings->UpdateConsumerConfig(rr.name(), *consumer);
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

    bool CheckReadRulesConfig(const NKikimrPQ::TPQTabletConfig& config,
                              const TClientServiceTypes& supportedClientServiceTypes,
                              TString& error, const NKikimrPQ::TPQConfig& pqConfig) {

        size_t consumerCount = NPQ::ConsumerCount(config);
        if (consumerCount > MAX_READ_RULES_COUNT) {
            error = TStringBuilder() << "read rules count cannot be more than "
                                     << MAX_READ_RULES_COUNT << ", provided " << consumerCount;
            return false;
        }

        THashSet<TString> readRuleConsumers;
        for (auto consumer : config.GetConsumers()) {
            if (readRuleConsumers.find(consumer.GetName()) != readRuleConsumers.end()) {
                error = TStringBuilder() << "Duplicate consumer name " << consumer.GetName();
                return true;
            }
            readRuleConsumers.insert(consumer.GetName());

            if (consumer.GetImportant() && consumer.HasAvailabilityPeriodMs()) {
                error = TStringBuilder() << "Consumer '" << consumer.GetName() << "' has both an important flag and a limited availability_period, which are mutually exclusive";
                return false;
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
                error = TStringBuilder() << "Count of consumers with service type '" << type << "' is limited for " << limit << " for stream\n";
                return false;
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
                            error = TStringBuilder() << "for consumer '" << name << "' got unsupported codec " << (cc+1) << " which is suppored by topic";
                            return false;
                        }
                    }
                }
            }
        }

        return false;
    }

    Ydb::StatusIds::StatusCode CheckConfig(const NKikimrPQ::TPQTabletConfig& config,
                              const TClientServiceTypes& supportedClientServiceTypes,
                              TString& error, const NKikimrPQ::TPQConfig& pqConfig,
                              const Ydb::StatusIds::StatusCode dubsStatus)
    {
        if (config.GetPartitionConfig().HasStorageLimitBytes() && config.GetPartitionConfig().GetStorageLimitBytes() > 0) {
            auto hasMLP = AnyOf(config.GetConsumers(), [](const auto& consumer) {
                return consumer.GetType() == ::NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP;
            });
            if (hasMLP) {
                error = TStringBuilder() << "Retention by storage size is not supported for shared consumers";
                return Ydb::StatusIds::BAD_REQUEST;
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
            error = TStringBuilder() << "write_speed per second in partition must have values from set {" << JoinSeq(",", validLimits) << "}, got " << speed;
            return Ydb::StatusIds::BAD_REQUEST;
        }

        if (burst > speed * 2 && burst > 1_MB) {
            error = TStringBuilder()
                    << "Invalid write burst in partition specified: " << burst
                    << " vs " << Max(speed * 2, (ui32)1_MB);
            return Ydb::StatusIds::BAD_REQUEST;
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
            error = errStr << " provided values: hours " << lifeTimeSeconds / 3600 << ", storage " << storageBytes / 1_MB;
            return Ydb::StatusIds::BAD_REQUEST;
        }

        bool hasDuplicates = CheckReadRulesConfig(config, supportedClientServiceTypes, error, pqConfig);
        return error.empty() ? Ydb::StatusIds::SUCCESS : (hasDuplicates ? dubsStatus : Ydb::StatusIds::BAD_REQUEST);
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

        auto config = pqDescr->MutablePQTabletConfig();
        auto partConfig = config->MutablePartitionConfig();

        for (const auto& [attrName, attrValue] : attributes) {
            if (attrName == "_partitions_per_tablet") {
                try {
                    if (!alter)
                        pqDescr->SetPartitionPerTablet(FromString<ui32>(attrValue));
                    if (pqDescr->GetPartitionPerTablet() > 20) {
                        error = TStringBuilder() << "Attribute partitions_per_tablet is " << attrValue << ", which is greater than 20";
                        return Ydb::StatusIds::BAD_REQUEST;
                    }
                } catch(...) {
                    error = TStringBuilder() << "Attribute partitions_per_tablet is " << attrValue << ", which is not ui32";
                    return Ydb::StatusIds::BAD_REQUEST;
                }
            } else if (attrName == "_allow_unauthenticated_read") {
                if (attrValue.empty()) {
                    config->SetRequireAuthRead(true);
                } else  {
                    try {
                        config->SetRequireAuthRead(!FromString<bool>(attrValue));
                    } catch(...) {
                        error = TStringBuilder() << "Attribute allow_unauthenticated_read is " << attrValue << ", which is not bool";
                        return Ydb::StatusIds::BAD_REQUEST;
                    }
                }
            } else if (attrName == "_allow_unauthenticated_write") {
                if (attrValue.empty()) {
                    config->SetRequireAuthWrite(true);
                } else  {
                    try {
                        config->SetRequireAuthWrite(!FromString<bool>(attrValue));
                    } catch(...) {
                        error = TStringBuilder() << "Attribute allow_unauthenticated_write is " << attrValue << ", which is not bool";
                        return Ydb::StatusIds::BAD_REQUEST;
                    }
                }
            } else if (attrName == "_abc_slug") {
                config->SetAbcSlug(attrValue);
            }  else if (attrName == "_federation_account") {
                config->SetFederationAccount(attrValue);
            } else if (attrName == "_abc_id") {
                if (attrValue.empty()) {
                    config->SetAbcId(0);
                } else {
                    try {
                        config->SetAbcId(FromString<ui32>(attrValue));
                    } catch(...) {
                        error = TStringBuilder() << "Attribute abc_id is " << attrValue << ", which is not integer";
                        return Ydb::StatusIds::BAD_REQUEST;
                    }
                }
            } else if (attrName == "_max_partition_storage_size") {
                if (attrValue.empty()) {
                    partConfig->SetMaxSizeInPartition(Max<i64>());
                } else {
                    try {
                        i64 size = FromString<i64>(attrValue);
                        if (size < 0) {
                            error = TStringBuilder() << "_max_partiton_strorage_size can't be negative, provided " << size;
                            return Ydb::StatusIds::BAD_REQUEST;
                        }

                        partConfig->SetMaxSizeInPartition(size ? size : Max<i64>());

                    } catch(...) {
                        error = TStringBuilder() << "Attribute _max_partition_storage_size is " << attrValue << ", which is not ui64";
                        return Ydb::StatusIds::BAD_REQUEST;
                    }
                }
            }  else if (attrName == "_message_group_seqno_retention_period_ms") {
                partConfig->SetSourceIdLifetimeSeconds(NKikimrPQ::TPartitionConfig().GetSourceIdLifetimeSeconds());
                if (!attrValue.empty()) {
                    try {
                        i64 ms = FromString<i64>(attrValue);
                        if (ms < 0) {
                            error = TStringBuilder() << "_message_group_seqno_retention_period_ms can't be negative, provided " << ms;
                            return Ydb::StatusIds::BAD_REQUEST;
                        }

                        if (ms > DEFAULT_MAX_DATABASE_MESSAGEGROUP_SEQNO_RETENTION_PERIOD_MS) {
                            error = TStringBuilder() <<
                                "message_group_seqno_retention_period_ms (provided " << ms <<
                                ") must be less then default limit for database " <<
                                DEFAULT_MAX_DATABASE_MESSAGEGROUP_SEQNO_RETENTION_PERIOD_MS;
                            return Ydb::StatusIds::BAD_REQUEST;
                        }
                        if (ms > 0) {
                            partConfig->SetSourceIdLifetimeSeconds(ms > 999 ? ms / 1000 : 1);
                        }
                    } catch(...) {
                        error = TStringBuilder() << "Attribute " << attrName << " is " << attrValue << ", which is not ui64";
                        return Ydb::StatusIds::BAD_REQUEST;
                    }
                }

            } else if (attrName == "_max_partition_message_groups_seqno_stored") {
                partConfig->SetSourceIdMaxCounts(NKikimrPQ::TPartitionConfig().GetSourceIdMaxCounts());
                if (!attrValue.empty()) {
                    try {
                        i64 count = FromString<i64>(attrValue);
                        if (count < 0) {
                            error = TStringBuilder() << attrName << " can't be negative, provided " << count;
                            return Ydb::StatusIds::BAD_REQUEST;
                        }
                        if (count > 0) {
                            partConfig->SetSourceIdMaxCounts(count);
                        }
                    } catch(...) {
                        error = TStringBuilder() << "Attribute " << attrName << " is " << attrValue << ", which is not ui64";
                        return Ydb::StatusIds::BAD_REQUEST;
                    }
                }
            } else if (attrName == "_cleanup_policy") {
                config->SetEnableCompactification(attrValue == "compact");
            } else if (attrName == "_timestamp_type") {
                if (!attrValue || attrValue == NKafka::MESSAGE_TIMESTAMP_CREATE_TIME || attrValue == NKafka::MESSAGE_TIMESTAMP_LOG_APPEND) {
                    config->SetTimestampType(attrValue ? attrValue :  NKafka::MESSAGE_TIMESTAMP_CREATE_TIME);
                } else {
                    error = TStringBuilder() << "Attribute " << attrName << " is " << attrValue << ", which is an incorrect value.";
                    return Ydb::StatusIds::BAD_REQUEST;
                }
            } else if (attrName == "_advanced_monitoring") {
                if (topicsAreFirstClassCitizen) {
                    error = TStringBuilder() << "Attribute " << attrName << " is not supported in non-federation";
                    return Ydb::StatusIds::BAD_REQUEST;
                }
                if (std::expected m = TConsumersAdvancedMonitoringSettings::FromJson(attrValue); m.has_value()) {
                    consumersAdvancedMonitoringSettings = std::move(m).value();
                } else {
                    error = std::move(m).error();
                    return Ydb::StatusIds::BAD_REQUEST;
                }
            } else {
                error = TStringBuilder() << "Attribute " << attrName << " is not supported";
                return Ydb::StatusIds::BAD_REQUEST;
            }
        }
        return Ydb::StatusIds::SUCCESS;

    }

    std::optional<TYdbPqCodes> ValidatePartitionStrategy(const ::NKikimrPQ::TPQTabletConfig& config, TString& error) {
        if (!config.HasPartitionStrategy()) {
            return std::nullopt;
        }
        auto strategy = config.GetPartitionStrategy();
        if (strategy.GetMinPartitionCount() < 0) {
            error = TStringBuilder() << "Partitions count must be non-negative, provided " << strategy.GetMinPartitionCount();
            return TYdbPqCodes(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
        }
        if (strategy.GetMaxPartitionCount() < 0) {
            error = TStringBuilder() << "Partitions count must be non-negative, provided " << strategy.GetMaxPartitionCount();
            return TYdbPqCodes(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
        }
        if (strategy.GetMaxPartitionCount() != 0 && strategy.GetMaxPartitionCount() < strategy.GetMinPartitionCount()) {
            error = TStringBuilder() << "Max active partitions must be greater than or equal to partitions count or equals zero (unlimited), provided "
                << strategy.GetMaxPartitionCount() << " and " << strategy.GetMinPartitionCount();
            return TYdbPqCodes(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
        }
        if (strategy.GetScaleUpPartitionWriteSpeedThresholdPercent() < 0 || strategy.GetScaleUpPartitionWriteSpeedThresholdPercent() > 100) {
            error = TStringBuilder() << "Partition scale up threshold percent must be between 0 and 100, provided " << strategy.GetScaleUpPartitionWriteSpeedThresholdPercent();
            return TYdbPqCodes(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
        }
        if (strategy.GetScaleDownPartitionWriteSpeedThresholdPercent() < 0 || strategy.GetScaleDownPartitionWriteSpeedThresholdPercent() > 100) {
            error = TStringBuilder() << "Partition scale down threshold percent must be between 0 and 100, provided " << strategy.GetScaleDownPartitionWriteSpeedThresholdPercent();
            return TYdbPqCodes(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
        }
        if (strategy.GetScaleThresholdSeconds() <= 0) {
            error = TStringBuilder() << "Partition scale threshold time must be greater then 1 second, provided " << strategy.GetScaleThresholdSeconds() << " seconds";
            return TYdbPqCodes(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
        }
        if (config.GetPartitionConfig().HasStorageLimitBytes()) {
            error = TStringBuilder() << "Auto partitioning is incompatible with retention storage bytes option";
            return TYdbPqCodes(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
        }

        return std::nullopt;
    }

    static std::expected<i32, TString> CheckRetentionPeriod(auto seconds) {
        if (std::cmp_greater(seconds, Max<i32>())) {
            return std::unexpected{"retention_period must be less than " + ToString(ui64(Max<i32>()) + 1)};
        } else if (std::cmp_less_equal(seconds, 0)) {
            return std::unexpected{"retention_period must be positive"};
        }
        return seconds;
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
            if (AppData(ctx)->FeatureFlags.GetEnableTopicSplitMerge()) {
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

        return CheckConfig(*pqTabletConfig, supportedClientServiceTypes, error, pqConfig, Ydb::StatusIds::BAD_REQUEST);
    }

    static bool FillMeteringMode(Ydb::Topic::MeteringMode mode, NKikimrPQ::TPQTabletConfig& config,
            bool meteringEnabled, bool isAlter, Ydb::StatusIds::StatusCode& code, TString& error)
    {
        if (meteringEnabled) {
            switch (mode) {
                case Ydb::Topic::METERING_MODE_UNSPECIFIED:
                    if (!isAlter) {
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
                    code = Ydb::StatusIds::BAD_REQUEST;
                    error = "Unknown metering mode";
                    return false;
            }
        } else {
            switch (mode) {
                case Ydb::Topic::METERING_MODE_UNSPECIFIED:
                    break;
                default:
                    code = Ydb::StatusIds::PRECONDITION_FAILED;
                    error = "Metering mode can only be specified in a serverless database";
                    return false;
            }
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
            if (appData->FeatureFlags.GetEnableTopicSplitMerge() &&
                request.partitioning_settings().has_auto_partitioning_settings() &&
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

        return TYdbPqCodes(CheckConfig(*pqTabletConfig, supportedClientServiceTypes, error, pqConfig, Ydb::StatusIds::BAD_REQUEST),
                           Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
    }

    Ydb::StatusIds::StatusCode FillProposeRequestImpl(
            const Ydb::Topic::AlterTopicRequest& request,
            NKikimrSchemeOp::TPersQueueGroupDescription& pqDescr, TAppData* appData,
            TString& error, bool isCdcStream
    ) {
        #define CHECK_CDC  if (isCdcStream) {\
                    error = "Full alter of cdc stream is forbidden";\
                    return Ydb::StatusIds::BAD_REQUEST;\
            }

        const auto& pqConfig = appData->PQConfig;
        auto pqTabletConfig = pqDescr.MutablePQTabletConfig();
        NPQ::Migrate(*pqTabletConfig);
        auto partConfig = pqTabletConfig->MutablePartitionConfig();

        auto needHandleAutoPartitioning = false;
        if (appData->FeatureFlags.GetEnableTopicSplitMerge()) {

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
        }


        if (request.has_set_retention_storage_mb()) {
            CHECK_CDC;
            partConfig->ClearStorageLimitBytes();
            if (request.set_retention_storage_mb())
                partConfig->SetStorageLimitBytes(request.set_retention_storage_mb() * 1024 * 1024);
        }

        if (request.has_alter_partitioning_settings()) {
            const auto& settings = request.alter_partitioning_settings();
            if (settings.has_set_min_active_partitions()) {
                auto minParts = IfEqualThenDefault<i64>(settings.set_min_active_partitions(), 0L, 1L);
                pqDescr.SetTotalGroupCount(minParts);
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
            auto code = ValidatePartitionStrategy(*pqTabletConfig, error);
            if (code) return code->YdbCode;
        }

        if (request.alter_attributes().size()) {
            CHECK_CDC;
        }

        TConsumersAdvancedMonitoringSettings consumersAdvancedMonitoringSettings;
        auto res = ProcessAttributes(request.alter_attributes(), pqConfig.GetTopicsAreFirstClassCitizen(), &pqDescr, consumersAdvancedMonitoringSettings, error, true);
        if (res != Ydb::StatusIds::SUCCESS) {
            return res;
        }

        if (request.has_set_retention_period()) {
            if (auto retentionPeriodSeconds = CheckRetentionPeriod(request.set_retention_period().seconds())) {
                partConfig->SetLifetimeSeconds(retentionPeriodSeconds.value());
            } else {
                error = TStringBuilder() << retentionPeriodSeconds.error() << ", provided " << request.set_retention_period().ShortDebugString();
                return Ydb::StatusIds::BAD_REQUEST;
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
                    error = TStringBuilder() << "Unknown codec with value " << codec;
                    return Ydb::StatusIds::BAD_REQUEST;
                }
                ct->AddIds(codec - 1);
                ct->AddCodecs(Ydb::Topic::Codec_IsValid(codec) ? LegacySubstr(to_lower(Ydb::Topic::Codec_Name((Ydb::Topic::Codec)codec)), 6) : "CUSTOM");
            }
        }

        Ydb::StatusIds::StatusCode code;
        if (!FillMeteringMode(request.set_metering_mode(), *pqTabletConfig, pqConfig.GetBillingMeteringConfig().GetEnabled(), true, code, error)) {
            return code;
        }

        const auto& supportedClientServiceTypes = GetSupportedClientServiceTypes(pqConfig);


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
                    error = TStringBuilder() << "Cannot drop service consumer '" << consumer << "'from topic with compactification enabled";
                    return Ydb::StatusIds::BAD_REQUEST;
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
            error = "some consumers in drop_consumers are missing already";
            return Ydb::StatusIds::NOT_FOUND;
        }

        for (const auto& alter : request.alter_consumers()) {
            auto name = alter.name();
            auto oldName = NPersQueue::ConvertOldConsumerName(name, pqConfig);
            if (name == NPQ::CLIENTID_COMPACTION_CONSUMER || oldName == NPQ::CLIENTID_COMPACTION_CONSUMER) {
                error = TStringBuilder() << "Cannot alter service consumer '" << name;
                return Ydb::StatusIds::BAD_REQUEST;
            }
            bool found = false;
            for (auto& consumer : consumers) {
                if (consumer.second.name() == name || consumer.second.name() == oldName) {
                    found = true;
                    if (auto error_ = ProcessAlterConsumer(consumer.second, alter)) {
                        error = error_;
                        return Ydb::StatusIds::BAD_REQUEST;
                    }
                    consumer.first = true; // check service type
                    break;
                }
            }
            if (!found) {
                error = TStringBuilder() << "consumer '" << name << "' in alter_consumers is missing";
                return Ydb::StatusIds::NOT_FOUND;
            }
        }

        pqTabletConfig->ClearConsumers();

        for (const auto& rr : consumers) {
            auto messageAndCode = AddReadRuleToConfig(pqTabletConfig, rr.second, supportedClientServiceTypes, rr.first,
                                                      pqConfig, appData->FeatureFlags.GetEnableTopicDiskSubDomainQuota(),
                                                      appData,
                                                      &consumersAdvancedMonitoringSettings);
            if (messageAndCode.PQCode != Ydb::PersQueue::ErrorCode::OK) {
                error = messageAndCode.Message;
                return Ydb::StatusIds::BAD_REQUEST;
            }
        }
        if (auto errorCode = consumersAdvancedMonitoringSettings.CheckForUnknownConsumers(error); errorCode != Ydb::StatusIds::SUCCESS) {
            return errorCode;
        }

        if (request.has_set_metrics_level()) {
            pqTabletConfig->SetMetricsLevel(request.set_metrics_level());
        } else if (request.has_reset_metrics_level()) {
            pqTabletConfig->ClearMetricsLevel();
        }

        return CheckConfig(*pqTabletConfig, supportedClientServiceTypes, error, pqConfig, Ydb::StatusIds::ALREADY_EXISTS);
    }
}
