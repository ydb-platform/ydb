#include "pq_schema_actor.h"

#include <ydb/library/persqueue/obfuscate/obfuscate.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/core/base/feature_flags.h>

#include <ydb/public/lib/jwt/jwt.h>

#include <ydb/library/yql/public/decimal/yql_decimal.h>

#include <util/string/vector.h>

#include <library/cpp/digest/md5/md5.h>


namespace NKikimr::NGRpcProxy::V1 {

    constexpr TStringBuf GRPCS_ENDPOINT_PREFIX = "grpcs://";
    constexpr TStringBuf GRPC_ENDPOINT_PREFIX = "grpc://";
    constexpr i64 DEFAULT_MAX_DATABASE_MESSAGEGROUP_SEQNO_RETENTION_PERIOD_MS =
        TDuration::Days(16).MilliSeconds();
    constexpr ui64 DEFAULT_PARTITION_SPEED = 1_MB;
    constexpr i32 MAX_READ_RULES_COUNT = 3000;
    constexpr i32 MAX_SUPPORTED_CODECS_COUNT = 100;

    TClientServiceTypes GetSupportedClientServiceTypes(const TActorContext& ctx) {
        TClientServiceTypes serviceTypes;
        const auto& pqConfig = AppData(ctx)->PQConfig;
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

    TString ReadRuleServiceTypeMigration(NKikimrPQ::TPQTabletConfig *config, const TActorContext& ctx) {
        auto rrServiceTypes = config->MutableReadRuleServiceTypes();
        if (config->ReadRuleServiceTypesSize() > config->ReadRulesSize()) {
            rrServiceTypes->Clear();
        }
        if (config->ReadRuleServiceTypesSize() < config->ReadRulesSize()) {
            rrServiceTypes->Reserve(config->ReadRulesSize());
            const auto& pqConfig = AppData(ctx)->PQConfig;
            if (pqConfig.GetDisallowDefaultClientServiceType()) {
                return "service type must be set for all read rules";
            }
            for (ui64 i = rrServiceTypes->size(); i < config->ReadRulesSize(); ++i) {
                *rrServiceTypes->Add() = pqConfig.GetDefaultClientServiceType().GetName();
            }
        }
        return "";
    }

    TMsgPqCodes AddReadRuleToConfig(
        NKikimrPQ::TPQTabletConfig* config,
        const Ydb::PersQueue::V1::TopicSettings::ReadRule& rr,
        const TClientServiceTypes& supportedClientServiceTypes,
        const TActorContext& ctx
    ) {

        auto consumerName = NPersQueue::ConvertNewConsumerName(rr.consumer_name(), ctx);
        if (consumerName.empty()) {
            return TMsgPqCodes(TStringBuilder() << "consumer with empty name is forbidden", Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
        }
        if(consumerName.find("/") != TString::npos || consumerName.find("|") != TString::npos) {
            return TMsgPqCodes(
                TStringBuilder() << "consumer '" << rr.consumer_name() << "' has illegal symbols",
                Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT
            );
        }
        {
            TString migrationError = ReadRuleServiceTypeMigration(config, ctx);
            if (migrationError) {
                return TMsgPqCodes(migrationError, Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT);
            }
        }

        config->AddReadRules(consumerName);

        if (rr.starting_message_timestamp_ms() < 0) {
            return TMsgPqCodes(
                TStringBuilder() << "starting_message_timestamp_ms in read_rule can't be negative, provided " << rr.starting_message_timestamp_ms(),
                Ydb::PersQueue::ErrorCode::VALIDATION_ERROR
            );
        }
        config->AddReadFromTimestampsMs(rr.starting_message_timestamp_ms());

        if (!Ydb::PersQueue::V1::TopicSettings::Format_IsValid((int)rr.supported_format()) || rr.supported_format() == 0) {
            return TMsgPqCodes(
                TStringBuilder() << "Unknown format version with value " << (int)rr.supported_format()  << " for " << rr.consumer_name(),
                Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT
            );
        }
        config->AddConsumerFormatVersions(rr.supported_format() - 1);

        if (rr.version() < 0) {
            return TMsgPqCodes(
                TStringBuilder() << "version in read_rule can't be negative, provided " << rr.version(),
                Ydb::PersQueue::ErrorCode::VALIDATION_ERROR
            );
        }
        config->AddReadRuleVersions(rr.version());
        auto ct = config->AddConsumerCodecs();
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
            ct->AddIds(codec - 1);
            ct->AddCodecs(to_lower(Ydb::PersQueue::V1::Codec_Name((Ydb::PersQueue::V1::Codec)codec)).substr(6));
        }

        if (rr.important()) {
            if (AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
                return TMsgPqCodes(
                    TStringBuilder() << "important flag is forbiden for consumer " << rr.consumer_name(),
                    Ydb::PersQueue::ErrorCode::VALIDATION_ERROR
                );
            }
            config->MutablePartitionConfig()->AddImportantClientId(consumerName);
        }

        if (!rr.service_type().empty()) {
            if (!supportedClientServiceTypes.contains(rr.service_type())) {
                return TMsgPqCodes(
                    TStringBuilder() << "Unknown read rule service type '" << rr.service_type()
                                        << "' for consumer '" << rr.consumer_name() << "'",
                    Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT
                );
            }
            config->AddReadRuleServiceTypes(rr.service_type());
        } else {
            const auto& pqConfig = AppData(ctx)->PQConfig;
            if (pqConfig.GetDisallowDefaultClientServiceType()) {
                return TMsgPqCodes(
                    TStringBuilder() << "service type cannot be empty for consumer '" << rr.consumer_name() << "'",
                    Ydb::PersQueue::ErrorCode::VALIDATION_ERROR
                );
            }
            const auto& defaultCientServiceType = pqConfig.GetDefaultClientServiceType().GetName();
            config->AddReadRuleServiceTypes(defaultCientServiceType);
        }
        return TMsgPqCodes("", Ydb::PersQueue::ErrorCode::OK);
    }


    void ProcessAlterConsumer(Ydb::Topic::Consumer& consumer, const Ydb::Topic::AlterConsumer& alter) {
        if (alter.has_set_important()) {
            consumer.set_important(alter.set_important());
        }
        if (alter.has_set_read_from()) {
            consumer.mutable_read_from()->CopyFrom(alter.set_read_from());
        }
        if (alter.has_set_supported_codecs()) {
            consumer.mutable_supported_codecs()->CopyFrom(alter.set_supported_codecs());
        }
        for (auto& pair : alter.alter_attributes()) {
            (*consumer.mutable_attributes())[pair.first] = pair.second;
        }
    }

    TMsgPqCodes AddReadRuleToConfig(
        NKikimrPQ::TPQTabletConfig* config,
        const Ydb::Topic::Consumer& rr,
        const TClientServiceTypes& supportedClientServiceTypes,
        const bool checkServiceType,
        const TActorContext& ctx
    ) {
        auto consumerName = NPersQueue::ConvertNewConsumerName(rr.name(), ctx);
        if (consumerName.find("/") != TString::npos || consumerName.find("|") != TString::npos) {
            return TMsgPqCodes(TStringBuilder() << "consumer '" << rr.name() << "' has illegal symbols", Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT);
        }
        if (consumerName.empty()) {
            return TMsgPqCodes(TStringBuilder() << "consumer with empty name is forbidden", Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
        }
        {
            TString migrationError = ReadRuleServiceTypeMigration(config, ctx);
            if (migrationError) {
                return TMsgPqCodes(migrationError, migrationError.empty() ? Ydb::PersQueue::ErrorCode::OK : Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);  //find better issueCode
            }
        }

        config->AddReadRules(consumerName);

        if (rr.read_from().seconds() < 0) {
            return TMsgPqCodes(
                TStringBuilder() << "starting_message_timestamp_ms in read_rule can't be negative, provided " << rr.read_from().seconds(),
                Ydb::PersQueue::ErrorCode::VALIDATION_ERROR
            );
        }
        config->AddReadFromTimestampsMs(rr.read_from().seconds() * 1000);

        config->AddConsumerFormatVersions(0);
        TString serviceType;
        const auto& pqConfig = AppData(ctx)->PQConfig;

        const auto& defaultClientServiceType = pqConfig.GetDefaultClientServiceType().GetName();
        serviceType = defaultClientServiceType;

        TString passwordHash = "";
        bool hasPassword = false;

        ui32 version = 0;
        for (auto& pair : rr.attributes()) {
            if (pair.first == "_version") {
                try {
                    if (!pair.second.empty())
                        version = FromString<ui32>(pair.second);
                } catch(...) {
                    return TMsgPqCodes(
                        TStringBuilder() << "Attribute for consumer '" << rr.name() << "' _version is " << pair.second << ", which is not ui32",
                        Ydb::PersQueue::ErrorCode::VALIDATION_ERROR
                    );
                }
            } else if (pair.first == "_service_type") {
                if (!pair.second.empty()) {
                    if (!supportedClientServiceTypes.contains(pair.second)) {
                        return TMsgPqCodes(TStringBuilder() << "Unknown _service_type '" << pair.second
                                                << "' for consumer '" << rr.name() << "'", Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT);
                    }
                    serviceType = pair.second;
                }
            } else if (pair.first == "_service_type_password") {
                passwordHash = MD5::Data(pair.second);
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

        config->AddReadRuleServiceTypes(serviceType);
        config->AddReadRuleVersions(version);

        auto ct = config->AddConsumerCodecs();

        for(const auto& codec : rr.supported_codecs().codecs()) {
            if ((!Ydb::Topic::Codec_IsValid(codec) && codec < Ydb::Topic::CODEC_CUSTOM) || codec == 0) {
                return TMsgPqCodes(
                    TStringBuilder() << "Unknown codec for consumer '" << rr.name() << "' with value " << codec,
                    Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT
                );
            }
            ct->AddIds(codec - 1);
            ct->AddCodecs(Ydb::Topic::Codec_IsValid(codec) ? LegacySubstr(to_lower(Ydb::Topic::Codec_Name((Ydb::Topic::Codec)codec)), 6) : "CUSTOM");
        }

        if (rr.important()) {
            if (pqConfig.GetTopicsAreFirstClassCitizen() && !AppData(ctx)->FeatureFlags.GetEnableTopicDiskSubDomainQuota()) {
                return TMsgPqCodes(TStringBuilder() << "important flag is forbiden for consumer " << rr.name(), Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT);
            }
            config->MutablePartitionConfig()->AddImportantClientId(consumerName);
        }

        return TMsgPqCodes("", Ydb::PersQueue::ErrorCode::OK);
    }


    TString RemoveReadRuleFromConfig(
        NKikimrPQ::TPQTabletConfig* config,
        const NKikimrPQ::TPQTabletConfig& originalConfig,
        const TString& consumerName,
        const TActorContext& ctx
    ) {
        THashSet<TString> rulesToRemove;
        rulesToRemove.insert(consumerName);

        config->ClearReadRuleVersions();
        config->ClearReadRules();
        config->ClearReadFromTimestampsMs();
        config->ClearConsumerFormatVersions();
        config->ClearConsumerCodecs();
        config->MutablePartitionConfig()->ClearImportantClientId();
        config->ClearReadRuleServiceTypes();

        for (const auto& importantConsumer : originalConfig.GetPartitionConfig().GetImportantClientId()) {
            if (rulesToRemove.find(importantConsumer) == rulesToRemove.end()) {
                config->MutablePartitionConfig()->AddImportantClientId(importantConsumer);
            }
        }

        const auto& pqConfig = AppData(ctx)->PQConfig;
        for (size_t i = 0; i < originalConfig.ReadRulesSize(); i++) {
            if (auto it = rulesToRemove.find(originalConfig.GetReadRules(i)); it != rulesToRemove.end()) {
                rulesToRemove.erase(it);
                continue;
            }

            config->AddReadRuleVersions(originalConfig.GetReadRuleVersions(i));
            config->AddReadRules(originalConfig.GetReadRules(i));
            config->AddReadFromTimestampsMs(originalConfig.GetReadFromTimestampsMs(i));
            config->AddConsumerFormatVersions(originalConfig.GetConsumerFormatVersions(i));
            auto ct = config->AddConsumerCodecs();
            for (size_t j = 0; j < originalConfig.GetConsumerCodecs(i).CodecsSize(); j++) {
                ct->AddCodecs(originalConfig.GetConsumerCodecs(i).GetCodecs(j));
                ct->AddIds(originalConfig.GetConsumerCodecs(i).GetIds(j));
            }
            if (i < originalConfig.ReadRuleServiceTypesSize()) {
                config->AddReadRuleServiceTypes(originalConfig.GetReadRuleServiceTypes(i));
            } else {
                if (pqConfig.GetDisallowDefaultClientServiceType()) {
                    return TStringBuilder() << "service type cannot be empty for consumer '"
                        << originalConfig.GetReadRules(i) << "'";
                }
                config->AddReadRuleServiceTypes(pqConfig.GetDefaultClientServiceType().GetName());
            }
        }

        if (rulesToRemove.size() > 0) {
            return TStringBuilder() << "Rule for consumer " << *rulesToRemove.begin() << " doesn't exist";
        }

        return "";
    }

    bool CheckReadRulesConfig(const NKikimrPQ::TPQTabletConfig& config,
                              const TClientServiceTypes& supportedClientServiceTypes,
                              TString& error, const TActorContext& ctx) {

        if (config.GetReadRules().size() > MAX_READ_RULES_COUNT) {
            error = TStringBuilder() << "read rules count cannot be more than "
                                     << MAX_READ_RULES_COUNT << ", provided " << config.GetReadRules().size();
            return false;
        }

        THashSet<TString> readRuleConsumers;
        for (auto consumerName : config.GetReadRules()) {
            if (readRuleConsumers.find(consumerName) != readRuleConsumers.end()) {
                error = TStringBuilder() << "Duplicate consumer name " << consumerName;
                return true;
            }
            readRuleConsumers.insert(consumerName);
        }

        for (const auto& t : supportedClientServiceTypes) {

            auto type = t.first;
            auto count = std::count_if(config.GetReadRuleServiceTypes().begin(), config.GetReadRuleServiceTypes().end(),
                        [type](const TString& cType){
                            return type == cType;
                        });
            auto limit = t.second.MaxCount;
            if (count > limit) {
                error = TStringBuilder() << "Count of consumers with service type '" << type << "' is limited for " << limit << " for stream\n";
                return false;
            }
        }
        if (config.GetCodecs().IdsSize() > 0) {
            for (ui32 i = 0; i < config.ConsumerCodecsSize(); ++i) {
                TString name = NPersQueue::ConvertOldConsumerName(config.GetReadRules(i), ctx);

                auto& consumerCodecs = config.GetConsumerCodecs(i);
                if (consumerCodecs.IdsSize() > 0) {
                    THashSet<i64> codecs;
                    for (auto& cc : consumerCodecs.GetIds()) {
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
                              TString& error, const TActorContext& ctx, const Ydb::StatusIds::StatusCode dubsStatus)
    {
        ui32 speed = config.GetPartitionConfig().GetWriteSpeedInBytesPerSecond();
        ui32 burst = config.GetPartitionConfig().GetBurstSize();

        std::set<ui32> validLimits {};
        if (AppData(ctx)->PQConfig.ValidWriteSpeedLimitsKbPerSecSize() == 0) {
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

        bool hasDuplicates = CheckReadRulesConfig(config, supportedClientServiceTypes, error, ctx);
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


    Ydb::StatusIds::StatusCode ProcessAttributes(const ::google::protobuf::Map<TProtoStringType, TProtoStringType>& attributes, NKikimrSchemeOp::TPersQueueGroupDescription* pqDescr, TString& error, bool alter) {

        auto config = pqDescr->MutablePQTabletConfig();
        auto partConfig = config->MutablePartitionConfig();

        for (auto& pair : attributes) {
            if (pair.first == "_partitions_per_tablet") {
                try {
                    if (!alter)
                        pqDescr->SetPartitionPerTablet(FromString<ui32>(pair.second));
                    if (pqDescr->GetPartitionPerTablet() > 20) {
                        error = TStringBuilder() << "Attribute partitions_per_tablet is " << pair.second << ", which is greater than 20";
                        return Ydb::StatusIds::BAD_REQUEST;
                    }
                } catch(...) {
                    error = TStringBuilder() << "Attribute partitions_per_tablet is " << pair.second << ", which is not ui32";
                    return Ydb::StatusIds::BAD_REQUEST;
                }
            } else if (pair.first == "_allow_unauthenticated_read") {
                if (pair.second.empty()) {
                    config->SetRequireAuthRead(true);
                } else  {
                    try {
                        config->SetRequireAuthRead(!FromString<bool>(pair.second));
                    } catch(...) {
                        error = TStringBuilder() << "Attribute allow_unauthenticated_read is " << pair.second << ", which is not bool";
                        return Ydb::StatusIds::BAD_REQUEST;
                    }
                }
            } else if (pair.first == "_allow_unauthenticated_write") {
                if (pair.second.empty()) {
                    config->SetRequireAuthWrite(true);
                } else  {
                    try {
                        config->SetRequireAuthWrite(!FromString<bool>(pair.second));
                    } catch(...) {
                        error = TStringBuilder() << "Attribute allow_unauthenticated_write is " << pair.second << ", which is not bool";
                        return Ydb::StatusIds::BAD_REQUEST;
                    }
                }
            } else if (pair.first == "_abc_slug") {
                config->SetAbcSlug(pair.second);
            }  else if (pair.first == "_federation_account") {
                config->SetFederationAccount(pair.second);
            } else if (pair.first == "_abc_id") {
                if (pair.second.empty()) {
                    config->SetAbcId(0);
                } else {
                    try {
                        config->SetAbcId(FromString<ui32>(pair.second));
                    } catch(...) {
                        error = TStringBuilder() << "Attribute abc_id is " << pair.second << ", which is not integer";
                        return Ydb::StatusIds::BAD_REQUEST;
                    }
                }
            } else if (pair.first == "_max_partition_storage_size") {
                if (pair.second.empty()) {
                    partConfig->SetMaxSizeInPartition(Max<i64>());
                } else {
                    try {
                        i64 size = FromString<i64>(pair.second);
                        if (size < 0) {
                            error = TStringBuilder() << "_max_partiton_strorage_size can't be negative, provided " << size;
                            return Ydb::StatusIds::BAD_REQUEST;
                        }

                        partConfig->SetMaxSizeInPartition(size ? size : Max<i64>());

                    } catch(...) {
                        error = TStringBuilder() << "Attribute _max_partition_storage_size is " << pair.second << ", which is not ui64";
                        return Ydb::StatusIds::BAD_REQUEST;
                    }
                }
            }  else if (pair.first == "_message_group_seqno_retention_period_ms") {
                partConfig->SetSourceIdLifetimeSeconds(NKikimrPQ::TPartitionConfig().GetSourceIdLifetimeSeconds());
                if (!pair.second.empty()) {
                    try {
                        i64 ms = FromString<i64>(pair.second);
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
                        error = TStringBuilder() << "Attribute " << pair.first << " is " << pair.second << ", which is not ui64";
                        return Ydb::StatusIds::BAD_REQUEST;
                    }
                }

            } else if (pair.first == "_max_partition_message_groups_seqno_stored") {
                partConfig->SetSourceIdMaxCounts(NKikimrPQ::TPartitionConfig().GetSourceIdMaxCounts());
                if (!pair.second.empty()) {
                    try {
                        i64 count = FromString<i64>(pair.second);
                        if (count < 0) {
                            error = TStringBuilder() << pair.first << "can't be negative, provided " << count;
                            return Ydb::StatusIds::BAD_REQUEST;
                        }
                        if (count > 0) {
                            partConfig->SetSourceIdMaxCounts(count);
                        }
                    } catch(...) {
                        error = TStringBuilder() << "Attribute " << pair.first << " is " << pair.second << ", which is not ui64";
                        return Ydb::StatusIds::BAD_REQUEST;
                    }
                }
            } else {
                error = TStringBuilder() << "Attribute " << pair.first << " is not supported";
                return Ydb::StatusIds::BAD_REQUEST;
            }
        }
        return Ydb::StatusIds::SUCCESS;

    }


    Ydb::StatusIds::StatusCode FillProposeRequestImpl(
            const TString& name, const Ydb::PersQueue::V1::TopicSettings& settings,
            NKikimrSchemeOp::TModifyScheme& modifyScheme, const TActorContext& ctx,
            bool alter, TString& error, const TString& path, const TString& database, const TString& localDc
    ) {
        const auto& pqConfig = AppData(ctx)->PQConfig;

        modifyScheme.SetOperationType(alter ? NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup : NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup);

        auto pqDescr = alter ? modifyScheme.MutableAlterPersQueueGroup() : modifyScheme.MutableCreatePersQueueGroup();
        pqDescr->SetName(name);
        if (settings.partitions_count() <= 0) {
            error = TStringBuilder() << "Partitions count must be positive, provided " << settings.partitions_count();
            return Ydb::StatusIds::BAD_REQUEST;
        }

        pqDescr->SetTotalGroupCount(settings.partitions_count());

        auto* config = pqDescr->MutablePQTabletConfig();

        config->SetRequireAuthWrite(true);
        config->SetRequireAuthRead(true);
        if (!alter)
            pqDescr->SetPartitionPerTablet(1);

        auto res = ProcessAttributes(settings.attributes(), pqDescr, error, alter);
        if (res != Ydb::StatusIds::SUCCESS) {
            return res;
        }

        bool local = !settings.client_write_disabled();

        auto topicPath = NKikimr::JoinPath({modifyScheme.GetWorkingDir(), name});
        if (!pqConfig.GetTopicsAreFirstClassCitizen()) {
            auto converter = NPersQueue::TTopicNameConverter::ForFederation(
                    pqConfig.GetRoot(), pqConfig.GetTestDatabaseRoot(), name, path, database, local, localDc,
                    config->GetFederationAccount()
            );

            if (!converter->IsValid()) {
                error = TStringBuilder() << "Bad topic: " << converter->GetReason();
                return Ydb::StatusIds::BAD_REQUEST;
            }
            config->SetLocalDC(local);
            config->SetDC(converter->GetCluster());
            config->SetProducer(converter->GetLegacyProducer());
            config->SetTopic(converter->GetLegacyLogtype());
            config->SetIdent(converter->GetLegacyProducer());
        }

        //config->SetTopicName(name);
        //config->SetTopicPath(topicPath);

        //Sets legacy 'logtype'.

        auto partConfig = config->MutablePartitionConfig();

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

        switch (settings.retention_case()) {
            case Ydb::PersQueue::V1::TopicSettings::kRetentionPeriodMs: {
                partConfig->SetLifetimeSeconds(Max(settings.retention_period_ms() / 1000ll, 1ll));
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
        config->SetFormatVersion(settings.supported_format() - 1);

        auto ct = config->MutableCodecs();
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

        {
            error = ReadRuleServiceTypeMigration(config, ctx);
            if (error) {
                return Ydb::StatusIds::INTERNAL_ERROR;
            }
        }
        const auto& supportedClientServiceTypes = GetSupportedClientServiceTypes(ctx);
        for (const auto& rr : settings.read_rules()) {
            auto messageAndCode = AddReadRuleToConfig(config, rr, supportedClientServiceTypes, ctx);
            if (messageAndCode.PQCode != Ydb::PersQueue::ErrorCode::OK) {
                error = messageAndCode.Message;
                return Ydb::StatusIds::BAD_REQUEST;
            }
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

        return CheckConfig(*config, supportedClientServiceTypes, error, ctx, Ydb::StatusIds::BAD_REQUEST);
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
            NKikimrSchemeOp::TModifyScheme& modifyScheme, const TActorContext& ctx,
            TString& error, const TString& path, const TString& database, const TString& localDc
    ) {
        const auto& pqConfig = AppData(ctx)->PQConfig;

        modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup);
        auto pqDescr = modifyScheme.MutableCreatePersQueueGroup();

        pqDescr->SetName(name);
        ui32 parts = 1;
        if (request.has_partitioning_settings()) {
            if (request.partitioning_settings().min_active_partitions() < 0) {
                error = TStringBuilder() << "Partitions count must be positive, provided " << request.partitioning_settings().min_active_partitions();
                return TYdbPqCodes(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
            }
            parts = request.partitioning_settings().min_active_partitions();
            if (parts == 0) parts = 1;
        }

        pqDescr->SetTotalGroupCount(parts);

        auto config = pqDescr->MutablePQTabletConfig();
        auto partConfig = config->MutablePartitionConfig();

        config->SetRequireAuthWrite(true);
        config->SetRequireAuthRead(true);
        pqDescr->SetPartitionPerTablet(1);

        partConfig->SetMaxCountInPartition(Max<i32>());

        partConfig->SetSourceIdLifetimeSeconds(NKikimrPQ::TPartitionConfig().GetSourceIdLifetimeSeconds());
        partConfig->SetSourceIdMaxCounts(NKikimrPQ::TPartitionConfig().GetSourceIdMaxCounts());

        auto res = ProcessAttributes(request.attributes(), pqDescr, error, false);
        if (res != Ydb::StatusIds::SUCCESS) {
            return TYdbPqCodes(res, Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
        }

        bool local = true; // TODO: check here cluster;

        auto topicPath = NKikimr::JoinPath({modifyScheme.GetWorkingDir(), name});
        if (!pqConfig.GetTopicsAreFirstClassCitizen()) {
            auto converter = NPersQueue::TTopicNameConverter::ForFederation(
                    pqConfig.GetRoot(), pqConfig.GetTestDatabaseRoot(), name, path, database, local, localDc,
                    config->GetFederationAccount()
            );

            if (!converter->IsValid()) {
                error = TStringBuilder() << "Bad topic: " << converter->GetReason();
                return TYdbPqCodes(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT);
            }
            config->SetLocalDC(local);
            config->SetDC(converter->GetCluster());
            config->SetProducer(converter->GetLegacyProducer());
            config->SetTopic(converter->GetLegacyLogtype());
            config->SetIdent(converter->GetLegacyProducer());
        }

//        config->SetTopicName(name);
//        config->SetTopicPath(topicPath);

        //Sets legacy 'logtype'.


        const auto& channelProfiles = pqConfig.GetChannelProfiles();
        if (channelProfiles.size() > 2) {
            partConfig->MutableExplicitChannelProfiles()->CopyFrom(channelProfiles);
        }
        if (request.has_retention_period()) {
            if (request.retention_period().seconds() <= 0) {
                error = TStringBuilder() << "retention_period must be not negative, provided " <<
                        request.retention_period().DebugString();
                return TYdbPqCodes(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
            }
            partConfig->SetLifetimeSeconds(request.retention_period().seconds());
        } else {
            partConfig->SetLifetimeSeconds(TDuration::Days(1).Seconds());
        }

        if (request.retention_storage_mb())
            partConfig->SetStorageLimitBytes(request.retention_storage_mb() * 1024 * 1024);

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
        config->SetFormatVersion(0);

        auto ct = config->MutableCodecs();
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

        {
            error = ReadRuleServiceTypeMigration(config, ctx);
            if (error) {
                return TYdbPqCodes(Ydb::StatusIds::INTERNAL_ERROR, Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT);
            }
        }

        Ydb::StatusIds::StatusCode code;
        if (!FillMeteringMode(request.metering_mode(), *config, pqConfig.GetBillingMeteringConfig().GetEnabled(), false, code, error)) {
            return TYdbPqCodes(code, Ydb::PersQueue::ErrorCode::INVALID_ARGUMENT);
        }

        const auto& supportedClientServiceTypes = GetSupportedClientServiceTypes(ctx);


        for (const auto& rr : request.consumers()) {
            auto messageAndCode = AddReadRuleToConfig(config, rr, supportedClientServiceTypes, true, ctx);
            if (messageAndCode.PQCode != Ydb::PersQueue::ErrorCode::OK) {
                error = messageAndCode.Message;
                return TYdbPqCodes(Ydb::StatusIds::BAD_REQUEST, messageAndCode.PQCode);
            }
        }

        return TYdbPqCodes(CheckConfig(*config, supportedClientServiceTypes, error, ctx, Ydb::StatusIds::BAD_REQUEST), Ydb::PersQueue::ErrorCode::VALIDATION_ERROR);
    }

    Ydb::StatusIds::StatusCode FillProposeRequestImpl(
            const Ydb::Topic::AlterTopicRequest& request,
            NKikimrSchemeOp::TPersQueueGroupDescription& pqDescr, const TActorContext& ctx,
            TString& error, bool isCdcStream
    ) {
        #define CHECK_CDC  if (isCdcStream) {\
                    error = "Full alter of cdc stream is forbidden";\
                    return Ydb::StatusIds::BAD_REQUEST;\
            }

        const auto& pqConfig = AppData(ctx)->PQConfig;

        if (request.has_alter_partitioning_settings() && request.alter_partitioning_settings().has_set_min_active_partitions()) {
            CHECK_CDC;
            auto parts = request.alter_partitioning_settings().set_min_active_partitions();
            if (parts == 0) parts = 1;
            pqDescr.SetTotalGroupCount(parts);
        }


        auto config = pqDescr.MutablePQTabletConfig();
        auto partConfig = config->MutablePartitionConfig();

        if (request.alter_attributes().size()) {
            CHECK_CDC;
        }

        auto res = ProcessAttributes(request.alter_attributes(), &pqDescr, error, true);
        if (res != Ydb::StatusIds::SUCCESS) {
            return res;
        }

        if (request.has_set_retention_period()) {
            CHECK_CDC;
            partConfig->SetLifetimeSeconds(request.set_retention_period().seconds());
        }


        if (request.has_set_retention_storage_mb()) {
            CHECK_CDC;
            partConfig->ClearStorageLimitBytes();
            if (request.set_retention_storage_mb())
                partConfig->SetStorageLimitBytes(request.set_retention_storage_mb() * 1024 * 1024);
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
            config->ClearCodecs();
            auto ct = config->MutableCodecs();
            for(const auto& codec : request.set_supported_codecs().codecs()) {
                if ((!Ydb::Topic::Codec_IsValid(codec) && codec < Ydb::Topic::CODEC_CUSTOM) || codec == 0) {
                    error = TStringBuilder() << "Unknown codec with value " << codec;
                    return Ydb::StatusIds::BAD_REQUEST;
                }
                ct->AddIds(codec - 1);
                ct->AddCodecs(Ydb::Topic::Codec_IsValid(codec) ? LegacySubstr(to_lower(Ydb::Topic::Codec_Name((Ydb::Topic::Codec)codec)), 6) : "CUSTOM");
            }
        }
        {
            error = ReadRuleServiceTypeMigration(config, ctx);
            if (error) {
                return Ydb::StatusIds::INTERNAL_ERROR;
            }
        }

        Ydb::StatusIds::StatusCode code;
        if (!FillMeteringMode(request.set_metering_mode(), *config, pqConfig.GetBillingMeteringConfig().GetEnabled(), true, code, error)) {
            return code;
        }

        const auto& supportedClientServiceTypes = GetSupportedClientServiceTypes(ctx);


        std::vector<std::pair<bool, Ydb::Topic::Consumer>> consumers;

        i32 dropped = 0;

        for (ui32 i = 0; i < config->ReadRulesSize(); ++i) {
            TString oldName = config->GetReadRules(i);
            TString name = NPersQueue::ConvertOldConsumerName(oldName, ctx);
            bool erase = false;
            bool important = false;
            for (auto consumer: request.drop_consumers()) {
                if (consumer == name || consumer == oldName) {
                    erase = true;
                    ++dropped;
                    break;
                }
            }
            if (erase) continue;
            for (auto imp : partConfig->GetImportantClientId()) {
                if (imp == oldName) {
                    important = true;
                    break;
                }
            }
            consumers.push_back({false, Ydb::Topic::Consumer{}}); // do not check service type for presented consumers
            auto& consumer = consumers.back().second;
            consumer.set_name(name);
            consumer.set_important(important);
            consumer.mutable_read_from()->set_seconds(config->GetReadFromTimestampsMs(i) / 1000);
            (*consumer.mutable_attributes())["_service_type"] = config->GetReadRuleServiceTypes(i);
            (*consumer.mutable_attributes())["_version"] = TStringBuilder() << config->GetReadRuleVersions(i);
            for (ui32 codec : config->GetConsumerCodecs(i).GetIds()) {
                consumer.mutable_supported_codecs()->add_codecs(codec + 1);
            }
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
            auto oldName = NPersQueue::ConvertOldConsumerName(name, ctx);
            bool found = false;
            for (auto& consumer : consumers) {
                if (consumer.second.name() == name || consumer.second.name() == oldName) {
                    found = true;
                    ProcessAlterConsumer(consumer.second, alter);
                    consumer.first = true; // check service type
                    break;
                }
            }
            if (!found) {
                error = TStringBuilder() << "consumer '" << name << "' in alter_consumers is missing";
                return Ydb::StatusIds::NOT_FOUND;
            }
        }

        config->ClearReadRules();
        partConfig->ClearImportantClientId();
        config->ClearConsumerCodecs();
        config->ClearReadFromTimestampsMs();
        config->ClearConsumerFormatVersions();
        config->ClearReadRuleServiceTypes();
        config->ClearReadRuleGenerations();
        config->ClearReadRuleVersions();

        for (const auto& rr : consumers) {
            auto messageAndCode = AddReadRuleToConfig(config, rr.second, supportedClientServiceTypes, rr.first, ctx);
            if (messageAndCode.PQCode != Ydb::PersQueue::ErrorCode::OK) {
                error = messageAndCode.Message;
                return Ydb::StatusIds::BAD_REQUEST;
            }
        }

        return CheckConfig(*config, supportedClientServiceTypes, error, ctx, Ydb::StatusIds::ALREADY_EXISTS);
    }



}
