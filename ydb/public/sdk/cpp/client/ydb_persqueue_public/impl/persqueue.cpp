
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/impl/persqueue_impl.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/impl/common.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>

#include <ydb/library/persqueue/obfuscate/obfuscate.h>

#include <util/random/random.h>
#include <util/string/cast.h>
#include <util/string/subst.h>

namespace NYdb::NPersQueue {

class TCommonCodecsProvider {
public:
    TCommonCodecsProvider() {
        TCodecMap::GetTheCodecMap().Set((ui32)ECodec::GZIP, MakeHolder<TGzipCodec>());
        TCodecMap::GetTheCodecMap().Set((ui32)ECodec::ZSTD, MakeHolder<TZstdCodec>());
    }
};
TCommonCodecsProvider COMMON_CODECS_PROVIDER;

const TVector<ECodec>& GetDefaultCodecs() {
    static const TVector<ECodec> codecs = {};
    return codecs;
}

using TTopicSettingsCreate = TTopicSettings<TCreateTopicSettings>;
using TTopicSettingsAlter = TTopicSettings<TAlterTopicSettings>;

TCredentials::TCredentials(const Ydb::PersQueue::V1::Credentials& settings)
    : Credentials_(settings)
{
    switch (Credentials_.credentials_case()) {
        case Ydb::PersQueue::V1::Credentials::kOauthToken: {
            Mode_ = EMode::OAUTH_TOKEN;
            break;
        }
        case Ydb::PersQueue::V1::Credentials::kJwtParams: {
            Mode_ = EMode::JWT_PARAMS;
            break;
        }
        case Ydb::PersQueue::V1::Credentials::kIam: {
            Mode_ = EMode::IAM;
            break;
        }
        case Ydb::PersQueue::V1::Credentials::CREDENTIALS_NOT_SET: {
            Mode_ = EMode::NOT_SET;
            break;
        }
        default: {
            ythrow yexception() << "unsupported credentials type " << ::NPersQueue::ObfuscateString(ToString(Credentials_));
        }
    }
}

TCredentials::EMode TCredentials::GetMode() const {
    return Mode_;
}

TString TCredentials::GetOauthToken() const {
    Y_ENSURE(GetMode() == EMode::OAUTH_TOKEN);
    return Credentials_.oauth_token();
}

TString TCredentials::GetJwtParams() const {
    Y_ENSURE(GetMode() == EMode::JWT_PARAMS);
    return Credentials_.jwt_params();
}

TString TCredentials::GetIamEndpoint() const {
    Y_ENSURE(GetMode() == EMode::IAM);
    return Credentials_.iam().endpoint();
}

TString TCredentials::GetIamServiceAccountKey() const {
    Y_ENSURE(GetMode() == EMode::IAM);
    return Credentials_.iam().service_account_key();
}

TDescribeTopicResult::TDescribeTopicResult(TStatus status, const Ydb::PersQueue::V1::DescribeTopicResult& result)
    : TStatus(std::move(status))
    , TopicSettings_(result.settings())
    , Proto_(result)
{
}

TDescribeTopicResult::TTopicSettings::TTopicSettings(const Ydb::PersQueue::V1::TopicSettings& settings) {
    RetentionPeriod_ = TDuration::MilliSeconds(settings.retention_period_ms());
    SupportedFormat_ = static_cast<EFormat>(settings.supported_format());

    if (settings.has_auto_partitioning_settings()) {
        PartitionsCount_ = settings.auto_partitioning_settings().min_active_partitions();
        MaxPartitionsCount_ = settings.auto_partitioning_settings().max_active_partitions();
        StabilizationWindow_ = TDuration::Seconds(settings.auto_partitioning_settings().partition_write_speed().stabilization_window().seconds());
        UpUtilizationPercent_ = settings.auto_partitioning_settings().partition_write_speed().up_utilization_percent();
        DownUtilizationPercent_ = settings.auto_partitioning_settings().partition_write_speed().down_utilization_percent();
        AutoPartitioningStrategy_ = settings.auto_partitioning_settings().strategy();
    } else {
        PartitionsCount_ = settings.partitions_count();
    }

    for (const auto& codec : settings.supported_codecs()) {
        SupportedCodecs_.push_back(static_cast<ECodec>(codec));
    }
    MaxPartitionStorageSize_ = settings.max_partition_storage_size();
    MaxPartitionWriteSpeed_ = settings.max_partition_write_speed();
    MaxPartitionWriteBurst_ = settings.max_partition_write_burst();
    ClientWriteDisabled_ = settings.client_write_disabled();
    AllowUnauthenticatedRead_ = AllowUnauthenticatedWrite_ = false;

    for (auto& pair : settings.attributes()) {
        if (pair.first == "_partitions_per_tablet") {
            PartitionsPerTablet_ = FromString<ui32>(pair.second);
        } else if (pair.first == "_allow_unauthenticated_read") {
            AllowUnauthenticatedRead_ = FromString<bool>(pair.second);
        } else if (pair.first == "_allow_unauthenticated_write") {
            AllowUnauthenticatedWrite_ = FromString<bool>(pair.second);
        } else if (pair.first == "_abc_id") {
            AbcId_ = FromString<ui32>(pair.second);
        } else if (pair.first == "_abc_slug") {
            AbcSlug_ = pair.second;
        } else if (pair.first == "_federation_account") {
            FederationAccount_ = pair.second;
        }
    }
    for (const auto& readRule : settings.read_rules()) {
        ReadRules_.emplace_back(readRule);
    }
    if (settings.has_remote_mirror_rule()) {
        RemoteMirrorRule_ = settings.remote_mirror_rule();
    }
}


TDescribeTopicResult::TTopicSettings::TReadRule::TReadRule(const Ydb::PersQueue::V1::TopicSettings::ReadRule& settings) {

    ConsumerName_ = settings.consumer_name();
    Important_ = settings.important();
    StartingMessageTimestamp_ = TInstant::MilliSeconds(settings.starting_message_timestamp_ms());

    SupportedFormat_ = static_cast<EFormat>(settings.supported_format());
    for (const auto& codec : settings.supported_codecs()) {
        SupportedCodecs_.push_back(static_cast<ECodec>(codec));
    }
    Version_ = settings.version();
    ServiceType_ = settings.service_type();
}

TDescribeTopicResult::TTopicSettings::TRemoteMirrorRule::TRemoteMirrorRule(const Ydb::PersQueue::V1::TopicSettings::RemoteMirrorRule& settings)
    : Credentials_(settings.credentials())
{
    Endpoint_ = settings.endpoint();
    TopicPath_ = settings.topic_path();
    ConsumerName_ = settings.consumer_name();
    StartingMessageTimestamp_ = TInstant::MilliSeconds(settings.starting_message_timestamp_ms());
    Database_ = settings.database();
}

TPersQueueClient::TPersQueueClient(const TDriver& driver, const TPersQueueClientSettings& settings)
    : Impl_(std::make_shared<TImpl>(CreateInternalInterface(driver), settings))
{
}

TAsyncStatus TPersQueueClient::CreateTopic(const TString& path, const TCreateTopicSettings& settings) {
    return Impl_->CreateTopic(path, settings);
}

TAsyncStatus TPersQueueClient::AlterTopic(const TString& path, const TAlterTopicSettings& settings) {
    return Impl_->AlterTopic(path, settings);
}

TAsyncStatus TPersQueueClient::DropTopic(const TString& path, const TDropTopicSettings& settings) {
    return Impl_->DropTopic(path, settings);
}

TAsyncStatus TPersQueueClient::AddReadRule(const TString& path, const TAddReadRuleSettings& settings) {
    return Impl_->AddReadRule(path, settings);
}

TAsyncStatus TPersQueueClient::RemoveReadRule(const TString& path, const TRemoveReadRuleSettings& settings) {
    return Impl_->RemoveReadRule(path, settings);
}

TAsyncDescribeTopicResult TPersQueueClient::DescribeTopic(const TString& path, const TDescribeTopicSettings& settings) {
    return Impl_->DescribeTopic(path, settings);
}

std::shared_ptr<IReadSession> TPersQueueClient::CreateReadSession(const TReadSessionSettings& settings) {
    return Impl_->CreateReadSession(settings);
}

std::shared_ptr<IWriteSession> TPersQueueClient::CreateWriteSession(const TWriteSessionSettings& settings) {
    return Impl_->CreateWriteSession(settings);
}

std::shared_ptr<ISimpleBlockingWriteSession> TPersQueueClient::CreateSimpleBlockingWriteSession(
        const TWriteSessionSettings& settings
) {
    return Impl_->CreateSimpleWriteSession(settings);
}

}  // namespace NYdb::NPersQueue
