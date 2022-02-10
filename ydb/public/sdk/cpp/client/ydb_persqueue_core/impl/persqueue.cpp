
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/persqueue.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/persqueue_impl.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/read_session.h>

#include <ydb/library/persqueue/obfuscate/obfuscate.h>

#include <util/random/random.h>
#include <util/string/cast.h>
#include <util/string/subst.h>

namespace NYdb::NPersQueue {

const TVector<ECodec>& GetDefaultCodecs() {
    static const TVector<ECodec> codecs = {ECodec::RAW, ECodec::GZIP, ECodec::LZOP};
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

    PartitionsCount_ = settings.partitions_count();
    RetentionPeriod_ = TDuration::MilliSeconds(settings.retention_period_ms());
    SupportedFormat_ = static_cast<EFormat>(settings.supported_format());

    for (const auto& codec : settings.supported_codecs()) {
        SupportedCodecs_.push_back(static_cast<ECodec>(codec));
    }
    MaxPartitionStorageSize_ = settings.max_partition_storage_size();
    MaxPartitionWriteSpeed_ = settings.max_partition_write_speed();
    MaxPartitionWriteBurst_ = settings.max_partition_write_burst();
    ClientWriteDisabled_ = settings.client_write_disabled();
    AllowUnauthenticatedRead_ = AllowUnauthenticatedWrite_ = false; 
    AbcId_ = 0; 
    AbcSlug_ = ""; 
 
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

namespace {

struct TNoRetryState : IRetryState {
    TMaybe<TDuration> GetNextRetryDelay(const TStatus&) override {
        return Nothing();
    }
};

struct TNoRetryPolicy : IRetryPolicy {
    IRetryState::TPtr CreateRetryState() const override {
        return std::make_unique<TNoRetryState>();
    }
};

TDuration RandomizeDelay(TDuration baseDelay) {
    const TDuration::TValue half = baseDelay.GetValue() / 2;
    return TDuration::FromValue(half + RandomNumber<TDuration::TValue>(half));
}

struct TExponentialBackoffState : IRetryState {
    TExponentialBackoffState(TDuration minDelay,
                             TDuration minLongRetryDelay,
                             TDuration maxDelay,
                             size_t maxRetries,
                             TDuration maxTime,
                             double scaleFactor,
                             std::function<IRetryPolicy::ERetryErrorClass(EStatus)> retryErrorClassFunction)
        : MinLongRetryDelay(minLongRetryDelay)
        , MaxDelay(maxDelay)
        , MaxRetries(maxRetries)
        , MaxTime(maxTime)
        , ScaleFactor(scaleFactor)
        , StartTime(maxTime != TDuration::Max() ? TInstant::Now() : TInstant::Zero())
        , CurrentDelay(minDelay)
        , AttemptsDone(0)
        , RetryErrorClassFunction(retryErrorClassFunction)
    {
    }

    TMaybe<TDuration> GetNextRetryDelay(const TStatus& status) override {
        const IRetryPolicy::ERetryErrorClass errorClass = RetryErrorClassFunction(status.GetStatus());
        if (AttemptsDone >= MaxRetries || StartTime && TInstant::Now() - StartTime >= MaxTime || errorClass == IRetryPolicy::ERetryErrorClass::NoRetry) {
            return Nothing();
        }

        if (errorClass == IRetryPolicy::ERetryErrorClass::LongRetry) {
            CurrentDelay = Max(CurrentDelay, MinLongRetryDelay);
        }

        const TDuration delay = RandomizeDelay(CurrentDelay);

        if (CurrentDelay < MaxDelay) {
            CurrentDelay = Min(CurrentDelay * ScaleFactor, MaxDelay);
        }

        ++AttemptsDone;
        return delay;
    }

    const TDuration MinLongRetryDelay;
    const TDuration MaxDelay;
    const size_t MaxRetries;
    const TDuration MaxTime;
    const double ScaleFactor;
    const TInstant StartTime;
    TDuration CurrentDelay;
    size_t AttemptsDone;
    std::function<IRetryPolicy::ERetryErrorClass(EStatus)> RetryErrorClassFunction;
};

struct TExponentialBackoffPolicy : IRetryPolicy {
    TExponentialBackoffPolicy(TDuration minDelay,
                              TDuration minLongRetryDelay,
                              TDuration maxDelay,
                              size_t maxRetries,
                              TDuration maxTime,
                              double scaleFactor,
                              std::function<IRetryPolicy::ERetryErrorClass(EStatus)> customRetryClassFunction)
        : MinDelay(minDelay)
        , MinLongRetryDelay(minLongRetryDelay)
        , MaxDelay(maxDelay)
        , MaxRetries(maxRetries)
        , MaxTime(maxTime)
        , ScaleFactor(scaleFactor)
        , RetryErrorClassFunction(customRetryClassFunction ? customRetryClassFunction : GetRetryErrorClass)
    {
        Y_ASSERT(MinDelay < MaxDelay);
        Y_ASSERT(MinLongRetryDelay < MaxDelay);
        Y_ASSERT(MinLongRetryDelay >= MinDelay);
        Y_ASSERT(ScaleFactor > 1.0);
        Y_ASSERT(MaxRetries > 0);
        Y_ASSERT(MaxTime > MinDelay);
    }

    IRetryState::TPtr CreateRetryState() const override {
        return std::make_unique<TExponentialBackoffState>(MinDelay, MinLongRetryDelay, MaxDelay, MaxRetries, MaxTime, ScaleFactor, RetryErrorClassFunction);
    }

    const TDuration MinDelay;
    const TDuration MinLongRetryDelay;
    const TDuration MaxDelay;
    const size_t MaxRetries;
    const TDuration MaxTime;
    const double ScaleFactor;
    std::function<IRetryPolicy::ERetryErrorClass(EStatus)> RetryErrorClassFunction;
};

struct TFixedIntervalState : IRetryState {
    TFixedIntervalState(TDuration delay,
                        TDuration longRetryDelay,
                        size_t maxRetries,
                        TDuration maxTime,
                        std::function<IRetryPolicy::ERetryErrorClass(EStatus)> retryErrorClassFunction)
        : Delay(delay)
        , LongRetryDelay(longRetryDelay)
        , MaxRetries(maxRetries)
        , MaxTime(maxTime)
        , StartTime(maxTime != TDuration::Max() ? TInstant::Now() : TInstant::Zero())
        , AttemptsDone(0)
        , RetryErrorClassFunction(retryErrorClassFunction)
    {
    }

    TMaybe<TDuration> GetNextRetryDelay(const TStatus& status) override {
        const IRetryPolicy::ERetryErrorClass errorClass = RetryErrorClassFunction(status.GetStatus());
        if (AttemptsDone >= MaxRetries || StartTime && TInstant::Now() - StartTime >= MaxTime || errorClass == IRetryPolicy::ERetryErrorClass::NoRetry) {
            return Nothing();
        }

        const TDuration delay = RandomizeDelay(errorClass == IRetryPolicy::ERetryErrorClass::LongRetry ? LongRetryDelay : Delay);

        ++AttemptsDone;
        return delay;
    }

    const TDuration Delay;
    const TDuration LongRetryDelay;
    const size_t MaxRetries;
    const TDuration MaxTime;
    const TInstant StartTime;
    size_t AttemptsDone;
    std::function<IRetryPolicy::ERetryErrorClass(EStatus)> RetryErrorClassFunction;
};

struct TFixedIntervalPolicy : IRetryPolicy {
    TFixedIntervalPolicy(TDuration delay,
                         TDuration longRetryDelay,
                         size_t maxRetries,
                         TDuration maxTime,
                         std::function<IRetryPolicy::ERetryErrorClass(EStatus)> customRetryClassFunction)
        : Delay(delay)
        , LongRetryDelay(longRetryDelay)
        , MaxRetries(maxRetries)
        , MaxTime(maxTime)
        , RetryErrorClassFunction(customRetryClassFunction ? customRetryClassFunction : GetRetryErrorClass)
    {
        Y_ASSERT(MaxRetries > 0);
        Y_ASSERT(MaxTime > Delay);
        Y_ASSERT(MaxTime > LongRetryDelay);
        Y_ASSERT(LongRetryDelay >= Delay);
    }

    IRetryState::TPtr CreateRetryState() const override {
        return std::make_unique<TFixedIntervalState>(Delay, LongRetryDelay, MaxRetries, MaxTime, RetryErrorClassFunction);
    }

    const TDuration Delay;
    const TDuration LongRetryDelay;
    const size_t MaxRetries;
    const TDuration MaxTime;
    std::function<IRetryPolicy::ERetryErrorClass(EStatus)> RetryErrorClassFunction;
};

} // namespace

IRetryPolicy::TPtr IRetryPolicy::GetDefaultPolicy() {
    static IRetryPolicy::TPtr policy = GetExponentialBackoffPolicy();
    return policy;
}

IRetryPolicy::TPtr IRetryPolicy::GetNoRetryPolicy() {
    static IRetryPolicy::TPtr policy = std::make_shared<TNoRetryPolicy>();
    return policy;
}

IRetryPolicy::TPtr IRetryPolicy::GetExponentialBackoffPolicy(TDuration minDelay,
                                                             TDuration minLongRetryDelay,
                                                             TDuration maxDelay,
                                                             size_t maxRetries,
                                                             TDuration maxTime,
                                                             double scaleFactor,
                                                             std::function<IRetryPolicy::ERetryErrorClass(EStatus)> customRetryClassFunction)
{
    return std::make_shared<TExponentialBackoffPolicy>(minDelay, minLongRetryDelay, maxDelay, maxRetries, maxTime, scaleFactor, customRetryClassFunction);
}

IRetryPolicy::TPtr IRetryPolicy::GetFixedIntervalPolicy(TDuration delay,
                                                        TDuration longRetryDelay,
                                                        size_t maxRetries,
                                                        TDuration maxTime,
                                                        std::function<IRetryPolicy::ERetryErrorClass(EStatus)> customRetryClassFunction)
{
    return std::make_shared<TFixedIntervalPolicy>(delay, longRetryDelay, maxRetries, maxTime, customRetryClassFunction);
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

} // namespace NYdb::NPersQueue
