#pragma once

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/logger/actor.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

#include <ydb/library/actors/core/actor.h>
#include <library/cpp/logger/log.h>

#include <util/datetime/base.h>


namespace NKikimr::NPQ {

constexpr ui64 MaxMessageSize = 150_MB;

class IPersQueueMirrorReaderFactory {
public:
    IPersQueueMirrorReaderFactory()
        : ActorSystemPtr(std::make_shared<TDeferredActorLogBackend::TAtomicActorSystemPtr>(nullptr))
    {}

    virtual void Initialize(
        NActors::TActorSystem* actorSystem,
        const NKikimrPQ::TPQConfig::TPQLibSettings& settings
    ) const {
        Y_ABORT_UNLESS(!ActorSystemPtr->load(std::memory_order_relaxed), "Double init");
        ActorSystemPtr->store(actorSystem, std::memory_order_relaxed);

        auto driverConfig = NYdb::TDriverConfig()
            .SetMaxMessageSize(MaxMessageSize)
            .SetNetworkThreadsNum(settings.GetThreadsCount());
        Driver = std::make_shared<NYdb::TDriver>(driverConfig);
    }

    NThreading::TFuture<NYdb::TCredentialsProviderFactoryPtr> GetCredentialsProvider(
        const NKikimrPQ::TMirrorPartitionConfig::TCredentials& cred
    ) const noexcept {
        try {
            return GetCredentialsProviderImpl(cred);
        } catch(...) {
            return NThreading::MakeErrorFuture<NYdb::TCredentialsProviderFactoryPtr>(std::current_exception());
        }
    }

    virtual std::shared_ptr<NYdb::NTopic::IReadSession> GetReadSession(
        const NKikimrPQ::TMirrorPartitionConfig& config,
        ui32 partition,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
        ui64 maxMemoryUsageBytes,
        TMaybe<TLog> logger = Nothing()
    ) const = 0;

    virtual ~IPersQueueMirrorReaderFactory() = default;

    TDeferredActorLogBackend::TSharedAtomicActorSystemPtr GetSharedActorSystem() const {
        return ActorSystemPtr;
    }

protected:
    virtual NThreading::TFuture<NYdb::TCredentialsProviderFactoryPtr> GetCredentialsProviderImpl(
        const NKikimrPQ::TMirrorPartitionConfig::TCredentials& cred
    ) const = 0;


protected:
    mutable TDeferredActorLogBackend::TSharedAtomicActorSystemPtr ActorSystemPtr;
    mutable std::shared_ptr<NYdb::TDriver> Driver;
};

class TPersQueueMirrorReaderFactory : public IPersQueueMirrorReaderFactory {
protected:
    NThreading::TFuture<NYdb::TCredentialsProviderFactoryPtr> GetCredentialsProviderImpl(
        const NKikimrPQ::TMirrorPartitionConfig::TCredentials& cred
    ) const override {
        switch (cred.GetCredentialsCase()) {
            case NKikimrPQ::TMirrorPartitionConfig::TCredentials::CREDENTIALS_NOT_SET: {
                return NThreading::MakeFuture(NYdb::CreateInsecureCredentialsProviderFactory());
            }
            default: {
                ythrow yexception() << "unsupported credentials type " << ui64(cred.GetCredentialsCase());
            }
        }
    }

public:
    std::shared_ptr<NYdb::NTopic::IReadSession> GetReadSession(
        const NKikimrPQ::TMirrorPartitionConfig& config,
        ui32 partition,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
        ui64 maxMemoryUsageBytes,
        TMaybe<TLog> logger = Nothing()
    ) const override {
        NYdb::NTopic::TTopicClientSettings clientSettings = NYdb::NTopic::TTopicClientSettings()
            .DiscoveryEndpoint(TStringBuilder() << config.GetEndpoint() << ":" << config.GetEndpointPort())
            .DiscoveryMode(NYdb::EDiscoveryMode::Async)
            .CredentialsProviderFactory(credentialsProviderFactory)
            .SslCredentials(NYdb::TSslCredentials(config.GetUseSecureConnection()));
        if (config.HasDatabase()) {
            clientSettings.Database(config.GetDatabase());
        }

        NYdb::NTopic::TReadSessionSettings settings = NYdb::NTopic::TReadSessionSettings()
            .ConsumerName(config.GetConsumer())
            .MaxMemoryUsageBytes(maxMemoryUsageBytes)
            .Decompress(false)
            .RetryPolicy(NYdb::NTopic::IRetryPolicy::GetNoRetryPolicy());
        if (logger) {
            settings.Log(logger.GetRef());
        }
        if (config.HasReadFromTimestampsMs()) {
            settings.ReadFromTimestamp(TInstant::MilliSeconds(config.GetReadFromTimestampsMs()));
        }
        NYdb::NTopic::TTopicReadSettings topicSettings(config.GetTopic());
        topicSettings.AppendPartitionIds({partition});
        settings.AppendTopics(topicSettings);

        NYdb::NTopic::TTopicClient topicClient(*Driver, clientSettings);
        return topicClient.CreateReadSession(settings);
    }
};

} // namespace NKikimr::NSQS
