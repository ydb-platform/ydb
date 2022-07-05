#pragma once

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/logger/actor.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/persqueue.h>

#include <library/cpp/actors/core/actor.h>
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
        Y_VERIFY(!ActorSystemPtr->load(std::memory_order_relaxed), "Double init");
        ActorSystemPtr->store(actorSystem, std::memory_order_relaxed);

        auto driverConfig = NYdb::TDriverConfig()
            .SetMaxMessageSize(MaxMessageSize)
            .SetNetworkThreadsNum(settings.GetThreadsCount());
        Driver = std::make_shared<NYdb::TDriver>(driverConfig);
    }

    virtual std::shared_ptr<NYdb::ICredentialsProviderFactory> GetCredentialsProvider(
        const NKikimrPQ::TMirrorPartitionConfig::TCredentials& cred
    ) const = 0;

    virtual std::shared_ptr<NYdb::NPersQueue::IReadSession> GetReadSession(
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
    mutable TDeferredActorLogBackend::TSharedAtomicActorSystemPtr ActorSystemPtr;
    mutable std::shared_ptr<NYdb::TDriver> Driver;
};

class TPersQueueMirrorReaderFactory : public IPersQueueMirrorReaderFactory {
public:
    std::shared_ptr<NYdb::ICredentialsProviderFactory> GetCredentialsProvider(
        const NKikimrPQ::TMirrorPartitionConfig::TCredentials& cred
    ) const override {
        switch (cred.GetCredentialsCase()) {
            case NKikimrPQ::TMirrorPartitionConfig::TCredentials::CREDENTIALS_NOT_SET: {
                return NYdb::CreateInsecureCredentialsProviderFactory();
            }
            default: {
                ythrow yexception() << "unsupported credentials type " << ui64(cred.GetCredentialsCase());
            }
        }
    }

    std::shared_ptr<NYdb::NPersQueue::IReadSession> GetReadSession(
        const NKikimrPQ::TMirrorPartitionConfig& config,
        ui32 partition,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
        ui64 maxMemoryUsageBytes,
        TMaybe<TLog> logger = Nothing()
    ) const override {
        NYdb::NPersQueue::TPersQueueClientSettings clientSettings = NYdb::NPersQueue::TPersQueueClientSettings()
            .DiscoveryEndpoint(TStringBuilder() << config.GetEndpoint() << ":" << config.GetEndpointPort())
            .CredentialsProviderFactory(credentialsProviderFactory)
            .EnableSsl(config.GetUseSecureConnection());
        if (config.HasDatabase()) {
            clientSettings.Database(config.GetDatabase());
        }

        NYdb::NPersQueue::TReadSessionSettings settings = NYdb::NPersQueue::TReadSessionSettings()
            .ConsumerName(config.GetConsumer())
            .MaxMemoryUsageBytes(maxMemoryUsageBytes)
            .Decompress(false)
            .DisableClusterDiscovery(true)
            .ReadOnlyOriginal(true)
            .RetryPolicy(NYdb::NPersQueue::IRetryPolicy::GetNoRetryPolicy());
        if (logger) {
            settings.Log(logger.GetRef());
        }
        if (config.HasReadFromTimestampsMs()) {
            settings.StartingMessageTimestamp(TInstant::MilliSeconds(config.GetReadFromTimestampsMs()));
        }
        NYdb::NPersQueue::TTopicReadSettings topicSettings(config.GetTopic());
        topicSettings.AppendPartitionGroupIds({partition + 1});
        settings.AppendTopics(topicSettings);

        NYdb::NPersQueue::TPersQueueClient persQueueClient(*Driver, clientSettings);
        return persQueueClient.CreateReadSession(settings);
    }
};

} // namespace NKikimr::NSQS
