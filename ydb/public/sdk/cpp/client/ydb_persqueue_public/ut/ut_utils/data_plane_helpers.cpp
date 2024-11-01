#include "data_plane_helpers.h"
#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

namespace NKikimr::NPersQueueTests {

    using namespace NYdb::NPersQueue;

    std::shared_ptr<NYdb::NPersQueue::IWriteSession> CreateWriter(
        NYdb::TDriver& driver,
        const NYdb::NPersQueue::TWriteSessionSettings& settings,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> creds
    ) {
        TPersQueueClientSettings clientSettings;
        if (creds) clientSettings.CredentialsProviderFactory(creds);
        return TPersQueueClient(driver, clientSettings).CreateWriteSession(TWriteSessionSettings(settings).ClusterDiscoveryMode(EClusterDiscoveryMode::Off));
    }

    std::shared_ptr<NYdb::NPersQueue::IWriteSession> CreateWriter(
        NYdb::TDriver& driver,
        const TString& topic,
        const TString& sourceId,
        std::optional<ui32> partitionGroup,
        std::optional<TString> codec,
        std::optional<bool> reconnectOnFailure,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> creds
    ) {
        auto settings = TWriteSessionSettings().Path(topic).MessageGroupId(sourceId);
        if (partitionGroup) settings.PartitionGroupId(*partitionGroup);
        settings.RetryPolicy((reconnectOnFailure && *reconnectOnFailure) ? NYdb::NPersQueue::IRetryPolicy::GetDefaultPolicy() : NYdb::NPersQueue::IRetryPolicy::GetNoRetryPolicy());
        if (codec) {
            if (*codec == "raw")
                settings.Codec(ECodec::RAW);
            if (*codec == "zstd")
                settings.Codec(ECodec::ZSTD);
            if (*codec == "lzop")
                settings.Codec(ECodec::LZOP);
        }
        return CreateWriter(driver, settings, creds);
    }

    std::shared_ptr<NYdb::NPersQueue::ISimpleBlockingWriteSession> CreateSimpleWriter(
        NYdb::TDriver& driver,
        const NYdb::NPersQueue::TWriteSessionSettings& settings
    ) {
        return TPersQueueClient(driver).CreateSimpleBlockingWriteSession(TWriteSessionSettings(settings).ClusterDiscoveryMode(EClusterDiscoveryMode::Off));
    }

    std::shared_ptr<NYdb::NPersQueue::ISimpleBlockingWriteSession> CreateSimpleWriter(
        NYdb::TDriver& driver,
        const TString& topic,
        const TString& sourceId,
        std::optional<ui32> partitionGroup,
        std::optional<TString> codec,
        std::optional<bool> reconnectOnFailure,
        THashMap<TString, TString> sessionMeta,
        const TString& userAgent
    ) {
        auto settings = TWriteSessionSettings().Path(topic).MessageGroupId(sourceId);
        if (partitionGroup) settings.PartitionGroupId(*partitionGroup);
        settings.RetryPolicy((reconnectOnFailure && *reconnectOnFailure) ? NYdb::NPersQueue::IRetryPolicy::GetDefaultPolicy() : NYdb::NPersQueue::IRetryPolicy::GetNoRetryPolicy());
        if (codec) {
            if (*codec == "raw")
                settings.Codec(ECodec::RAW);
            if (*codec == "zstd")
                settings.Codec(ECodec::ZSTD);
            if (*codec == "lzop")
                settings.Codec(ECodec::LZOP);
        }
        settings.MaxMemoryUsage(1024*1024*1024*1024ll);
        settings.Meta_.Fields = sessionMeta;
        if (!userAgent.empty()) {
            settings.Header({{NYdb::YDB_APPLICATION_NAME, userAgent}});
        }
        return CreateSimpleWriter(driver, settings);
    }

    std::shared_ptr<NYdb::NPersQueue::IReadSession> CreateReader(
        NYdb::TDriver& driver,
        const NYdb::NPersQueue::TReadSessionSettings& settings,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> creds
    ) {
        TPersQueueClientSettings clientSettings;
        if (creds) clientSettings.CredentialsProviderFactory(creds);
        return TPersQueueClient(driver, clientSettings).CreateReadSession(TReadSessionSettings(settings).DisableClusterDiscovery(true));
    }

    std::shared_ptr<NYdb::NTopic::IReadSession> CreateReader(
        NYdb::TDriver& driver,
        const NYdb::NTopic::TReadSessionSettings& settings,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> creds,
        const TString& userAgent
    ) {
        NYdb::NTopic::TTopicClientSettings clientSettings;
        if (creds) clientSettings.CredentialsProviderFactory(creds);
        auto readerSettings = settings;
        if (!userAgent.empty()) {
            readerSettings.Header({{NYdb::YDB_APPLICATION_NAME, userAgent}});
        }
        return NYdb::NTopic::TTopicClient(driver, clientSettings).CreateReadSession(readerSettings);
    }

    TMaybe<TReadSessionEvent::TDataReceivedEvent> GetNextMessageSkipAssignment(std::shared_ptr<IReadSession>& reader, TDuration timeout) {
        while (true) {
            auto future = reader->WaitEvent();
            future.Wait(timeout);

            TMaybe<NYdb::NPersQueue::TReadSessionEvent::TEvent> event = reader->GetEvent(false, 1);
            if (!event)
                return {};
            if (auto dataEvent = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                return *dataEvent;
            } else if (auto* createPartitionStreamEvent = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent>(&*event)) {
                createPartitionStreamEvent->Confirm();
            } else if (auto* destroyPartitionStreamEvent = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TDestroyPartitionStreamEvent>(&*event)) {
                destroyPartitionStreamEvent->Confirm();
            } else if (auto* closeSessionEvent = std::get_if<NYdb::NPersQueue::TSessionClosedEvent>(&*event)) {
                return {};
            }
        }
        return {};
    }

    TMaybe<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent> GetNextMessageSkipAssignment(std::shared_ptr<NYdb::NTopic::IReadSession>& reader, TDuration timeout) {
        while (true) {
            auto future = reader->WaitEvent();
            future.Wait(timeout);

            TMaybe<NYdb::NTopic::TReadSessionEvent::TEvent> event = reader->GetEvent(false, 1);
            if (!event)
                return {};
            if (auto e = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                return *e;
            } else if (auto* e = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
                e->Confirm();
            } else if (auto* e = std::get_if<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&*event)) {
                e->Confirm();
            } else if (std::get_if<NYdb::NTopic::TSessionClosedEvent>(&*event)) {
                return {};
            }
        }
        return {};
    }
}
