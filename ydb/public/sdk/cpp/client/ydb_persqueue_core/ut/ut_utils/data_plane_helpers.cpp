#include "data_plane_helpers.h"

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
        THashMap<TString, TString> sessionMeta
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


}
