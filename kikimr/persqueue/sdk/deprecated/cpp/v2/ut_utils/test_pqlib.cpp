#include "test_pqlib.h"

namespace NPersQueue::NTests {

    std::tuple<THolder<IProducer>, TProducerCreateResponse> TTestPQLib::CreateProducer(
            const TProducerSettings& settings, bool deprecated
    ) {
        auto producer = PQLib->CreateProducer(settings, Logger, deprecated);
        TProducerCreateResponse response = producer->Start().GetValueSync();
        return std::tuple<THolder<IProducer>, TProducerCreateResponse>(std::move(producer), response);
    }

    std::tuple<THolder<IProducer>, TProducerCreateResponse> TTestPQLib::CreateProducer(
            const TString& topic,
            const TString& sourceId,
            std::optional<ui32> partitionGroup,
            std::optional<NPersQueueCommon::ECodec> codec,
            std::optional<bool> reconnectOnFailure,
            bool deprecated
    ) {
        TProducerSettings s;
        s.Server = TServerSetting{"localhost", Server.GrpcPort};
        s.Topic = topic;
        s.SourceId = sourceId;
        if (partitionGroup) {
            s.PartitionGroup = partitionGroup.value();
        }
        if (codec) {
            s.Codec = codec.value();
        }
        if (reconnectOnFailure) {
            s.ReconnectOnFailure = reconnectOnFailure.value();
        }
        return CreateProducer(s, deprecated);
    }

    std::tuple<THolder<IConsumer>, TConsumerCreateResponse> TTestPQLib::CreateConsumer(
            const TConsumerSettings& settings
    ) {
        auto consumer = PQLib->CreateConsumer(settings, Logger);
        TConsumerCreateResponse response = consumer->Start().GetValueSync();
        return std::tuple<THolder<IConsumer>, TConsumerCreateResponse>(std::move(consumer), response);
    }

    std::tuple<THolder<IConsumer>, TConsumerCreateResponse> TTestPQLib::CreateDeprecatedConsumer(
            const TConsumerSettings& settings
    ) {
        auto consumer = PQLib->CreateConsumer(settings, Logger, true);
        TConsumerCreateResponse response = consumer->Start().GetValueSync();
        return std::tuple<THolder<IConsumer>, TConsumerCreateResponse>(std::move(consumer), response);
    }

    TConsumerSettings TTestPQLib::MakeSettings(
            const TVector<TString>& topics,
            const TString& consumerName
    ) {
        TConsumerSettings s;
        s.Server = TServerSetting{"localhost", Server.GrpcPort};
        s.ClientId = consumerName;
        s.Topics = topics;
        return s;
    }

    std::tuple<THolder<IConsumer>, TConsumerCreateResponse> TTestPQLib::CreateConsumer(
            const TVector<TString>& topics,
            const TString& consumerName,
            std::optional<ui32> maxCount,
            std::optional<bool> useLockSession,
            std::optional<bool> readMirroredPartitions,
            std::optional<bool> unpack,
            std::optional<ui32> maxInflyRequests,
            std::optional<ui32> maxMemoryUsage
    ) {
        auto s = MakeSettings(topics, consumerName);
        if (maxCount) {
            s.MaxCount = maxCount.value();
        }
        if (unpack) {
            s.Unpack = unpack.value();
        }
        if (readMirroredPartitions) {
            s.ReadMirroredPartitions = readMirroredPartitions.value();
        }
        if (maxInflyRequests) {
            s.MaxInflyRequests = maxInflyRequests.value();
        }
        if (maxMemoryUsage) {
            s.MaxMemoryUsage = maxMemoryUsage.value();
        }
        if (useLockSession) {
            s.UseLockSession = useLockSession.value();
        }
        return CreateConsumer(s);
    }
} // namespace NPersQueue::NTests