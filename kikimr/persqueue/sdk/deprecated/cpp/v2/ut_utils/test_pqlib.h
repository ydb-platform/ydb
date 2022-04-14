#pragma once
#include "test_server.h"

namespace NPersQueue::NTests {

class TTestPQLib {
public:
    TTestPQLib(TTestServer& server)
        : Logger(new TCerrLogger(DEBUG_LOG_LEVEL))
        , Server(server)
    {
        TPQLibSettings settings;
        settings.DefaultLogger = Logger;
        PQLib = MakeHolder<TPQLib>(settings);
    }

    std::tuple<THolder<IProducer>, TProducerCreateResponse> CreateProducer(
            const TProducerSettings& settings, bool deprecated
    );

    std::tuple<THolder<IProducer>, TProducerCreateResponse> CreateProducer(
            const TString& topic, const TString& sourceId, std::optional<ui32> partitionGroup = {},
            std::optional<NPersQueueCommon::ECodec> codec = ECodec::RAW, std::optional<bool> reconnectOnFailure = {},
            bool deprecated = false);

    std::tuple<THolder<IConsumer>, TConsumerCreateResponse>  CreateConsumer(const TConsumerSettings& settings);

    std::tuple<THolder<IConsumer>, TConsumerCreateResponse>  CreateDeprecatedConsumer(const TConsumerSettings& settings);

    TConsumerSettings MakeSettings(const TVector<TString>& topics, const TString& consumerName = "user");

    std::tuple<THolder<IConsumer>, TConsumerCreateResponse> CreateConsumer(
            const TVector<TString>& topics, const TString& consumerName = "user", std::optional<ui32> maxCount = {},
            std::optional<bool> useLockSession = {}, std::optional<bool> readMirroredPartitions = {},
            std::optional<bool> unpack = {}, std::optional<ui32> maxInflyRequests = {},
            std::optional<ui32> maxMemoryUsage = {});

private:
    TIntrusivePtr<TCerrLogger> Logger;
    TTestServer& Server;
    THolder<TPQLib> PQLib;
};
} // namespace NPersQueue::NTests