#pragma once

#include "types.h"
#include "iproducer.h"
#include "iconsumer.h"
#include "iprocessor.h"
#include "responses.h"

#include <library/cpp/threading/future/future.h>

#include <util/generic/noncopyable.h>
#include <util/generic/ptr.h>

namespace NPersQueue {

class TPQLibPrivate;

class TPQLib : public TNonCopyable {
public:
    explicit TPQLib(const TPQLibSettings& settings = TPQLibSettings());
    ~TPQLib();

    // Deprecated flag means that PQLib doen't wait its objects during destruction.
    // This behaviour leads to potential thread leakage and thread sanitizer errors.
    // It is recommended to specify deprecated = false. In that case PQLib will cancel
    // all objects and correctly wait for its threads in destructor. But, unfortunately,
    // we can't migrate all current clients to this default behaviour automatically,
    // because this can break existing programs unpredictably.

    // Producers creation
    THolder<IProducer> CreateProducer(const TProducerSettings& settings, TIntrusivePtr<ILogger> logger = nullptr, bool deprecated = false);
    THolder<IProducer> CreateMultiClusterProducer(const TMultiClusterProducerSettings& settings, TIntrusivePtr<ILogger> logger = nullptr, bool deprecated = false);

    // Consumers creation
    THolder<IConsumer> CreateConsumer(const TConsumerSettings& settings, TIntrusivePtr<ILogger> logger = nullptr, bool deprecated = false);

    // Processors creation
    THolder<IProcessor> CreateProcessor(const TProcessorSettings& settings, TIntrusivePtr<ILogger> logger = nullptr, bool deprecated = false);

    void SetLogger(TIntrusivePtr<ILogger> logger);

    TString GetUserAgent() const;
    void SetUserAgent(const TString& userAgent);

private:
    TIntrusivePtr<TPQLibPrivate> Impl;
    TAtomic Alive = 1; // Debug check for valid PQLib usage.
};

}
