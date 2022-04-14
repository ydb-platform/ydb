#pragma once

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/iprocessor.h>
#include "interface_common.h"

namespace NPersQueue {

class TPQLibPrivate;

struct IProcessorImpl: public IProcessor, public TSyncDestroyed {
    using IProcessor::GetNextData;

    IProcessorImpl(std::shared_ptr<void> destroyEventRef, TIntrusivePtr<TPQLibPrivate> pqLib);

    // Initialization after constructor (for example, for correct call of shared_from_this())
    virtual void Init() {
    }
    virtual void GetNextData(NThreading::TPromise<TOriginData>& promise) noexcept = 0;
};

// Processor that is given to client.
class TPublicProcessor: public IProcessor {
public:
    explicit TPublicProcessor(std::shared_ptr<IProcessorImpl> impl);
    ~TPublicProcessor();

    NThreading::TFuture<TOriginData> GetNextData() noexcept override {
        return Impl->GetNextData();
    }

private:
    std::shared_ptr<IProcessorImpl> Impl;
};

} // namespace NPersQueue
