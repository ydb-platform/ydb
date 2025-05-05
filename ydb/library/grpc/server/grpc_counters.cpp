#include "grpc_counters.h"

namespace NYdbGrpc {
namespace {

class TFakeCounterBlock final: public ICounterBlock {
private:
    void CountNotOkRequest() override {
    }

    void CountNotOkResponse() override {
    }

    void CountNotAuthenticated() override {
    }

    void CountResourceExhausted() override {
    }

    void CountRequestBytes(ui32 /*requestSize*/) override {
    }

    void CountResponseBytes(ui32 /*responseSize*/) override {
    }

    void StartProcessing(ui32 /*requestSize*/, TInstant /*deadline*/) override {
    }

    void FinishProcessing(
            ui32 /*requestSize*/,
            ui32 /*responseSize*/,
            bool /*ok*/,
            ui32 /*status*/,
            TDuration /*requestDuration*/) override
    {
    }
};

} // namespace

ICounterBlockPtr FakeCounterBlock() {
    return MakeIntrusive<TFakeCounterBlock>();
}

} // namespace NYdbGrpc
