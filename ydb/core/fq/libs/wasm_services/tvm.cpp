#include "tvm.h"

namespace NFq::NWasmServices {
namespace {

class TUnavailableRequest final: public ITvmTicketRequest {
public:
    void Cancel() noexcept override {
    }
};

class TUnavailableProvider final: public ITvmTicketProvider {
public:
    bool IsAvailable() const override {
        return false;
    }

    std::shared_ptr<ITvmTicketRequest> Request(const TTvmCredentials&, TInstant, TCompletion completion) override {
        completion({ .Status = ETvmTicketStatus::Failed });
        return std::make_shared<TUnavailableRequest>();
    }
};

}   // namespace

std::shared_ptr<ITvmTicketProvider> CreateTvmTicketProvider() {
    return std::make_shared<TUnavailableProvider>();
}

}   // namespace NFq::NWasmServices
