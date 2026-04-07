#include "grpc_helper.h"

namespace NKikimr {
namespace NGRpcService {

//using namespace NActors;

NYdbGrpc::IGRpcRequestLimiterPtr TCreateLimiterCB::operator()(const TString& controlName, THotSwap<TControl>& icbControl, i64 limit) const {
    return LimiterRegistry->RegisterRequestType(controlName, icbControl, limit);
}

class TRequestInFlightLimiter : public NYdbGrpc::IGRpcRequestLimiter {
private:
    NYdbGrpc::TInFlightLimiterImpl<TControlWrapper> RequestLimiter;

public:
    explicit TRequestInFlightLimiter(TControlWrapper limiter)
        : RequestLimiter(std::move(limiter))
    {}

    bool IncRequest() override {
        return RequestLimiter.Inc();
    }

    void DecRequest() override {
        RequestLimiter.Dec();
    }
};


NYdbGrpc::IGRpcRequestLimiterPtr TInFlightLimiterRegistry::RegisterRequestType(const TString& controlName, THotSwap<TControl>& icbControl, i64 limit) {
    TGuard<TMutex> g(Lock);
    if (!PerTypeLimiters.count(controlName)) {
        TControlWrapper control(limit, 0, 1000000);
        TControlBoard::RegisterSharedControl(control, icbControl);
        PerTypeLimiters[controlName] = new TRequestInFlightLimiter(control);
    }

    return PerTypeLimiters[controlName];
}

} // namespace NGRpcService
} // namespace NKikimr
