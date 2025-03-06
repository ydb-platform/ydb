#include "grpc_helper.h"

namespace NKikimr {
namespace NGRpcService {

//using namespace NActors;

NYdbGrpc::IGRpcRequestLimiterPtr TCreateLimiterCB::operator()(EStaticControlType controlType, i64 limit) const {
    return LimiterRegistry->RegisterRequestType(controlType, limit);
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


NYdbGrpc::IGRpcRequestLimiterPtr TInFlightLimiterRegistry::RegisterRequestType(EStaticControlType controlType, i64 limit) {
    TGuard<TMutex> g(Lock);
    if (!PerTypeLimiters.count(controlType)) {
        TControlWrapper control(limit, 0, 1000000);
        StaticControlBoard->RegisterSharedControl(control, controlType);
        PerTypeLimiters[controlType] = new TRequestInFlightLimiter(control);
    }

    return PerTypeLimiters[controlType];
}

} // namespace NGRpcService
} // namespace NKikimr
