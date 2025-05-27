#include "grpc_helper.h"

namespace NKikimr {
namespace NGRpcService {

//using namespace NActors;

NYdbGrpc::IGRpcRequestLimiterPtr TCreateLimiterCB::operator()(const char* serviceName, const char* requestName, i64 limit) const {
    TString fullName = "GRpcControls.RequestConfigs." + TString(serviceName) + "_" + requestName;
    return LimiterRegistry->RegisterRequestType(fullName, limit);
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


NYdbGrpc::IGRpcRequestLimiterPtr TInFlightLimiterRegistry::RegisterRequestType(TString name, i64 limit) {
    TGuard<TMutex> g(Lock);
    if (!PerTypeLimiters.count(name)) {
        TControlWrapper control(limit, 0, 1000000);
        Icb->RegisterSharedControl(control, name + ".MaxInFlight");
        PerTypeLimiters[name] = new TRequestInFlightLimiter(control);
    }

    return PerTypeLimiters[name];
}

} // namespace NGRpcService
} // namespace NKikimr
