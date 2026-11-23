#include "yql_pq_gateway.h"
#include "yql_pq_gateway_factory.h"

namespace NYql {

namespace {

class TPqNativeGatewayFactory final: public IPqGatewayFactory {
public:
    explicit TPqNativeGatewayFactory(const NYql::TPqGatewayServices& services)
        : Services(services)
    {
    }

    IPqGateway::TPtr CreatePqGateway() final {
        return CreatePqNativeGateway(Services);
    }

private:
    const NYql::TPqGatewayServices Services;
};

}   // anonymous namespace

IPqGatewayFactory::TPtr CreatePqNativeGatewayFactory(const TPqGatewayServices& services) {
    return MakeIntrusive<TPqNativeGatewayFactory>(services);
}

}   // namespace NYql
