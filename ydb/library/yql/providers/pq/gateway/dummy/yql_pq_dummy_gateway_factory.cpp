#include "yql_pq_dummy_gateway_factory.h"

namespace NYql {

namespace {

class TPqFileGatewayFactory final : public IPqGatewayFactory {
public:
    explicit TPqFileGatewayFactory(IPqGateway::TPtr pqGateway)
        : PqGateway(std::move(pqGateway))
    {}

    IPqGateway::TPtr CreatePqGateway() final {
        return PqGateway;
    }

private:
    const IPqGateway::TPtr PqGateway;
};

} // anonymous namespace

IPqGatewayFactory::TPtr CreatePqFileGatewayFactory(IPqGateway::TPtr pqGateway) {
    return MakeIntrusive<TPqFileGatewayFactory>(std::move(pqGateway));
}

} // namespace NYql
