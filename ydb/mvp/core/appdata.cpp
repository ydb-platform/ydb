#include "appdata.h"
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>

namespace NMVP{

TMVPAppData::TMVPAppData()
    : GRpcClientLow(std::make_shared<NYdbGrpc::TGRpcClientLow>())
{}

TMVPAppData* MVPAppData() {
    return NActors::TActivationContext::ActorSystem()->template AppData<TMVPAppData>();
}

} // namespace NMVP
