#include "service_query.h"
#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr::NGRpcService {
namespace NQuery {

void DoAttachSession(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&) {
    Cerr << "DoAttachSession" << Endl;
    p->ReplyWithRpcStatus(grpc::StatusCode::UNIMPLEMENTED);
}

}
}
