#include "accessor_init.h"
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

namespace NKikimr::NMetadataProvider {

bool TDSAccessorInitialized::Handle(TEvRequestResult<TDialogCreateTable>::TPtr& /*ev*/) {
    InitializedFlag = true;
    return true;
}

void TDSAccessorInitialized::Bootstrap(const NActors::TActorContext& /*ctx*/) {
    RegisterState();
    Register(new TYDBRequest<TDialogCreateTable>(GetTableSchema(), SelfId(), Config));
}

}
