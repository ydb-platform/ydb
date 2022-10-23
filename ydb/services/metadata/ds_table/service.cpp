#include "service.h"

#include "accessor_subscribe.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/services/metadata/service.h>

namespace NKikimr::NMetadataProvider {

IActor* CreateService(const TConfig& config) {
    return new TService(config);
}

void TService::Handle(TEvSubscribeExternal::TPtr& ev) {
    auto it = Accessors.find(ev->Get()->GetSnapshotParser()->GetTablePath());
    if (it == Accessors.end()) {
        THolder<TExternalData> actor = MakeHolder<TExternalData>(Config, ev->Get()->GetSnapshotParser());
        it = Accessors.emplace(ev->Get()->GetSnapshotParser()->GetTablePath(), Register(actor.Release())).first;
    }
    Send<TEvSubscribe>(it->second, ev->Sender);
}

void TService::Handle(TEvUnsubscribeExternal::TPtr& ev) {
    auto it = Accessors.find(ev->Get()->GetSnapshotParser()->GetTablePath());
    if (it != Accessors.end()) {
        Send<TEvUnsubscribe>(it->second, ev->Sender);
    }
}

}
