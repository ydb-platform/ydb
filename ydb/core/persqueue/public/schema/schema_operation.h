#pragma once

#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/library/actors/core/actorsystem_fwd.h>

namespace NKikimr::NPQ::NSchema {

NActors::IActor* CreateSchemaOperation(
    NActors::TActorId parentId,
    const TString& path,
    std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction>&& operation,
    ui64 cookie = 0
);

} // namespace NKikimr::NPQ::NSchema
