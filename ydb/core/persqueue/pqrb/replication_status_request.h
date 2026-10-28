#pragma once

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>

#include <memory>

namespace NKikimr::NPQ {

IActor* CreateReplicationStatusRequest(const TActorId& parent,
    std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposal);

} // namespace NKikimr::NPQ
