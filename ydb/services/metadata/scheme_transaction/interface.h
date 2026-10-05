#pragma once

#include <ydb/core/tx/tx_proxy/proxy.h>

#include <library/cpp/object_factory/object_factory.h>

namespace NKikimr::NMetadata {

class ISchemeTransactionFactory {
public:
    // Registered by NKikimrSchemeOp::EOperationType.
    using TFactory = NObjectFactory::TObjectFactory<ISchemeTransactionFactory, ui32>;

    virtual ~ISchemeTransactionFactory() = default;
    virtual NActors::IActor* CreateActor(TEvTxUserProxy::TEvProposeTransaction::TPtr request) const = 0;
};

} // namespace NKikimr::NMetadata
