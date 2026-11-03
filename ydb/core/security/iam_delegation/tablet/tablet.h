#pragma once

#include "public/events.h"
#include "schema.h"

#include <ydb/core/tablet_flat/tablet_flat_executed.h>

namespace NKikimr::NIamDelegation {

class TIamDelegationTablet final
    : public TActor<TIamDelegationTablet>
    , public NTabletFlatExecutor::TTabletExecutedFlat
{
public:
    TIamDelegationTablet(const TActorId& tablet, TTabletStorageInfo* info);

private:
    enum class ETxType : ui32 { InitSchema, Request };

    struct TTxInit;
    struct TTxRequest;

    void OnDetach(const TActorContext& ctx) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) override;
    void OnActivateExecutor(const TActorContext& ctx) override;
    void DefaultSignalTabletActive(const TActorContext& ctx) override;
    void Handle(TEvIamDelegationTablet::TEvRequest::TPtr& ev, const TActorContext& ctx);

    STFUNC(StateInit);
    STFUNC(StateWork);
};

} // namespace NKikimr::NIamDelegation
