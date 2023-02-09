#pragma once

#include <util/generic/ptr.h>
#include "tablet_counters.h"


namespace NKikimr {

class ILabeledCounters : public virtual TThrRefBase {
public:
    using TPtr = TIntrusivePtr<ILabeledCounters>;

    virtual void Apply(ui64 TabletID, const NKikimr::TTabletLabeledCountersBase* labeledCounters) = 0;
    virtual void ForgetTablet(ui64 tabletID) = 0;
    virtual void UseDatabase(const TString& database) { Y_UNUSED(database); }
};

TIntrusivePtr<NSysView::IDbCounters> CreateLabeledDbCounters(::NMonitoring::TDynamicCounterPtr externalGroup);

} // namespace NKikimr
