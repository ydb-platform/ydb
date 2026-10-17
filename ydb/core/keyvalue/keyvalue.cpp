#include "keyvalue_flat_impl.h"

namespace NKikimr {

namespace NKeyValue {

bool TKeyValueFlat::TTxRequest::CheckConsistency(NTabletFlatExecutor::TTransactionContext &txc) {
#ifdef KIKIMR_KEYVALUE_CONSISTENCY_CHECKS
    Y_ABORT_UNLESS(Self->State.GetStateBytes() == Self->State.RecountStateBytes(), "StateBytes# %s Recount# %s",
        Self->State.GetStateBytes().ToString().c_str(), Self->State.RecountStateBytes().ToString().c_str());
    // Load feeds the tablet counters and resource metrics, so the scratch state needs its own
    TKeyValueState state;
    state.SetupTabletCounters(MakeTabletCounters());
    NMetrics::TResourceMetrics scratchMetrics(Self->TabletID(), 0, TActorId());
    state.SetupResourceMetrics(&scratchMetrics);
    if (!TTxInit::LoadStateFromDB(state, txc.DB)) {
        return false;
    }
    Y_ABORT_UNLESS(!state.GetIsDamaged());
    state.VerifyEqualIndex(Self->State);
    txc.DB.NoMoreReadsForTx();
    return true;
#else
    Y_UNUSED(txc);
    return true;
#endif
}

} // NKeyValue

IActor* CreateKeyValueFlat(const TActorId &tablet, TTabletStorageInfo *info) {
    return new NKeyValue::TKeyValueFlat(tablet, info);
}

} // NKikimr
