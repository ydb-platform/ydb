#include "fresh_data.h"

namespace NKikimr {

    template class TFreshData<TKeyLogoBlob, TMemRecLogoBlob>;
    template class TFreshData<TKeyBarrier, TMemRecBarrier>;
    template class TFreshData<TKeyBlock, TMemRecBlock>;

} // NKikimr
