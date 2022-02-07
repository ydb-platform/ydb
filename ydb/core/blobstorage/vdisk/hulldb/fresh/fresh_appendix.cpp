#include "fresh_appendix.h"

namespace NKikimr {

    template class TFreshAppendix<TKeyLogoBlob, TMemRecLogoBlob>;
    template class TFreshAppendix<TKeyBarrier, TMemRecBarrier>;
    template class TFreshAppendix<TKeyBlock, TMemRecBlock>;

    template class TFreshAppendixTreeSnap<TKeyLogoBlob, TMemRecLogoBlob>;
    template class TFreshAppendixTreeSnap<TKeyBarrier, TMemRecBarrier>;
    template class TFreshAppendixTreeSnap<TKeyBlock, TMemRecBlock>;

    template class TFreshAppendixTree<TKeyLogoBlob, TMemRecLogoBlob>;
    template class TFreshAppendixTree<TKeyBarrier, TMemRecBarrier>;
    template class TFreshAppendixTree<TKeyBlock, TMemRecBlock>;

} // NKikimr
