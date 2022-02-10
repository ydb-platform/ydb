#pragma once

#include <util/system/yassert.h>

class TExtraRef {
    TAtomic Holds;

public:
    TExtraRef()
        : Holds(false)
    {
    }
    ~TExtraRef() {
        Y_VERIFY(!Holds); 
    }

    template <typename TThis>
    void Retain(TThis* thiz) {
        if (AtomicGet(Holds)) {
            return;
        }
        if (AtomicCas(&Holds, 1, 0)) {
            thiz->Ref();
        }
    }

    template <typename TThis>
    void Release(TThis* thiz) {
        if (!AtomicGet(Holds)) {
            return;
        }
        if (AtomicCas(&Holds, 0, 1)) {
            thiz->UnRef();
        }
    }
};
