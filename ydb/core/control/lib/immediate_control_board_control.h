#pragma once
#include "defs.h"

#include <util/generic/ptr.h>
#include <library/cpp/deprecated/atomic/atomic.h>

namespace NKikimr {

class TControl : public TThrRefBase {
    TAtomic Value;
    TAtomic Default;
    TAtomicBase LowerBound;
    TAtomicBase UpperBound;

public:
    TControl(TAtomicBase defaultValue, TAtomicBase lowerBound, TAtomicBase upperBound);

    void Set(TAtomicBase newValue);
    void Reset(TAtomicBase defaultValue, TAtomicBase lowerBound, TAtomicBase upperBound);

    TAtomicBase SetFromHtmlRequest(TAtomicBase newValue);

    TAtomicBase Get() const;

    TAtomicBase GetDefault() const;

    void RestoreDefault();

    bool IsDefault() const;

    TString RangeAsString() const;
};

}
