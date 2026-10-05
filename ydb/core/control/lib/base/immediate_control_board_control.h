#pragma once

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

    // Restore the default and report the value transition made by this call.
    void RestoreDefault(TAtomicBase& outPrevValue, TAtomicBase& outNewValue);

    bool IsDefault() const;

    TString RangeAsString() const;
};

}
