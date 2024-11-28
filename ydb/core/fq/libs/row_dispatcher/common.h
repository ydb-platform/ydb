#pragma once

#include <util/generic/ptr.h>
#include <util/system/mutex.h>

#include <yql/essentials/public/purecalc/common/fwd.h>

namespace NFq {

class IPureCalcProgramFactory : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IPureCalcProgramFactory>;

    struct TSettings {
        bool EnabledLLVM = false;

        std::strong_ordering operator<=>(const TSettings& other) const = default;
    };

public:
    virtual NYql::NPureCalc::IProgramFactoryPtr GetFactory(const TSettings& settings) const = 0;

    // Before creating purecalc program factory should be locked
    virtual TGuard<TMutex> LockFactory() const = 0;
};

IPureCalcProgramFactory::TPtr CreatePureCalcProgramFactory();

TString CleanupCounterValueString(const TString& value);

} // namespace NFq
