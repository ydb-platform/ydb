#pragma once

#include <util/generic/ptr.h>

#include <ydb/library/yql/public/purecalc/common/fwd.h>

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
};

IPureCalcProgramFactory::TPtr CreatePureCalcProgramFactory();

} // namespace NFq
