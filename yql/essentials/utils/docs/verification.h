#pragma once

#include "page.h"
#include "link.h"

#include <util/generic/map.h>
#include <util/generic/set.h>

namespace NYql::NDocs {

    enum class EFame {
        BadLinked,
        Unknown,
        Mentioned,
        Documented,
    };

    using TStatusesByName = TMap<TString, TString>;

    using TFameReport = THashMap<EFame, TStatusesByName>;

    struct TVerificationInput {
        TLinks Links;
        TPages Pages;
        TSet<TString> Names;
        TMap<TString, TString> ShortHands;
    };

    TFameReport Verify(TVerificationInput input);

    double Coverage(const TFameReport& report, const TVector<TString>& names);

} // namespace NYql::NDocs
