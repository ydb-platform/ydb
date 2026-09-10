#pragma once

#include <util/generic/string.h>

namespace NKikimr {
    using TIsSuperLemmerSupported = bool (*)(const TString&);
    using TApplySuperLemmer = void (*)(const TString&, TString&);

    void RegisterSuperLemmer(TIsSuperLemmerSupported isSupported, TApplySuperLemmer apply);

    void ApplySuperLemmerInplace(const TString& language, TString& word);
    bool IsSuperLemmerSupportedLanguage(const TString& language);
}
