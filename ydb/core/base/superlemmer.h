#pragma once

namespace NKikimr {
    void ApplySuperLemmerInplace(const TString& language, TString& word);
    bool IsSuperLemmerSupportedLanguage(const TString& language);
}
