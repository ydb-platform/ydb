#pragma once

namespace NKikimr {
    void ApplySuperLemmerInplace(TString& key);
    bool IsSuperLemmerSupportedLanguage(const TString& language);
}
