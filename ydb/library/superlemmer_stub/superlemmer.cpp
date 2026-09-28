#include <util/generic/string.h>
#include <util/generic/yexception.h>

using TIsSuperLemmerSupported = bool (*)(const TString&);
using TApplySuperLemmer = void (*)(const TString&, TString&);

namespace {
    static TIsSuperLemmerSupported IsSuperLemmerSupported = nullptr;
    static TApplySuperLemmer ApplySuperLemmer = nullptr;
} // namespace

namespace NKikimr {
    void RegisterSuperLemmer(TIsSuperLemmerSupported isSupported, TApplySuperLemmer apply) {
        IsSuperLemmerSupported = isSupported;
        ApplySuperLemmer = apply;
    }

    void ApplySuperLemmerInplace(const TString& language, TString& word) {
        if (ApplySuperLemmer != nullptr) {
            ApplySuperLemmer(language, word);
            return;
        }
        Y_UNUSED(language);
        Y_UNUSED(word);
        throw yexception() << "Superlemmer can't be enabled in opensource ydb build";
    }

    bool IsSuperLemmerSupportedLanguage(const TString& language) {
        if (IsSuperLemmerSupported != nullptr) {
            return IsSuperLemmerSupported(language);
        }

        return language == "russian" || language == "english";
    }
} // namespace NKikimr
