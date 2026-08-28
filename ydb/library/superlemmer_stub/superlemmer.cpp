#include <util/generic/string.h>
#include <util/generic/yexception.h>

namespace NKikimr {
    void ApplySuperLemmerInplace(const TString& language, TString& word) {
        Y_UNUSED(language);
        Y_UNUSED(word);
        throw yexception() << "Superlemmer can't be enabled in opensource ydb build";
    }

    bool IsSuperLemmerSupportedLanguage(const TString& language) {
        return language == "russian" || language == "english";
    }
}
