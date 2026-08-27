#include <util/generic/string.h>
#include <util/generic/yexception.h>

namespace NKikimr {
    void ApplySuperLemmerInplace(TString& key) {
        Y_UNUSED(key);
        throw yexception() << "Superlemmer can't be enabled in opensource ydb build";
    }

    bool IsSuperLemmerSupportedLanguage(const TString& language) {
        return language == "russian" || language == "english";
    }
}
