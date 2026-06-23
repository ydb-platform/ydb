#include <util/generic/string.h>
#include <util/generic/yexception.h>

namespace NKikimr {
    void ApplySuperLemmerInplace(TString& key) {
        Y_UNUSED(key);
        ythrow yexception() << "Shouldn't be called anyway";
    }
}
