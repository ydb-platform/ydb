#include "method.h"

#include <util/generic/hash_set.h>

namespace NLsp {

bool IsReadonlyMethod(TStringBuf method) {
    static const THashSet<TStringBuf> Methods = {
        Method.TextDocument.Completion,
    };

    return Methods.contains(method);
};

} // namespace NLsp
