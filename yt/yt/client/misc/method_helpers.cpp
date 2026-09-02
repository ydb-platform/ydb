#include "method_helpers.h"

#include <yt/yt/core/misc/error.h>

namespace NYT {

[[noreturn]] void ThrowUnimplementedClientMethodError(TStringBuf methodName, TStringBuf reason)
{
    THROW_ERROR_EXCEPTION("%v method %v", reason, methodName);
}

} // namespace NYT
