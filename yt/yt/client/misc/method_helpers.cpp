#include "method_helpers.h"

#include <yt/yt/core/misc/error.h>

namespace NYT {

[[noreturn]] void ThrowUnimplementedClientMethodError(TStringBuf methodName, TStringBuf reason)
{
    throw TErrorException() <<= TError("%v method: %v", reason, methodName);
}

} // namespace NYT
