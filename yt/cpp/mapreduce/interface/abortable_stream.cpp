#include "abortable_stream.h"

#include <yt/cpp/mapreduce/interface/errors.h>

#include <util/system/yassert.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void IAbortableInputStream::Abort()
{
    Y_ABORT("Unimplemented");
}

bool IAbortableInputStream::IsAborted() const
{
    return false;
}

bool IAbortableInputStream::IsAbortedError(const std::exception_ptr& error)
{
    try {
        std::rethrow_exception(error);
    } catch (const TInputStreamAbortedError& ex) {
        return true;
    } catch (...) {
        return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
