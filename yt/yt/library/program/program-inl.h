#ifndef PROGRAM_INL_H_
#error "Direct inclusion of this file is not allowed, include program.h"
// For the sake of sane code completion.
#include "program.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class E>
    requires std::is_enum_v<E>
void TProgram::Abort(E exitCode) noexcept
{
    Abort(ToUnderlying(exitCode));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
