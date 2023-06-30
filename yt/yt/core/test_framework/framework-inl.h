#ifndef FRAMEWORK_INL_H_
#error "Direct inclusion of this file is not allowed, include framework.h"
// For the sake of sane code completion.
#include "framework.h"
#endif

#include <yt/yt/core/misc/common.h>

#include <util/system/type_name.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void PrintTo(const TIntrusivePtr<T>& arg, std::ostream* os)
{
    *os << "TIntrusivePtr<"
        << TypeName<T>()
        << ">@0x"
        << std::hex
        << reinterpret_cast<uintptr_t>(arg.Get())
        << std::dec
        << " [";
    if (arg) {
        ::testing::internal::UniversalPrinter<T>::Print(*arg, os);
    }
    *os << "]";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
