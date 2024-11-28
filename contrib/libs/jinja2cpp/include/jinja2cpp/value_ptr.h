#ifndef JINJA2CPP_VALUE_PTR_H
#define JINJA2CPP_VALUE_PTR_H

#include "polymorphic_value.h"

namespace jinja2 {
namespace types {

using namespace isocpp_p0201;

template<typename T>
using ValuePtr = polymorphic_value<T>;

template<class T, class... Ts>
ValuePtr<T> MakeValuePtr(Ts&&... ts)
{
    return make_polymorphic_value<T>(std::forward<Ts&&>(ts)...);
}

template<class T, class U, class... Ts>
ValuePtr<T> MakeValuePtr(Ts&&... ts)
{
    return make_polymorphic_value<T, U>(std::forward<Ts>(ts)...);
}

} // namespace types
} // namespace jinja2

#endif // JINJA2CPP_VALUE_PTR_H
