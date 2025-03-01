/*******************************************************************************
 * tlx/meta/has_method.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_META_HAS_METHOD_HEADER
#define TLX_META_HAS_METHOD_HEADER

namespace tlx {

//! \addtogroup tlx_meta
//! \{

/******************************************************************************/
// SFINAE check whether a class method is callable with given parameter types.

/*!
 * Macro template for callable class method SFINAE test.

 Usage:
 \code
 TLX_MAKE_HAS_METHOD(myfunc);

 static_assert(has_method_myfunc<MyClass, int(std::string)>::value,
               "check MyClass for existence of myfunc "
               "with signature int(std::string)");
 \endcode
*/
#define TLX_MAKE_HAS_METHOD(Method)                                         \
    template <typename Class, typename Signature>                           \
    class has_method_ ## Method;                                            \
                                                                            \
    template <typename Class, typename Return, typename... Args>            \
    class has_method_ ## Method<Class, Return(Args...)>                     \
    {                                                                       \
        template <typename C>                                               \
        static char test(                                                   \
            decltype(static_cast<Return (C::*)(Args...)>(&C::Method)));     \
        template <typename C>                                               \
        static int test(...);                                               \
    public:                                                                 \
        static const bool value = (sizeof(test<Class>(0)) == sizeof(char)); \
    }

/*!
 * Macro template for callable class method SFINAE test.

 Usage:
 \code
 TLX_MAKE_HAS_STATIC_METHOD(myfunc);

 static_assert(has_method_myfunc<MyClass, int(std::string)>::value,
               "check MyClass for existence of static myfunc "
               "with signature int(std::string)");
 \endcode
*/
#define TLX_MAKE_HAS_STATIC_METHOD(Method)                                  \
    template <typename Class, typename Signature>                           \
    class has_method_ ## Method;                                            \
                                                                            \
    template <typename Class, typename Return, typename... Args>            \
    class has_method_ ## Method<Class, Return(Args...)>                     \
    {                                                                       \
        template <typename C>                                               \
        static char test(                                                   \
            decltype(static_cast<Return (*)(Args...)>(&C::Method)));        \
        template <typename C>                                               \
        static int test(...);                                               \
    public:                                                                 \
        static const bool value = (sizeof(test<Class>(0)) == sizeof(char)); \
    }

/*!
 * Macro template for callable class method SFINAE test.

 Usage:
 \code
 TLX_MAKE_HAS_TEMPLATE_METHOD(myfunc);

 static_assert(has_method_myfunc<MyClass, int(std::string), float, int>::value,
               "check MyClass for existence of template myfunc "
               "with signature int(std::string) "
               "if the template method is instantiated with <float, int>");
 \endcode
*/
#define TLX_MAKE_HAS_TEMPLATE_METHOD(Method)                                \
    template <typename Class, typename Signature, typename... Cons>         \
    class has_method_ ## Method;                                            \
                                                                            \
    template <typename Class,                                               \
              typename Return, typename... Args, typename... Cons>          \
    class has_method_ ## Method<Class, Return(Args...), Cons...>            \
    {                                                                       \
        template <typename C>                                               \
        static char test(                                                   \
            decltype(static_cast<Return (C::*)(Args...)>(                   \
                         &C::template Method<Cons...>)));                   \
        template <typename C>                                               \
        static int test(...);                                               \
    public:                                                                 \
        static const bool value = (sizeof(test<Class>(0)) == sizeof(char)); \
    }

//! \}

} // namespace tlx

#endif // !TLX_META_HAS_METHOD_HEADER

/******************************************************************************/
