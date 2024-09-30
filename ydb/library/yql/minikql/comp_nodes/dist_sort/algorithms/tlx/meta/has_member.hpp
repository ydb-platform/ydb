/*******************************************************************************
 * tlx/meta/has_member.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_META_HAS_MEMBER_HEADER
#define TLX_META_HAS_MEMBER_HEADER

namespace tlx {

//! \addtogroup tlx_meta
//! \{

/******************************************************************************/
// SFINAE check whether a class member exists.

// based on http://stackoverflow.com/questions/257288/is-it-possible
// -to-write-a-c-template-to-check-for-a-functions-existence

/*!
 * Macro template for class member / attribute SFINAE test

 Usage:
 \code
 TLX_MAKE_HAS_METHOD(myfunc);

 static_assert(has_method_myfunc<MyClass>::value,
               "check MyClass for existence of attribute/method myfunc");
 \endcode
*/
#define TLX_MAKE_HAS_MEMBER(Member)                      \
    template <typename Type>                             \
    class has_member_ ## Member                          \
    {                                                    \
        template <typename C>                            \
        static char test(decltype(&C::Member));          \
        template <typename C>                            \
        static int test(...);                            \
    public:                                              \
        static const bool value = (         /* NOLINT */ \
            sizeof(test<Type>(0)) == sizeof(char));      \
    }

/*!
 * Macro template for class template member SFINAE test

 Usage:
 \code
 TLX_MAKE_HAS_TEMPLATE_METHOD(myfunc);

 static_assert(has_method_myfunc<MyClass, float, int>::value,
               "check MyClass for existence of attribute/method myfunc "
               "if instantiated with <float, int>");
 \endcode
*/
#define TLX_MAKE_HAS_TEMPLATE_MEMBER(Member)                      \
    template <typename Type, typename... Args>                    \
    class has_member_ ## Member                                   \
    {                                                             \
        template <typename C>                                     \
        static char test(decltype(&C::template Member<Args...>)); \
        template <typename C>                                     \
        static int test(...);                                     \
    public:                                                       \
        static const bool value = (                  /* NOLINT */ \
            sizeof(test<Type>(0)) == sizeof(char));               \
    }

//! \}

} // namespace tlx

#endif // !TLX_META_HAS_MEMBER_HEADER

/******************************************************************************/
