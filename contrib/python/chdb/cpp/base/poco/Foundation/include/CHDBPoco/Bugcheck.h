//
// Bugcheck.h
//
// Library: Foundation
// Package: Core
// Module:  Bugcheck
//
// Definition of the Bugcheck class and the self-testing macros.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Foundation_Bugcheck_INCLUDED
#define CHDB_Foundation_Bugcheck_INCLUDED


#include <cstdlib>
#include <string>
#include "CHDBPoco/Foundation.h"
#if defined(_DEBUG)
#    include <iostream>
#endif


namespace CHDBPoco
{


class Foundation_API Bugcheck
/// This class provides some static methods that are
/// used by the
/// CHDB_poco_assert_dbg(), CHDB_poco_assert(), CHDB_poco_check_ptr(),
/// CHDB_poco_bugcheck() and CHDB_poco_unexpected() macros.
/// You should not invoke these methods
/// directly. Use the macros instead, as they
/// automatically provide useful context information.
{
public:
    static void assertion(const char * cond, const char * file, int line, const char * text = 0);
    /// An assertion failed. Break into the debugger, if
    /// possible, then throw an AssertionViolationException.

    static void nullPointer(const char * ptr, const char * file, int line);
    /// An null pointer was encountered. Break into the debugger, if
    /// possible, then throw an NullPointerException.

    static void bugcheck(const char * file, int line);
    /// An internal error was encountered. Break into the debugger, if
    /// possible, then throw an BugcheckException.

    static void bugcheck(const char * msg, const char * file, int line);
    /// An internal error was encountered. Break into the debugger, if
    /// possible, then throw an BugcheckException.

    static void unexpected(const char * file, int line);
    /// An exception was caught in a destructor. Break into debugger,
    /// if possible and report exception. Must only be called from
    /// within a catch () block as it rethrows the exception to
    /// determine its class.

    static void debugger(const char * file, int line);
    /// An internal error was encountered. Break into the debugger, if
    /// possible.

    static void debugger(const char * msg, const char * file, int line);
    /// An internal error was encountered. Break into the debugger, if
    /// possible.

protected:
    static std::string what(const char * msg, const char * file, int line, const char * text = 0);
};


} // namespace CHDBPoco


//
// useful macros (these automatically supply line number and file name)
//
#if defined(__KLOCWORK__) || defined(__clang_analyzer__)


// Short-circuit these macros when under static analysis.
// Ideally, static analysis tools should understand and reason correctly about
// noreturn methods such as Bugcheck::bugcheck(). In practice, they don't.
// Help them by turning these macros into std::abort() as described here:
// https://developer.klocwork.com/documentation/en/insight/10-1/tuning-cc-analysis#Usingthe__KLOCWORK__macro

#    include <cstdlib> // for abort
#    define CHDB_poco_assert_dbg(cond) \
        do \
        { \
            if (!(cond)) \
                std::abort(); \
        } while (0)
#    define CHDB_poco_assert_msg_dbg(cond, text) \
        do \
        { \
            if (!(cond)) \
                std::abort(); \
        } while (0)
#    define CHDB_poco_assert(cond) \
        do \
        { \
            if (!(cond)) \
                std::abort(); \
        } while (0)
#    define CHDB_poco_assert_msg(cond, text) \
        do \
        { \
            if (!(cond)) \
                std::abort(); \
        } while (0)
#    define CHDB_poco_check_ptr(ptr) \
        do \
        { \
            if (!(ptr)) \
                std::abort(); \
        } while (0)
#    define CHDB_poco_bugcheck() \
        do \
        { \
            std::abort(); \
        } while (0)
#    define CHDB_poco_bugcheck_msg(msg) \
        do \
        { \
            std::abort(); \
        } while (0)


#else // defined(__KLOCWORK__) || defined(__clang_analyzer__)


#    if defined(_DEBUG)
#        define CHDB_poco_assert_dbg(cond) \
            if (!(cond)) \
                CHDBPoco::Bugcheck::assertion(#cond, __FILE__, __LINE__); \
            else \
                (void)0

#        define CHDB_poco_assert_msg_dbg(cond, text) \
            if (!(cond)) \
                CHDBPoco::Bugcheck::assertion(#cond, __FILE__, __LINE__, text); \
            else \
                (void)0
#    else
#        define CHDB_poco_assert_msg_dbg(cond, text)
#        define CHDB_poco_assert_dbg(cond)
#    endif


#    define CHDB_poco_assert(cond) \
        if (!(cond)) \
            CHDBPoco::Bugcheck::assertion(#cond, __FILE__, __LINE__); \
        else \
            (void)0


#    define CHDB_poco_assert_msg(cond, text) \
        if (!(cond)) \
            CHDBPoco::Bugcheck::assertion(#cond, __FILE__, __LINE__, text); \
        else \
            (void)0


#    define CHDB_poco_check_ptr(ptr) \
        if (!(ptr)) \
            CHDBPoco::Bugcheck::nullPointer(#ptr, __FILE__, __LINE__); \
        else \
            (void)0


#    define CHDB_poco_bugcheck() CHDBPoco::Bugcheck::bugcheck(__FILE__, __LINE__)


#    define CHDB_poco_bugcheck_msg(msg) CHDBPoco::Bugcheck::bugcheck(msg, __FILE__, __LINE__)


#endif // defined(__KLOCWORK__) || defined(__clang_analyzer__)


#define CHDB_poco_unexpected() CHDBPoco::Bugcheck::unexpected(__FILE__, __LINE__);


#define CHDB_poco_debugger() CHDBPoco::Bugcheck::debugger(__FILE__, __LINE__)


#define CHDB_poco_debugger_msg(msg) CHDBPoco::Bugcheck::debugger(msg, __FILE__, __LINE__)


#if defined(_DEBUG)
#    define CHDB_poco_stdout_dbg(outstr) std::cout << __FILE__ << '(' << std::dec << __LINE__ << "):" << outstr << std::endl;
#else
#    define CHDB_poco_stdout_dbg(outstr)
#endif


#if defined(_DEBUG)
#    define CHDB_poco_stderr_dbg(outstr) std::cerr << __FILE__ << '(' << std::dec << __LINE__ << "):" << outstr << std::endl;
#else
#    define CHDB_poco_stderr_dbg(outstr)
#endif


//
// CHDB_poco_static_assert
//
// The following was ported from <boost/static_assert.hpp>
//


template <bool x>
struct POCO_STATIC_ASSERTION_FAILURE;


template <>
struct POCO_STATIC_ASSERTION_FAILURE<true>
{
    enum
    {
        value = 1
    };
};


template <int x>
struct poco_static_assert_test
{
};


#if defined(__GNUC__) && (__GNUC__ == 3) && ((__GNUC_MINOR__ == 3) || (__GNUC_MINOR__ == 4))
#    define CHDB_poco_static_assert(B) \
        typedef char CHDB_POCO_JOIN(poco_static_assert_typedef_, __LINE__)[POCO_STATIC_ASSERTION_FAILURE<(bool)(B)>::value]
#else
#    define CHDB_poco_static_assert(B) \
        typedef poco_static_assert_test<sizeof(POCO_STATIC_ASSERTION_FAILURE<(bool)(B)>)> CHDB_POCO_JOIN(poco_static_assert_typedef_, __LINE__) \
            CHDB_POCO_UNUSED
#endif


#endif // CHDB_Foundation_Bugcheck_INCLUDED
