//
// SignalHandler.h
//
// Library: Foundation
// Package: Threading
// Module:  SignalHandler
//
// Definition of the SignalHandler class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_SignalHandler_INCLUDED
#define DB_Foundation_SignalHandler_INCLUDED


#include "DBPoco/Foundation.h"


#if defined(DB_POCO_OS_FAMILY_UNIX) && !defined(POCO_VXWORKS)


#    include <vector>
#    include <setjmp.h>


namespace DBPoco
{


class Foundation_API SignalHandler
/// This helper class simplifies the handling of POSIX signals.
///
/// The class provides a signal handler (installed with
/// installHandlers()) that translates certain POSIX
/// signals (SIGILL, SIGBUS, SIGSEGV, SIGSYS) into
/// C++ exceptions.
///
/// Internally, a stack of sigjmp_buf structs is maintained for
/// each thread. The constructor pushes a new sigjmp_buf onto
/// the current thread's stack. The destructor pops the sigjmp_buf
/// from the stack.
///
/// The DB_poco_throw_on_signal macro creates an instance of SignalHandler
/// on the stack, which results in a new sigjmp_buf being created.
/// The sigjmp_buf is then set-up with sigsetjmp().
///
/// The handleSignal() method, which is invoked when a signal arrives,
/// checks if a sigjmp_buf is available for the current thread.
/// If so, siglongjmp() is used to jump out of the signal handler.
///
/// Typical usage is as follows:
///
///     try
///     {
///          DB_poco_throw_on_signal;
///          ...
///     }
///     catch (DBPoco::SignalException&)
///     {
///         ...
///     }
///
/// The best way to deal with a SignalException is to log as much context
/// information as possible, to aid in debugging, and then to exit.
///
/// The SignalHandler can be disabled globally by compiling POCO and client
/// code with the POCO_NO_SIGNAL_HANDLER macro defined.
{
public:
    SignalHandler();
    /// Creates the SignalHandler.

    ~SignalHandler();
    /// Destroys the SignalHandler.

    sigjmp_buf & jumpBuffer();
    /// Returns the top-most sigjmp_buf for the current thread.

    static void throwSignalException(int sig);
    /// Throws a SignalException with a textual description
    /// of the given signal as argument.

    static void install();
    /// Installs signal handlers for SIGILL, SIGBUS, SIGSEGV
    /// and SIGSYS.

protected:
    static void handleSignal(int sig);
    /// The actual signal handler.

    struct JumpBuffer
    /// sigjmp_buf cannot be used to instantiate a std::vector,
    /// so we provide a wrapper struct.
    {
        sigjmp_buf buf;
    };
    typedef std::vector<JumpBuffer> JumpBufferVec;

    static JumpBufferVec & jumpBufferVec();
    /// Returns the JumpBufferVec for the current thread.

private:
    static JumpBufferVec _jumpBufferVec;

    friend class ThreadImpl;
};


#    ifndef POCO_NO_SIGNAL_HANDLER
#        define DB_poco_throw_on_signal \
            DBPoco::SignalHandler _poco_signalHandler; \
            int _poco_signal = sigsetjmp(_poco_signalHandler.jumpBuffer(), 1); \
            if (_poco_signal) \
                _poco_signalHandler.throwSignalException(_poco_signal);
#    else
#        define DB_poco_throw_on_signal
#    endif


} // namespace DBPoco


#endif // DB_POCO_OS_FAMILY_UNIX


#endif // DB_Foundation_SignalHandler_INCLUDED
