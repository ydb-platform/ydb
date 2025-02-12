/*******************************************************************************
 * tlx/logger/core.hpp
 *
 * Simple logging methods using ostream output.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2015-2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_LOGGER_CORE_HEADER
#define TLX_LOGGER_CORE_HEADER

#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace tlx {

//! template class for formatting. contains a print() method.
template <typename AnyType, typename Enable = void>
class LoggerFormatter;

/*!

\brief LOG and sLOG for development and debugging

This is a short description of how to use \ref LOG and \ref sLOG for rapid
development of modules with debug output, and how to **keep it afterwards**.

There are two classes Logger and SpacingLogger, but one does not use these
directly.

Instead there are the macros: \ref LOG and \ref sLOG that can be used as such:
\code
LOG << "This will be printed with a newline";
sLOG << "Print variables a" << a << "b" << b << "c" << c;
\endcode

There macros only print the lines if the boolean variable **debug** is
true. This variable is searched for in the scope of the LOG, which means it can
be set or overridden in the function scope, the class scope, from **inherited
classes**, or even the global scope.

\code
class MyClass
{
    static constexpr bool debug = true;

    void func1()
    {
        LOG << "Hello World";

        LOG0 << "This is temporarily disabled.";
    }

    void func2()
    {
        static constexpr bool debug = false;
        LOG << "This is not printed any more.";

        LOG1 << "But this is forced.";
    }
};
\endcode

There are two variation of \ref LOG and \ref sLOG : append 0 or 1 for
temporarily disabled or enabled debug lines. These macros are then \ref LOG0,
\ref LOG1, \ref sLOG0, and \ref sLOG1. The suffix overrides the debug variable's
setting.

After a module works as intended, one can just set `debug = false`, and all
debug output will disappear and be optimized out.
 */
class Logger
{
private:
    //! collector stream
    std::ostringstream oss_;

public:
    //! construction: add prefix if desired
    Logger();

    //! output any type, including io manipulators
    template <typename AnyType>
    Logger& operator << (const AnyType& at) {
        LoggerFormatter<AnyType>::print(oss_, at);
        return *this;
    }

    //! destructor: output a newline
    ~Logger();
};

/*!
 * A logging class which outputs spaces between elements pushed via
 * operator<<. Depending on the real parameter the output may be suppressed.
 */
class SpacingLogger
{
private:
    //! true until the first element it outputted.
    bool first_ = true;

    //! collector stream
    std::ostringstream oss_;

public:
    //! construction: add prefix if desired
    SpacingLogger();

    //! output any type, including io manipulators
    template <typename AnyType>
    SpacingLogger& operator << (const AnyType& at) {
        if (!first_) oss_ << ' ';
        else first_ = false;
        LoggerFormatter<AnyType>::print(oss_, at);
        return *this;
    }

    //! destructor: output a newline
    ~SpacingLogger();
};

class LoggerVoidify
{
public:
    void operator & (Logger&) { }
    void operator & (SpacingLogger&) { }
};

//! Explicitly specify the condition for logging
#define TLX_LOGC(cond) \
    !(cond) ? (void)0 : ::tlx::LoggerVoidify()& ::tlx::Logger()

//! Default logging method: output if the local debug variable is true.
#define TLX_LOG TLX_LOGC(debug)

//! Override default output: never or always output log.
#define TLX_LOG0 TLX_LOGC(false)
#define TLX_LOG1 TLX_LOGC(true)

//! Explicitly specify the condition for logging
#define TLX_sLOGC(cond) \
    !(cond) ? (void)0 : ::tlx::LoggerVoidify() & ::tlx::SpacingLogger()

//! Default logging method: output if the local debug variable is true.
#define TLX_sLOG TLX_sLOGC(debug)

//! Override default output: never or always output log.
#define TLX_sLOG0 TLX_sLOGC(false)
#define TLX_sLOG1 TLX_sLOGC(true)

/******************************************************************************/
// Hook to add prefixes to log lines

//! Abstract class to implement prefix output hooks for logging
class LoggerPrefixHook
{
public:
    //! virtual destructor
    virtual ~LoggerPrefixHook();

    //! method to add prefix to log lines
    virtual void add_log_prefix(std::ostream& os) = 0;
};

//! Set new LoggerPrefixHook instance to prefix global log lines. Returns the
//! old hook.
LoggerPrefixHook * set_logger_prefix_hook(LoggerPrefixHook* hook);

/******************************************************************************/
// Hook to collect logger output

//! Abstract class to implement output hooks for logging
class LoggerOutputHook
{
public:
    //! virtual destructor
    virtual ~LoggerOutputHook();

    //! method the receive log lines
    virtual void append_log_line(const std::string& line) = 0;
};

//! set new LoggerOutputHook instance to receive global log lines. returns the
//! old hook.
LoggerOutputHook * set_logger_output_hook(LoggerOutputHook* hook);

//! install default logger to cerr / stderr instead of stdout. returns the old
//! hook.
LoggerOutputHook * set_logger_to_stderr();

/*----------------------------------------------------------------------------*/

//! Class to hook logger output in the local thread
class LoggerCollectOutput : public LoggerOutputHook
{
public:
    explicit LoggerCollectOutput(bool echo = false);
    ~LoggerCollectOutput();

    //! return transcript of log
    std::string get();

    //! clear transcript
    void clear();

    //! method the receive log lines
    void append_log_line(const std::string& line) final;

protected:
    //! previous logger, will be restored by destructor
    LoggerOutputHook* next_;

    //! whether to echo each line to next logger output
    bool echo_;

    //! string stream collecting
    std::ostringstream oss_;
};

/******************************************************************************/
// Formatters

template <typename AnyType>
class LoggerFormatter<AnyType>
{
public:
    static void print(std::ostream& os, const AnyType& t) {
        os << t;
    }
};

template <typename A, typename B>
class LoggerFormatter<std::pair<A, B> >
{
public:
    static void print(std::ostream& os, const std::pair<A, B>& p) {
        os << '(';
        LoggerFormatter<A>::print(os, p.first);
        os << ',';
        LoggerFormatter<B>::print(os, p.second);
        os << ')';
    }
};

template <typename T, class A>
class LoggerFormatter<std::vector<T, A> >
{
public:
    static void print(std::ostream& os, const std::vector<T, A>& data) {
        os << '[';
        for (typename std::vector<T>::const_iterator it = data.begin();
             it != data.end(); ++it)
        {
            if (it != data.begin()) os << ',';
            LoggerFormatter<T>::print(os, *it);
        }
        os << ']';
    }
};

} // namespace tlx

#endif // !TLX_LOGGER_CORE_HEADER

/******************************************************************************/
