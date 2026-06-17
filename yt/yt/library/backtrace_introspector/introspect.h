#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/threading/public.h>

#include <yt/yt/core/tracing/public.h>

namespace NYT::NBacktraceIntrospector {

////////////////////////////////////////////////////////////////////////////////
// Thread introspection API

struct TThreadIntrospectionInfo
{
    NThreading::TThreadId ThreadId;
    NConcurrency::TFiberId FiberId;
    std::string ThreadName;
    NTracing::TTraceId TraceId;
    //! Empty if no trace context is known.
    std::string TraceLoggingTag;
    std::vector<const void*> Backtrace;
};

std::vector<TThreadIntrospectionInfo> IntrospectThreads();

////////////////////////////////////////////////////////////////////////////////
// Fiber introspection API

struct TFiberIntrospectionInfo
{
    NConcurrency::EFiberState State;
    NConcurrency::TFiberId FiberId;
    //! Zero if fiber is not waiting.
    TInstant WaitingSince;
    //! |InvalidThreadId| is fiber is not running.
    NThreading::TThreadId ThreadId;
    //! Empty if fiber is not running.
    std::string ThreadName;
    NTracing::TTraceId TraceId;
    //! Empty if no trace context is known.
    std::string TraceLoggingTag;
    std::vector<const void*> Backtrace;
};

std::vector<TFiberIntrospectionInfo> IntrospectFibers();

////////////////////////////////////////////////////////////////////////////////

std::string FormatIntrospectionInfos(const std::vector<TThreadIntrospectionInfo>& infos);
std::string FormatIntrospectionInfos(const std::vector<TFiberIntrospectionInfo>& infos);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktraceIntrospector
