#pragma once
#include "clickhouse_config.h"

#include <Common/Exception.h>
#include <Core/Types.h>

#if FIU_ENABLE

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdocumentation"
#pragma clang diagnostic ignored "-Wreserved-macro-identifier"
#endif

#error #include <fiu.h>
#error #include <fiu-control.h>

#ifdef __clang__
#pragma clang diagnostic pop
#endif

#else
#define fiu_do_on(name, action)
#endif

namespace DB
{

/// This is a simple named failpoint library inspired by https://github.com/pingcap/tiflash
/// The usage is simple:
/// 1. define failpoint with a 'failpoint_name' in FailPoint.cpp
/// 2. inject failpoint in normal code
///   2.1 use fiu_do_on which can inject any code blocks, when it is a regular-triggered / once-triggered failpoint
///   2.2 use pauseFailPoint when it is a pausable failpoint
/// 3. in test file, we can use system failpoint enable/disable 'failpoint_name'

class FailPointInjection
{
public:

    static void pauseFailPoint(const String & fail_point_name);

    static void enableFailPoint(const String & fail_point_name);

    static void enablePauseFailPoint(const String & fail_point_name, UInt64 time);

    static void disableFailPoint(const String & fail_point_name);

    static void wait(const String & fail_point_name);
};
}
