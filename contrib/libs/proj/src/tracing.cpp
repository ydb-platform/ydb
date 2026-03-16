/******************************************************************************
 *
 * Project:  PROJ
 * Purpose:  Tracing/profiling
 * Author:   Even Rouault <even dot rouault at spatialys dot com>
 *
 ******************************************************************************
 * Copyright (c) 2019, Even Rouault <even dot rouault at spatialys dot com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 ****************************************************************************/

#ifdef ENABLE_TRACING

#ifndef FROM_PROJ_CPP
#define FROM_PROJ_CPP
#endif

#include <stdlib.h>

#include "proj/internal/internal.hpp"
#include "proj/internal/tracing.hpp"

//! @cond Doxygen_Suppress

#if defined(_WIN32) && !defined(__CYGWIN__)
#include <sys/timeb.h>
#else
#include <sys/time.h> /* for gettimeofday() */
#define CPLTimeVal timeval
#define CPLGettimeofday(t, u) gettimeofday(t, u)
#endif

NS_PROJ_START

using namespace internal;

namespace tracing {

#if defined(_WIN32) && !defined(__CYGWIN__)
struct CPLTimeVal {
    time_t tv_sec; /* seconds */
    long tv_usec;  /* and microseconds */
};

// ---------------------------------------------------------------------------

static void CPLGettimeofday(struct CPLTimeVal *tp, void * /* timezonep*/) {
    struct _timeb theTime;

    _ftime(&theTime);
    tp->tv_sec = static_cast<time_t>(theTime.time);
    tp->tv_usec = theTime.millitm * 1000;
}
#endif

struct Singleton {
    FILE *f = nullptr;
    int callLevel = 0;
    int minDelayMicroSec = 10 * 1000; // 10 millisec
    long long startTimeStamp = 0;
    std::string componentsWhiteList{};
    std::string componentsBlackList{};

    Singleton();
    ~Singleton();

    Singleton(const Singleton &) = delete;
    Singleton &operator=(const Singleton &) = delete;

    void logTraceRaw(const std::string &str);
};

// ---------------------------------------------------------------------------

Singleton::Singleton() {
    const char *traceFile = getenv("PROJ_TRACE_FILE");
    if (traceFile)
        f = fopen(traceFile, "wb");
    if (!f)
        f = stderr;

    const char *minDelay = getenv("PROJ_TRACE_MIN_DELAY");
    if (minDelay) {
        minDelayMicroSec = atoi(minDelay);
    }

    const char *whiteList = getenv("PROJ_TRACE_WHITE_LIST");
    if (whiteList) {
        componentsWhiteList = whiteList;
    }

    const char *blackList = getenv("PROJ_TRACE_BLACK_LIST");
    if (blackList) {
        componentsBlackList = blackList;
    }

    CPLTimeVal ts;
    CPLGettimeofday(&ts, nullptr);
    startTimeStamp = static_cast<long long>(ts.tv_sec) * 1000000 + ts.tv_usec;

    logTraceRaw("<log>");
    ++callLevel;
}

// ---------------------------------------------------------------------------

Singleton::~Singleton() {
    --callLevel;
    logTraceRaw("</log>");
    fflush(f);

    if (f != stderr)
        fclose(f);
}

// ---------------------------------------------------------------------------

static Singleton &getSingleton() {
    static Singleton singleton;
    return singleton;
}

// ---------------------------------------------------------------------------

void Singleton::logTraceRaw(const std::string &str) {
    CPLTimeVal ts;
    CPLGettimeofday(&ts, nullptr);
    const auto ts_usec =
        static_cast<long long>(ts.tv_sec) * 1000000 + ts.tv_usec;
    fprintf(f, "<!-- %03d.%06d --> ",
            static_cast<int>((ts_usec - startTimeStamp) / 1000000),
            static_cast<int>((ts_usec - startTimeStamp) % 1000000));
    for (int i = 0; i < callLevel; i++)
        fprintf(f, " ");
    fprintf(f, "%s\n", str.c_str());
    fflush(f);
}

// ---------------------------------------------------------------------------

void logTrace(const std::string &str, const std::string &component) {
    auto &singleton = getSingleton();
    if (!singleton.componentsWhiteList.empty() &&
        (component.empty() ||
         singleton.componentsWhiteList.find(component) == std::string::npos)) {
        return;
    }
    if (!singleton.componentsBlackList.empty() && !component.empty() &&
        singleton.componentsBlackList.find(component) != std::string::npos) {
        return;
    }
    std::string rawStr("<trace");
    if (!component.empty()) {
        rawStr += " component='" + component + '\'';
    }
    rawStr += '>';
    rawStr += str;
    rawStr += "</trace>";
    singleton.logTraceRaw(rawStr);
}

// ---------------------------------------------------------------------------

struct EnterBlock::Private {
    std::string msg_{};
    CPLTimeVal startTimeStamp_{};
};

// ---------------------------------------------------------------------------

EnterBlock::EnterBlock(const std::string &msg) : d(new Private()) {
    auto &singleton = getSingleton();
    d->msg_ = msg;
    CPLGettimeofday(&d->startTimeStamp_, nullptr);
    singleton.logTraceRaw("<block_level_" + toString(singleton.callLevel) +
                          ">");
    ++singleton.callLevel;
    singleton.logTraceRaw("<enter>" + d->msg_ + "</enter>");
}

// ---------------------------------------------------------------------------

EnterBlock::~EnterBlock() {
    auto &singleton = getSingleton();
    CPLTimeVal endTimeStamp;
    CPLGettimeofday(&endTimeStamp, nullptr);
    int delayMicroSec = static_cast<int>(
        (endTimeStamp.tv_usec - d->startTimeStamp_.tv_usec) +
        1000000 * (endTimeStamp.tv_sec - d->startTimeStamp_.tv_sec));
    std::string lengthStr;
    if (delayMicroSec >= singleton.minDelayMicroSec) {
        lengthStr = " length='" + toString(delayMicroSec / 1000) + "." +
                    toString((delayMicroSec % 1000) / 100) + " msec'";
    }
    singleton.logTraceRaw("<leave" + lengthStr + ">" + d->msg_ + "</leave>");
    --singleton.callLevel;
    singleton.logTraceRaw("</block_level_" + toString(singleton.callLevel) +
                          ">");
}

} // namespace tracing

NS_PROJ_END

//! @endcond

#endif // ENABLE_TRACING
